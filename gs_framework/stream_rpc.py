# -*- coding: UTF-8 -*-
"""
用 kafka stream 的方式模拟 RPC 的操作
"""
import logging
import asyncio
import inspect
import faust
import traceback
import sys

from typing import NamedTuple, Optional, Any, Mapping, Sequence, Dict, Union, Tuple
from gs_framework.stateful_interfaces import PkMixin

from .service import StatelessService, ServiceUnit

from .utilities import generate_uuid
from .state_var_change_dispatcher import state_var_change_handler, pick_one_change
from .state_stream import ObjectStateStream
from .state_variable import StateVariable
from .stateful_object import State, create_stateful_object

logger = logging.getLogger(__name__)


# region classes related to RPC communication protocol

class RPCEndPoint:
    __slots__ = ("pk", "cls_full_name")

    def __init__(self, service_provider: Union[PkMixin, type, Any]):
        if isinstance(service_provider, type):
            self.cls_full_name = service_provider.__qualname__
            self.pk = None
        else:
            try:
                self.pk = service_provider.pk
            except AttributeError:
                # consider service_provider the pk itself
                self.pk = service_provider
            self.cls_full_name = None

    def __eq__(self, other):
        return isinstance(other, RPCEndPoint) and self.pk == other.pk and self.cls_full_name == other.cls_full_name

    # define __iter__ so HashCalculation.value_to_hash_str can work with RPCEndPoint
    def __iter__(self):
        return (self.pk, self.cls_full_name).__iter__()


class RPCReq(NamedTuple):
    call_uuid: str
    """call_uuid 由 caller 负责指派"""

    endpoint: RPCEndPoint

    resp_topic: faust.types.TP

    method_name: str
    args: Sequence[Any] = []
    kwargs: Mapping[str, Any] = {}


class RPCResp(NamedTuple):
    call_uuid: str
    """每次 call 都会约定一个 uuid，以此来区分不同的 call 对应关系"""
    ret_val: Optional[Any] = None
    ret_error: Optional[str] = None
    """exception"""


# the pk of this message is endpoint of the callee
class RPCReqMessage(State):
    req = StateVariable(dtype=RPCReq, default_val=None, help="rpc request message")


# the pk of this message is endpoint of the callee
class RPCRespMessage(State):
    resp = StateVariable(dtype=RPCResp, default_val=None, help="rpc response message")


# endregion

class RPCStubData(NamedTuple):
    topic: faust.types.TP
    endpoint: RPCEndPoint


# region class for callee side

class RPCEndPointServiceUnit(ServiceUnit):

    rpc_callee_stream = ObjectStateStream.bind_at_runtime()
    """because after faust app started, new created agent cannot receive messages; new created topic can send message,
        so for both caller and callee sides have to fix the agent receiving messages; and create topic on the fly for
        sending messages
    """

    # the service provider is either an instance having pk or a class
    def __init__(self, service_provider: Union[PkMixin, type]):
        super().__init__()
        self._rpc_endpoint = RPCEndPoint(service_provider)
        self._rpc_service_provider = service_provider

    @property
    def rpc_stub_data(self) -> RPCStubData:
        return RPCStubData(self.rpc_callee_stream.topic_define, self._rpc_endpoint)

    @state_var_change_handler(state_vars=RPCReqMessage.req, state_var_source=rpc_callee_stream)
    @pick_one_change
    async def _on_rpc_call(self, state_var_owner_pk: Any, state_var_name: str, rpc_req: RPCReq):
        # ignore rpc call not sent to myself
        if rpc_req.endpoint == self._rpc_endpoint:
            try:
                res = getattr(self._rpc_service_provider, rpc_req.method_name)(*rpc_req.args, **rpc_req.kwargs)
                if inspect.isawaitable(res):
                    res = await res
                rpc_resp = RPCResp(call_uuid=rpc_req.call_uuid, ret_val=res)
            except Exception:
                exception_type, exception, call_stack = sys.exc_info()
                rpc_resp = RPCResp(call_uuid=rpc_req.call_uuid,
                                   ret_error='.'.join(traceback.format_exception(exception_type, exception, call_stack)))

            rpc_resp_message = create_stateful_object(rpc_req.endpoint, RPCRespMessage)
            rpc_resp_message[RPCRespMessage.resp].VALUE = rpc_resp

            resp_stream = ObjectStateStream(rpc_req.resp_topic)
            resp_stream.initialize(self._app)
            rpc_resp_message.commit_state_var_changes(resp_stream)


class RPCEndPointService(StatelessService):

    def __init__(self, service_provider: Union[PkMixin, type]):
        super().__init__()
        self._rpc_end_point_service_unit = RPCEndPointServiceUnit(service_provider)
        self.add_service_units(self._rpc_end_point_service_unit)

    # If define a property rpc_callee_stream here, RPCEndPointService.rpc_callee_stream will conflicts
    # with RPCEndPointServiceUnit.rpc_callee_stream when member name is used for agent name.
    # Thus a function is defined to expose rpc_callee_stream.bind, instead of expose
    # RPCEndPointServiceUnit.rpc_callee_stream directly as RPCEndPointService.rpc_callee_stream
    def bind_rpc_callee_stream(self, topic_define: faust.types.TP):
        self._rpc_end_point_service_unit.rpc_callee_stream.bind(topic_define)

    @property
    def rpc_stub_data(self) -> RPCStubData:
        return self._rpc_end_point_service_unit.rpc_stub_data


# endregion


# region classes for caller side

class RPCCaller:
    """The base class for classes will call RPC"""

    rpc_caller_stream = ObjectStateStream.bind_at_runtime()
    """because after faust app started, new created agent cannot receive messages; new created topic can send message,
        so for both caller and callee sides have to fix the agent receiving messages; and create topic on the fly for
        sending messages
    """

    def __init__(self):
        super().__init__()
        self._call_result_futures_by_call_uuid: Dict[str, asyncio.Future] = dict()

    def generate_result_future(self) -> Tuple[str, asyncio.Future]:
        call_uuid = generate_uuid()
        call_result_future = asyncio.get_event_loop().create_future()
        self._call_result_futures_by_call_uuid[call_uuid] = call_result_future
        return call_uuid, call_result_future

    def drop_result_future(self, call_uuid: str):
        # use pop instead of del in case the call_uuid has also been dropped by rpc response
        self._call_result_futures_by_call_uuid.pop(call_uuid, None)

    def _create_rpc_stub(self, rpc_stub_data: RPCStubData):
        return RPCStub(self, rpc_stub_data)

    @state_var_change_handler(state_vars=RPCRespMessage.resp, state_var_source=rpc_caller_stream)
    @pick_one_change
    def _on_rpc_resp(self, state_var_owner_pk: Any, state_var_name: str, rpc_resp: RPCResp):
        try:
            call_result_future = self._call_result_futures_by_call_uuid.pop(rpc_resp.call_uuid)
            call_result_future.set_result(rpc_resp)
        except KeyError:
            pass  # the call result might belong to someone else or an expired call


class RPCStub:

    __slots__ = ("rpc_caller", "stub_data")

    def __init__(self, rpc_caller: RPCCaller, stub_data: RPCStubData):
        super().__init__()
        self.rpc_caller = rpc_caller
        self.stub_data = stub_data

    async def call_rpc(self, remote_func, *args, **kwargs):
        return getattr(self, remote_func.__name__)(*args, **kwargs)

    def __getattr__(self, name: str):
        return RPCMethodStub(self, name)


class RPCMethodStub:
    __slots__ = ("_rpc_stub", "_method_name")

    def __init__(self, rpc_stub: RPCStub, method_name: str):
        self._rpc_stub = rpc_stub
        self._method_name = method_name

    async def __call__(self, *args, rpc_timeout_seconds=24 * 3600, **kwargs):
        rpc_stub = self._rpc_stub
        rpc_caller = rpc_stub.rpc_caller

        call_uuid, call_result_future = rpc_caller.generate_result_future()

        # send msg to rpc call
        callee_endpoint = rpc_stub.stub_data.endpoint
        rpc_req_message = create_stateful_object(callee_endpoint, RPCReqMessage)
        rpc_req_message[RPCReqMessage.req].VALUE = RPCReq(call_uuid=call_uuid, endpoint=callee_endpoint,
                                                          resp_topic=rpc_caller.rpc_caller_stream.topic_define,
                                                          method_name=self._method_name, args=args, kwargs=kwargs)

        req_stream = ObjectStateStream(rpc_stub.stub_data.topic)
        req_stream.initialize(rpc_caller.app)  # assert isinstance(rpc_caller, Service)
        await rpc_req_message.commit_state_var_changes(req_stream)

        # await call result future be set result by _on_rpc_resp
        try:
            await asyncio.wait_for(call_result_future, rpc_timeout_seconds)
        except asyncio.TimeoutError as err:
            call_result_future.set_result(RPCResp(call_uuid=call_uuid, ret_error=str(err)))
            rpc_caller.drop_result_future(call_uuid)

        rpc_ret: RPCResp = call_result_future.result()
        if rpc_ret.ret_error:
            raise RuntimeError(rpc_ret.ret_error)
        else:
            return rpc_ret.ret_val

# endregion
