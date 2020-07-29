import asyncio
import logging
from typing import Any, List, Dict

import faust
from gs_framework.utilities import generate_uuid

from gs_framework.activatable_stateful_service import Activatable

from gs_framework.colab.colab_related_states import ColabPoolEnvStateQueryMessage, ColabPoolEnvState, \
    ColabPoolEnvStateQueryResponseMessage

from gs_framework.stateful_object import create_stateful_object, read_stateful_object, MessageAsStateReader

from gs_framework.state_var_change_dispatcher import state_var_change_handler, pick_one_change

from gs_framework.state_stream import ObjectStateStream

from gs_framework.service import ServiceUnit
from gs_framework.stream_rpc import RPCCaller, RPCStubData, RPCEndPoint, RPCStub

logger = logging.getLogger(__name__)


class ColabPoolClient(RPCCaller, ServiceUnit):

    pool_env_status_stream: ObjectStateStream = ObjectStateStream.bind_at_runtime()

    def __init__(self, pool_name: str, pool_env_rpc_callee_topic: faust.types.TP):
        super().__init__()
        self._pool_name = pool_name
        self._pool_env_rpc_callee_topic = pool_env_rpc_callee_topic
        self._pool_env_rpc_stub: RPCStub = None
        self._future_4_ready = asyncio.Future()
        self._pool_state_query_uuid: str = None

    def __repr__(self):
        return f"{self.__class__.__qualname__}({self._pool_name}, {self._pool_env_rpc_callee_topic})"

    # in case the pool env is restarted and resent its pk
    @state_var_change_handler(state_vars=Activatable.active, state_var_source=pool_env_status_stream)
    def on_pool_env_active(self, pool_env_pk: str, state_vars: Dict[str, Any], triggering_state_var_names: List[str]):
        pool_state: ColabPoolEnvState = read_stateful_object(pool_env_pk, ColabPoolEnvState,
                                                             MessageAsStateReader(pool_env_pk, state_vars))
        if pool_state.active.VALUE:
            print(f"pool env active: name: {pool_state.name.VALUE}, pk: {pool_env_pk}")
            if pool_state.name.VALUE == self._pool_name:
                self._update_pool_env_pk(pool_env_pk, set_future_4_ready=False)

    @state_var_change_handler(state_vars=ColabPoolEnvStateQueryResponseMessage, state_var_source=pool_env_status_stream)
    def on_pool_env_state_query_response(self, pool_env_pk: str, state_vars: Dict[str, Any],
                                         triggering_state_var_names: List[str]):
        pool_state_query_response: ColabPoolEnvStateQueryResponseMessage \
            = read_stateful_object(pool_env_pk, ColabPoolEnvStateQueryResponseMessage,
                                   MessageAsStateReader(pool_env_pk, state_vars))
        print(f"pool state query response received: name: {pool_state_query_response.name.VALUE}, pk: {pool_env_pk}")
        if pool_state_query_response.query_message_id.VALUE == self._pool_state_query_uuid and\
                pool_state_query_response.name.VALUE == self._pool_name:
            self._update_pool_env_pk(pool_env_pk, set_future_4_ready=True)

    def _update_pool_env_pk(self, pool_env_pk: str, set_future_4_ready: bool):
        if self._pool_env_rpc_stub is None or self._pool_env_rpc_stub.stub_data.endpoint.pk != pool_env_pk:
            self._pool_env_rpc_stub = self._create_rpc_stub(RPCStubData(self._pool_env_rpc_callee_topic,
                                                                        RPCEndPoint(pool_env_pk)))
            print(f"colab pool {self._pool_name} found with pk {pool_env_pk}")

        if set_future_4_ready and not self._future_4_ready.done():
            print(f"colab pool {self._pool_name} with pk {pool_env_pk} is ready for use")
            self._future_4_ready.set_result(True)

    async def get_ready(self):
        if not self._future_4_ready.done():
            # send out message to query pool status by name
            pool_state_query_message = create_stateful_object("dummy_pk", ColabPoolEnvStateQueryMessage)
            pool_state_query_message[ColabPoolEnvStateQueryMessage.name].VALUE = self._pool_name
            pool_state_query_message[ColabPoolEnvStateQueryMessage.message_id].VALUE = \
                self._pool_state_query_uuid = generate_uuid()
            await pool_state_query_message.commit_state_var_changes(self.pool_env_status_stream)
            print(f"ColabPoolEnvStateQueryMessage sent for name {self._pool_name}")

            await self._future_4_ready

    async def submit(self, notebook_id: str, task_group: str, chrome_debug_port: int = 9222,
                     chrome_service_name: str = None) -> str:
        return await self._pool_env_rpc_stub.submit(notebook_id, task_group, chrome_debug_port, chrome_service_name)

    async def cancel_task(self, task_id: str) -> bool:
        return await self._pool_env_rpc_stub.cancel_task(task_id)

    async def cancel_task_group(self, task_group: str) -> bool:
        return await self._pool_env_rpc_stub.cancel_task_group(task_group)
