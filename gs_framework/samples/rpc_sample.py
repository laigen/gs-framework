# -*- coding: UTF-8 -*-

"""利用kafka模拟  RPC 的调用 """
import asyncio
import logging
from datetime import datetime

from gs_framework.instance_hash_calculation import HashCalculation

from gs_framework.stateful_interfaces import PkMixin

from gs_framework.activatable_stateful_service import Env
from gs_framework.stream_rpc import RPCCaller, RPCStub, RPCStubData
from gs_framework.timer_handler import timer
from gs_framework.utilities import get_random_int

logger = logging.getLogger(__name__)


class DoAdd(PkMixin):
    def __init__(self, name: str):
        super().__init__()
        self.name: str = name
        self._pk = HashCalculation.calc_inst_hash(self.__class__, name)

    async def add(self, a, b) -> str:
        sleep_time = float(get_random_int(100, 1000) / 100.)
        await asyncio.sleep(sleep_time)
        return f"[in '{self.name.upper()}' sleep {sleep_time} secs]add: {a} + {b} = {a + b}"


class RPCCallDoAddEnv(RPCCaller, Env):
    def __init__(self):
        super().__init__()
        self.do_add_stub: RPCStub = None

    def create_do_add_stub(self, add_stub_data: RPCStubData):
        self.do_add_stub = self._create_rpc_stub(add_stub_data)

    async def add(self, a, b) -> str:
        sleep_time = float(get_random_int(3, 150) / 100.)
        await asyncio.sleep(sleep_time)

        v = await self.do_add_stub.add(a, b)
        return f"[in v1 Sleep {sleep_time} secs] {v}"


class SimLocalEnv(RPCCaller, Env):

    def __init__(self, rpc_add_stub_data: RPCStubData):
        super().__init__()
        self.rpc_add_stub_data = rpc_add_stub_data

    @timer(interval=5.0)
    async def on_timer(self):
        start_t = datetime.now()
        rpc_stub: RPCStub = self._create_rpc_stub(self.rpc_add_stub_data)
        a = get_random_int(5, 1000)
        b = get_random_int(5, 1000)
        v = await rpc_stub.add(a, b)
        logger.info(f"RPC call result: '{a} + {b} = {v}' , used {(datetime.now() - start_t).total_seconds()} secs")
