# -*- coding: UTF-8 -*-
"""
Env / Task 的基类对象
NOTE: _ActionContainer class 没有定义在 base_classes 是为了避免出现 cross import 的情况
"""
import asyncio
import logging

# from .common_feature_in_srv import _CommonFeaturesInSrv
from typing import Any

from .state_var_change_dispatcher import state_var_change_handler, pick_one_change

from .stateful_object import State
from .service import StatefulService
from .state_variable import StateVariable
from .utilities import install_terminate_handler

logger = logging.getLogger(__name__)


class Activatable(State):

    active = StateVariable(dtype=int, default_val=0, memory_only=True, compare_value_4_change=True,
                           help="env activation state")


class Env(StatefulService, Activatable):

    async def start(self):
        await super().start()
        self.active.VALUE = 1
        await self.commit_state_var_changes()

        def on_terminate_signal(signum, stack_frame):
            async def async_task():
                self.active.VALUE = 0
                await self.commit_state_var_changes()
                loop = asyncio.get_event_loop()
                loop.stop()

            asyncio.ensure_future(async_task())

        install_terminate_handler(on_terminate_signal)


class Episode(StatefulService, Activatable):
    pass


class Agent(StatefulService, Activatable):
    pass


class TerminationEnv(Env):

    terminated = StateVariable(dtype=int, default_val=0, help="env termination state")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._terminate_future = asyncio.get_event_loop().create_future()

    def wait_until_terminated(self, safety_seconds_4_async_operations_to_finish: int = 60):
        async def terminated():
            await self._terminate_future
            if safety_seconds_4_async_operations_to_finish > 0:
                await asyncio.sleep(safety_seconds_4_async_operations_to_finish)
        asyncio.get_event_loop().run_until_complete(asyncio.ensure_future(terminated))

    @state_var_change_handler(state_vars=terminated)
    @pick_one_change
    def on_terminate_flag_set(self, state_var_owner_pk: Any, state_var_name: str, state_var_value: Any):
        if state_var_value:
            self._terminate_future.set_result(state_var_value)
