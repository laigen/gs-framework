# -*- coding: UTF-8 -*-
from typing import Any

from gs_framework.stateful_object import State, create_stateful_object

from gs_framework.handler import StatefulObjectAndCommitStream

from gs_framework.timer_handler import timer

from gs_framework.state_var_change_dispatcher import state_var_change_handler, pick_one_change

from gs_framework.state_variable import StateVariable

from gs_framework.activatable_stateful_service import Env


class TestState(State):

    val = StateVariable(dtype=str, default_val=None, help="val of test state")


class TestEnv(Env):

    _test_var = StateVariable(dtype=int, help="test var")

    def __init__(self, _):
        super().__init__()

    @state_var_change_handler(state_vars=[_test_var, TestState.val])
    @pick_one_change
    def on_test_var_change(self, state_var_owner_pk: Any, state_var_name: str, test_var: int):
        print(f"{state_var_owner_pk} {state_var_name} {test_var}")

    @timer(interval=5.0)
    async def on_timer(self):
        test_val = 'abcde'
        test_val_object = create_stateful_object(self.pk, TestState)
        test_val_object[TestState.val].VALUE = test_val
        return StatefulObjectAndCommitStream(test_val_object, self.state_vars_storage.stream)

    @staticmethod
    async def run_test():
        import faust.types

        test_env = TestEnv("abcd")
        test_env_status_topic = faust.types.TP(topic="test_env", partition=1)
        test_env.bind(topic_define=test_env_status_topic)

        await test_env.start()

        print("started")
        test_env._test_var.VALUE = 1
        print("assigned")
        await test_env.commit_state_var_changes()
        print("published")


if __name__ == "__main__":
    import asyncio

    loop = asyncio.get_event_loop()
    loop.run_until_complete(TestEnv.run_test())
    loop.run_forever()
