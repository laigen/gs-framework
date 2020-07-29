# -*- coding: UTF-8 -*-
from datetime import datetime

from gs_framework.activatable_stateful_service import Env, StateVariable, Episode
from gs_framework.decorators import actionable, init_actions
import logging
logger = logging.getLogger(__name__)


class TestEnv(Env):
    @init_actions()
    def __init__(self, env):
        super().__init__()
        self.dummy_env = env
        self.reset_time = StateVariable(value_cls=datetime, default_val=None, help="重置时间")
        self.player_step_count = StateVariable(value_cls=int, default_val=0, help="player")
        self.current_player = StateVariable(value_cls=str, default_val="A", help="当前的玩家")

    @actionable(variables=["self.env.player_a", "self.current_player"])
    async def a_on_player_step(self, s):
        logger.debug(f"{s}")
        self.player_step_count += 1  # __add__ 的方式修改 state variable
        # self.current_player("B")
        self.current_player.VALUE = "B"  # 修改 current_player 为 B
        # self.current_player = "B"
        # self.current_player <= "B"


class TestTask(Episode):
    @init_actions()
    def __init__(self, env):
        super().__init__()
        self._env = env


class MultiInputTask(Episode):
    @init_actions()
    def __init__(self, env, agent=None):
        super().__init__()
        self._env = env
        self._agent = agent
