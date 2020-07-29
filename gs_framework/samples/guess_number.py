# -*- coding: UTF-8 -*-
"""
猜数字的游戏

    Env :  1 - 1000 取一个随机数， agent 猜一个数字，env 的 state 告知猜的数字是大了还是小了
    Agent : 根据数字是大了还是小了，猜下一个数字
"""
import logging

from typing import NamedTuple, Any, List, Dict

from gs_framework.state_var_change_dispatcher import state_var_change_handler, pick_one_change

from gs_framework.object_reference import ObjectRef
from gs_framework.activatable_stateful_service import Env, Agent, Activatable
from gs_framework.state_variable import StateVariable
from gs_framework.utilities import get_random_int
logger = logging.getLogger(__name__)


class NumberRange(NamedTuple):

    lower_bound: int
    upper_bound: int

    def __repr__(self):
        return f"[{self.lower_bound},{self.upper_bound}]"


class GuessNumberGameEnv(Env):

    agent = ObjectRef.bind_at_runtime()

    init_action_space = StateVariable(dtype=NumberRange, default_val=NumberRange(1, 100),
                                      help="初始状态，竞猜数字的最大范围")
    init_state_space = StateVariable(dtype=NumberRange, default_val=NumberRange(-1, 1),
                                     help="状态空间，-1 ~ 1 的值")
    state = StateVariable(dtype=int, default_val=0, help="-1:数字小了,0:随机猜一个，1:数字大了")
    reward = StateVariable(dtype=float, default_val=0., help="reward")

    def __init__(self):
        super().__init__()
        self._real_num: int = 0
        """真实的数字，可以不是 state variable，暂假定一个 episode 中 env inst 不会 deactivate"""

    def _reset(self) -> int:
        """模拟 gym.Env 的 reset """
        state_space = self.init_action_space.VALUE
        self._real_num = get_random_int(state_space.lower_bound, state_space.upper_bound)
        return 0

    @state_var_change_handler(state_vars=[Activatable.active])
    def on_active_flag_change(self, state_var_owner_pk: Any, state_vars: Dict[str, Any],
                              triggering_state_var_names: List[str]):
        v = state_vars['active']
        if v == 1:  # 标记 active 当做是 start new episode，后续可以单独做一个 episode 的 variable
            self.init_action_space.VALUE = NumberRange(1, 100)
            self.init_state_space.VALUE = NumberRange(-1, 1)
            self.state.VALUE = self._reset()
            logger.info(f"[Env] Game start... , real number is {self._real_num}")

    @state_var_change_handler(state_vars=["action"], state_var_source=agent)
    def on_agent_action(self, state_var_owner_pk: Any, state_vars: Dict[str, Any],
                        triggering_state_var_names: List[str]):
        action = state_vars['action']
        if action == self._real_num:
            self.reward.VALUE = 1000.
            logger.info(f"[Env] Bingo!")
        elif action > self._real_num:
            self.state.VALUE = 1
            self.reward.VALUE = -0.01  # 假设每猜错一次，有负的reward
            logger.info(f"[Env] Greater than real num . Guess num. : {action} , Real num. {self._real_num}")
        elif action < self._real_num:
            self.state.VALUE = -1
            self.reward.VALUE = -0.01
            logger.info(f"[Env] Less than real num . Guess num. : {action} , Real num. {self._real_num}")


class GuessNumberAgent(Agent):
    """基于 rule base 做的一个猜数字的agent"""

    env = ObjectRef.bind_at_runtime()
    action = StateVariable(dtype=int, default_val=0, help="action")

    def __init__(self):
        super().__init__()
        self._upper_bound = 1000  # 这个可以认为是 agent 的内部变量，可以先不用成为 state variable , 更像是 model parameter
        self._lower_bound = 0

    @state_var_change_handler(state_vars="init_action_space", state_var_source=env)
    @pick_one_change
    def on_init_action_space(self, state_var_owner_pk: Any, state_var_name: str, state_var_value: Any):
        self._upper_bound = state_var_value.upper_bound
        self._lower_bound = state_var_value.lower_bound

    @state_var_change_handler(state_vars="state", state_var_source=env)
    @pick_one_change
    def on_env_state_changed(self, state_var_owner_pk: Any, state_var_name: str, state_var_value: Any):
        if state_var_value > 0:  # 猜的数字大了，调整上边界
            self._upper_bound = self.action.VALUE
            self.action.VALUE = get_random_int(self._lower_bound, self._upper_bound)
        elif state_var_value < 0:  # 猜的数字小了，调整下边界
            self._lower_bound = self.action.VALUE
            self.action.VALUE = get_random_int(self._lower_bound, self._upper_bound)
        else:
            self.action.VALUE = get_random_int(self._lower_bound, self._upper_bound)
        logger.info(f"[Agent] guess in range [{self._lower_bound},{self._upper_bound}]")

    @state_var_change_handler(state_vars="reward", state_var_source=env)
    def on_env_reward(self, state_var_owner_pk: Any, state_vars: Dict[str, Any],
                      triggering_state_var_names: List[str]):
        # do nothing , 做成 RL Model 时有用
        pass

