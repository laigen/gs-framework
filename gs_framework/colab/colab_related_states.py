
from gs_framework.stateful_object import State

from gs_framework.activatable_stateful_service import Activatable

from gs_framework.state_variable import StateVariable


class ColabPoolEnvState(Activatable):

    name = StateVariable(dtype=str, default_val=None, help="name of the pool")


class ColabPoolEnvStateQueryMessage(State):

    name = StateVariable(dtype=str, default_val=None, help="name of the the name of the pool whose state is queried")
    message_id = StateVariable(dtype=str, default_val=None, help="id used to identify response messages")


class ColabPoolEnvStateQueryResponseMessage(State):

    name = StateVariable(dtype=str, default_val=None, help="name of the pool")
    query_message_id = StateVariable(dtype=str, default_val=None, help="id of the query message")
