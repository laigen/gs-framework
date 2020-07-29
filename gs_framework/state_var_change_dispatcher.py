import inspect
import itertools
import re

from typing import List, Optional, Union, Dict, Iterable, Callable, Any, Awaitable, Pattern

from dataclasses import dataclass
from gs_framework.handler import CHANGE_HANDLER_RESULT, process_handler_sync_result, FUNC_STATE_VAR_CHANGE_HANDLER

from .stateful_object import State
from .service import StatefulService
from .state_stream import ObjectStateStream
from .state_storage import StateStreamStorage
from .utilities import get_item
from .state_variable import StateVariable
from .object_reference import ObjectRef

StateVariableOrStateOrName = Union[str, StateVariable, State]
"""
Refer to a state variable either by name or the class member whose type is StateVariable. 
When refer to variable of other stateful inst, it might be easier to refer by name.
"""

StateVarSource = Union[str, ObjectRef, StateStreamStorage, ObjectStateStream]
"""Refer to a variable owner either by name or the class level member defined in class"""


@dataclass(frozen=True)
class StateVarSubscriptionDetail:
    state_var_or_names: Iterable[Union[str, StateVariable]]
    state_var_source: StateVarSource


@dataclass(frozen=True)
class StateVarReference:
    """Serve as the key for mapping property to handlers"""
    state_var_name: str
    state_var_source_name: str

    def __hash__(self):
        return hash(self.__repr__())

    def __repr__(self):
        # return self.state_var_name if self.name_of_state_var_from is None \
        #     else f"{self.name_of_state_var_from}.{self.state_var_name}"
        return f"{self.state_var_source_name}.{self.state_var_name}"


class StateVarChangeDispatcher:

    FUNC_PICK_ONE_CHANGE_HANDLER = Callable[[Any, Any, str, Any], CHANGE_HANDLER_RESULT]
    """
    The parameters are (from left to right):
    self
    state_var_owner_pk: pk of object the state vars belong to
    state_var_name: name of the first found state var triggering the handler
    state_var_value: the value of the first found state var triggering the handler
    """

    FUNC_ON_HANDLERS_CALLED = Callable[[], Union[Awaitable[None], None]]

    ATTR_STATE_VAR_SUBSCRIPTION_DETAIL = "_state_var_subscription_detail"

    __slots__ = ("_func_on_handlers_called", "_state_var_2_handlers")

    def __init__(self, handlers_owner: object, func_on_handlers_called: FUNC_ON_HANDLERS_CALLED):
        super().__init__()
        self._func_on_handlers_called = func_on_handlers_called
        self._state_var_2_handlers: Dict[StateVarReference, List[FUNC_STATE_VAR_CHANGE_HANDLER]] = dict()
        self._collect_variable_change_handlers(handlers_owner)

    def _collect_variable_change_handlers(self, handlers_owner: object):

        name_and_method_and_subscription_details = \
            filter(lambda name_and_method_and_subscription_detail:
                   name_and_method_and_subscription_detail[2] is not None,
                   map(lambda name_and_method: (name_and_method[0], name_and_method[1],
                                                getattr(name_and_method[1],
                                                        StateVarChangeDispatcher.ATTR_STATE_VAR_SUBSCRIPTION_DETAIL,
                                                        None)),
                       inspect.getmembers(handlers_owner, inspect.ismethod)))

        # cls = handlers_owner.__class__

        for name, handler, subscription_detail in name_and_method_and_subscription_details:
            subscribed_state_var_or_names = subscription_detail.state_var_or_names
            for i in range(0, len(subscribed_state_var_or_names)):
                state_var_or_name = subscribed_state_var_or_names[i]
                if isinstance(state_var_or_name, StateVariable):
                    subscribed_state_var_or_names[i] = state_var_or_name.name
            subscribed_state_var_names: Iterable[str] = subscribed_state_var_or_names
            state_var_source: StateVarSource = subscription_detail.state_var_source
            state_var_source_name = state_var_source if isinstance(state_var_source, str) else state_var_source.name

            for state_var_reference in map(
                    lambda state_var_name: StateVarReference(state_var_name, state_var_source_name),
                    subscribed_state_var_names):
                state_var_handlers = self._state_var_2_handlers.setdefault(state_var_reference, list())
                state_var_handlers.append(handler)

    _regex_matching_top_level_state_var_names: Pattern = re.compile("(?i)^[a-z_0-9]+\\.([a-z_0-9]+)$")
    """
    the first capture group of this returns the member name of the state variable
    """

    async def on_state_var_changes(self, state_var_owner_pk: Any, state_vars: Dict[str, Any],
                                   state_var_source_name: Optional[str]):
        # if the incoming state var name is not a name of state variable defined in nested class, for example,
        # like "class.member", not "class1.class2.member",
        # the handler might refer to it with string "member" instead of "class.member", so add additional
        # entry with key "member" to state_vars
        top_level_name_matches = list(filter(
            lambda m: m is not None,
            map(lambda name: StateVarChangeDispatcher._regex_matching_top_level_state_var_names.fullmatch(name),
                state_vars)))
        # group(0) is full name, group(1) is member name
        state_vars.update(map(lambda m: (m.group(1), state_vars[m.group(0)]), top_level_name_matches))

        # use set to remove duplicates among handlers
        handlers = set(itertools.chain(
            *filter(lambda handlers_4_state_var: handlers_4_state_var is not None,
                    map(lambda state_var_name: self._state_var_2_handlers.get(
                        StateVarReference(state_var_name, state_var_source_name), None
                    ), state_vars))))

        for handler in handlers:
            subscription_detail: StateVarSubscriptionDetail = \
                getattr(handler, StateVarChangeDispatcher.ATTR_STATE_VAR_SUBSCRIPTION_DETAIL)
            res = handler(state_var_owner_pk, state_vars, subscription_detail.state_var_or_names)
            await process_handler_sync_result(res)

        if self._func_on_handlers_called is not None:
            res = self._func_on_handlers_called()
            if inspect.isawaitable(res):
                await res


def _state_vars_2_iterable(state_vars: Union[StateVariableOrStateOrName, Iterable[StateVariableOrStateOrName]]):
    # NOTE：不能使用 isiterable() 的方法判断， 因为 str 对象也是 iterable 的
    if isinstance(state_vars, (str, StateVariable)):
        return state_vars,
    elif isinstance(state_vars, type) and issubclass(state_vars, State):
        return state_vars.get_all_state_vars()
    else:
        assert isinstance(state_vars, Iterable)
        return itertools.chain(*map(_state_vars_2_iterable, state_vars))


def state_var_change_handler(
        state_vars: Union[StateVariableOrStateOrName, Iterable[StateVariableOrStateOrName]],
        state_var_source: Optional[StateVarSource] = StatefulService.state_vars_storage):
    """
    decorator to define state var change handler

    Parameters
    ----------
    state_vars:  the watched properties
    state_var_source: the class member where the changed object comes from, or the name of the class member. None for self
    -------
    """

    def decorator(func):
        if state_vars is not None:
            state_var_or_names = list(_state_vars_2_iterable(state_vars))
            setattr(func, StateVarChangeDispatcher.ATTR_STATE_VAR_SUBSCRIPTION_DETAIL,
                    StateVarSubscriptionDetail(state_var_or_names=state_var_or_names,
                                               state_var_source=state_var_source))
        return func

    return decorator


def pick_one_change(handler: StateVarChangeDispatcher.FUNC_PICK_ONE_CHANGE_HANDLER):
    def change_handler_4_multiple_vars(self, state_var_owner_pk: Any, state_vars: Dict[str, Any],
                                       triggering_state_var_names: List[str]):
        # doesn't give a default value: this next call should not raise StopIteration
        picked_change = next(filter(lambda name_and_val: name_and_val[1][0],
                                    map(lambda var_name: (var_name, get_item(state_vars, var_name)),
                                        triggering_state_var_names)))
        picked_var_name, (_, picked_var_value) = picked_change
        return handler(self, state_var_owner_pk, picked_var_name, picked_var_value)

    return change_handler_4_multiple_vars
