# -*- coding: UTF-8 -*-
"""
State Variable 对象
"""
import asyncio
import logging
from typing import TypeVar, Generic, Callable, Any, Dict

from .state_stream import ObjectStateStream
from .stateful_interfaces import CloneableT, SINGLE_OBJECT_STATE_READER

logger = logging.getLogger(__name__)

T = TypeVar('T')


class StateVariable(CloneableT, Generic[T]):
    """
    State variables are object properties that auto publish changes (to kafka topic) when modified
    """

    FUNC_ON_VARIABLE_CHANGED = Callable[[str, Any], None]
    """
    The parameter is: variable name, variable value 
    """

    __slots__ = ("dtype", "_default_val", "_memory_only", "_help", "_name", "_v", "_on_variable_changed",
                 "_compare_value_4_change")

    def __init__(self, *, dtype: T, default_val=None, memory_only=True, compare_value_4_change=False, help=""):
        """
        Parameters
        ----------
        dtype : type
            Variable 的数据类型对象
        default_val : object
            Variable 的缺省值，暂时没用用到
        help : str
            variable 的信息描述，为以后产生 graph 时使用
        """

        super().__init__()

        self.dtype: T = dtype
        self._default_val = default_val
        self._memory_only: bool = memory_only
        self._compare_value_4_change: bool = compare_value_4_change
        self._help = help

        self._name = None
        self._v = None
        self._on_variable_changed: StateVariable.FUNC_ON_VARIABLE_CHANGED = None

    def clone(self):
        clone = StateVariable[T](dtype=self.dtype, default_val=self._default_val, memory_only=self._memory_only,
                                 compare_value_4_change=self._compare_value_4_change, help=self._help)
        clone.name = self.name
        return clone

    def initialize(self, v: Any, on_variable_changed: FUNC_ON_VARIABLE_CHANGED):
        """ If lazy loading of the initial value is required later, pass in a function to read the value instead of v"""
        if v != self._v and v != self._default_val:
            self._v = v
        self._on_variable_changed = on_variable_changed

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, v: str):
        assert self._name is None
        self._name = v

    @property
    def VALUE(self) -> T:
        if self._v is None:
            return self._default_val
        else:
            return self._v

    @VALUE.setter
    def VALUE(self, v: T):
        # do not compare v with self._v, assign same value at different time should trigger each time. For example,
        # same delta be applied twice
        if not self._compare_value_4_change or self._v != v:
            self._v = v
            if self._on_variable_changed is not None:
                self._on_variable_changed(self._name, v)

    def mark_changed(self):
        if not self._compare_value_4_change and self._on_variable_changed is not None:
            self._on_variable_changed(self._name, self._v)

    @property
    def default_val(self):
        return self._default_val

    @property
    def memory_only(self) -> bool:
        return self._memory_only

    def __repr__(self):
        return f"{self._name}: {self.VALUE}"


class StateVariableCommitter:

    __slots__ = ("_state_vars_changes", )

    def __init__(self):
        super().__init__()
        """ changed state variable and values since last commit"""
        self._state_vars_changes: Dict[str, Any] = dict()

    def initialize_state(self, state: 'State', state_var_reader: SINGLE_OBJECT_STATE_READER):
        class_level_state_vars = state.__class__.get_all_state_vars()

        for class_level_state_var in class_level_state_vars:
            state_var_value = state_var_reader(class_level_state_var.name, class_level_state_var.default_val)
            instance_state_var = state[class_level_state_var]
            instance_state_var.initialize(state_var_value, self._on_state_var_changed)

    def commit_state_var_changes(self, *, object_pk: Any, object_pk_bytes: bytes,
                                 stream_publishing_changes: ObjectStateStream,
                                 stream_saving_changes: ObjectStateStream) -> asyncio.Future:
        state_vars_changes = self._state_vars_changes
        if len(state_vars_changes) > 0:
            # move the changed variable out in case it's modified during sending changes
            self._state_vars_changes = dict()

            stream_saving_changes = stream_saving_changes or stream_publishing_changes
            if id(stream_saving_changes) == id(stream_publishing_changes):
                return stream_publishing_changes.upsert_object_state(object_pk_bytes=object_pk_bytes,
                                                                     object_state_vars=state_vars_changes)
            else:
                changes_4_publish = dict()
                changes_4_save = dict()
                for state_var_name, state_var_value in state_vars_changes.items():
                    _, _, name_after_last_dot = state_var_name.rpartition('.')
                    changes_dict = changes_4_save if name_after_last_dot.startswith('_') else changes_4_publish
                    changes_dict[state_var_name] = state_var_value

                async_tasks = list()
                if len(changes_4_publish) > 0:
                    async_tasks.append(stream_publishing_changes.upsert_object_state(object_pk_bytes=object_pk_bytes,
                                                                                     object_state_vars=changes_4_publish))
                if len(changes_4_save) > 0:
                    async_tasks.append(stream_saving_changes.upsert_object_state(object_pk=object_pk,
                                                                                 object_state_vars=changes_4_save))
                return asyncio.ensure_future(asyncio.wait(async_tasks, return_when=asyncio.ALL_COMPLETED))
        else:
            return asyncio.ensure_future(asyncio.sleep(0))

    def _on_state_var_changed(self, state_var_name: str, state_var_value: Any):
        self._state_vars_changes[state_var_name] = state_var_value
