import asyncio
import itertools
from typing import List, Tuple, Iterable, Union, Any, Dict

from dataclasses import dataclass

from .stateful_interfaces import PkMixin
from .state_storage import StateStorage
from .stateful_interfaces import SINGLE_OBJECT_STATE_READER, OBJECT_STATE_READER
from .utilities import object_2_bytes

from .state_stream import ObjectStateStream
from .state_variable import StateVariable, StateVariableCommitter


class StateMeta(type):
    """
    Used as metaclass for classes whose class level state variable member will be initialize with fullname of the
    member. Nested class should be decorated too.
    """

    def __init__(cls, name: str, bases: Tuple, dct: Dict[str, Any]):
        super().__init__(name, bases, dct)

        # this __init__ will be called for each derived class of State, or each class in type of StateMeta,
        # starting from base class.
        # If use dir(cls), the state variables of base classes will be re-initialized again in derived class,
        # thus we use cls.__dict__.items() to list only members container in current class, excluding members
        # inherited from base class
        for state_var_name, state_var in \
                filter(lambda name_and_member: isinstance(name_and_member[1], StateVariable), cls.__dict__.items()):
            state_var.name = f"{cls.__qualname__}.{state_var_name}"

    def __setattr__(cls, name, value):
        """重载 setattr 是为了保护对 state 已经有值的情况下，直接赋值可能会产生错误的问题"""
        existing_value = getattr(cls, name, None)
        if isinstance(existing_value, (StateVariable, StateMeta)):
            raise RuntimeError(f"The type of class member {cls.__qualname__}.{name} is {type(existing_value)}, "
                               f"cannot be assigned.")
        else:
            super().__setattr__(name, value)


def _iter_stateful_object_and_state_var_members(cls: type) \
        -> Iterable[Tuple[str, Union[StateMeta, StateVariable]]]:
    return filter(lambda name_and_member: isinstance(name_and_member[1], (StateMeta, StateVariable)),
                  map(lambda attr_name: (attr_name, getattr(cls, attr_name)), dir(cls)))


def _get_stateful_object_and_state_var_members(cls: type) \
        -> Tuple[List[Tuple[str, StateMeta]], List[Tuple[str, StateVariable]]]:
    stateful_objects: List[str, StateMeta] = list()
    state_vars: List[str, StateVariable] = list()

    for member_name, class_member in _iter_stateful_object_and_state_var_members(cls):
        item = (member_name, class_member)
        if isinstance(class_member, StateMeta):
            stateful_objects.append(item)
        else:
            assert isinstance(class_member, StateVariable)
            state_vars.append(item)

    return stateful_objects, state_vars


def _get_all_state_vars(cls: type, result: List) -> List[StateVariable]:
    stateful_objects, state_vars = _get_stateful_object_and_state_var_members(cls)

    result.extend(map(lambda name_and_var: name_and_var[1], state_vars))

    for name, stateful_object in stateful_objects:
        _get_all_state_vars(stateful_object, result)

    return result


class State(metaclass=StateMeta):

    @classmethod
    def get_all_state_vars(cls) -> List[StateVariable]:
        return _get_all_state_vars(cls, list())

    def __new__(cls, *args, **kwargs):
        self = super().__new__(cls)

        # member _state_vars need to be ready before __init__ is called, because code in decorators for __init__ might
        # run before __init__ is called, and it will update attributes of self, which uses _state_vars
        self._state_vars: Dict[str, StateVariable] = dict(map(
            lambda v: (v.name, v.clone()), cls.get_all_state_vars()))
        return self

    def __getitem__(self, item):
        if isinstance(item, StateVariable):
            return self._state_vars[item.name]
        elif isinstance(item, str):
            return self._state_vars[item]
        else:
            raise TypeError(f"Incorrect index type: {item.__class__.__qualname__}")

    def __setitem__(self, key, value):
        raise RuntimeError("Cannot change state variable")

    def __delitem__(self, key):
        raise RuntimeError("Cannot delete state variable")

    def __contains__(self, item):
        if isinstance(item, StateVariable):
            return item.name in self._state_vars
        if isinstance(item, str):
            return item in self._state_vars
        else:
            raise TypeError(f"Incorrect index type: {item.__class__.__qualname__}")

    def __getattribute__(self, name):
        # the attributes accessed in this function needs to be short circuited to avoid infinite recursion
        if name in ('_state_vars', '__class__'):
            return super().__getattribute__(name)
        else:
            try:
                # first check whether the name is a full name of state variables
                return self._state_vars[name]
            except AttributeError:  # self._state_vars has not been set yet.
                return super().__getattribute__(name)
            except KeyError:
                # check whether the name is a name of a class member
                cls = self.__class__
                cls_member = getattr(cls, name, None)
                if isinstance(cls_member, StateVariable):
                    return self._state_vars[cls_member.name]
                else:
                    return super().__getattribute__(name)

    def _is_name_state_var(self, name: str):
        if name == '_state_vars':
            return False
        elif name in self._state_vars:
            return True
        else:
            cls = self.__class__
            cls_member = getattr(cls, name, None)
            return isinstance(cls_member, StateVariable)

    def __setattr__(self, name, value):
        if self._is_name_state_var(name):
            raise RuntimeError(f"attribute {name} of class {self.__class__.__qualname__} "
                               f"is state variable, cannot be assigned.")
        else:
            super().__setattr__(name, value)

    def __delattr__(self, name):
        if self._is_name_state_var(name):
            raise RuntimeError(f"attribute {name} of class {self.__class__.__qualname__} "
                               f"is state variable, cannot be deleted.")
        else:
            super().__delattr__(name)

    def __dir__(self) -> Iterable[str]:
        return itertools.chain(super().__dir__(), self._state_vars.keys())

    def __eq__(self, other) -> bool:
        if isinstance(other, State):
            self_cls_state_vars = self.__class__.get_all_state_vars()
            other_cls_state_vars = other.__class__.get_all_state_vars()

            if len(self_cls_state_vars) == len(other_cls_state_vars):
                self_cls_state_vars.sort(key=lambda v: v.name)
                other_cls_state_vars.sort(key=lambda v: v.name)

                def compare_state_variables(self_and_other_cls_state_vars) -> bool:
                    self_cls_state_var, other_cls_state_var = self_and_other_cls_state_vars
                    return self_cls_state_var.name == other_cls_state_var.name and \
                        self[self_cls_state_var].VALUE == other[other_cls_state_var].VALUE

                return all(map(compare_state_variables, zip(self_cls_state_vars, other_cls_state_vars)))

        return False

    def __hash__(self):
        self_cls_state_vars = self.__class__.get_all_state_vars()
        self_cls_state_vars.sort(key=lambda v: v.name)

        return hash('|'.join(map(lambda v: repr(self[v]), self_cls_state_vars)))


class StatefulObject(PkMixin, State):

    def __init__(self):
        super().__init__()
        self._state_var_committer = StateVariableCommitter()
        assert self.pk is not None
        self._object_pk_bytes: bytes = object_2_bytes(self.pk)

    def get_all_instance_state_vars(self) -> List[StateVariable]:
        class_level_state_vars = self.__class__.get_all_state_vars()
        return [self[class_level_state_var] for class_level_state_var in class_level_state_vars]

    def initialize_state(self, state_var_reader: SINGLE_OBJECT_STATE_READER):
        self._state_var_committer.initialize_state(self, state_var_reader)

    def mark_all_state_variable_changed(self):
        for stat_var in self.get_all_instance_state_vars():
            stat_var.mark_changed()

    def commit_state_var_changes(self, stream_publishing_changes: ObjectStateStream,
                                 stream_saving_changes: ObjectStateStream = None) -> asyncio.Future:
        return self._state_var_committer.commit_state_var_changes(object_pk=self.pk,
                                                                  object_pk_bytes=self._object_pk_bytes,
                                                                  stream_publishing_changes=stream_publishing_changes,
                                                                  stream_saving_changes=stream_saving_changes)


def read_stateful_object(pk: Any, state: StateMeta, *object_state_readers: OBJECT_STATE_READER) -> StatefulObject:

    assert issubclass(state, State)

    class StatefulObjectCreated(StatefulObject, state):

        def __init__(self, *args, **kwargs):
            self._pk = pk
            super().__init__(*args, **kwargs)

            def state_var_reader(name: str, default_val: Any):
                return next(filter(lambda v: v != default_val,
                                   map(lambda reader: reader(pk, name, default_val),
                                       object_state_readers)), default_val)

            super().initialize_state(state_var_reader)

    return StatefulObjectCreated()


def create_stateful_object(pk: Any, state: StateMeta):
    return read_stateful_object(pk, state)


@dataclass(frozen=True)
class StorageAsStateReader:
    """
    wraps a state storage to state reader
    """

    storage: StateStorage

    def __call__(self, object_pk: Any, name: str, default_val: Any = None):
        return self.storage.read_state_var(object_pk, name, default_val)


@dataclass(frozen=True)
class MessageAsStateReader:
    """
    wraps a message read from stream to state reader
    """

    pk: Any
    state_vars: Dict[str, Any]

    def __call__(self, object_pk: Any, name: str, default_val: Any = None):
        return self.state_vars.get(name, default_val) if object_pk == self.pk else default_val


class PropertiesAsStateReader:
    """
    wraps a set of properties to state reader
    """
    def __init__(self, props: Dict[Union[str, StateVariable], Any]):
        super().__init__()
        self._props = dict(map(
            lambda key_val: (key_val[0].name if isinstance(key_val[0], StateVariable) else key_val[0], key_val[1]),
            props.items()))

    def __call__(self, object_pk: Any, name: str, default_val: Any = None):
        return self._props.get(name, default_val)
