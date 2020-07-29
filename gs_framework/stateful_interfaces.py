from typing import Any, Callable, Mapping, Optional, Type, Dict


class CloneableT:

    def clone(self):
        ...


class Clone2InstanceAttr(CloneableT):

    def __set_name__(self, owner: Type, name: str):
        self.name = name
        self._owner = owner

    def __get__(self, instance: object, owner: Type):
        if instance is None:  # accessing the class member
            return self
        else:
            inst_dict = instance.__dict__
            inst_member = inst_dict.get(self.name, None)
            return inst_dict.setdefault(self.name, self.clone()) if inst_member is None else inst_member

    def __set__(self, instance: object, value: Any):
        raise RuntimeError(f"Cannot assign to member {self.name} of instance of {self._owner.__qualname__}")


SINGLE_OBJECT_STATE_READER = Callable[[str, Any], Any]
"""
The parameters are: name, default_val = None.
return value read
"""

STATE_TRANSFORMER = Callable[[Any, Mapping[str, Any], Mapping[str, Any], bytes], Optional[Mapping[str, Any]]]
"""
object transformer function can be used to select a subset of state vars, add derived state vars, or determine 
whether this object should be removed from transformed result. it can be used to generate new streams from existing 
stream, or determine what to be saved to stream storage.

The parameters are: object_pk, object_state_vars, headers, object_pk_bytes.

Return the transformed state_vars, which might be sent to new stream or saved to stream storage, 
derived state_vars can also be added.
Return None means nothing is generated for result. Same as return an empty Mapping
"""

OBJECT_STATE_READER = Callable[[Any, str, Any], Any]
"""
The parameters are: object_pk, name, default_val = None.
return value read
"""

STATEFUL_STATE_TRANSFORMER = Callable[[OBJECT_STATE_READER, Any, Mapping[str, Any], Dict[str, Any], bytes],
                                      Optional[Dict[str, Any]]]
"""
stateful means this observer has it own state, which is represented by ObjectStateReaderT 
The parameters are: state_reader, object_pk, object_state_vars, headers, object_pk_bytes.
"""


class PkMixin:

    # an attribute _pk will be set by other code like decorator or __init__
    @property
    def pk(self):
        return self._pk


# this class is not used for now.
# class PK2Hashable:
#
#     __slots__ = ('_pk', "v")
#
#     def __init__(self, pkMixin: PkMixin):
#         self._pk = pkMixin.pk
#         self.v = pkMixin
#
#     def __eq__(self, other):
#         return isinstance(other, PK2Hashable) and self._pk == other._pk
#
#     def __hash__(self):
#         return hash(self._pk)


class AppIdMixin:

    # an attribute _app_id will be set by other code like decorator or __init__

    @property
    def app_id(self):
        return self._app_id
