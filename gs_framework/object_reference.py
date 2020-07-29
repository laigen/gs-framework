import inspect
import logging
from typing import Any, Mapping, Optional

from faust.types import AppT, TP

from .stateful_interfaces import Clone2InstanceAttr, PkMixin
from .utilities import object_2_bytes

from .state_stream import ObjectStateStream, STATE_OBSERVER, StreamBinder

logger = logging.getLogger(__name__)


class ObjectRef(StreamBinder, Clone2InstanceAttr, PkMixin):

    __slots__ = ("_stream_template", "_state_stream", "_pk")

    def __init__(self, object_pk: Any, *, stream_as_template: Optional[ObjectStateStream] = None,
                 topic_define: Optional[TP] = None):
        """
        Args:
            object_pk (Any):
            stream_as_template:
            topic_define:
        """
        super().__init__()
        self._pk: Any = None
        self.bind(object_pk, stream_as_template=stream_as_template, topic_define=topic_define)

    @staticmethod
    def bind_at_runtime() -> 'ObjectRef':
        return ObjectRef(None)

    def bind(self, object_pk: Any, *, stream_as_template: Optional[ObjectStateStream] = None,
             topic_define: Optional[TP] = None):
        """
        Args:
            object_pk (Any):
            stream_as_template:
            topic_define:
        """
        super().bind(stream_as_template=stream_as_template, topic_define=topic_define)
        self._pk = object_pk

    def clone(self) -> 'ObjectRef':
        stream_as_template, topic_define = self._stream_template
        return ObjectRef(self._pk, stream_as_template=stream_as_template, topic_define=topic_define)

    def initialize(self, app: AppT, ref_name: str, observer: STATE_OBSERVER):
        # always create a state stream object from stream template
        # Faust allows create multiple Topic objects for same topic name
        """
        Args:
            app (AppT):
            ref_name (str):
            observer (STATE_OBSERVER):
        """
        referred_object_pk_bytes = object_2_bytes(self._pk)

        async def object_ref_observer(object_pk: Any, object_state_vars: Mapping[str, Any], headers: Mapping[str, Any],
                                      object_pk_bytes: bytes):
            if referred_object_pk_bytes == object_pk_bytes and observer is not None:
                res = observer(object_pk, object_state_vars, headers, object_pk_bytes)
                if inspect.isawaitable(res):
                    await res

        super().initialize(app, ref_name, object_ref_observer)


# @staticmethod
# def create(def_cls: Type, *args, **kwargs):
#     return StatefulAppRef(InstWithHashGid.calc_inst_hash_gid(def_cls, *args, **kwargs))
#
# @staticmethod
# def from_inst_creation_data(inst_creation_data: InstCreationData) -> 'StatefulAppRef':
#     assert isinstance(inst_creation_data, InstCreationData)
#     return inst_creation_data.create_customized_instance(StatefulAppRef.create)

