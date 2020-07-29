import inspect
from typing import Dict, Any, Mapping, Union, Tuple, Iterable, Optional, NamedTuple

from faust import ChannelT
from faust.types import AppT, TP

from .state_stream import STATE_OBSERVER, ObjectStateStream, StreamBinder
from .stateful_interfaces import STATEFUL_STATE_TRANSFORMER, Clone2InstanceAttr
from .utilities import bytes_2_object, object_2_bytes


class StorageKey(NamedTuple):

    object_pk: Any
    state_var_name: str

    def to_bytes(self):
        return object_2_bytes(self)


class StateStorage(STATE_OBSERVER):
    """
    Create one table for each property name.
    """
    __slots__ = ("_app", "_name", "_num_of_partitions", "_table")

    def __init__(self, app: AppT, name: str, num_of_partitions: int):
        super().__init__()
        table_name = f"table_of_storage_{name}"
        table_help = table_name.replace('_', ' ')
        self._table = app.Table(name=table_name, default=lambda: None, key_type=bytes, value_type=bytes,
                                help=table_help, partitions=num_of_partitions)

    def __call__(self, object_pk: Any, object_state_vars: Mapping[str, Any], headers_not_used: Mapping[str, Any],
                 object_pk_bytes_not_used: bytes):
        state_vars_to_be_saved = None if object_state_vars is None else object_state_vars.items()
        if state_vars_to_be_saved is not None:
            self.save_state_vars(object_pk, *state_vars_to_be_saved)

    def contains_state_var(self, object_pk: Any, state_var_name: str) -> bool:
        return StorageKey(object_pk, state_var_name).to_bytes() in self._table

    def read_state_var(self, object_pk: Any, state_var_name: str, default_val: Any = None) -> Any:
        bytes_read = self._table.get(StorageKey(object_pk, state_var_name).to_bytes(), default_val)
        return default_val if bytes_read == default_val else bytes_2_object(bytes_read)

    def save_state_vars(self, object_pk: Any, *state_vars: (str, Any)):
        for var_name, var_value in state_vars:
            self._table[StorageKey(object_pk, var_name).to_bytes()] = object_2_bytes(var_value)

    def delete_state_vars(self, object_pk: Any, *state_var_names: str):
        for var_name in state_var_names:
            self._table.pop(StorageKey(object_pk, var_name).to_bytes(), None)


class StateStreamStorage(StreamBinder, Clone2InstanceAttr):

    __slots__ = ("_stream_template", "_state_stream", "_stateful_transformer", "_state_storage",
                 "_forward_through_in_mem_channel", "_in_mem_channel_stream")

    # StateStreamStorage also accepts and observer to send out variable value it received
    # thus it expects result in this format
    class TransformedResult(NamedTuple):

        value: Any
        memory_only: bool

    def __init__(self, *, stream_as_template: Optional[ObjectStateStream] = None, topic_define: Optional[TP] = None,
                 stateful_transformer: Optional[STATEFUL_STATE_TRANSFORMER] = None):
        """
        :param stream_as_template: refer to class StreamTemplate
        :param topic_define: refer to class StreamTemplate
        :param stateful_transformer: the transformer function applied to state var messages received.
        If not given, there will be no transform
        """
        super().__init__()
        self._stateful_transformer: Optional[STATEFUL_STATE_TRANSFORMER] = None
        self._state_storage: StateStorage = None
        self._forward_through_in_mem_channel = False
        self._in_mem_channel_stream: ObjectStateStream = None

        self.bind(stream_as_template=stream_as_template, topic_define=topic_define,
                  stateful_transformer=stateful_transformer)

    @staticmethod
    def bind_at_runtime():
        return StateStreamStorage()

    @property
    def storage(self) -> StateStorage:
        return self._state_storage

    @property
    def in_mem_channel_stream(self) -> ObjectStateStream:
        return self._in_mem_channel_stream

    def bind(self, *, stream_as_template: Optional[ObjectStateStream] = None, topic_define: Optional[TP] = None,
             stateful_transformer: Optional[STATEFUL_STATE_TRANSFORMER] = None,
             forward_through_in_mem_channel: bool = False):
        super().bind(stream_as_template=stream_as_template, topic_define=topic_define)
        if stateful_transformer is not None:
            self._stateful_transformer = stateful_transformer
        if forward_through_in_mem_channel is not None:
            self._forward_through_in_mem_channel = forward_through_in_mem_channel

    def initialize(self, app: AppT, storage_name: str, observer: STATE_OBSERVER):
        state_storage = StateStorage(app, storage_name,
                                     1 if self._forward_through_in_mem_channel
                                     else self._stream_template.effective_topic_define.partition)
        self._state_storage = state_storage

        stateful_transformer = self._stateful_transformer

        def do_transform(object_pk: Any, object_state_vars: Mapping[str, Any], headers: Dict[str, Any],
                         object_pk_bytes: bytes):
            if stateful_transformer is None:
                object_state_vars_2_save = object_state_vars_4_observer = object_state_vars
            else:
                transform_result: Mapping[str, StateStreamStorage.TransformedResult] = \
                    stateful_transformer(state_storage, object_pk, object_state_vars, headers, object_pk_bytes)

                if transform_result is not None:
                    object_state_vars_2_save = dict(
                        map(lambda name_and_result: (name_and_result[0], name_and_result[1].value),
                            filter(lambda name_and_result: not name_and_result[1].memory_only,
                                   transform_result.items()))
                    )

                    object_state_vars_4_observer = dict(
                        map(lambda name_and_result: (name_and_result[0], name_and_result[1].value),
                            transform_result.items())
                    )
                else:
                    object_state_vars_2_save = object_state_vars_4_observer = None

            return object_state_vars_2_save, object_state_vars_4_observer

        async def process_transformed_results(object_pk: Any, object_state_vars_2_save: Mapping[str, Any],
                                              object_state_vars_4_observer: Mapping[str, Any],
                                              headers: Dict[str, Any], object_pk_bytes: bytes):
            if object_state_vars_2_save:
                state_storage(object_pk, object_state_vars_2_save, headers, object_pk_bytes)

            if observer is not None and object_state_vars_4_observer:
                res = observer(object_pk, object_state_vars_4_observer, headers, object_pk_bytes)
                if inspect.isawaitable(res):
                    await res

        if self._forward_through_in_mem_channel:
            async def in_mem_channel_observer(object_pk: Any,
                                              object_state_vars: Union[Dict[str, Any],
                                                                       Tuple[Dict[str, Any], Dict[str, Any]]],
                                              headers: Dict[str, Any], object_pk_bytes: bytes):
                if isinstance(object_state_vars, tuple):
                    object_state_vars_2_save, object_state_vars_4_observer = object_state_vars
                else:
                    object_state_vars_2_save, object_state_vars_4_observer = \
                        do_transform(object_pk, object_state_vars, headers, object_pk_bytes)

                await process_transformed_results(object_pk, object_state_vars_2_save, object_state_vars_4_observer,
                                                  headers, object_pk_bytes)

            in_mem_channel = app.channel()
            in_mem_stream_transformer = StreamBinder()
            in_mem_stream_transformer.bind(stream_as_template=None, topic_define=None)
            in_mem_stream_transformer.initialize(in_mem_channel, storage_name, in_mem_channel_observer)
            in_mem_channel_stream = self._in_mem_channel_stream = in_mem_stream_transformer.stream

            def state_stream_observer(object_pk: Any, object_state_vars: Mapping[str, Any],
                                      headers: Dict[str, Any], object_pk_bytes: bytes):
                object_state_vars_2_save, object_state_vars_4_observer = \
                    do_transform(object_pk, object_state_vars, headers, object_pk_bytes)

                # object_state_vars_4_observer should contain object_state_vars_2_save thus
                # only need to check object_state_vars_4_observer
                if object_state_vars_4_observer:
                    in_mem_channel_stream.upsert_object_state(
                        object_pk=object_pk,
                        object_state_vars=(object_state_vars_2_save, object_state_vars_4_observer))

        else:
            async def state_stream_observer(object_pk: Any, object_state_vars: Mapping[str, Any],
                                            headers: Dict[str, Any], object_pk_bytes: bytes):
                object_state_vars_2_save, object_state_vars_4_observer = \
                    do_transform(object_pk, object_state_vars, headers, object_pk_bytes)

                await process_transformed_results(object_pk, object_state_vars_2_save, object_state_vars_4_observer,
                                                  headers, object_pk_bytes)

        super().initialize(app, storage_name, state_stream_observer)

    def clone(self):
        stream_as_template, topic_define = self._stream_template
        return StateStreamStorage(stream_as_template=stream_as_template, topic_define=topic_define,
                                  stateful_transformer=self._stateful_transformer)
