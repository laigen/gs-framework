import asyncio
import inspect

from typing import Callable, Any, Mapping, Awaitable, Union, Optional, NamedTuple, Dict, Tuple

from faust import ChannelT, TopicT
from faust.types import AppT, TP

from .stateful_interfaces import Clone2InstanceAttr, STATE_TRANSFORMER

from .faust_utilities import FaustUtilities
from .topic_channel_wrapper import TopicWrapper, InMemoryChannelWrapper

STATE_OBSERVER = Callable[[Any, Union[Dict[str, Any], Tuple[Dict[str, Any], Dict[str, Any]]], Dict[str, Any], bytes],
                          Union[None, Awaitable[None]]]
"""
The parameters are in turn: object_pk, object_state_vars, headers, object_pk_bytes
object_pk_bytes is intended to avoid repeatedly serialize and serialize object pk

Note that the type of PK can be other than string or integer. 
if object_props is None, it means the object is removed from the stream
"""


class ObjectStateStream(Clone2InstanceAttr):
    """
    Wrap basic read / write functions for stateful object properties through kafka topic or in memory channel
    """

    __slots__ = ("_topic_define", "_channel_wrapper")

    def __init__(self, topic_define: Optional[TP]):
        super().__init__()
        self._topic_define = topic_define
        self._channel_wrapper: Union[InMemoryChannelWrapper, TopicWrapper] = None

    @staticmethod
    def bind_at_runtime():
        return ObjectStateStream(None)

    def bind(self, topic_define: TP):
        assert topic_define is not None
        assert self._topic_define is None
        self._topic_define = topic_define

    def clone(self):
        return ObjectStateStream(self._topic_define)

    def initialize(self, app_or_channel: Union[AppT, ChannelT], observer: STATE_OBSERVER = None, agent_name: str = None):
        if self._channel_wrapper is None:
            if isinstance(app_or_channel, AppT):
                self._channel_wrapper = TopicWrapper(app_or_channel, self._topic_define, None)
            else:
                assert isinstance(app_or_channel, ChannelT)
                self._channel_wrapper = InMemoryChannelWrapper(app_or_channel, None)

        if observer is not None:
            self.set_state_observer(observer, agent_name)

    @property
    def topic(self) -> Optional[TopicT]:
        channel_wrapper = self._channel_wrapper
        return channel_wrapper.topic if isinstance(channel_wrapper, TopicWrapper) else None

    @property
    def topic_define(self) -> Optional[TP]:
        return self._topic_define

    def set_state_observer(self, observer: STATE_OBSERVER, agent_name: str = None):
        channel_wrapper = self._channel_wrapper
        assert channel_wrapper is not None, "stream not initialized"

        if isinstance(channel_wrapper, TopicWrapper):
            channel_wrapper.set_message_handler(observer, agent_name)
        else:
            assert isinstance(channel_wrapper, InMemoryChannelWrapper)

            async def in_memory_channel_observer(object_pk: Any, object_state_vars: Mapping[str, Any],
                                                 headers: Dict[str, Any]):
                res = observer(object_pk, object_state_vars, headers, None)
                if inspect.isawaitable(res):
                    await res

            channel_wrapper.set_message_handler(in_memory_channel_observer, agent_name)

    def upsert_object_state(self, *, object_pk: Any = None, object_pk_bytes: bytes = None,
                            # StateStreamStorage.initialize.state_stream_observer sends a tuple
                            object_state_vars: Union[Dict[str, Any], Tuple[Dict[str, Any], Dict[str, Any]]] = None,
                            headers: Mapping[str, Any] = None) -> asyncio.Future:
        channel_wrapper = self._channel_wrapper
        assert channel_wrapper is not None, "stream not initialized"

        if isinstance(channel_wrapper, TopicWrapper):
            return FaustUtilities.send_message(channel_wrapper.topic, key=object_pk, key_bytes=object_pk_bytes,
                                               value=object_state_vars, headers=headers)
        else:
            assert isinstance(channel_wrapper, InMemoryChannelWrapper), f"channel_wrapper:{channel_wrapper}"
            assert object_pk is not None
            return asyncio.ensure_future(
                channel_wrapper.channel.send(key=object_pk, value=object_state_vars, headers=headers, force=True))


class StreamTemplate(NamedTuple):
    """
    This class wraps two ways of declaring a stream as init parameter for stream observers (including stream storage,
     object ref):
    -- use a class member in type of ObjectStateStream, the parameter is typically named stream_as_template
    -- declare the topic name and partition number directly, the parameter is typically named topic_define

    The reason why the stream object passed to init function of stream storage object is named as a template:

    When define class member which is a stream storage, it's nature to pass in another class member which is a stream
    as init parameters. But at run time, instance will have it's own members replacing both the class level
    stream storage member and the stream member, and the instance level stream storage object will not receive messages
    from the class level stream object.
    """

    stream_as_template: Optional[ObjectStateStream] = None
    topic_define: Optional[TP] = None

    @property
    def effective_topic_define(self) -> Optional[TP]:
        """
        if both are given, topic_define is ignored
        """
        return self.stream_as_template.topic_define if self.stream_as_template is not None else self.topic_define


class StreamBinder:

    def __init__(self):
        super().__init__()
        self._stream_template: StreamTemplate = None
        self._state_stream: ObjectStateStream = None

    def bind(self, *, stream_as_template: Optional[ObjectStateStream], topic_define: Optional[TP]):
        self._stream_template = StreamTemplate(stream_as_template, topic_define)

    def initialize(self, app_or_channel: Union[AppT, ChannelT], member_name: str, observer: STATE_OBSERVER):
        # always create a state stream object from stream template
        # Faust allows create multiple Topic objects for same topic name
        topic_define = self._stream_template.effective_topic_define
        agent_name = f"agent_of_topic_{topic_define.topic}_for_member_{member_name}" \
            if isinstance(app_or_channel, AppT) else f"agent_of_in_memory_channel_for_member_{member_name}"
        self._state_stream = ObjectStateStream(topic_define)
        self._state_stream.initialize(app_or_channel, observer, agent_name)

    @property
    def stream(self) -> ObjectStateStream:
        return self._state_stream

    @property
    def topic_define(self) -> Optional[TP]:
        return self._stream_template.effective_topic_define


# this class is not used anymore because StatefulService.state_vars_stream is removed
# class TransformedStream(StreamBinder, Clone2InstanceAttr):
#
#     __slots__ = ("_stream_template", "_state_stream", "_transformer")
#
#     def __init__(self, *, stream_as_template: Optional[ObjectStateStream] = None, topic_define: Optional[TP] = None,
#                  transformer: Optional[STATE_TRANSFORMER] = None):
#         """
#         :param stream_as_template: refer to class StreamTemplate
#         :param topic_define: refer to class StreamTemplate
#         :param transformer: the transformer function applied to state var messages received.
#         If not given, there will be no transform
#         """
#         super().__init__()
#         self._transformer: Optional[STATE_TRANSFORMER] = None
#         self.bind(stream_as_template=stream_as_template, topic_define=topic_define, transformer=transformer)
#
#     @staticmethod
#     def bind_at_runtime():
#         return TransformedStream()
#
#     def bind(self, *, stream_as_template: Optional[ObjectStateStream] = None, topic_define: Optional[TP] = None,
#              transformer: Optional[STATE_TRANSFORMER] = None):
#         super().bind(stream_as_template=stream_as_template, topic_define=topic_define)
#         self._transformer = transformer
#
#     def initialize(self, app: AppT, member_name: str, observer: STATE_OBSERVER):
#         transformer = self._transformer
#
#         async def state_stream_observer(object_pk: Any, object_state_vars: Mapping[str, Any],
#                                         headers: Dict[str, Any], object_pk_bytes: bytes):
#             object_state_vars_transformed = object_state_vars if transformer is None else \
#                 transformer(object_pk, object_state_vars, headers, object_pk_bytes)
#             if observer is not None:
#                 res = observer(object_pk, object_state_vars_transformed, headers, object_pk_bytes)
#                 if inspect.isawaitable(res):
#                     await res
#
#         super().initialize(app, member_name, state_stream_observer)
#
#     def clone(self):
#         stream_as_template, topic_define = self._stream_template
#         return TransformedStream(stream_as_template=stream_as_template, topic_define=topic_define,
#                                  transformer=self._transformer)
