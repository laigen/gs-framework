import asyncio
import inspect
from typing import Callable, Any, Mapping, Union, Awaitable, Dict

from faust import TopicT, StreamT, ChannelT
from faust.types import AppT, TP

from gs_framework.faust_utilities import FaustUtilities


class InMemoryChannelWrapper:

    FUNC_PROCESS_MESSAGE = Callable[[Any, Any, Mapping[str, Any]], Union[None, Awaitable[None]]]
    """
    The parameters are in turn: message_key, message_value, headers 
    The parameter message_key_bytes is designed to avoid serializing key to bytes again
     when saving message value to tables
    """

    def __init__(self, in_mem_channel: ChannelT, func_process_message: FUNC_PROCESS_MESSAGE, agent_name: str = None):
        """
        :param app: the faust app
        :param in_mem_channel: the in memory channel
        :param func_process_message: event process handler. If none, then message from this topic will not be read
        :return:
        """
        self.channel = in_mem_channel
        if func_process_message is not None:
            self.set_message_handler(func_process_message, agent_name)

    def set_message_handler(self, func_process_message: FUNC_PROCESS_MESSAGE, agent_name: str = None):
        assert func_process_message is not None

        async def agent_function(stream: StreamT):
            async for event in stream.events():
                res = func_process_message(event.message.key, event.message.value, event.headers)
                if inspect.isawaitable(res):
                    asyncio.ensure_future(res)

        app = self.channel.app

        agent_name = agent_name or f"agent for in memory channel"
        assert agent_name not in app.agents, f"{agent_name} has been created for app {app}"

        app.agent(self.channel, name=agent_name)(agent_function)


class TopicWrapper:

    FUNC_PROCESS_MESSAGE = Callable[[Any, Any, Dict[str, Any], bytes], Union[None, Awaitable[None]]]
    """
    The parameters are in turn: message_key, message_value, headers, message_key_bytes
    The parameter message_key_bytes is designed to avoid serializing key to bytes again
     when saving message value to tables
    """

    def __init__(self, app: AppT, topic: TP, func_process_message: FUNC_PROCESS_MESSAGE,
                 agent_name: str = None):
        """
        :param app: the faust app
        :param topic:
        :param func_process_message: event process handler. If none, then message from this topic will not be read
        :return:
        """
        self.topic: TopicT = FaustUtilities.create_topic(app, topic)
        if func_process_message is not None:
            self.set_message_handler(func_process_message, agent_name)

    def set_message_handler(self, func_process_message: FUNC_PROCESS_MESSAGE, agent_name: str = None):
        assert func_process_message is not None

        async def agent_function(stream: StreamT):
            async for event in stream.events():
                message_key, message_value, headers = FaustUtilities.decode_message(event)
                message_key_bytes = event.message.key
                res = func_process_message(message_key, message_value, headers, message_key_bytes)
                if inspect.isawaitable(res):
                    asyncio.ensure_future(res)

        app = self.topic.app
        topic_name = self.topic.get_topic_name()

        agent_name = agent_name or f"agent_of_{topic_name}"
        assert agent_name not in app.agents, f"{agent_name} has been created for app {app}"

        app.agent(self.topic, name=agent_name)(agent_function)
