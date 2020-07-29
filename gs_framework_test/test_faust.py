import asyncio
from typing import Any, Mapping

import faust
from confluent_kafka.admin import AdminClient
from faust.types import TP
from faust.types.tables import TableT

from gs_framework.faust_utilities import FaustUtilities
from gs_framework.state_stream import ObjectStateStream

from gs_framework.topic_channel_wrapper import TopicWrapper
from gs_framework.utilities import bytes_2_object, get_random_str, object_2_bytes

topic2_define = faust.types.TP("martin-gs_framework_test-guess-number-3", 1)
app_listen = faust.App("martin-gs_framework_test-app", broker="kafka://gftoffice.sedns.cn:31090", store="rocksdb://", datadir="/opt/gsfaust")

# FaustUtilities.Admin.check_and_create_topic(topic2_define)
topic2 = app_listen.topic(topic2_define.topic, key_type=str, value_type=str, partitions=topic2_define.partition)


async def agent_function(stream):
    async for event in stream.events():
        message = event.message
        print(f"key={message.key}, value={message.value}")


app_listen.agent(topic2, name="agent_function")(agent_function)

loop = asyncio.get_event_loop()
loop.run_until_complete(app_listen.start())

app_send = faust.App("martin-gs_framework_test-send-app", broker="kafka://gftoffice.sedns.cn:31090", store="rocksdb://", datadir="/opt/gsfaust")

loop.run_until_complete(app_send.start())

topic_4_send = app_send.topic(topic2_define.topic, key_type=str, value_type=str, partitions=topic2_define.partition)
asyncio.ensure_future(topic_4_send.send(key="123", value="123"))

loop.run_until_complete(app_listen.wait_for_stopped())
loop.run_until_complete(app_send.wait_for_stopped())
