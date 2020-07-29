# -*- coding: UTF-8 -*-
"""
Test timer
"""
import asyncio
import logging
import time
from typing import Any, Dict, List

import faust
from gs_framework.activatable_stateful_service import Env

from gs_framework.state_var_change_dispatcher import state_var_change_handler

from gs_framework.state_stream import ObjectStateStream

from gs_framework.state_variable import StateVariable

from gs_framework.stateful_object import State, create_stateful_object, MessageAsStateReader, read_stateful_object

from gs_framework.timer_handler import timer

logger = logging.getLogger(__name__)


class HeartBeatMessage(State):

    sender = StateVariable(dtype=str, default_val=None, help="name of the sender")
    time = StateVariable(dtype=str, default_val=None, help="")


class HeartBeatTest(Env):

    heart_beat_stream: ObjectStateStream = ObjectStateStream.bind_at_runtime()

    def __init__(self, as_sender: bool, sender_name: str):
        super().__init__()
        self._count = 0
        self._as_sender = as_sender
        self._sender_name = sender_name

    @timer(interval=60 * 2)
    async def on_timer(self):
        if self._as_sender:
            heart_beat_message = create_stateful_object(self.pk, HeartBeatMessage)
            heart_beat_message[HeartBeatMessage.sender].VALUE = self._sender_name
            heart_beat_message[HeartBeatMessage.time].VALUE = time.asctime(time.gmtime())
            await heart_beat_message.commit_state_var_changes(self.heart_beat_stream)
            if 0 == self._count % 5:
                print(f"send heart beat message at {time.asctime(time.gmtime())}")
            if self._count >= 1000000:
                self._count = 0
            self._count = self._count + 1

    @state_var_change_handler(state_vars=HeartBeatMessage, state_var_source=heart_beat_stream)
    async def on_heart_beat_message(self, sender_pk: str, state_vars: Dict[str, Any],
                                    triggering_state_var_names: List[str]):
        if not self._as_sender:
            heart_beat_message: HeartBeatMessage =\
                read_stateful_object(sender_pk, HeartBeatMessage, MessageAsStateReader(sender_pk, state_vars))
            print(f"heart beat received: {heart_beat_message.time.VALUE} from {heart_beat_message.sender.VALUE}")

    @staticmethod
    async def run(as_sender: bool, sender_name: str):
        test_env = HeartBeatTest(as_sender, sender_name)

        heart_beat_topic = faust.types.TP(topic="heart_beat_test", partition=1)
        test_env.bind(topic_define=heart_beat_topic)
        test_env.heart_beat_stream.bind(topic_define=heart_beat_topic)

        await test_env.start()
        logger.info(f"HeartBeatTest has started. As sender: {as_sender}, sender_name: {sender_name}")
