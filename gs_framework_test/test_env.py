# -*- coding: UTF-8 -*-
"""
这里是模拟 framework 要做的事情
"""
import logging

import faust

from gs_framework.samples.guess_number import GuessNumberGameEnv, GuessNumberAgent
import asyncio

logger = logging.getLogger(__name__)


async def start_guess_number():
    logger.info(f"----------- preparing -------------")
    topic_define = faust.types.TP("martin-gs_framework_test-guess-number", 1)

    env = GuessNumberGameEnv()
    agent = GuessNumberAgent()

    env.bind(topic_define=topic_define)
    agent.bind(topic_define=topic_define)

    env.agent.bind(agent.pk, topic_define=topic_define)
    agent.env.bind(env.pk, topic_define=topic_define)

    logger.info(f"----------- start env -------------")
    await env.start()

    logger.info(f"----------- start agent -------------")
    await agent.start()

    env.active.VALUE = 1  # framework 修改的 variable
    env.commit_state_var_changes()

    logger.info(f"----------- 启动完成 -------------")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(start_guess_number())
    loop.run_forever()

