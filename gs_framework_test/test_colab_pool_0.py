# -*- coding: UTF-8 -*-
import logging

import asyncio

from gs_framework.activatable_stateful_service import Env

logger = logging.getLogger(__name__)


class ColabPoolTestEnv(Env):

    def __init__(self, trial_uuid: str):
        super().__init__()
        self.trial_uuid = trial_uuid

    async def run_colab(self):
        from gs_framework.service import StatelessService
        from gs_framework.colab.colab_pool_client import ColabPoolClient
        from gs_framework.colab.colab_pool_0_constants import Configuration as colab_config

        class _ColabPoolService(StatelessService):

            def __init__(self, pool_client: ColabPoolClient, _):
                super().__init__()
                self._pool_client: ColabPoolClient = pool_client
                self.add_service_units(pool_client)

            async def start(self):
                await super().start()
                await self._pool_client.get_ready()

        pool_client = ColabPoolClient(colab_config.pool_name, colab_config.pool_env_rpc_callee_topic)
        pool_client.pool_env_status_stream.bind(topic_define=colab_config.pool_env_status_topic)
        pool_client.rpc_caller_stream.bind(topic_define=colab_config.pool_client_rpc_caller_topic)

        pool_test_service = _ColabPoolService(pool_client, self.trial_uuid)
        await pool_test_service.start()
        logger.info(f"colab pool test service started")

        task_group = "test_task_group"

        notebook_file_id = '14v0jU3Gr0DZFOkeNSLvA4Q5YMHI4Ztpt'  # this one ends soon, let it run
        task_id = await pool_client.submit(notebook_file_id, task_group)
        logger.info(f"after submit({notebook_file_id}, {task_group}): {task_id}")

        notebook_file_id = '1OahGijhaYQDiOl5hI1HRP9DVTXA0mPIi'  # this one run for hours, test cancel
        task_id = await pool_client.submit(notebook_file_id, task_group)
        logger.info(f"after submit({notebook_file_id}, {task_group}): {task_id}")

        await asyncio.sleep(60)
        cancelled = await pool_client.cancel_task(task_id)
        logger.info(f"{task_id} cancelled: {cancelled}")

        await asyncio.sleep(10)
        await pool_client.cancel_task_group(task_group)
        logger.info(f"{task_group} cancelled: {cancelled}")

    @staticmethod
    async def run_test():
        import faust.types

        test_env = ColabPoolTestEnv("abcd")
        test_env.bind(topic_define=faust.types.TP(topic="test_colab_pool_0", partition=1))
        await test_env.start()
        await test_env.run_colab()
        await asyncio.sleep(100000)  # 这里模拟等待
        await test_env.stop()


# debugging entrance run in ide
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(ColabPoolTestEnv.run_test())
    # loop.run_forever()
