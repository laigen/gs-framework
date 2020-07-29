# -*- coding: UTF-8 -*-
import logging
import faust
import asyncio

from gs_framework.samples.rpc_sample import SimLocalEnv, DoAdd, RPCCallDoAddEnv
from gs_framework.stream_rpc import RPCEndPointService, RPCEndPointServiceUnit

logger = logging.getLogger(__name__)


async def start_rpc_test():
    logger.info("----------- preparing -------------")

    # create an object provide RPC call implementation
    do_add = DoAdd("do add")

    # expose the object through RPC service
    topic_4_do_add = faust.types.TP("topic_4_do_add", 1)
    do_add_rpc_service = RPCEndPointService(do_add)
    do_add_rpc_service.bind_rpc_callee_stream(topic_4_do_add)
    await do_add_rpc_service.start()

    logger.info("do_add_rpc_service started")

    # create an env who will call do_add_rpc_service after start
    topic_4_call_do_add_env = faust.types.TP("topic_4_call_do_add_env", 1)
    call_do_add_env = RPCCallDoAddEnv()
    call_do_add_env.bind(topic_define=topic_4_call_do_add_env)
    call_do_add_env.rpc_caller_stream.bind(topic_4_call_do_add_env)

    # expose call_do_add_env through rpc service
    call_do_add_env_rpc_service_unit = RPCEndPointServiceUnit(call_do_add_env)
    call_do_add_env_rpc_service_unit.rpc_callee_stream.bind(topic_4_call_do_add_env)
    call_do_add_env.add_service_units(call_do_add_env_rpc_service_unit)

    await call_do_add_env.start()

    logger.info("call_do_add_env started")

    # create rpc stub to call do add
    call_do_add_env.create_do_add_stub(do_add_rpc_service.rpc_stub_data)

    # create local env calling call_do_add_env_rpc_service
    topic_4_local_env = faust.types.TP("topic_4_call_do_add_env", 1)
    local_env = SimLocalEnv(call_do_add_env_rpc_service_unit.rpc_stub_data)
    local_env.bind(topic_define=topic_4_local_env)
    local_env.rpc_caller_stream.bind(topic_4_local_env)
    await local_env.start()

    logger.info("local_env started")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(start_rpc_test())
    loop.run_forever()

