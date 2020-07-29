# -*- coding: UTF-8 -*-
from .state_variable import StateVariable
from .stateful_object import State
from .service import StatefulService, StatelessService
from .activatable_stateful_service import Env, Agent, Episode
from .object_reference import ObjectRef
from .state_stream import ObjectStateStream
from .state_var_change_dispatcher import state_var_change_handler, pick_one_change
from .state_storage import StateStreamStorage
from .stateful_object import create_stateful_object
from .timer_handler import timer
from .crontab_handler import crontab
from .handler import StatefulObjectAndCommitStream

__all__ = ["StateVariable", "State", "StatefulService", "StatelessService", "Env", "Agent", "Episode", "ObjectRef",
           "ObjectStateStream", "state_var_change_handler", "pick_one_change",
           "StateStreamStorage", "create_stateful_object", "timer", "crontab","StatefulObjectAndCommitStream"]

# 暂时为了方便，将 loging 的输出级别写在了 __init__ 中
# !!! 在 __init__ 中写 logging 的输出方式并不规范，应该是在后续具体的应用模块中设定。这里只是为了全局调试方便的一种临时方案
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(name)s,line:%(lineno)d] - %(levelname)s - %(message)s')

# 调高一些干扰项的 logger level 内容
# for n in ["hpack.hpack", "purerpc.anyio_monkeypatch", "asyncio", "hpack.table", "aiokafka.consumer.group_coordinator",
#           "faust.transport.drivers.aiokafka", "faust.tables.recovery", "aiokafka.consumer.subscription_state",
#           "faust.stores.rocksdb", "faust.tables.table"]:
#     other_logger = logging.getLogger(n)
#     other_logger.setLevel(logging.WARNING)

# faust 用到了 click 处理 command ， 需要将 cmd 的编码方式改为 UTF8
import os
os.environ["LC_ALL"] = "en_US.utf-8"
os.environ["LANG"] = "en_US.utf-8"

# 去掉 send_msg 没加 await 引起的重复 warning
import warnings
warnings.filterwarnings("once")


