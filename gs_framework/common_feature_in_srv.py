# -*- coding: UTF-8 -*-
"""
在 Stateful , Stateless Srv 中都支持的一些 feature
"""
import inspect
import traceback
from datetime import datetime
import logging
from gs_framework.common_prop_dtypes import StringT, DatetimeT, GlobalUniqueInst, ClassFullname
from gs_framework.object_reference import DCERef
from gs_framework.platform_srv.srv_dce_define import ErrorLogCLCT, ErrLogInfo
from gs_framework.utilities import generate_uuid, get_k8s_stateful_set_pod_id
logger = logging.getLogger(__name__)


class _CommonFeaturesInSrv:
    """Stateful , Stateless Srv 中都支持的一些 feature"""
    def __init__(self):
        self._dce_err_log = DCERef(ErrorLogCLCT, write=True, read=False)

    async def log_error(self, err_msg):
        # log error 的部分，本身不能再抛异常，否则会产生递归的问题
        try:
            await self._dce_err_log.upsert_entity_props(GlobalUniqueInst(generate_uuid()), {
                ErrLogInfo.pod: StringT(get_k8s_stateful_set_pod_id()),
                ErrLogInfo.cls: ClassFullname(module_name=self.__class__.__module__, qualname=self.__class__.__qualname__),
                ErrLogInfo.inst_gid: GlobalUniqueInst(self._hash_gid),
                ErrLogInfo.err_msg: StringT(err_msg),
                ErrLogInfo.err_time: DatetimeT(datetime.now()),
                ErrLogInfo.err_traceback: StringT(
                    "\r\n".join([f"{fi.filename} - [func:{fi.function}] - [ln:{fi.lineno}]" for fi in inspect.stack()]))
            }, with_pk=True)
        except Exception as ex:
            logging.error(f"raise error when log_error {ex}")
        logger.error(f"{self.__class__.__module__}.{self.__class__.__qualname__} raise error {err_msg} \r\n {traceback.format_exc()}")
