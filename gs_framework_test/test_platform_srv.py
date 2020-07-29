# -*- coding: UTF-8 -*-

# todo 测试 NamedTuple 的迭代 Composition 的序列化，反序列化是否正确
# todo 获取服务器状态信息
import asyncio

from gs_framework.debug_utilities import start_stateful_srv
from gs_framework.framework_constants import SRV_CMD_START, SRV_CMD_STOP
from gs_framework.object_reference import get_stateful_srv_inst_reference
from gs_framework.platform_srv.dce_prop_dtypes import StatefulSrv, StatefulSrvSubscription, SrvWithPodName
from gs_framework.platform_srv.srv_dce_define import StatefulServiceCLCT
from gs_framework.platform_srv.stateful_srvs import PodPlatformEnv
from gs_framework.samples.author_data_extraction import ScholarDCESrv, ScholarPaperDCESrv, WebPageMonitorEnv, \
    ExtractAuthorFromWebPageEnv, SyncAuthorInfoFromGoogleEnv
from gs_framework.samples.debug_srvs import WritePlatformDataEnv
from gs_framework.samples.guess_number import GuessNumberAgent, GuessNumberGameEnv
from gs_framework.utilities import get_installed_packages, get_k8s_stateful_set_pod_id
import logging
logger = logging.getLogger(__name__)


async def run_stateful_in_platform():
    # 启动平台服务
    # await start_stateful_srv(StatefulSrv(INST(PodPlatformEnv, pod_name=get_pod_name())))

    # 启动模拟添加 srv 的服务
    debug_add_srv = await start_stateful_srv(StatefulSrv(INST_CREATION_DATA(WritePlatformDataEnv)))

    srv_cmd = SRV_CMD_START

    srv1 = StatefulSrv(INST_CREATION_DATA(GuessNumberGameEnv),
                       srv_subscription=[StatefulSrvSubscription("agent",
                                                                 INST_CREATION_DATA(GuessNumberAgent))]
                       )

    await debug_add_srv.update_variable_from_external("service_to_add",
                                                      SrvWithPodName(stateful_srv=srv1, pod_name=[get_k8s_stateful_set_pod_id()], cmd=srv_cmd))

    srv2 = StatefulSrv(INST_CREATION_DATA(GuessNumberAgent),
                       srv_subscription=[StatefulSrvSubscription("env",
                                                                 INST_CREATION_DATA(GuessNumberGameEnv))]
                       )
    await debug_add_srv.update_variable_from_external("service_to_add",
                                                      SrvWithPodName(stateful_srv=srv2, pod_name=[get_k8s_stateful_set_pod_id()], cmd=srv_cmd))


    # DCE 的例子
    case2_cmd = SRV_CMD_STOP

    await debug_add_srv.update_variable_from_external("service_to_add",
                                                      SrvWithPodName(stateless_srv=INST_CREATION_DATA(ScholarDCESrv),
                                                           pod_name=[get_k8s_stateful_set_pod_id()], cmd=case2_cmd))

    await debug_add_srv.update_variable_from_external("service_to_add",
                                                      SrvWithPodName(stateless_srv=INST_CREATION_DATA(ScholarPaperDCESrv),
                                                           pod_name=[get_k8s_stateful_set_pod_id()], cmd=case2_cmd))

    await debug_add_srv.update_variable_from_external("service_to_add",
                                                      SrvWithPodName(stateful_srv=StatefulSrv(INST_CREATION_DATA(WebPageMonitorEnv)),
                                                           pod_name=[get_k8s_stateful_set_pod_id()], cmd=case2_cmd))

    srv3 = StatefulSrv(INST_CREATION_DATA(ExtractAuthorFromWebPageEnv),
                       srv_subscription=[StatefulSrvSubscription("WebPageEnv",
                                                                 INST_CREATION_DATA(WebPageMonitorEnv))]
                       )

    await debug_add_srv.update_variable_from_external("service_to_add",
                                                      SrvWithPodName(stateful_srv=srv3, pod_name=[get_k8s_stateful_set_pod_id()],
                                                           cmd=case2_cmd))

    await debug_add_srv.update_variable_from_external("service_to_add",
                                                      SrvWithPodName(stateful_srv=StatefulSrv(INST_CREATION_DATA(SyncAuthorInfoFromGoogleEnv)),
                                                           pod_name=[get_k8s_stateful_set_pod_id()], cmd=case2_cmd))

    await asyncio.sleep(1.0)


if __name__ == "__main__":
    cls = StatefulServiceCLCT
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_stateful_in_platform())
