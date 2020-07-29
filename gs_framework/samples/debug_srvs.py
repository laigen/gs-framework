# -*- coding: UTF-8 -*-
"""
适用于 Debug 的一些 Stateful / Stateless Service
"""
from gs_framework.common_prop_dtypes import GlobalUniqueInst, StringT, StringSet
from gs_framework.decorators import init_actions, actionable
from gs_framework.object_reference import DCERef, tuple_2_inst_ref
from gs_framework.platform_srv.dce_prop_dtypes import SrvWithPodName
from gs_framework.platform_srv.srv_dce_define import StatefulServiceCLCT, StatelessServiceCLCT, StatefulRuntimeInfo, \
    StatelessAssignedInfo
from gs_framework.state_variable import StateVariable
from gs_framework.activatable_stateful_service import Env
from gs_framework.utilities import get_k8s_stateful_set_pod_id


class WritePlatformDataEnv(Env):
    """用于辅助往 Platform 相关的 DCE 上写输入的 Srv"""

    @init_actions()
    def __init__(self):
        super().__init__()

        self.stateful_srv_clct_ref = DCERef(StatefulServiceCLCT, write=True, read=False)
        self.stateless_srv_clct_ref = DCERef(StatelessServiceCLCT, write=True, read=False)

        self.service_to_add = StateVariable(value_cls=SrvWithPodName, default_val=None, help="需要添加的service信息")

    @actionable(variables=["service_to_add"])
    async def a_on_service_to_add(self, v: SrvWithPodName):
        if v.stateful_srv is not None:
            inst_ref = tuple_2_inst_ref(v.stateful_srv.srv_inst)  # 生成一个 ref 对象，用于计算 hash gid
            pk_obj = GlobalUniqueInst(inst_ref.ref_hash_gid)
            await self.stateful_srv_clct_ref.add_entity(pk_obj)

            await self.stateful_srv_clct_ref.upsert_entity_props(pk_obj, {StatefulRuntimeInfo.inst: v.stateful_srv})
            await self.stateful_srv_clct_ref.upsert_entity_props(pk_obj, {
                StatefulRuntimeInfo.assigned_pod_name: StringT(v.pod_name[0])})
            if v.cmd:
                await self.stateful_srv_clct_ref.upsert_entity_props(pk_obj, {
                    StatefulRuntimeInfo.cmd_on_srv: StringT(v.cmd)})
        elif v.stateless_srv is not None:
            pk_obj = v.stateless_srv
            await self.stateless_srv_clct_ref.add_entity(pk_obj)
            await self.stateless_srv_clct_ref.upsert_entity_props(pk_obj, {
                StatelessAssignedInfo.assigned_pod_names: StringSet({v.pod_name[0]})})
            if v.cmd:
                await self.stateless_srv_clct_ref.upsert_entity_props(pk_obj, {
                    StatelessAssignedInfo.cmd_on_srv: StringT(v.cmd)})
