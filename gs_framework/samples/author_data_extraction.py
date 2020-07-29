# -*- coding: UTF-8 -*-
"""
Hello world 级的示例，示范搜索并得到 google scholar 中 author 的 h-index 等信息内容
"""
import logging
from typing import List, Any, Dict

from dataclasses import dataclass

from gs_framework.object_reference import ObjectRef
from gs_framework.service import StatelessService
from gs_framework.state_stream import ObjectStateStream
from gs_framework.state_var_change_dispatcher import state_var_change_handler, pick_one_change
from gs_framework.state_variable import StateVariable
from gs_framework.activatable_stateful_service import Env, Activatable
from gs_framework.stateful_object import State, read_stateful_object, \
    MessageAsStateReader, create_stateful_object
from gs_framework.handler import StatefulObjectAndCommitStream

logger = logging.getLogger(__name__)


# Logic overview:
# WebPageMonitorEnv发送html变化，
# ExtractAuthorFromWebPageEnv收到html从中提取出author基本信息，
# author基本信息被ScholarService收到，增加一些derived信息后再发出去，
# 再被一个SyncAuthorInfoFromGoogleEnv收到, 处理这些derived信息

@dataclass
class PersonBasicInfo:
    """个人的最基本信息，比如 名字 """
    name: str = None


@dataclass
class ScholarInfo:
    """从 google scholar 页面上同步到的学者基本信息"""
    full_name: str = None
    person_description: str = None
    research_domain: List[str] = None
    citations: int = None
    citations_last_5_years: int = None
    h_index: int = None
    h_index_last_5_years: int = None
    i10_index: int = None
    i10_index_last_5_years: int = None


class URLBasedPerson(State):

    url = StateVariable(dtype=str, default_val=None, help="url identify the author")


class Author(URLBasedPerson):

    class Google(State):

        basic_info = StateVariable(dtype=PersonBasicInfo, default_val=None, help="basic person info from Google")
        scholar_info = StateVariable(dtype=ScholarInfo, default_val=None, help="scholar info from google scholar")

    class Yahoo(State):
        basic_info = StateVariable(dtype=PersonBasicInfo, default_val=None, help="basic person info from Yahoo")


class WebPageMonitorEnv(Env):
    """该 Env 模拟监控用户浏览器上的 HTML 内容"""

    html = StateVariable(dtype=str, default_val=None, help="网页的html内容")

    @state_var_change_handler(state_vars=[Activatable.active])
    @pick_one_change
    def on_active_flag_change(self, state_var_owner_pk: Any, state_var_name: str, state_var_value: Any):
        if state_var_value == 1:  # 标记 active 当做是 start new episode，后续可以单独做一个 episode 的 variable
            self.html.VALUE = "dummy"
            logger.info(f"[Env] {self.__class__.__qualname__} started, send out html value {self.html.VALUE}")


class ExtractAuthorFromWebPageEnv(Env):
    """该 Env 用于分析 WebPage 中有那些 author 对象，往  ScholarDistCollection 中写入这些对象"""

    web_page_env = ObjectRef.bind_at_runtime()
    entity_stream = ObjectStateStream.bind_at_runtime()

    @state_var_change_handler(state_vars=[WebPageMonitorEnv.html], state_var_source=web_page_env)
    @pick_one_change
    async def on_page_html(self, state_var_owner_pk: Any, state_var_name: str, state_var_value: Any):
        """得到Html的内容，抽取 author 的数据，写入 ScholarDistCollection 中"""
        # NOTE : 先 hardcode 创建 固定的 author ，下一步改为从 Html 提取该信息
        url = "https://scholar.google.com/citations?user=DMKfNFMAAAAJ"

        url_based_person = create_stateful_object(url, URLBasedPerson)
        url_based_person[URLBasedPerson.url].VALUE = url
        logger.info(f"[Env] {self.__class__.__qualname__} send out URLBasedPerson with url"
                    f" {url_based_person[URLBasedPerson.url].VALUE}")
        return StatefulObjectAndCommitStream(url_based_person, self.entity_stream)


class ScholarService(StatelessService):

    entity_stream = ObjectStateStream.bind_at_runtime()

    @state_var_change_handler(state_vars=URLBasedPerson.url, state_var_source=entity_stream)
    async def on_person_url(self, state_var_owner_pk: Any, state_vars: Dict[str, Any],
                            triggering_state_var_names: List[str]):
        author = read_stateful_object(state_var_owner_pk, Author,
                                      MessageAsStateReader(pk=state_var_owner_pk, state_vars=state_vars))
        extract_h_index = 25  # extract_html( author[URLBasedPerson.url].VALUE , j_query_selector )
        author[Author.Google.scholar_info].VALUE = ScholarInfo(h_index=extract_h_index)

        logger.info(f"[StatelessService] {self.__class__.__qualname__} send out Author for "
                    f"{author[URLBasedPerson.url].VALUE} with {author[Author.Google.scholar_info].VALUE}")
        return StatefulObjectAndCommitStream(author, self.entity_stream)


class SyncGoogleAuthorInfoEnv(Env):

    entity_stream = ObjectStateStream.bind_at_runtime()

    @state_var_change_handler(state_vars=Author.Google.scholar_info, state_var_source=entity_stream)
    async def on_scholar_info(self, state_var_owner_pk: Any, state_vars: Dict[str, Any],
                              triggering_state_var_names: List[str]):
        author = read_stateful_object(state_var_owner_pk, Author,
                                      MessageAsStateReader(pk=state_var_owner_pk, state_vars=state_vars))
        logger.info(f"[Env] {self.__class__.__qualname__} received author with pk {state_var_owner_pk}, "
                    f"scholar info {author[Author.Google.scholar_info].VALUE}")
