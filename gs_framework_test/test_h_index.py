# -*- coding: UTF-8 -*-
"""
Google Scholar + H-Index 的例子
"""
import asyncio
import logging

import faust

from gs_framework.samples.author_data_extraction import WebPageMonitorEnv, ExtractAuthorFromWebPageEnv, ScholarService, \
    SyncGoogleAuthorInfoEnv

logger = logging.getLogger(__name__)


async def start_hindex():
    topic_define = faust.types.TP("martin-gs_framework_test-hindex", 1)

    logger.info(f"----------- start WebPageMonitorEnv -------------")
    web_page_env = WebPageMonitorEnv()
    web_page_env.bind(topic_define=topic_define)
    await web_page_env.start()

    logger.info(f"----------- start ExtractAuthorFromWebPageEnv -------------")
    extract_author_env = ExtractAuthorFromWebPageEnv()
    extract_author_env.bind(topic_define=topic_define)
    extract_author_env.web_page_env.bind(web_page_env.pk, topic_define=topic_define)
    extract_author_env.entity_stream.bind(topic_define)
    await extract_author_env.start()

    logger.info(f"----------- start ScholarService -------------")
    scholar_service = ScholarService()
    scholar_service.entity_stream.bind(topic_define)
    await scholar_service.start()

    logger.info(f"----------- start SyncGoogleAuthorInfoEnv -------------")
    sync_google_author_info_env = SyncGoogleAuthorInfoEnv()
    sync_google_author_info_env.bind(topic_define=topic_define)
    sync_google_author_info_env.entity_stream.bind(topic_define)
    await sync_google_author_info_env.start()

    logger.info(f"----------- 启动完成 -------------")
    logger.info(f"----------- kick off -------------")
    web_page_env.active.VALUE = 1  # framework 修改的 variable
    web_page_env.commit_state_var_changes()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(start_hindex())
    loop.run_forever()
