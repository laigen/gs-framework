# -*- coding: UTF-8 -*-

"""
一些约定：
1) 允许有多个 pool， pool 通过 name 作为 unique 的信息
2) 一个 google account 只能在一个 pool 中

"""
import logging
import sys
import functools
import time
import asyncio

from typing import List, Dict, Any, Iterable, NamedTuple

from gs_framework.colab.colab_notebook import BackendNotAvailableError
from gs_framework.colab.webpage import WebPage

from gs_framework.pool import Pool
from gs_framework.state_stream import ObjectStateStream

from gs_framework.state_var_change_dispatcher import state_var_change_handler, pick_one_change

from gs_framework.colab.colab_related_states import ColabPoolEnvState, ColabPoolEnvStateQueryMessage, \
    ColabPoolEnvStateQueryResponseMessage

from gs_framework.activatable_stateful_service import Env, Activatable

from gs_framework.stateful_object import read_stateful_object, MessageAsStateReader, create_stateful_object
from gs_framework.sub_process_executor import AsyncSubProcessProtocol, SubProcessExecInfo, run_sub_process, \
    SubProcessExecutor, IDGrouping, TaskRetry
from gs_framework.utilities import to_para

logger = logging.getLogger(__name__)


def run_notebook_process(google_acct: str, notebook_id: str, chrome_debug_port: int, chrome_service_name: str):
    chrome_service_name = f"chrome-{google_acct.replace('.', '-dot-')}" if chrome_service_name is None \
        else chrome_service_name

    from gs_framework.colab.colab_notebook import ColabNotebook
    try:
        notebook_url = f"https://colab.research.google.com/drive/{notebook_id}"
        running_notebook = ColabNotebook(notebook_url, chrome_service_name, chrome_debug_port)
        running_notebook.prepare()

        # figure out who is running
        running_cell_index = running_notebook.get_current_running_cell_index()
        if running_cell_index is not None and running_cell_index > 0:
            cell_options = running_notebook.get_cell_options(running_cell_index)
            # if the running cell is mount google drive, stop it. else let it run
            if cell_options.mount_google_drive:
                print(f"{running_notebook.title} - Cell {running_cell_index} is mounting google drive, stop it")
                running_notebook.stop_cell(running_cell_index)
            else:
                print(f"{running_notebook.title} - wait for running cell {running_cell_index} with option {cell_options} to stop")

            running_notebook.wait_4_cell_stop(running_cell_index)
            print(f"{running_notebook.title} - Cell {running_cell_index} stopped")

            if cell_options.mount_google_drive:
                running_cell_index = running_cell_index - 1  # make code below rerun the cell

        elif 'Busy' == running_notebook.get_vm_status():
            # if no cell is running, and status is busy restart runtime
            print(f"{running_notebook.title} - VM is busy but no cell is running.")
            running_notebook.factory_reset_runtime()

        cell_index = 1 if running_cell_index is None else running_cell_index + 1
        while running_notebook.cell_exists(cell_index):
            running_notebook.run_cell(cell_index)
            cell_index = cell_index + 1

        print(f"{running_notebook.title} - run notebook done")

        # try:
        #     WebPage('about:blank', chrome_service_name, chrome_debug_port)
        # except Exception:
        #     pass
    except BackendNotAvailableError:
        print(f"{running_notebook.title} - backend not available")
        sys.exit(SubProcessExecutor.EXIT_CODE_4_STOP_RESOURCE_USAGE)


class RunColabNotebookProtocol(AsyncSubProcessProtocol):
    def __init__(self, google_account: str, notebook_id: str):
        super().__init__()
        self._google_account = google_account
        self._notebook_id = notebook_id

    def pipe_data_received(self, fd, data):
        output = f"{self._google_account} {self._notebook_id}: {data.decode('ascii').rstrip()}"
        if fd == 2:
            logger.error(output, file=sys.stderr)
        else:
            logger.info(output)


class NotebookRunData(NamedTuple):
    notebook_id: str
    task_group: str


class ColabPoolEnv(ColabPoolEnvState, Env):

    pool_env_state_stream: ObjectStateStream = ObjectStateStream.bind_at_runtime()

    def __init__(self, name: str, google_accounts: Iterable[str] = None):
        super().__init__()

        pool = Pool[str]()
        if google_accounts is not None:
            for google_account in google_accounts:
                pool.add_object(google_account)

        self._pool = pool
        self._executor: SubProcessExecutor[str, NotebookRunData] = SubProcessExecutor(pool)
        self._last_notebook_run_time_in_seconds: int = None
        self._task_id_grouping: IDGrouping[SubProcessExecutor.TASK_ID] = IDGrouping()

        def cb_post_start():
            # these state variables are all memory only.
            self.name.VALUE = name
            # to ensure these changes will be broadcasted when active
            self.mark_all_state_variable_changed()

        self._cb_post_start = cb_post_start

    @state_var_change_handler(state_vars=Activatable.active)
    @pick_one_change
    def on_active(self, state_var_owner_pk: Any, state_var_name: str, active: int):
        if active:
            logger.info(f"colab env pool {self.name.VALUE} is active")

    @state_var_change_handler(state_vars=ColabPoolEnvStateQueryMessage, state_var_source=pool_env_state_stream)
    async def on_query_pool_state_message(self, querying_pk: str, state_vars: Dict[str, Any],
                                          triggering_state_var_names: List[str]):
        query_message: ColabPoolEnvStateQueryMessage =\
            read_stateful_object(querying_pk, ColabPoolEnvStateQueryMessage,
                                 MessageAsStateReader(querying_pk, state_vars))
        print(f"on_query_pool_state_message: queried name: {query_message.name.VALUE}, my name: {self.name.VALUE}")
        if query_message.name.VALUE == self.name.VALUE:
            pool_state_response_message = create_stateful_object(self.pk, ColabPoolEnvStateQueryResponseMessage)
            pool_state_response_message[ColabPoolEnvStateQueryResponseMessage.name].VALUE = self.name.VALUE
            pool_state_response_message[ColabPoolEnvStateQueryResponseMessage.query_message_id].VALUE \
                = query_message.message_id.VALUE
            await pool_state_response_message.commit_state_var_changes(self.pool_env_state_stream)
            print(f"on_query_pool_state_message: poll name published")

    async def run_notebook(self, google_acct: str, notebook_id: str, chrome_debug_port: int,
                           chrome_service_name: str) -> SubProcessExecInfo:
        #  run next note book at least 3 minute after the previous one,
        #  to avoid proxy bandwidth jam for mount google drive
        run_note_book_interval_seconds = 3 * 60  # 10 change the interval for running on gcloud
        now_in_seconds = int(time.time())

        # print(f"{google_acct}: now_in_seconds: {now_in_seconds} last_run: {self._last_notebook_run_time_in_seconds}")

        wait_seconds = 0 if self._last_notebook_run_time_in_seconds is None \
            else max(0, self._last_notebook_run_time_in_seconds + run_note_book_interval_seconds - now_in_seconds)
        self._last_notebook_run_time_in_seconds = now_in_seconds + wait_seconds

        # print(f"{google_acct}: now_in_seconds: {now_in_seconds} wait_seconds: {wait_seconds} "
        #       f"last_run: {self._last_notebook_run_time_in_seconds}")

        if wait_seconds > 0:
            print(f"{google_acct} wait {wait_seconds} seconds before run notebook {notebook_id}")
            await asyncio.sleep(wait_seconds)
            print(f"{google_acct} waited {wait_seconds} seconds. start run notebook {notebook_id}")

        code = f"from gs_framework.colab.colab_pool_env import run_notebook_process; " \
               f"run_notebook_process({to_para(google_acct)}, {to_para(notebook_id)}, {to_para(chrome_debug_port)}, " \
               f"{to_para(chrome_service_name)})"
        return await run_sub_process(code, lambda: RunColabNotebookProtocol(google_acct, notebook_id))

    @staticmethod
    def _on_notebook_run(task_id: SubProcessExecutor.TASK_ID, sub_process_exec_info: SubProcessExecInfo,
                         google_acct: str, task_data: NotebookRunData, task_retry: TaskRetry):
        print(f"Run task {task_id} for notebook {task_data.notebook_id} of group {task_data.task_group} "
              f"with google account {google_acct}, {task_retry}")

    def _on_notebook_run_ended(self, task_id: SubProcessExecutor.TASK_ID, sub_process_exec_info: SubProcessExecInfo,
                               google_acct: str, task_data: NotebookRunData, task_retry: TaskRetry):
        task_group = task_data.task_group
        notebook_id = task_data.notebook_id
        print(f"Task {task_id} for notebook {notebook_id} of group {task_group} with google "
              f"account {google_acct} ended with exit code {sub_process_exec_info.transport.get_returncode()}, "
              f"{task_retry}")
        if task_group is not None:
            self._task_id_grouping.drop_id(task_id, task_group)

    def _on_notebook_run_timeout(self, task_id: SubProcessExecutor.TASK_ID, sub_process_exec_info: SubProcessExecInfo,
                                 google_acct: str, task_data: NotebookRunData, task_retry: TaskRetry):
        task_group = task_data.task_group
        notebook_id = task_data.notebook_id
        print(f"Task {task_id} for notebook {notebook_id} of group {task_group} with google account {google_acct} "
              f"timeout and ended with exit code {sub_process_exec_info.transport.get_returncode()}, {task_retry}")
        if task_group is not None:
            self._task_id_grouping.drop_id(task_id, task_group)

    def submit(self, notebook_id: str, task_group: str, chrome_debug_port: int = 9222, chrome_service_name: str = None,
               # 超过 16H colab 还在运行，可以认为没有正确的回收资源，清理 env，重新标记为 idle 等待新任务的指派
               time_out_seconds: int = 16 * 3600) -> SubProcessExecutor.TASK_ID:
        task_id = self._executor.submit(functools.partial(self.run_notebook, notebook_id=notebook_id,
                                                          chrome_debug_port=chrome_debug_port,
                                                          chrome_service_name=chrome_service_name),
                                        task_data=NotebookRunData(notebook_id=notebook_id, task_group=task_group),
                                        time_out_seconds=time_out_seconds,
                                        cb_on_process_created=ColabPoolEnv._on_notebook_run,
                                        cb_on_process_ended=self._on_notebook_run_ended,
                                        cb_on_process_timeout=self._on_notebook_run_timeout)
        print(f"Notebook {notebook_id} of group {task_group} submitted with task id {task_id}")
        if task_group is not None:
            self._task_id_grouping.add_id_2_group(task_id, task_group)
        return task_id

    def _on_notebook_run_cancelled(self, task_id: SubProcessExecutor.TASK_ID, sub_process_exec_info: SubProcessExecInfo,
                                   google_acct: str, task_data: NotebookRunData, task_retry: TaskRetry):
        task_group = task_data.task_group
        notebook_id = task_data.notebook_id
        if sub_process_exec_info:
            print(f"Task {task_id} for notebook {notebook_id} of group {task_group} with google account {google_acct} "
                  f"is cancelled and ended with exit code {sub_process_exec_info.transport.get_returncode()}, "
                  f"{task_retry}")
        else:
            print(f"Task {task_id} for notebook {notebook_id} of group {task_group} is cancelled before run, {task_retry}")
        if task_group is not None:
            self._task_id_grouping.drop_id(task_id, task_group)

    async def cancel_task(self, task_id: str) -> bool:
        cancelled = await self._executor.cancel_task(task_id,
                                                     cb_on_process_cancelled=self._on_notebook_run_cancelled)
        if not cancelled:
            print(f"Task {task_id} not found")
        return cancelled

    async def cancel_task_group(self, task_group: str):
        print(f"Canceling task group {task_group}")
        await asyncio.wait(map(lambda task_id: self.cancel_task(task_id), self._task_id_grouping[task_group]))
        print(f"Task group {task_group} cancelled")

    @staticmethod
    async def run_with_rpc_enabled(configuration: Any):
        pool_env = ColabPoolEnv(configuration.pool_name, configuration.init_google_accounts)
        pool_env.bind(topic_define=configuration.pool_env_status_topic)
        pool_env.pool_env_state_stream.bind(topic_define=configuration.pool_env_status_topic)

        from gs_framework.stream_rpc import RPCEndPointServiceUnit
        pool_env_rpc_service_unit = RPCEndPointServiceUnit(pool_env)
        pool_env_rpc_service_unit.rpc_callee_stream.bind(topic_define=configuration.pool_env_rpc_callee_topic)
        pool_env.add_service_units(pool_env_rpc_service_unit)

        await pool_env.start()
        logger.info(f"colab pool env {configuration.pool_name} with pk {pool_env.pk} started with rpc enabled")


# debugging entrance run in ide
# if __name__ == "__main__":
    # from gs_framework.colab.colab_pool_0_constants import Configuration
    #
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(ColabPoolEnv.run_with_rpc_enabled(Configuration))
    # loop.run_forever()

    # google_acct = 'laigen.test9'
    # notebook_id = '1KE-2ywxAaLwOBzBZfhpCJlGaDbhcc1GX'
    # chrome_debug_port = 9229
    # chrome_service_name = '127.0.0.1'
    # run_notebook_process(google_acct, notebook_id, chrome_debug_port, chrome_service_name)
