import logging
import asyncio
import random
import sys
import time
from asyncio.transports import SubprocessTransport
from collections import OrderedDict
from typing import NamedTuple, Callable, Dict, Awaitable, Optional, Union, Tuple, TypeVar, Generic, Set, Iterable

from gs_framework.pool import Pool
from gs_framework.resource_usage_control import ResourceUsageControl
from gs_framework.utilities import generate_uuid, ensure_await


logger = logging.getLogger(__name__)


class AsyncSubProcessProtocol(asyncio.SubprocessProtocol):
    def __init__(self):
        super().__init__()
        self.exit_future = asyncio.Future()

    def pipe_data_received(self, fd, data):
        data_str = data.decode('ascii').rstrip()
        if fd == 2:
            print(data_str, file=sys.stderr)
        else:
            print(data_str)

    def process_exited(self):
        self.exit_future.set_result(True)


class SubProcessExecInfo(NamedTuple):
    transport: SubprocessTransport
    protocol: AsyncSubProcessProtocol

    def kill(self):
        self.transport.kill()

    @property
    def exit_future(self):
        return self.protocol.exit_future


async def run_sub_process(code: str, protocol_factory: Callable[[], AsyncSubProcessProtocol]) -> SubProcessExecInfo:
    sub_process = asyncio.get_event_loop().subprocess_exec(protocol_factory, sys.executable, "-c", code,
                                                           stdin=None, stderr=None)
    sub_process_transport, sub_process_protocol = await sub_process
    return SubProcessExecInfo(sub_process_transport, sub_process_protocol)


POOL_ITEM_TYPE = TypeVar('POOL_ITEM_TYPE')
TASK_DATA_TYPE = TypeVar('TASK_DATA_TYPE')

random.seed()


class TaskRetry:

    __slots__ = ["_max_retry", "_retry_times", "_next_run_time_in_seconds", "_retry_interval_in_seconds",
                 "_wait_interval_seconds"]

    def __init__(self, max_retry: int, retry_interval_in_seconds: int):
        super().__init__()
        self._max_retry: int = max_retry
        self._retry_times: int = 0
        self._next_run_time_in_seconds: int = None
        self._retry_interval_in_seconds: int = retry_interval_in_seconds
        self._wait_interval_seconds: int = None

    def mark_retry(self) -> Tuple[bool, int]:
        if self._retry_times >= self._max_retry:
            self._wait_interval_seconds = None
            return False, 0
        else:
            self._retry_times = self._retry_times + 1
            now_in_seconds = int(time.time())
            wait_interval_seconds = self._retry_times * self._retry_interval_in_seconds + \
                random.randint(-1 * self._retry_interval_in_seconds, self._retry_interval_in_seconds) / 2
            self._next_run_time_in_seconds = now_in_seconds + wait_interval_seconds
            self._wait_interval_seconds = wait_interval_seconds
            return True, wait_interval_seconds

    async def wait_for_run(self):
        if self._next_run_time_in_seconds is not None:
            now_in_seconds = int(time.time())
            wait_seconds = max(0, self._next_run_time_in_seconds - now_in_seconds)
            if wait_seconds > 0:
                await asyncio.sleep(wait_seconds)
            self._wait_interval_seconds = None

    def __repr__(self) -> str:
        return f"have retried {self._retry_times} times" if self._wait_interval_seconds is None else \
            f"retry NO. {self._retry_times} time in at least {self._wait_interval_seconds} seconds"


class SubProcessExecutor(Generic[POOL_ITEM_TYPE, TASK_DATA_TYPE]):

    FUNC_CREATE_SUB_PROCESS = Callable[[POOL_ITEM_TYPE], Awaitable[SubProcessExecInfo]]
    TASK_ID = str
    FUNC_TASK_PROCESS_CALLBACK = Callable[[TASK_ID, SubProcessExecInfo, POOL_ITEM_TYPE, TASK_DATA_TYPE, TaskRetry],
                                          Union[Awaitable[None], None]]

    FUNC_ASYNC_TASK_4_PROCESS = Callable[[POOL_ITEM_TYPE], Awaitable[None]]
    TASK_WAITING_DATA = Tuple[FUNC_ASYNC_TASK_4_PROCESS, TASK_DATA_TYPE, TaskRetry]
    TASK_RUNNING_DATA = Tuple[SubProcessExecInfo, POOL_ITEM_TYPE, TASK_DATA_TYPE, TaskRetry]

    EXIT_CODE_4_STOP_RESOURCE_USAGE = 20

    def __init__(self, pool: Pool[POOL_ITEM_TYPE]):
        super().__init__()
        self._pool = pool
        self._account_usage_control = ResourceUsageControl((3600 * 8, 3600 * 2, 3600))
        self._waiting_queue: Dict[SubProcessExecutor.TASK_ID, SubProcessExecutor.TASK_WAITING_DATA] = OrderedDict()
        self._running_processes: Dict[SubProcessExecutor.TASK_ID, SubProcessExecutor.TASK_RUNNING_DATA] = OrderedDict()

    # return task id
    def submit(self, func_create_sub_process: FUNC_CREATE_SUB_PROCESS, task_data: TASK_DATA_TYPE, time_out_seconds: int,
               cb_on_process_created: Optional[FUNC_TASK_PROCESS_CALLBACK] = None,
               cb_on_process_ended: Optional[FUNC_TASK_PROCESS_CALLBACK] = None,
               cb_on_process_timeout: Optional[FUNC_TASK_PROCESS_CALLBACK] = None) -> TASK_ID:

        waiting_queue = self._waiting_queue
        running_processes = self._running_processes
        pool = self._pool
        account_usage_control = self._account_usage_control

        task_uuid: SubProcessExecutor.TASK_ID = generate_uuid()
        task_retry = TaskRetry(max_retry=10, retry_interval_in_seconds=60 * 2)

        def next_waiting_task(obj_exec: POOL_ITEM_TYPE):
            try:
                _, (next_task, *_) = waiting_queue.popitem(last=False)
                asyncio.ensure_future(next_task(obj_exec))
            except KeyError:
                pool.return_object(obj_exec)
                logger.info(f"{pool.get_num_active()} active items in pool: {pool.get_active_items()}, "
                            f"{account_usage_control.get_usage_control_repr()}")

        async def async_task(obj_exec: POOL_ITEM_TYPE):
            stop_resource_usage = False

            try:
                sub_process_exec_info: SubProcessExecInfo = await func_create_sub_process(obj_exec)
                running_processes[task_uuid] = (sub_process_exec_info, obj_exec, task_data, task_retry)

                logger.info(f"{len(running_processes)} tasks are running, {len(waiting_queue)} tasks in queue,"
                            f" {self._account_usage_control.get_usage_control_repr()}")

                if cb_on_process_created:
                    await ensure_await(cb_on_process_created(task_uuid, sub_process_exec_info, obj_exec, task_data,
                                                             task_retry))

                try:
                    await asyncio.wait_for(sub_process_exec_info.exit_future, timeout=time_out_seconds)

                    exit_code = sub_process_exec_info.transport.get_returncode()
                    # 1 means terminated because of exception,  0 means end normally, 9 means be killed
                    if 1 == exit_code:
                        # the process terminates with an exception
                        ok_2_retry, wait_interval_seconds = task_retry.mark_retry()
                        if ok_2_retry:
                            async def async_retry_task(next_obj_exec: POOL_ITEM_TYPE):
                                await task_retry.wait_for_run()
                                await async_task(next_obj_exec)

                            assert task_uuid not in waiting_queue
                            waiting_queue[task_uuid] = (async_retry_task, task_data, task_retry)
                    elif SubProcessExecutor.EXIT_CODE_4_STOP_RESOURCE_USAGE == exit_code:
                        stop_resource_usage = True

                        print(f"{obj_exec} will be dropped from pool")
                        remove_result = pool.remove_object(obj_exec)
                        assert remove_result

                        def test_account_usage():
                            print(f"add {obj_exec} back to pool")
                            add_result = pool.add_object(obj_exec)
                            assert add_result
                            next_waiting_task(pool.borrow_object())

                        asyncio.ensure_future(account_usage_control.stop_usage(obj_exec, test_account_usage))

                        assert task_uuid not in waiting_queue
                        waiting_queue[task_uuid] = (async_task, task_data, task_retry)
                    elif 0 == exit_code:
                        account_usage_control.resume_resource(obj_exec)
                    elif -9 == exit_code:
                        # the process is killed. leave the resource in pool but do not cancel resource usage control
                        pass
                    else:
                        # stop the process immediately
                        raise RuntimeError(f"Unexpected sub process exit code: {exit_code}")

                    if cb_on_process_ended:
                        await ensure_await(cb_on_process_ended(task_uuid, sub_process_exec_info, obj_exec, task_data,
                                                               task_retry))
                except asyncio.TimeoutError:
                    try:
                        sub_process_exec_info.kill()
                    except Exception:  # in case the process has gone
                        pass
                    if cb_on_process_timeout:
                        await ensure_await(cb_on_process_timeout(task_uuid, sub_process_exec_info, obj_exec, task_data,
                                                                 task_retry))
            finally:
                running_processes.pop(task_uuid, None)
                if stop_resource_usage:
                    try:
                        next_obj_available: POOL_ITEM_TYPE = self._pool.borrow_object()
                    except RuntimeError:
                        next_obj_available = None

                    if next_obj_available is not None:
                        next_waiting_task(next_obj_available)
                else:
                    next_waiting_task(obj_exec)

        try:
            obj_available: POOL_ITEM_TYPE = self._pool.borrow_object()
            asyncio.ensure_future(async_task(obj_available))
        except RuntimeError:
            waiting_queue[task_uuid] = (async_task, task_data, task_retry)

        return task_uuid

    async def cancel_task(self, task_id: TASK_ID, cb_on_process_cancelled: Optional[FUNC_TASK_PROCESS_CALLBACK] = None)\
            -> bool:
        task_waiting_data: SubProcessExecutor.TASK_WAITING_DATA = self._waiting_queue.pop(task_id, None)
        if task_waiting_data is not None:
            if cb_on_process_cancelled:
                _, task_data, task_retry = task_waiting_data
                await ensure_await(cb_on_process_cancelled(task_id, None, None, task_data, task_retry))
            return True
        else:
            task_running_data: SubProcessExecutor.TASK_RUNNING_DATA = self._running_processes.get(task_id, None)
            if task_running_data is not None:
                sub_process_exec_info, obj_exec, task_data, task_retry = task_running_data
                try:
                    sub_process_exec_info.kill()
                except Exception:  # in case the process has gone
                    pass
                if cb_on_process_cancelled:
                    await ensure_await(cb_on_process_cancelled(task_id, sub_process_exec_info, obj_exec, task_data,
                                                               task_retry))
                return True
            else:
                return False


ID_TYPE = TypeVar('ID_TYPE')


class IDGrouping(Generic[ID_TYPE]):

    EMPTY_IDS = ()

    def __init__(self):
        self._ids_by_group: Dict[str, Set[ID_TYPE]] = dict()

    def add_id_2_group(self, id_: ID_TYPE, group: str):
        self._ids_by_group.setdefault(group, set()).add(id_)

    def drop_id(self, id_: ID_TYPE, group: str):
        ids_of_group: Set[ID_TYPE] = self._ids_by_group.get(group, None)
        if ids_of_group is not None:
            ids_of_group.discard(id_)
            if 0 == len(ids_of_group):
                self._ids_by_group.pop(group, None)

    def __getitem__(self, group: str) -> Iterable[ID_TYPE]:
        return self._ids_by_group.get(group, IDGrouping.EMPTY_IDS)
