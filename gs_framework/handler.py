import asyncio
import inspect
from typing import Iterable, Union, Callable, Any, Dict, List, Awaitable, NamedTuple

from gs_framework.state_stream import ObjectStateStream
from gs_framework.stateful_object import StatefulObject


class StatefulObjectAndCommitStream(NamedTuple):

    stateful_object: StatefulObject
    commit_stream: ObjectStateStream


CHANGE_HANDLER_SYNC_RESULT = Union[StatefulObjectAndCommitStream, Iterable[StatefulObjectAndCommitStream]]

CHANGE_HANDLER_RESULT = Union[CHANGE_HANDLER_SYNC_RESULT, Awaitable[CHANGE_HANDLER_SYNC_RESULT]]

FUNC_STATE_VAR_CHANGE_HANDLER = Callable[[Any, Dict[str, Any], List[str]], CHANGE_HANDLER_RESULT]
"""
The parameters are (from left to right):
state_var_owner_pk: pk of object the state vars belong to
state_vars: name and values for changed state variable
triggering_state_var_names: names of state variables be able to trigger this handler
"""


async def process_handler_sync_result(res: CHANGE_HANDLER_SYNC_RESULT):
    if inspect.isawaitable(res):
        res = await res

    if res is not None:
        if isinstance(res, StatefulObjectAndCommitStream):
            await res.stateful_object.commit_state_var_changes(res.commit_stream)
        elif isinstance(res, Iterable):
            async_tasks = list()
            for item in res:
                assert isinstance(item, StatefulObjectAndCommitStream)
                async_tasks.append(item.stateful_object.commit_state_var_changes(item.commit_stream))
            if len(async_tasks) > 0:
                await asyncio.wait(async_tasks, return_when=asyncio.ALL_COMPLETED)
        else:
            raise RuntimeError(f"Unexpected handler return type: {res.__class__.__qualname__}")