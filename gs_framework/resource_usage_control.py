import asyncio
import time
from typing import Callable, Awaitable, Iterable, Dict, Any, Optional

from gs_framework.utilities import ensure_await


FUNC_TEST_USAGE = Callable[[], Optional[Awaitable[None]]]


class UsageControlData:

    __slots__ = ["_resource", "_it_wait_seconds", "_wait_seconds", "_start_control_seconds"]

    # reuse the last item of wait_seconds_in_turn
    def __init__(self, resource: Any, wait_seconds_in_turn: Iterable[int]):
        super().__init__()
        self._resource = resource
        self._it_wait_seconds = wait_seconds_in_turn.__iter__()
        self._wait_seconds = next(self._it_wait_seconds)
        self._start_control_seconds = int(time.time())

    async def stop_usage(self, cb_test_usage: FUNC_TEST_USAGE):
        print(f"Stop {self._resource} from using for {self._wait_seconds} seconds")
        await asyncio.sleep(self._wait_seconds)
        print(f"{self._resource} has been stopped from using for {self._wait_seconds} seconds. Try it.")

        try:
            self._wait_seconds = next(self._it_wait_seconds)
        except StopIteration:
            pass  # leave self._wait_seconds with the last value

        asyncio.ensure_future(ensure_await(cb_test_usage()))

    def __repr__(self):
        return f"{self._resource} {self.controlled_seconds} seconds"

    @property
    def controlled_seconds(self):
        return int(time.time()) - self._start_control_seconds


class ResourceUsageControl:

    __slots__ = ["_wait_seconds_in_turn", "_usage_control_data_by_resource"]

    def __init__(self, wait_seconds_in_turn: Iterable[int]):
        super().__init__()
        self._wait_seconds_in_turn = wait_seconds_in_turn
        self._usage_control_data_by_resource: Dict[Any, UsageControlData] = dict()

    async def stop_usage(self, resource: Any, cb_test_usage: FUNC_TEST_USAGE):
        usage_control_data = \
            self._usage_control_data_by_resource.setdefault(resource,
                                                            UsageControlData(resource, self._wait_seconds_in_turn))
        await usage_control_data.stop_usage(cb_test_usage)

    def resume_resource(self, resource: Any) -> bool:
        return self._usage_control_data_by_resource.pop(resource, None) is not None

    def get_usage_control_repr(self):
        controlled_resources = sorted(self._usage_control_data_by_resource.values(),
                                      key=lambda r: r.controlled_seconds, reverse=True)
        return f"{len(self._usage_control_data_by_resource)} resources are in control: " \
               f"{' | '.join(map(repr, controlled_resources))}"
