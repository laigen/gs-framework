import inspect
from typing import Callable, Optional

from faust import App
from faust.types import AppT
from gs_framework.handler import process_handler_sync_result


class TimerHandler:

    ATTR_TIMER_INTERVAL = "_timer_interval"
    """decorator saves the timer interval to decorated method with this attribute name, and code in this class read
    it out to schedule the timer. 
    The presence of this attribute also indicates whether this function is decorated by @timer
    """

    @staticmethod
    def init_faust_timers(app: AppT, handlers_owner: object, on_handler_executed: Optional[Callable[[], None]]):

        name_and_method_and_timer_intervals = \
            filter(lambda name_and_method_and_timer_interval:
                   name_and_method_and_timer_interval[2] is not None,
                   map(lambda name_and_method: (name_and_method[0], name_and_method[1],
                                                getattr(name_and_method[1],
                                                        TimerHandler.ATTR_TIMER_INTERVAL,
                                                        None)),
                       inspect.getmembers(handlers_owner, inspect.ismethod)))

        for name, method_obj, timer_interval in name_and_method_and_timer_intervals:
            async def timer_method(*args, **kwargs):
                args_with_app_dropped = args
                if len(args) >= 1 and isinstance(args[-1], App):
                    args_with_app_dropped = args[0:len(args) - 1]
                res = await method_obj(*args_with_app_dropped, **kwargs)
                await process_handler_sync_result(res)

                if on_handler_executed is not None:
                    res = on_handler_executed()
                    if inspect.isawaitable(res):
                        await res

            app.timer(interval=timer_interval)(timer_method)


def timer(interval: float):
    """
    decorator to define an async def function to be run at periodic intervals.

    Parameters
    ----------
    interval : float
        间隔频率，单位 秒
    """
    assert interval > 0.01, f"interval should be greater than 0.01 , now is {interval}"

    def timer_decorator(func):
        assert inspect.iscoroutinefunction(
            func), f"Timer action '{func.__name__}' must be coroutine, code should be 'async def xxx'!"

        # @wraps(func)
        # async def func_wrapper(*args, **kwargs):
        #     # the timer function can still accept multiple parameters having default values.
        #     # the default values can be changed when the function is called as a normal function
        #
        #     # faust app.timer 会多传入一个 app 的参数，这里去掉该参数项
        #     args_with_app_dropped = args
        #     if len(args) >= 2 and isinstance(args[-1], App):
        #         args_with_app_dropped = args[0:len(args) - 1]
        #
        #     rlt = await func(*args_with_app_dropped, **kwargs)
        #     return rlt

        setattr(func, TimerHandler.ATTR_TIMER_INTERVAL, interval)
        return func

    return timer_decorator
