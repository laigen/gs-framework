import inspect
from datetime import tzinfo
from typing import NamedTuple, Callable, Optional

from faust import App
from faust.types import AppT
from gs_framework.handler import process_handler_sync_result


class CrontabDetail(NamedTuple):
    format: str
    timezone: tzinfo


class CrontabHandler:

    ATTR_CRONTAB_DETAIL = "_crontab_detail"
    """decorator saves the crontab detail to decorated method with this attribute name, and code in this class read
    it out to schedule the crontab task. 
    The presence of this attribute also indicates whether this function is decorated by @crontab
    """

    @staticmethod
    def init_faust_crontabs(app: AppT, handlers_owner: object, on_handler_executed: Optional[Callable[[], None]]):

        name_and_method_and_crontab_details = \
            filter(lambda name_and_method_and_crontab_detail:
                   name_and_method_and_crontab_detail[2] is not None,
                   map(lambda name_and_method: (name_and_method[0], name_and_method[1],
                                                getattr(name_and_method[1],
                                                        CrontabHandler.ATTR_CRONTAB_DETAIL,
                                                        None)),
                       inspect.getmembers(handlers_owner, inspect.ismethod)))

        for name, method_obj, crontab_detail in name_and_method_and_crontab_details:
            async def crontab_method(*args, **kwargs):
                args_with_app_dropped = args
                if len(args) >= 1 and isinstance(args[-1], App):
                    args_with_app_dropped = args[0:len(args) - 1]

                res = await method_obj(*args_with_app_dropped, **kwargs)
                await process_handler_sync_result(res)

                if on_handler_executed is not None:
                    res = on_handler_executed()
                    if inspect.isawaitable(res):
                        await res

            app.crontab(cron_format=crontab_detail.format, timezone=crontab_detail.timezone)(crontab_method)


def crontab(cron_format: str, timezone: tzinfo = None):
    """
    decorator to define an async def function to be run at 指定时间点

    Parameters
    ----------
    cron_format : str
        see: http://www.nncron.ru/help/EN/working/cron-format.htm
    timezone
    """
    def crontab_decorator(func):
        assert inspect.iscoroutinefunction(
            func), f"crontab action '{func.__name__}' must be coroutine, code should be 'async def xxx'!"

        # @wraps(func)
        # async def func_wrapper(*args, **kwargs):
        #     # the crontab function can still accept multiple parameters having default values.
        #     # the default values can be changed when the function is called as a normal function
        #
        #     # faust app.crontab 会多传入一个 app 的参数，这里去掉该参数项
        #     args_with_app_dropped = args
        #     if len(args) >= 2 and isinstance(args[-1], App):
        #         args_with_app_dropped = args[0:len(args)-1]
        #
        #     rlt = await func(*args_with_app_dropped, **kwargs)
        #     return rlt
        #
        # setattr(func_wrapper, CrontabHandler.ATTR_CRONTAB_DETAIL, CrontabDetail(cron_format, timezone))
        return func

    return crontab_decorator
