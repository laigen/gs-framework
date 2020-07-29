import functools
import inspect
import logging
import pickle
from datetime import datetime, date
from functools import wraps
from typing import Iterable, Callable

from .utilities import md5_str

logger = logging.getLogger(__name__)


class HashCalculation:

    @staticmethod
    def value_to_hash_str(v) -> str:
        from .stateful_interfaces import PkMixin

        if isinstance(v, PkMixin):
            return HashCalculation.value_to_hash_str(v.pk)
        elif isinstance(v, type):
            return HashCalculation.value_to_hash_str(v.__qualname__)
        elif isinstance(v, set):
            return HashCalculation.value_to_hash_str(sorted(v))
        elif isinstance(v, dict):
            return HashCalculation.value_to_hash_str(sorted(v.items()))
        elif v is None or isinstance(v, (str, int, float, datetime, date, type)):
            return pickle.dumps(v, protocol=4).hex().upper()
        elif isinstance(v, Iterable):
            return "[" + ",".join(map(HashCalculation.value_to_hash_str, v)) + "]"
        elif hasattr(v, '__repr__'):
            return repr(v)
        else:
            raise Exception(f"Can't hash data type: {type(v)} : {v}")

    @staticmethod
    def calc_inst_hash(def_cls, *args, **kwargs) -> str:
        """ 计算某个成员函数调用的 hash 值，通常为 __init__ 函数，也可以用于 get_inst_stub() 这类的函数
        !!! 注意：*args 是去掉了 self 参数的内容 """

        def_cls_str = f"{def_cls.__module__}:{def_cls.__qualname__}"
        init_func = getattr(def_cls, "__init__")

        input_arg_names = inspect.getfullargspec(init_func).args
        if len(input_arg_names) > 0 and input_arg_names[0] == "self":
            input_arg_names.pop(0)

        all_para_values = dict()

        for (arg_name, arg_val) in zip(input_arg_names, args):
            all_para_values[arg_name] = arg_val

        for arg_name, arg_val in kwargs.items():
            all_para_values[arg_name] = arg_val

        # 填入 default 的参数值
        signature_data = inspect.signature(init_func)
        for arg_name in input_arg_names:
            if arg_name not in all_para_values:  # 是 default 的值
                all_para_values[arg_name] = signature_data.parameters[arg_name].default

        inputs_str = ",".join(map(lambda key_and_value:
                                  f"{key_and_value[0]}={HashCalculation.value_to_hash_str(key_and_value[1])}",
                                  sorted(all_para_values.items())))

        str_to_hash = f"{def_cls_str}({inputs_str})"

        logger.debug(f"inst to hash : {str_to_hash}")
        return md5_str(str_to_hash)
