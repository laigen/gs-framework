# -*- coding: UTF-8 -*-
import hashlib
import inspect
import logging
import pickle
import string
import itertools
from datetime import datetime
from random import choice, randrange, random

import pyarrow
import gzip
import signal
import sys

from typing import Tuple, Optional, Callable, Iterable, Generator, Any, Type, Mapping

from .common_prop_dtypes import PyPackageSet, PyPackage

logger = logging.getLogger(__name__)

BIN_TYPE_PICKLE = 0x01
BIN_TYPE_PICKLE_GZIP = 0x02
BIN_TYPE_ARROW = 0x03


def _gzip_compress(bin: bytes) -> bytes:
    return gzip.compress(bin)


def _gzip_decompress(bin: bytes) -> bytes:
    return gzip.decompress(bin)


def _object_to_pickle_bytes(obj) -> bytes:
    return pickle.dumps(obj, protocol=4)


def _pickle_bytes_to_object(bin_data) -> Any:
    try:
        return pickle.loads(bin_data)
    except AttributeError:  # could be caused by class not exist
        return None
    except ModuleNotFoundError: # could be caused by module not exist
        return None


def _object_to_arrow_bytes(obj) -> bytes:
    return pyarrow.default_serialization_context().serialize(obj).to_buffer()


def _arrow_bytes_to_object(bin_data) -> Any:
    buf = pyarrow.py_buffer(bin_data)
    return pyarrow.default_serialization_context().deserialize(buf)


def _is_object_pickle_preferred(obj) -> bool:
    """
        Object 转成 bytes 的规则：
            1) object 有属性 "__getstate__" / "__setstate__" 时用 pickles , len(x) > 2k  时压缩

            2) inspect ismodule, isclass, ismethod, isfunction, isgeneratorfunction, isgenerator,
                iscoroutinefunction, iscoroutine, istraceback, iscode, isbuiltin 成立时，用 pickles
                len(x) > 2k  时压缩

            3) str 类型的，转成 pickle，为了能够压缩

            4) sys.getsizeof(x) <= 2k 时 pickles , > 2k 时 arrow

        NOTE: 发现有一个平衡点，在 2k 以下的数据， arrow 得到的字节数大约是 pickle 的五到十倍
        """
    if obj is None:
        return True
    if hasattr(obj, "__getstate__") and hasattr(obj, "__setstate__"):
        return True
    if inspect.ismodule(obj) or inspect.isclass(obj) or inspect.ismethod(obj) or inspect.isfunction(obj) \
            or inspect.isgeneratorfunction(obj) or inspect.isgenerator(obj) or inspect.iscoroutinefunction(obj) \
            or inspect.iscoroutine(obj) or inspect.istraceback(obj) or inspect.iscode(obj) or inspect.isbuiltin(obj):
        return True
    if isinstance(obj, str):
        return True
    if sys.getsizeof(obj) <= 2048:
        return True
    return False


def object_2_bytes(obj) -> bytes:
    """object 转成 bytes对象，bytes[0]表示了转换的方式"""
    use_pickle = _is_object_pickle_preferred(obj)
    if use_pickle:
        ret_bytes = _object_to_pickle_bytes(obj)
        if len(ret_bytes) <= 2048:
            return bytes([BIN_TYPE_PICKLE]) + ret_bytes
        else:
            return bytes([BIN_TYPE_PICKLE_GZIP]) + _gzip_compress(ret_bytes)
    else:
        return bytes([BIN_TYPE_ARROW]) + _object_to_arrow_bytes(obj)


def bytes_2_object(bin_data) -> Any:
    if bin_data is None:
        return None

    assert len(bin_data) > 1
    serialization_method = bin_data[0]
    if serialization_method == BIN_TYPE_ARROW:
        return _arrow_bytes_to_object(bin_data[1:])
    elif serialization_method == BIN_TYPE_PICKLE:
        return _pickle_bytes_to_object(bin_data[1:])
    elif serialization_method == BIN_TYPE_PICKLE_GZIP:
        return _pickle_bytes_to_object(_gzip_decompress(bin_data[1:]))
    else:
        raise RuntimeError(f"Unknown binary type: {serialization_method}")


def get_installed_packages() -> PyPackageSet:
    import os
    import re
    ls_rlt = list()
    set_pkgs = set()

    with os.popen("pip list", "r") as console_f:
        cmd_rlt = console_f.read()
        pkgs = cmd_rlt.split("\n")[2:]
        pattern = re.compile(r"(\S*)\s*(\S*)")
        for pkg_line in pkgs:
            # print(pkg_line)
            tmp = pattern.match(pkg_line).groups()
            if len(tmp) == 2 and tmp[0]:
                set_pkgs.add(PyPackage(pkg_name=tmp[0], pkg_ver=tmp[1]))
            elif len(tmp) == 1 and tmp[0]:
                set_pkgs.add(PyPackage(pkg_name=tmp[0], pkg_ver=None))
    return PyPackageSet(packages=set_pkgs)


def get_random_str(min_length=3, max_length=20) -> str:
    return ''.join(choice(string.ascii_uppercase + string.digits) for _ in range(randrange(min_length, max_length)))


def get_random_int(min_v=3, max_v=10000) -> int:
    if min_v > max_v:
        return randrange(max_v, min_v)
    elif min_v < max_v:
        return randrange(min_v, max_v)
    else:
        return min_v


def get_random_float() -> float:
    return random()


def generate_uuid() -> str:
    import uuid
    return uuid.uuid4().hex.upper()


def partition(items: Iterable, predicate: Callable[[object], bool]) -> Tuple[Generator, Generator]:
    a, b = itertools.tee((predicate(item), item) for item in items)
    return ((item for pred, item in a if not pred),
            (item for pred, item in b if pred))


def md5_str(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).digest().hex().upper()


# def get_k8s_stateful_set_pod_id() -> str:
#     import os
#     # pod 上的脚本，会设置 stateful set 的环境变量
#     if K8S_STATEFUL_SET_POD_ID in os.environ:
#         return os.environ[K8S_STATEFUL_SET_POD_ID]
#     else: # debug 的情况下，用 nodename 代替 os name
#         return os.uname()[1]


def is_named_tuple(v) -> bool:
    return isinstance(v, tuple) and hasattr(v, "_fields")


def _named_tuple_hash_str(v) -> str:
    assert is_named_tuple(v)
    all_prop_values = dict()
    for prop_name in v._fields:
        prop_val = getattr(v, prop_name)
        if prop_val is not None:
            all_prop_values[prop_name] = _prop_not_none_value_to_hash_str(prop_val)
    props_str = ",".join([f"{key}={all_prop_values[key]}" for key in sorted(all_prop_values.keys())])
    str_to_hash = f"{v.__module__}.{v.__class__.__qualname__}({props_str})"
    return str_to_hash


def _prop_not_none_value_to_hash_str(v) -> str:
    """将值转成需要被 hash 的字符串"""
    if isinstance(v, str):
        return f"\"{v}\""
    elif isinstance(v, (int, float, datetime)):  # scalar
        return str(v)
    elif isinstance(v, dict):
        ls_val = []
        for (k, val) in v.items():
            if val is not None:
                ls_val.append(f"{k}:{_prop_not_none_value_to_hash_str(val)}")
        return "{" + ",".join(ls_val) + "}"
    elif is_named_tuple(v):
        return _named_tuple_hash_str(v)
    elif isinstance(v, Iterable):
        ls_val = []
        for item in v:
            if item is not None:
                ls_val.append(_prop_not_none_value_to_hash_str(item))
        return "[" + ",".join(ls_val) + "]"
    elif isinstance(v, type):
        return v.__module__ + "." + v.__qualname__
    else:
        raise Exception(f"Can't hashed value type, {type(v)} : {v}")


def namedtuple_hash_gid(v: tuple) -> str:
    """
    计算 NamedTuple 的 Hash UUID 值

    Notes 需考虑以下几种情况发生时 NamedTuple 的 hash 值不变：
        1) NamedTuple 增加了一项内容，且缺省值为 None 时，计算的 hash gid 不会改变
    """
    str_to_hash = _named_tuple_hash_str(v)
    return md5_str(str_to_hash)


def func_2_bytes(f) -> bytes:
    """将 function 对象，连同 function body 一并序列化成 binary
        ALERT : function body 中如果调用了其他函数，而其他函数在反序列化端不存在的话，函数调用将会失败
    """
    # SEE https://medium.com/@emlynoregan/serialising-all-the-functions-in-python-cd880a63b591
    import dill
    return dill.dumps(f)


def bytes_2_func(bin: bytes):
    """将binary转回 function """
    import dill
    f = dill.loads(bin)
    return f


def reload_module(module_name: str):
    """重新加载 module ，适用于代码升级后，更新环境"""
    import sys
    import importlib
    if module_name not in sys.modules:
        importlib.import_module(module_name)
    importlib.reload(sys.modules[module_name])


def obj_2_log_str(v: object, max_length: int = 10) -> str:
    s = str(v)
    if len(s) < max_length:
        return s
    else:
        return f"{s[0:max_length-1]}...{len(s)}..."


# def find_obj_member_name_by_id(obj: Any, member_id: int) -> Optional[str]:
#     name_and_member = next(filter(lambda attr_name_and_value: id(attr_name_and_value[1]) == member_id,
#                                   map(lambda attr: (attr, getattr(obj, attr)), dir(obj))), None)
#     return None if name_and_member is None else name_and_member[0]
#
#
# def find_obj_member_name(obj: Any, member: Any) -> Optional[str]:
#     return find_obj_member_name_by_id(obj, id(member))
#
#
# def ensure_find_obj_member_name(obj: Type, member: Any) -> str:
#     name = find_obj_member_name(obj, member)
#     assert name is not None, f"{member} not found in class {obj}"
#     return name
#
#
# def ensure_find_obj_member_name_by_id(obj: Any, member_id: int) -> str:
#     name = find_obj_member_name_by_id(obj, member_id)
#     assert name is not None, f"member whose id is {member_id} not found in class {obj}"
#     return name


def get_item(mapping: Mapping, key) -> Tuple[bool, Any]:
    try:
        return True, mapping[key]
    except KeyError:
        return False, None


def iter_members_by_types(obj: Any, types) -> Iterable[Tuple[str, type]]:
    return filter(lambda name_and_member: isinstance(name_and_member[1], types),
                  map(lambda attr_name: (attr_name, getattr(obj, attr_name)), dir(obj)))


# Refer to https://cloud.google.com/blog/products/gcp/kubernetes-best-practices-terminating-with-grace
# Kubernetes waits for a specified time called the termination grace period. By default, this is 30 seconds
def install_terminate_handler(handler: Callable[[int, Any], None]):
    signals_nums = [signal.SIGINT, signal.SIGTERM]
    signal_and_original_handlers = list(zip(signals_nums, map(signal.getsignal, signals_nums)))

    def term_signal_handler(signum, stack_frame):
        try:
            handler(signum, stack_frame)
        except object:
            pass

        original_handler = next(filter(lambda signal_and_handler: signal_and_handler[1] == signum,
                                       signal_and_original_handlers), None)
        if original_handler is not None:
            original_handler = original_handler[1]
            if original_handler is None or signal.SIG_DFL == original_handler:
                sys.exit()
            elif signal.SIG_IGN != original_handler:
                original_handler(signum, stack_frame)
        else:
            sys.exit()  # this should not happen. Just write here in case

    for signal_num in signals_nums:
        signal.signal(signal_num, term_signal_handler)


def to_para(v) -> str:
    return 'None' if v is None else f"'{v}'" if isinstance(v, str) else f"{v}"


async def ensure_await(res):
    return await res if inspect.isawaitable(res) else res
