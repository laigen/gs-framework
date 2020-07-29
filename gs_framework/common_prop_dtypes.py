# -*- coding: UTF-8 -*-
"""
一些非常通用的 property 数据类型
"""
from datetime import datetime
from enum import Enum
from typing import Any, Callable, NamedTuple, Set, List, Dict


class BytesT(NamedTuple):
    v: bytes = None


class StringT(NamedTuple):
    v: str = None


class StringSet(NamedTuple):
    v: Set[str] = None


class FloatT(NamedTuple):
    v: float = None


class IntT(NamedTuple):
    v: int = None


class DatetimeT(NamedTuple):
    v: datetime = None


class GlobalUniqueInst(NamedTuple):
    """Global Unique Inst 的对象"""
    hash_gid: str


class ClassFullname(NamedTuple):
    """def 信息"""
    module_name: str = None
    qualname: str = None

    def to_class_obj(self):
        """
        允许 caller 在没有 import package 的情况下， load class object

        Examples
        --------
            ClassFullname("gs_framework.platform_srv.dce","A").to_class_obj
            get_class_obj("gs_framework.platform_srv.dce","A.B").to_class_obj
        """
        import sys
        import importlib

        module_name = self.module_name
        qual_name = self.qual_name

        if module_name not in sys.modules:
            importlib.import_module(module_name)
        if qual_name.find(".") < 0:
            return getattr(sys.modules[module_name], qual_name)
        else:
            cls_path = qual_name.split(".")
            rlt_obj = getattr(sys.modules[module_name], cls_path[0])
            for cls_name in cls_path[1:]:
                rlt_obj = getattr(rlt_obj, cls_name)
            return rlt_obj


class FunctionParameters(NamedTuple):
    """函数的参数信息"""
    args: List[Any] = None
    kwargs: Dict[str, Any] = None


class InstCreationData(NamedTuple):
    """一个 inst 创建的描述信息"""
    cls_name: ClassFullname = None
    init_paras: FunctionParameters = None

    @staticmethod
    def from_class_and_init_paras(def_cls, *args, **kwargs) -> 'InstCreationData':
        """创建一个 object inst 的 named tuple 的描述对象，可用于序列化之后的数据传递"""
        cls_name = ClassFullname(module_name=def_cls.__module__, qualname=def_cls.__qualname__)
        init_paras = FunctionParameters(args=args, kwargs=kwargs)
        return InstCreationData(cls_name=cls_name, init_paras=init_paras)

    def create_instance(self) -> Any:
        """生成一个 object inst 的实例对象"""
        cls = self.cls_name.to_class_obj()
        return cls(*self.init_paras.args, **self.init_paras.kwargs)

    def create_customized_instance(self, func_create_instance: Callable):
        cls = self.cls_name.to_class_obj()
        return func_create_instance(cls, *self.init_paras.args, **self.init_paras.kwargs)


class PyPackage(NamedTuple):
    pkg_name: str = None
    pkg_ver: str = None


class PyPackageSet(NamedTuple):
    packages: Set[PyPackage] = None


class UrlUniqueEntity(NamedTuple):
    """一种标识 entity 的方法，以 URL 的方式确定某一个 entity 的 unique """
    url: str


class OneDimUIObject(NamedTuple):
    """一维的UI object 的位置信息"""
    start: int = None
    end: int = None
    text_in_range: str = None
    """text_in_range 该区域的文字内容
    这是冗余信息，在文本中可以直接根据 start/end 得到 text 内容。这里的冗余是为了对数据做校验用"""


class TwoDimsUIObject(NamedTuple):
    """二维的 ui object ， 常用的别名包括：bounding-box"""
    left: int = None
    top: int = None
    width: int = None
    height: int = None
    # NOTE: 相对于 OneDimUIObject，这里表征二维数据的是像素点矩阵，暂时不保留这部分数据


class RGBColor(NamedTuple):
    R: int = 0
    G: int = 0
    B: int = 0
    alpha: float = 0.
    """透明度"""


class DataType(Enum):
    str = 1
    int = 2
    float = 3
    bool = 4
    datetime = 5
    # leave 6 to 9 for possible basic data types like date, time, etc.
    html_form_ele = 10
    """是一个 html form 的组件对象"""
