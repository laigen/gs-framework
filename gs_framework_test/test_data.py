import inspect
from functools import wraps
from typing import Tuple, Dict, Any

from dataclasses import dataclass


# case 1: Call dataclass in meta class: no chance to pass parameters to function dataclass

class GSDataClassMeta(type):

    def __new__(mcs, name: str, bases: Tuple, dct: Dict[str, Any]):

        cls = super().__new__(mcs, name, bases, dct)
        cls_name = cls.__qualname__

        print(f"creating class {cls_name}")

        cls = dataclass(cls)
        original_init = cls.__init__

        @wraps(original_init)
        def init_wrapper(self, *args, **kwargs):
            print(f"in init_wrapper of {cls_name}")
            original_init(self, *args, **kwargs)

        cls.__init__ = init_wrapper
        return cls


class GSDataClass(metaclass=GSDataClassMeta):
    pass


class UserClass(GSDataClass):

    member1: str = "abc"


init_method = UserClass.__init__
sig = inspect.signature(init_method)

o = UserClass(member1="efg")
print(o.member1)


# case 2: add base class in decorator

class GSOpMixin:

    def m1(self):
        pass


def gs_dataclass(cls, *, init=True, repr=True, eq=True, order=False, unsafe_hash=False, frozen=False):

    cls = dataclass(cls, init=init, repr=repr, eq=eq, order=order, unsafe_hash=unsafe_hash, frozen=frozen)

    cls_created = type(cls.__qualname__, (GSOpMixin, cls), {})

    def __init__(self, *args, **kwargs):
        print(f"before call __init__of {cls.__qualname__}")
        super(cls_created, self).__init__(*args, **kwargs)

    cls_created.__init__ = __init__
    return cls_created


@gs_dataclass
class UserClass2:

    member2: str = "xyz"


o2 = UserClass2()
print(o2.member2)
print(o2.__class__.__qualname__)


def wrap_init(init_func):

    @wraps(init_func)
    def init_wrapper(*args, _op_pipeline=None, **kwargs):
        print("inside _wrapping_init")
        init_func(*args, **kwargs)

    return init_wrapper


class TestMeta(type):

    def __setattr__(self, name, value):
        if name == '__init__':
            print("calling wrap_init in TestMeta")
            value = wrap_init(value)
        return super().__setattr__(name, value)


class TestBase(metaclass=TestMeta):
    def __init__(self):
        super().__init__()


class Test(TestBase):
    pass


@dataclass
class Test2(TestBase):

    mmm: str = "abc"


class Test3(Test2):

    @wraps(Test2.__init__)
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


a = Test()
b = Test()
c = Test2()
d = Test3()
