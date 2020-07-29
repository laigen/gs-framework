import asyncio
import inspect
from collections import Awaitable


def test(var: str):

    """
    Args:
        var (str):
    """
    def test_decorator(func):
        print(var)
        return func

    return test_decorator


class C:
    CLS_MEMBER2: str = "cls_member_b2"


class B(C):
    CLS_MEMBER: str = "cls_member_b"


class A(B):
    CLS_MEMBER: str = "cls_member_a"
    NONE_MEMBER = None
    ATTR: str = None

    @test(var=B.CLS_MEMBER2)
    def func_test(self):

        pass


async def test_async():
    await asyncio.sleep(10)


if __name__ == "__main__":
    print(__name__)
    a = A()
    a.CLS_MEMBER = "new value"
    print(a.CLS_MEMBER)
    print(A.CLS_MEMBER)
    for name, member in inspect.getmembers(A):
        if name == 'ATTR':
            pass
    res = test_async()
    print(isinstance(res, Awaitable))

    none_member = A.NONE_MEMBER
    print(id(none_member))
    print(id(a.__class__.NONE_MEMBER))
