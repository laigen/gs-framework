# -*- coding: UTF-8 -*-
"""
测试将一个函数序列化成binary，然后反序列化之后变成可调用的函数
"""
import inspect
import string
from random import choice

# from gs_framework.samples.dce_author_data import high_score_author
from gs_framework.utilities import bytes_2_func, func_2_bytes, get_random_int



# class TestClass:
#     @staticmethod
#     def dummy_hello():
#         print("dummy_hello")
#
#     @staticmethod
#     def my_test_func(a: int, b: str) -> str:
#         def tmp_get_random_str():
#             return ''.join(choice(string.ascii_uppercase + string.digits) for _ in range(10))
#         print(a)
#         print(b)
#         print("TestClass")
#
#         return f"my_test_func_v2({a},{b},{tmp_get_random_str()})"


if __name__ == "__main__":
    # v = TestClass.my_test_func(1, "hello")
    # print(v)

    # print(v)
    # see https://medium.com/@emlynoregan/serialising-all-the-functions-in-python-cd880a63b591
    # import dill

    # func_bin = func_2_bytes(high_score_author)
    # with open("/tmp/laigen/func.bin", "wb") as f:
    #     f.write(func_bin)

    with open("/tmp/laigen/func.bin", "rb") as f:
        func_bin = f.read()
        f = bytes_2_func(func_bin)
        print(getattr(f, "scholar", None))





    # f = dill.loads(func_bin)
    # v = f(1, "hello")
    # print(v)
