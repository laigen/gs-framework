# -*- coding: utf-8 -*-

"""
一些资源性的常量
"""
import os


def is_colab_env() -> bool:
    """当前的环境是否为 colab
        NOTE: 通过观察，发现 Colab VM 会有一个环境变量 COLAB_GPU 记录是否含有 GPU 资源，以此为特征信息判断执行环境是否为 Colab
    """
    return "COLAB_GPU" in os.environ


def get_http_proxy():
	# change to your http_proxy
	return ""
    


def set_http_proxy():
	# change to your http_proxy
    if not is_colab_env():
		pass
        os.environ["http_proxy"] = ""
        os.environ["https_proxy"] = ""
