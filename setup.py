# -*- coding: UTF-8 -*-

from setuptools import setup, find_packages

setup(name='gs-framework',
      version='0.1',
      description='GS Framework',
      author='GS',
      packages=find_packages(exclude=["install"]),
      install_requires=[
			'python-rocksdb==0.6.9',
            'dnspython',
            'pyarrow',
            'orderedset',
            'dataclasses',
            'confluent-kafka',
            'dill',
            'psutil',
            'faust[rocksdb]',
            'pymongo',
      ],
      extras_require={
            "browser": ["selenium"]
      })
