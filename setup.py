#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/6/14 16:07
@File    : setup.py
@contact : mmmaaaggg@163.com
@desc    : 
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding='utf-8') as rm:
    long_description = rm.read()

setup(name='RestIFindPy',
      version='0.0.5',
      description='基于Restplus实现同花顺iFinD接口分布式调用',
      long_description=long_description,
      long_description_content_type="text/markdown",
      author='MG',
      author_email='mmmaaaggg@163.com',
      url='https://github.com/DataIntegrationAlliance/RestIFindPy',
      packages=find_packages(),
      python_requires='>=3.5',
      classifiers=(
          "Programming Language :: Python :: 3 :: Only",
          "Programming Language :: Python :: 3.5",
          "Programming Language :: Python :: 3.6",
          "Programming Language :: Python :: 3.7",
          "License :: OSI Approved :: MIT License",
          "Development Status :: 5 - Production/Stable",
          "Environment :: No Input/Output (Daemon)",
          "Intended Audience :: Developers",
          "Natural Language :: Chinese (Simplified)",
          "Topic :: Software Development",
      ),
      install_requires=[
          'pandas>=0.23.0',
          'requests>=2.19.1',
          'DIRestInvoker>=0.0.1',
          'mysqlclient>=1.3.12',
          'celery>=4.2.1',
          'librabbitmq>=2.0.0',
          'eventlet>=0.23.0',
          'sqlalchemy>=1.2.10',
          'xlrd>=1.1.0',
      ])
