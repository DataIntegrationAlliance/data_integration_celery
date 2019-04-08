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

setup(name='data_integration_celery',
      version='0.1.0',
      description="""通过celery定期执行更相关任务，
      将万得wind，同花顺ifind，东方财富choice、Tushrae、JQDataSDK、pytdx、CMC等数据终端的数据进行整合，清洗，一致化，
      供其他系统数据分析使用""",
      long_description=long_description,
      long_description_content_type="text/markdown",
      author='MG',
      author_email='mmmaaaggg@163.com',
      url='https://github.com/DataIntegrationAlliance/RestIFindPy',
      packages=find_packages(),
      python_requires='>=3.5',
      classifiers=(
          "Programming Language :: Python :: 3 :: Only",
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
          'celery>=4.2.1',
          'Click>=7.0',
          'librabbitmq>=2.0.0',
          'eventlet>=0.24.1',
          'numpy>=1.16.2',
          'pandas>=0.24.2',
          'sqlalchemy>=1.3.1',
          'mysqlclient>=1.4.2',
          'xlrd>=1.2.0',
          'xlwt>=1.3.0',
          'bs4',
          'tushare>=1.2.21',
          'cryptocmd',
          'pytdx',
          'matplotlib>=3.0.3',
          'DIRestInvoker>=0.2.2',
          'jqdatasdk',
          'IBATS-Utils>=1.1.0',
      ])
