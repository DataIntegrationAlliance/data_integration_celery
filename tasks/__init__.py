#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/7/23 15:01
@File    : __init__.py.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from celery import Celery
from tasks.config import celery_config
import logging

logger = logging.getLogger()
app = Celery('tasks',
             config_source=celery_config,
             # broker='amqp://mg:***@localhost:5672/celery_tasks',
             # backend='amqp://mg:***@localhost:5672/backend',
             # backend='rpc://',
             )
app.config_from_object(celery_config)


@app.task
def add(x, y):
    """only for test use"""
    return x + y


try:
    from tasks.task2 import *
except ImportError:
    logger.exception("加载 tasks.task2 失败")

try:
    from tasks.ifind import *
except ImportError:
    logger.exception("加载 tasks.ifind 失败")

try:
    from tasks.wind import *
except ImportError:
    logger.exception("加载 tasks.wind 失败")

try:
    from tasks.cmc import *
except ImportError:
    logger.exception("加载 tasks.cmc 失败")

try:
    from tasks.tushare import *
except ImportError:
    logger.exception("加载 tasks.tushare 失败")
