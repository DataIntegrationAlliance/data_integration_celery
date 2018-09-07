#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/7/23 15:01
@File    : __init__.py.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from celery import Celery, group
from tasks.config import celery_config
import logging

logger = logging.getLogger()
app = Celery('tasks',
             config_source=celery_config,
             # broker='amqp://mg:123456@localhost:3306/celery_tasks',
             # backend='amqp://mg:123456@localhost:3306/backend',
             # backend='rpc://',
             )
app.config_from_object(celery_config)


@app.task
def add(x, y):
    """only for test use"""
    return x + y


@app.task
def test_task_n(chain_param=None, n=1):
    """
    only for test use
    :param chain_param: 该参数仅用于 task.chain 串行操作时，上下传递参数使用
    :param n:
    :return:
    """
    import time
    time.sleep(n)
    logger.info('test task %d', n)


@app.task
def test_task():
    """only for test use"""
    logger.info('test task')


# group 语句中的任务并行执行，chain语句串行执行
# group([(test_task_n.s(n=4)|test_task_n.s(n=2)), (test_task_n.s(n=1)|test_task_n.s(n=3))])()


try:
    from tasks.task2 import *
except ImportError:
    logger.exception("加载 tasks.task2 失败，该异常不影响其他功能正常使用")

try:
    from tasks.ifind import *
except ImportError:
    logger.exception("加载 tasks.ifind 失败，该异常不影响其他功能正常使用")

try:
    from tasks.wind import *
except ImportError:
    logger.exception("加载 tasks.wind 失败，该异常不影响其他功能正常使用")

try:
    from tasks.cmc import *
except ImportError:
    logger.exception("加载 tasks.cmc 失败，该异常不影响其他功能正常使用")

try:
    from tasks.tushare import *
except ImportError:
    logger.exception("加载 tasks.tushare 失败，该异常不影响其他功能正常使用")

daily_task_group = group([wind_daily_task, ifind_daily_task])

weekly_task_group = group([wind_weekly_task, ifind_weekly_task])

once_task_group = group([wind_import_once, ifind_import_once])
