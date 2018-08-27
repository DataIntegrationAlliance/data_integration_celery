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


from tasks.task2 import *
from tasks.ifind import *
from tasks.cmc import *
from tasks.tushare import *
