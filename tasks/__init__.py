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

app = Celery('tasks',
             broker='amqp://mg:Abcd1234@localhost:5672/celery_tasks',
             backend='amqp://mg:Abcd1234@localhost:5672/backend',
             # backend='rpc://',
             )


@app.task
def add(x, y):
    return x + y
