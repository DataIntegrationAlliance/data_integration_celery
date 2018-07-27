#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/7/23 19:26
@File    : wind.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from tasks import app


@app.task
def div(x, y):
    """only for test use"""
    return x / y
