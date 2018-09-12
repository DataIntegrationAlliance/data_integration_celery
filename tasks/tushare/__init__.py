#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/8/21 13:48
@File    : __init__.py.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
import tushare as ts
import logging

logger = logging.getLogger()
try:
    pro = ts.pro_api()
except AttributeError:
    logger.exception('獲取pro_api失敗,但是不影響合並')
    pro = None

# 以下语句不能够提前，将会导致循环引用异常
from tasks.tushare.coin import *
