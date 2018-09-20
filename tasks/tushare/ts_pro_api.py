#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/9/20 17:49
@File    : ts_pro_api.py
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
