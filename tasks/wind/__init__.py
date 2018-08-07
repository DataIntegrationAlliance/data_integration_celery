#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/8/5 11:09
@File    : __init__.py.py
@contact : mmmaaaggg@163.com
@desc    : 
"""

from tasks.config import config
from direstinvoker.iwind import WindRestInvoker
invoker = WindRestInvoker(config.WIND_REST_URL)