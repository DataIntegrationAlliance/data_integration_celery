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

# 以下语句不能够提前，将会导致循环引用异常
# from tasks.wind.bonus_import import *
from tasks.wind.commodity import *
from tasks.wind.convertible_bond import *
from tasks.wind.future import *
from tasks.wind.private_fund import *
from tasks.wind.stock import *
from tasks.wind.stock_hk import *
from tasks.wind.trade_date import *


if __name__ == "__main__":
    # 仅供接口测试使用
    df = invoker.wset("sectorconstituent", "date=2018-01-04;sectorid=a001010100000000")
    print(df)
