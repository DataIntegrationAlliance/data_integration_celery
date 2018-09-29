#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/8/27 9:44
@File    : __init__.py.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from tasks.cmc.coin import *

cmc_daily_task = (
    import_coin_daily.s() |
    import_coin_latest.s() |
    merge_latest.s()
)

cmc_weekly_task = (
    import_coin_info.s()
)

cmc_import_once = (
    import_coin_info.s()
)
