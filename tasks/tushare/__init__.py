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
# from tasks.tushare.coin import *
from tasks.tushare.trade_cal import *
from tasks.tushare.tushare_stock_daily.stock import *
from tasks.tushare.tushare_stock_daily.adj_factor import *
from tasks.tushare.tushare_stock_daily.daily_basic import *
from tasks.tushare.tushare_stock_daily.ggt_top10 import *
from tasks.tushare.tushare_stock_daily.hsgt_top10 import *
from tasks.tushare.tushare_stock_daily.margin import *
from tasks.tushare.tushare_stock_daily.margin_detail import *
from tasks.tushare.tushare_stock_daily.moneyflow_hsgt import *
from tasks.tushare.tushare_stock_daily.suspend import *
from tasks.tushare.tushare_stock_daily.index_daily import *
from tasks.tushare.tushare_fina_reports.balancesheet import *
from tasks.tushare.tushare_fina_reports.cashflow import *
from tasks.tushare.tushare_fina_reports.fina_audit import *
from tasks.tushare.tushare_fina_reports.fina_indicator import *
from tasks.tushare.tushare_fina_reports.fina_mainbz import *
from tasks.tushare.tushare_fina_reports.income import *
from tasks.tushare.tushare_fina_reports.patch_balancesheet import *
from tasks.tushare.tushare_fina_reports.patch_cashflow import *
from tasks.tushare.tushare_fina_reports.patch_fina_indicator import *
from tasks.tushare.tushare_fina_reports.top10_holders import *
from tasks.tushare.tushare_fina_reports.top10_floatholders import *


# 日级别加载的程序
tushare_daily_task = (
        import_tushare_adj_factor.s() |
        import_tushare_daily_basic.s() |
        import_tushare_ggt_top10.s() |
        import_tushare_hsgt_top10.s() |
        import_tushare_margin.s() |
        import_tushare_margin_detail.s() |
        import_tushare_moneyflow_hsgt.s() |
        import_tushare_stock_daily.s() |
        import_tushare_suspend.s()|
        import_tushare_stock_index_daily.s()
        # import_coinbar.s()
)
# 周级别加载的程序
tushare_weekly_task = (
        import_tushare_stock_info.s() |
        import_tushare_stock_balancesheet.s() |
        import_tushare_stock_cashflow.s() |
        import_tushare_stock_fina_audit.s() |
        import_tushare_stock_fina_indicator.s() |
        import_tushare_stock_fina_mainbz.s() |
        import_tushare_stock_income.s() |
        import_tushare_stock_balancesheet.s() |
        import_tushare_stock_cashflow.s() |
        import_tushare_stock_fina_indicator.s() |
        import_tushare_stock_top10_holders.s() |
        import_tushare_stock_top10_floatholders()
        # import_coin_info.s() |
        # import_coin_pair_info.s()
)
# 一次性加载的程序
tushare_import_once = (
        import_exchange_info.s() |
        import_trade_date.s()
)
