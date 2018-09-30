#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/8/21 13:48
@File    : __init__.py.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
# from tasks.tushare.coin import import_coinbar, import_coin_info, import_coin_pair_info
from tasks.tushare.trade_cal import import_trade_date
from tasks.tushare.tushare_stock_daily.stock import import_tushare_stock_daily, import_tushare_stock_info
from tasks.tushare.tushare_stock_daily.adj_factor import import_tushare_adj_factor
from tasks.tushare.tushare_stock_daily.daily_basic import import_tushare_daily_basic
from tasks.tushare.tushare_stock_daily.ggt_top10 import import_tushare_ggt_top10
from tasks.tushare.tushare_stock_daily.hsgt_top10 import import_tushare_hsgt_top10
from tasks.tushare.tushare_stock_daily.margin import import_tushare_margin
from tasks.tushare.tushare_stock_daily.margin_detail import import_tushare_margin_detail
from tasks.tushare.tushare_stock_daily.moneyflow_hsgt import import_tushare_moneyflow_hsgt
from tasks.tushare.tushare_stock_daily.suspend import import_tushare_suspend
from tasks.tushare.tushare_stock_daily.index_daily import import_tushare_stock_index_daily
from tasks.tushare.tushare_stock_daily.top_list import import_tushare_top_list
from tasks.tushare.tushare_stock_daily.top_list_detail import import_tushare_top_inst
from tasks.tushare.tushare_fina_reports.balancesheet import import_tushare_stock_balancesheet
from tasks.tushare.tushare_fina_reports.cashflow import import_tushare_stock_cashflow
from tasks.tushare.tushare_fina_reports.fina_audit import import_tushare_stock_fina_audit
from tasks.tushare.tushare_fina_reports.fina_indicator import import_tushare_stock_fina_indicator
from tasks.tushare.tushare_fina_reports.fina_mainbz import import_tushare_stock_fina_mainbz
from tasks.tushare.tushare_fina_reports.income import import_tushare_stock_income
# from tasks.tushare.tushare_fina_reports.patch_balancesheet import *
# from tasks.tushare.tushare_fina_reports.patch_cashflow import *
# from tasks.tushare.tushare_fina_reports.patch_fina_indicator import *
from tasks.tushare.tushare_fina_reports.top10_holders import import_tushare_stock_top10_holders
from tasks.tushare.tushare_fina_reports.top10_floatholders import import_tushare_stock_top10_floatholders





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
        import_tushare_suspend.s() |
        import_tushare_stock_index_daily.s()|
        import_tushare_top_list.s()|
        import_tushare_top_inst.s()
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
        import_tushare_stock_top10_holders.s() |
        import_tushare_stock_top10_floatholders.s()
        # import_coin_info.s() |
        # import_coin_pair_info.s()
)
# 一次性加载的程序
tushare_import_once = (
        import_tushare_stock_info.s() |
        import_trade_date.s()
)
