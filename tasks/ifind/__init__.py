#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/7/26 8:32
@File    : __init__.py.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from direstinvoker.ifind import IFinDInvoker
from ibats_utils.mess import zip_split

from tasks.config import config

invoker = IFinDInvoker(config.IFIND_REST_URL)


def print_indicator_param_dic(*args):
    item_list = zip_split(*args, sep=';')
    print('[')
    for item in item_list:
        print(str(item) + ',')
    print(']')


# 以下语句不能够提前，将会导致循环引用异常
from tasks.ifind.edb import *
from tasks.ifind.future.future_info_daily import *
from tasks.ifind.future.future_min import *
from tasks.ifind.private_fund import *
from tasks.ifind.pub_fund import *
from tasks.ifind.stock import *
from tasks.ifind.stock_hk import *
from tasks.ifind.trade_date import *
from tasks.ifind.index import *

# 日级别加载的程序
ifind_daily_task = (
        import_edb.s() |
        import_future_daily_his.s() |
        import_index_daily_his.s() |
        import_index_daily_ds.s() |
        import_future_min.s() |
        import_private_fund_daily.s() |
        import_pub_fund_daily.s() |
        import_stock_daily_his.s() |
        import_stock_daily_ds.s() |
        import_stock_hk_daily_ds.s() |
        import_stock_hk_daily_his.s()
)
# 周级别加载的程序
ifind_weekly_task = (
        import_future_info.s() |
        import_private_fund_info.s() |
        import_pub_fund_info.s() |
        import_stock_info.s() |
        import_stock_report_date.s() |
        import_stock_fin.s() |
        import_stock_hk_info.s() |
        import_stock_hk_report_date.s() |
        import_stock_hk_fin_by_report_date_weekly.s() |
        import_stock_hk_fin_quarterly.s()
)
# 一次性加载的程序
ifind_import_once = (
        import_edb.s() |
        import_index_info.s() |
        import_trade_date.s()
)

ERROR_CODE_MSG_DIC = {
    -206: "数据为空",
    -4001: "数据为空",
    -4210: "参数错误",
    -4302: "your usage of quote data has exceeded 150 million this week.",
}

if __name__ == '__main__':
    print_indicator_param_dic(
        'ths_corp_total_shares_hks;ths_pe_ttm_hks;ths_pb_hks;ths_pcf_operating_cash_flow_ttm_hks;ths_pcf_cash_net_flow_ttm_hks;ths_ps_ttm_hks;ths_dividend_rate_hks;ths_market_value_hks',
        ';100;2017,103;100;100;100;2017;HKD'
    )
