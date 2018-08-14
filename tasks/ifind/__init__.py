#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/7/26 8:32
@File    : __init__.py.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from tasks.utils.fh_utils import zip_split
from tasks.config import config
from direstinvoker.ifind import IFinDInvoker
invoker = IFinDInvoker(config.IFIND_REST_URL)


def print_indicator_param_dic(*args):
    item_list = zip_split(*args, sep=';')
    print('[')
    for item in item_list:
        print(str(item) + ',')
    print(']')

# 以下语句不能够提前，将会导致循环引用异常
from tasks.ifind.stock import *
from tasks.ifind.trade_date import *
from tasks.ifind.future import *

if __name__ == '__main__':
    print_indicator_param_dic(
        'ths_corp_total_shares_hks;ths_pe_ttm_hks;ths_pb_hks;ths_pcf_operating_cash_flow_ttm_hks;ths_pcf_cash_net_flow_ttm_hks;ths_ps_ttm_hks;ths_dividend_rate_hks;ths_market_value_hks',
        ';100;2017,103;100;100;100;2017;HKD'
    )
