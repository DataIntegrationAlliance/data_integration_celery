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
from ifind_rest.invoke import IFinDInvoker
invoker = IFinDInvoker(config.IFIND_REST_URL)


def print_indicator_param_dic(*args):
    item_list = zip_split(*args, sep=';')
    print('[')
    for item in item_list:
        print(str(item) + ',')
    print(']')

# 着两条语句不能够提前，将会导致循环引用异常
from .stock import *
from .trade_date import *


if __name__ == '__main__':
    print_indicator_param_dic(
        'ths_stock_short_name_stock;ths_stock_code_stock;ths_thscode_stock;ths_stock_varieties_stock;ths_ipo_date_stock;ths_listing_exchange_stock;ths_delist_date_stock;ths_corp_cn_name_stock;ths_corp_name_en_stock;ths_established_date_stock',';;;;;;;;;'
    )
