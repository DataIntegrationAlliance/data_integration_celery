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

# 着两条语句不能够提前，将会导致循环引用异常
from tasks.ifind.stock import *
from tasks.ifind.trade_date import *
from tasks.ifind.future import *

if __name__ == '__main__':
    print_indicator_param_dic(
        'ths_qhjc_future;ths_qhdm_future;ths_zqlx_future;ths_jypz_future;ths_jydw_future;ths_contract_multiplier_future;ths_bjdw_future;ths_zxbdjw_future;ths_zdfxz_future;ths_jybzj_future;ths_contract_listed_date_future;ths_ksjyr_future;ths_zhjyr_future;ths_zhjgr_future;ths_jgyf_future;ths_gpjzj_future;ths_zcjybzj_future;ths_hyyfsm_future;ths_jysjsm_future;ths_zhjyrsm_future;ths_jgrqsm_future;ths_jysjc_future;ths_hyywjc_future;ths_hyywmc_future',';;;;;;;;;;;;;;;;;;;;;;;'
    )
