#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 19-4-30 下午2:01
@File    : future_member_position_rank.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from tasks.jqdata import finance_run_query, finance, query

query(finance.FUT_MEMBER_POSITION_RANK).filter(finance.FUT_MEMBER_POSITION_RANK.code=='A1905.XDCE')

if __name__ == "__main__":
    pass
