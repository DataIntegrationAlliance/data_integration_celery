#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/11/2 17:26
@File    : repair_table.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from tasks.backend import engine_md
from ibats_utils.db import execute_sql, with_db_session
from datetime import datetime


def repair_table():
    datetime_start = datetime.now()
    with with_db_session(engine_md) as session:
        session.execute('REPAIR TABLE pytdx_stock_tick USE_FRM')

    datetime_end = datetime.now()
    span = datetime_end - datetime_start
    print('花费时间 ', span)


if __name__ == "__main__":
    repair_table()
