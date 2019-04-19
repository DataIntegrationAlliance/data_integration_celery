#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 19-4-3 上午11:05
@File    : balance_2_daily.py
@contact : mmmaaaggg@163.com
@desc    : 将 balance 表从季度数据转换为日级别数据
"""
from tasks.jqdata.stock.finance_report import transfer_report_2_daily
from tasks.jqdata.stock.finance_report.balance import DTYPE as DTYPE_REPORT, TABLE_NAME as TABLE_NAME_FIN_REPORT
from tasks.jqdata.trade_date import TABLE_NAME as TABLE_NAME_TRADE_DATE
from tasks import app
import logging


logger = logging.getLogger(__name__)
TABLE_NAME = f"{TABLE_NAME_FIN_REPORT}_daily"
DTYPE = DTYPE_REPORT.copy()
# 通过 tasks/jqdata/finance_report/__init__.py get_accumulation_col_names_for('income') 函数计算获得
ACCUMULATION_COL_NAME_LIST = [
    'salaries_payable']

for key in ('company_name', 'company_id', 'a_code', 'b_code', 'h_code', 'source'):
    del DTYPE[key]


@app.task
def save_2_daily_balance(chain_param=None):
    """
    将 cashflow 表从季度数据转换为日级别数据
    :return:
    """
    transfer_report_2_daily(
        TABLE_NAME_FIN_REPORT, TABLE_NAME, TABLE_NAME_TRADE_DATE, DTYPE, ACCUMULATION_COL_NAME_LIST)


if __name__ == "__main__":
    save_2_daily_balance()
