#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2019/3/1 12:42
@File    : app_tasks.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from tasks.jqdata.stock_info import import_jq_stock_info
from tasks.jqdata.finance_report.balance import import_jq_stock_balance
from tasks.jqdata.finance_report.cashflow import import_jq_stock_cashflow
from tasks.jqdata.finance_report.income import import_jq_stock_income
from tasks.jqdata.available_check.check import check_all


jq_once_task = (
    import_jq_stock_info.s()
)
jq_weekly_task = (
    import_jq_stock_info.s()
)
jq_finance_task = (
    import_jq_stock_income.s() |
    import_jq_stock_cashflow.s() |
    import_jq_stock_balance.s() |
    check_all.s()
)


def run_daily_job_local():
    import_jq_stock_income()
    import_jq_stock_cashflow()
    import_jq_stock_balance()
    check_all()


def run_once_job_local():
    import_jq_stock_info()


if __name__ == '__main__':
    run_once_job_local()
    run_daily_job_local()
