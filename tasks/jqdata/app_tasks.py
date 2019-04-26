#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2019/3/1 12:42
@File    : app_tasks.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from tasks import app
from tasks.jqdata.fund.fund_info import import_jq_fund_info
from tasks.jqdata.future.future_info import import_jq_future_info
from tasks.jqdata.index.index_info import import_jq_index_info
from tasks.jqdata.index.index_stocks import import_jq_index_stocks
from tasks.jqdata.stock.finance_report.balance_2_daily import save_2_daily_balance
from tasks.jqdata.stock.finance_report.cashflow_2_daily import save_2_daily_cashflow
from tasks.jqdata.stock.finance_report.income_2_daily import save_2_daily_income
from tasks.jqdata.stock.finance_report.indicator import import_jq_stock_indicator
from tasks.jqdata.stock.finance_report.valuation import import_jq_stock_valuation
from tasks.jqdata.stock.stock_daily import import_jq_stock_daily
from tasks.jqdata.stock.stock_info import import_jq_stock_info
from tasks.jqdata.stock.finance_report.balance import import_jq_stock_balance
from tasks.jqdata.stock.finance_report.cashflow import import_jq_stock_cashflow
from tasks.jqdata.stock.finance_report.income import import_jq_stock_income
from tasks.jqdata.stock.available_check.check import check_all


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
jq_daily_task = (
    import_jq_stock_daily.s() |
    import_jq_stock_valuation.s() |
    import_jq_stock_indicator.s()
)


def run_finance_job_local():
    import_jq_stock_income()
    import_jq_stock_cashflow()
    import_jq_stock_balance()
    import_jq_stock_daily()
    check_all()


def run_finance_2_daily_job_local():
    save_2_daily_balance()
    save_2_daily_cashflow()
    save_2_daily_income()


def run_daily_job_local():
    import_jq_stock_daily()
    import_jq_stock_valuation()
    import_jq_stock_indicator()
    import_jq_index_stocks()


def run_once_job_local():
    import_jq_stock_info()
    import_jq_future_info()
    import_jq_fund_info()
    import_jq_index_info()


@app.task
def jq_tasks_local_first_time():
    jq_tasks_local(True)


@app.task
def jq_tasks_local(first_time=False):
    if first_time:
        run_once_job_local()

    run_daily_job_local()
    run_finance_job_local()
    run_finance_2_daily_job_local()


if __name__ == '__main__':
    # run_once_job_local()
    run_daily_job_local()
    run_finance_job_local()
    # run_finance_2_daily_job_local()
