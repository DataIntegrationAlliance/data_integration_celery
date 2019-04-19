#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 19-3-25 下午3:45
@File    : income_2_daily.py
@contact : mmmaaaggg@163.com
@desc    : 将 income 表从季度数据转换为日级别数据
"""
from tasks.jqdata.stock.finance_report import transfer_report_2_daily
from tasks.jqdata.stock.finance_report.income import DTYPE as DTYPE_INCOME, TABLE_NAME as TABLE_NAME_FIN_REPORT
from tasks.jqdata.trade_date import TABLE_NAME as TABLE_NAME_TRADE_DATE
from tasks import app
import logging


logger = logging.getLogger(__name__)
TABLE_NAME = f"{TABLE_NAME_FIN_REPORT}_daily"
DTYPE = DTYPE_INCOME.copy()
# 通过 tasks/jqdata/finance_report/__init__.py get_accumulation_col_names_4_income 函数计算获得
ACCUMULATION_COL_NAME_LIST = [
    'total_operating_revenue', 'operating_revenue', 'total_operating_cost', 'operating_tax_surcharges',
    'administration_expense', 'asset_impairment_loss', 'investment_income', 'invest_income_associates',
    'exchange_income', 'other_items_influenced_income', 'operating_profit', 'non_operating_revenue',
    'non_operating_expense', 'total_profit', 'income_tax', 'net_profit', 'np_parent_company_owners',
    'basic_eps', 'diluted_eps']

for key in ('company_name', 'company_id', 'a_code', 'b_code', 'h_code', 'source'):
    del DTYPE[key]


@app.task
def save_2_daily_income(chain_param=None):
    transfer_report_2_daily(
        TABLE_NAME_FIN_REPORT, TABLE_NAME, TABLE_NAME_TRADE_DATE, DTYPE, ACCUMULATION_COL_NAME_LIST)


if __name__ == "__main__":
    save_2_daily_income()
