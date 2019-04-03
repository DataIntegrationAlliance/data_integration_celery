#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 19-4-3 上午10:57
@File    : cashflow_2_daily.py
@contact : mmmaaaggg@163.com
@desc    : 将 cashflow 表从季度数据转换为日级别数据
"""
from tasks.jqdata.finance_report import transfer_report_2_daily
from tasks.jqdata.finance_report.cashflow import DTYPE as DTYPE_CASHFLOW, TABLE_NAME as TABLE_NAME_FIN_REPORT
from tasks.jqdata.trade_date import TABLE_NAME as TABLE_NAME_TRADE_DATE
from tasks import app
import logging


logger = logging.getLogger(__name__)
TABLE_NAME = f"{TABLE_NAME_FIN_REPORT}_daily"
DTYPE = DTYPE_CASHFLOW.copy()
# 通过 tasks/jqdata/finance_report/__init__.py get_accumulation_col_names_for('income') 函数计算获得
ACCUMULATION_COL_NAME_LIST = [
    'subtotal_operate_cash_inflow', 'staff_behalf_paid', 'tax_payments', 'subtotal_operate_cash_outflow',
    'invest_withdrawal_cash', 'invest_proceeds', 'subtotal_invest_cash_inflow', 'fix_intan_other_asset_acqui_cash',
    'invest_cash_paid', 'subtotal_invest_cash_outflow', 'subtotal_finance_cash_outflow', 'assets_depreciation_reserves',
    'fixed_assets_depreciation', 'intangible_assets_amortization', 'operate_operate_payable_increase',
    'net_operate_cash_flow_indirect']


for key in ('company_name', 'company_id', 'a_code', 'b_code', 'h_code', 'source'):
    del DTYPE[key]


@app.task
def save_2_daily_cashflow(chain_param=None):
    """
    将 cashflow 表从季度数据转换为日级别数据
    :return:
    """
    transfer_report_2_daily(
        TABLE_NAME_FIN_REPORT, TABLE_NAME, TABLE_NAME_TRADE_DATE, DTYPE, ACCUMULATION_COL_NAME_LIST)


if __name__ == "__main__":
    save_2_daily_cashflow()
