#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2019/3/1 12:31
@File    : balance.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from tasks.jqdata.stock.finance_report import FinanceReportSaver
from tasks.jqdata import finance
from tasks import app
from sqlalchemy.types import String, Date, Integer
from sqlalchemy.dialects.mysql import DOUBLE

DTYPE = {
        "id": Integer,
        "company_id": Integer,
        "company_name": String(100),
        "code": String(12),
        "a_code": String(12),
        "b_code": String(12),
        "h_code": String(12),
        "pub_date": Date,
        "end_date": Date,
        "report_date": Date,
        "report_type": Integer,
        "source_id": Integer,
        "source": String(60),
        "cash_equivalents": DOUBLE,
        "trading_assets": DOUBLE,
        "bill_receivable": DOUBLE,
        "account_receivable": DOUBLE,
        "advance_payment": DOUBLE,
        "other_receivable": DOUBLE,
        "affiliated_company_receivable": DOUBLE,
        "interest_receivable": DOUBLE,
        "dividend_receivable": DOUBLE,
        "inventories": DOUBLE,
        "expendable_biological_asset": DOUBLE,
        "non_current_asset_in_one_year": DOUBLE,
        "total_current_assets": DOUBLE,
        "hold_for_sale_assets": DOUBLE,
        "hold_to_maturity_investments": DOUBLE,
        "longterm_receivable_account": DOUBLE,
        "longterm_equity_invest": DOUBLE,
        "investment_property": DOUBLE,
        "fixed_assets": DOUBLE,
        "constru_in_process": DOUBLE,
        "construction_materials": DOUBLE,
        "fixed_assets_liquidation": DOUBLE,
        "biological_assets": DOUBLE,
        "oil_gas_assets": DOUBLE,
        "intangible_assets": DOUBLE,
        "development_expenditure": DOUBLE,
        "good_will": DOUBLE,
        "long_deferred_expense": DOUBLE,
        "deferred_tax_assets": DOUBLE,
        "total_non_current_assets": DOUBLE,
        "total_assets": DOUBLE,
        "shortterm_loan": DOUBLE,
        "trading_liability": DOUBLE,
        "notes_payable": DOUBLE,
        "accounts_payable": DOUBLE,
        "advance_peceipts": DOUBLE,
        "salaries_payable": DOUBLE,
        "taxs_payable": DOUBLE,
        "interest_payable": DOUBLE,
        "dividend_payable": DOUBLE,
        "other_payable": DOUBLE,
        "affiliated_company_payable": DOUBLE,
        "non_current_liability_in_one_year": DOUBLE,
        "total_current_liability": DOUBLE,
        "longterm_loan": DOUBLE,
        "bonds_payable": DOUBLE,
        "longterm_account_payable": DOUBLE,
        "specific_account_payable": DOUBLE,
        "estimate_liability": DOUBLE,
        "deferred_tax_liability": DOUBLE,
        "total_non_current_liability": DOUBLE,
        "total_liability": DOUBLE,
        "paidin_capital": DOUBLE,
        "capital_reserve_fund": DOUBLE,
        "specific_reserves": DOUBLE,
        "surplus_reserve_fund": DOUBLE,
        "treasury_stock": DOUBLE,
        "retained_profit": DOUBLE,
        "equities_parent_company_owners": DOUBLE,
        "minority_interests": DOUBLE,
        "foreign_currency_report_conv_diff": DOUBLE,
        "irregular_item_adjustment": DOUBLE,
        "total_owner_equities": DOUBLE,
        "total_sheet_owner_equities": DOUBLE,
        "other_comprehesive_income": DOUBLE,
        "deferred_earning": DOUBLE,
        "settlement_provi": DOUBLE,
        "lend_capital": DOUBLE,
        "loan_and_advance_current_assets": DOUBLE,
        "derivative_financial_asset": DOUBLE,
        "insurance_receivables": DOUBLE,
        "reinsurance_receivables": DOUBLE,
        "reinsurance_contract_reserves_receivable": DOUBLE,
        "bought_sellback_assets": DOUBLE,
        "hold_sale_asset": DOUBLE,
        "loan_and_advance_noncurrent_assets": DOUBLE,
        "borrowing_from_centralbank": DOUBLE,
        "deposit_in_interbank": DOUBLE,
        "borrowing_capital": DOUBLE,
        "derivative_financial_liability": DOUBLE,
        "sold_buyback_secu_proceeds": DOUBLE,
        "commission_payable": DOUBLE,
        "reinsurance_payables": DOUBLE,
        "insurance_contract_reserves": DOUBLE,
        "proxy_secu_proceeds": DOUBLE,
        "receivings_from_vicariously_sold_securities": DOUBLE,
        "hold_sale_liability": DOUBLE,
        "estimate_liability_current": DOUBLE,
        "deferred_earning_current": DOUBLE,
        "preferred_shares_noncurrent": DOUBLE,
        "pepertual_liability_noncurrent": DOUBLE,
        "longterm_salaries_payable": DOUBLE,
        "dother_equity_tools": DOUBLE,
        "preferred_shares_equity": DOUBLE,
        "pepertual_liability_equity": DOUBLE,
    }
TABLE_NAME = 'jq_stock_balance'


@app.task
def import_jq_stock_balance(chain_param=None):
    """
    插入股票日线数据到最近一个工作日-1。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    :return:
    """
    saver = FinanceReportSaver(TABLE_NAME, DTYPE, finance.STK_BALANCE_SHEET)
    saver.save()


if __name__ == "__main__":
    import_jq_stock_balance()

