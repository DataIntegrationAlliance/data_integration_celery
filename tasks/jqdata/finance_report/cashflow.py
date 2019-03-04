#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2019/2/28 17:01
@File    : cashflow.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from tasks.jqdata.finance_report import FinanceReportSaver
from tasks.jqdata import finance
from tasks import app
from sqlalchemy.types import String, Date, Integer
from sqlalchemy.dialects.mysql import DOUBLE


@app.task
def import_jq_stock_cashflow(chain_param=None, ts_code_set=None):
    """
    插入股票日线数据到最近一个工作日-1。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    :return:
    """
    dtype = {
        "id": Integer,
        "company_id": Integer,
        "company_name": String(100),
        "code": String(12),
        "a_code": String(12),
        "b_code": String(12),
        "h_code": String(12),
        "pub_date": Date,
        "start_date": Date,
        "end_date": Date,
        "report_date": Date,
        "report_type": Integer,
        "source_id": Integer,
        "source": String(60),
        "goods_sale_and_service_render_cash": DOUBLE,
        "tax_levy_refund": DOUBLE,
        "subtotal_operate_cash_inflow": DOUBLE,
        "goods_and_services_cash_paid": DOUBLE,
        "staff_behalf_paid": DOUBLE,
        "tax_payments": DOUBLE,
        "subtotal_operate_cash_outflow": DOUBLE,
        "net_operate_cash_flow": DOUBLE,
        "invest_withdrawal_cash": DOUBLE,
        "invest_proceeds": DOUBLE,
        "fix_Integeran_other_asset_dispo_cash": DOUBLE,
        "net_cash_deal_subcompany": DOUBLE,
        "subtotal_invest_cash_inflow": DOUBLE,
        "fix_Integeran_other_asset_acqui_cash": DOUBLE,
        "invest_cash_paid": DOUBLE,
        "impawned_loan_net_increase": DOUBLE,
        "net_cash_from_sub_company": DOUBLE,
        "subtotal_invest_cash_outflow": DOUBLE,
        "net_invest_cash_flow": DOUBLE,
        "cash_from_invest": DOUBLE,
        "cash_from_borrowing": DOUBLE,
        "cash_from_bonds_issue": DOUBLE,
        "subtotal_finance_cash_inflow": DOUBLE,
        "borrowing_repayment": DOUBLE,
        "dividend_Integererest_payment": DOUBLE,
        "subtotal_finance_cash_outflow": DOUBLE,
        "net_finance_cash_flow": DOUBLE,
        "exchange_rate_change_effect": DOUBLE,
        "other_reason_effect_cash": DOUBLE,
        "cash_equivalent_increase": DOUBLE,
        "cash_equivalents_at_beginning": DOUBLE,
        "cash_and_equivalents_at_end": DOUBLE,
        "net_profit": DOUBLE,
        "assets_depreciation_reserves": DOUBLE,
        "fixed_assets_depreciation": DOUBLE,
        "Integerangible_assets_amortization": DOUBLE,
        "defferred_expense_amortization": DOUBLE,
        "fix_Integeran_other_asset_dispo_loss": DOUBLE,
        "fixed_asset_scrap_loss": DOUBLE,
        "fair_value_change_loss": DOUBLE,
        "financial_cost": DOUBLE,
        "invest_loss": DOUBLE,
        "deffered_tax_asset_decrease": DOUBLE,
        "deffered_tax_liability_increase": DOUBLE,
        "inventory_decrease": DOUBLE,
        "operate_receivables_decrease": DOUBLE,
        "operate_payable_increase": DOUBLE,
        "others": DOUBLE,
        "net_operate_cash_flow_indirect": DOUBLE,
        "debt_to_capital": DOUBLE,
        "cbs_expiring_in_one_year": DOUBLE,
        "financial_lease_fixed_assets": DOUBLE,
        "cash_at_end": DOUBLE,
        "cash_at_beginning": DOUBLE,
        "equivalents_at_end": DOUBLE,
        "equivalents_at_beginning": DOUBLE,
        "other_reason_effect_cash_indirect": DOUBLE,
        "cash_equivalent_increase_indirect": DOUBLE,
        "net_deposit_increase": DOUBLE,
        "net_borrowing_from_central_bank": DOUBLE,
        "net_borrowing_from_finance_co": DOUBLE,
        "net_original_insurance_cash": DOUBLE,
        "net_cash_received_from_reinsurance_business": DOUBLE,
        "net_insurer_deposit_investment": DOUBLE,
        "net_deal_trading_assets": DOUBLE,
        "Integererest_and_commission_cashin": DOUBLE,
        "net_increase_in_placements": DOUBLE,
        "net_buyback": DOUBLE,
        "net_loan_and_advance_increase": DOUBLE,
        "net_deposit_in_cb_and_ib": DOUBLE,
        "original_compensation_paid": DOUBLE,
        "handling_charges_and_commission": DOUBLE,
        "policy_dividend_cash_paid": DOUBLE,
        "cash_from_mino_s_invest_sub": DOUBLE,
        "proceeds_from_sub_to_mino_s": DOUBLE,
        "investment_property_depreciation": DOUBLE,
    }
    table_name = 'jq_stock_cashflow'
    saver = FinanceReportSaver(table_name, dtype, finance.STK_CASHFLOW_STATEMENT)
    saver.save()


if __name__ == "__main__":
    import_jq_stock_cashflow()
