#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 19-4-17 下午3:33
@File    : indicator.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from jqdatasdk import indicator
from tasks.jqdata.finance_report import FundamentalTableSaver
from tasks import app
from sqlalchemy.types import String, Date, Integer
from sqlalchemy.dialects.mysql import DOUBLE

DTYPE = {
    "id": Integer,
    "code": String(12),
    "pubDate": Date,
    "statDate": Date,
    "day": Date,
    "eps": DOUBLE,
    "adjusted_profit": DOUBLE,
    "operating_profit": DOUBLE,
    "value_change_profit": DOUBLE,
    "roe": DOUBLE,
    "inc_return": DOUBLE,
    "roa": DOUBLE,
    "net_profit_margin": DOUBLE,
    "gross_profit_margin": DOUBLE,
    "expense_to_total_revenue": DOUBLE,
    "operation_profit_to_total_revenue": DOUBLE,
    "net_profit_to_total_revenue": DOUBLE,
    "operating_expense_to_total_revenue": DOUBLE,
    "ga_expense_to_total_revenue": DOUBLE,
    "financing_expense_to_total_revenue": DOUBLE,
    "operating_profit_to_profit": DOUBLE,
    "invesment_profit_to_profit": DOUBLE,
    "adjusted_profit_to_profit": DOUBLE,
    "goods_sale_and_service_to_revenue": DOUBLE,
    "ocf_to_revenue": DOUBLE,
    "ocf_to_operating_profit": DOUBLE,
    "inc_total_revenue_year_on_year": DOUBLE,
    "inc_total_revenue_annual": DOUBLE,
    "inc_revenue_year_on_year": DOUBLE,
    "inc_revenue_annual": DOUBLE,
    "inc_operation_profit_year_on_year": DOUBLE,
    "inc_operation_profit_annual": DOUBLE,
    "inc_net_profit_year_on_year": DOUBLE,
    "inc_net_profit_annual": DOUBLE,
    "inc_net_profit_to_shareholders_year_on_year": DOUBLE,
    "inc_net_profit_to_shareholders_annual": DOUBLE,

}
TABLE_NAME = 'jq_stock_indicator'


@app.task
def import_jq_stock_indicator(chain_param=None):
    """
    插入股票日线数据到最近一个工作日-1。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    :return:
    """
    saver = FundamentalTableSaver(TABLE_NAME, DTYPE, indicator)
    saver.save()


if __name__ == "__main__":
    import_jq_stock_indicator()
