#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 19-4-19 上午9:39
@File    : valuation.py
@contact : mmmaaaggg@163.com
@desc    : 市值数据（每日更新）
每天更新，可以使用get_fundamentals(query(valuation),date),指定date为某一交易日,获取该交易日的估值数据。查询方法详见get_fundamentals()接口说明
表名: valuation
列名	列的含义	解释	公式
code	股票代码	带后缀.XSHE/.XSHG
day	日期	取数据的日期
capitalization	总股本(万股)	公司已发行的普通股股份总数(包含A股，B股和H股的总股本)
circulating_cap	流通股本(万股)	公司已发行的境内上市流通、以人民币兑换的股份总数(A股市场的流通股本)
market_cap	总市值(亿元)	A股收盘价*已发行股票总股本（A股+B股+H股）
circulating_market_cap	流通市值(亿元)	流通市值指在某特定时间内当时可交易的流通股股数乘以当时股价得出的流通股票总价值。	A股市场的收盘价*A股市场的流通股数
turnover_ratio	换手率(%)	指在一定时间内市场中股票转手买卖的频率，是反映股票流通性强弱的指标之一。	换手率=[指定交易日成交量(手)100/截至该日股票的自由流通股本(股)]100%
pe_ratio	市盈率(PE, TTM)	每股市价为每股收益的倍数，反映投资人对每元净利润所愿支付的价格，用来估计股票的投资报酬和风险	市盈率（PE，TTM）=（股票在指定交易日期的收盘价 * 当日人民币外汇挂牌价* 截止当日公司总股本）/归属于母公司股东的净利润TTM。
pe_ratio_lyr	市盈率(PE)	以上一年度每股盈利计算的静态市盈率. 股价/最近年度报告EPS	市盈率（PE）=（股票在指定交易日期的收盘价 * 当日人民币外汇牌价 * 截至当日公司总股本）/归属母公司股东的净利润。
pb_ratio	市净率(PB)	每股股价与每股净资产的比率	市净率=（股票在指定交易日期的收盘价 * 当日人民币外汇牌价 * 截至当日公司总股本）/归属母公司股东的权益。
ps_ratio	市销率(PS, TTM)	市销率为股票价格与每股销售收入之比，市销率越小，通常被认为投资价值越高。	市销率TTM=（股票在指定交易日期的收盘价 * 当日人民币外汇牌价 * 截至当日公司总股本）/营业总收入TTM
pcf_ratio	市现率(PCF, 现金净流量TTM)	每股市价为每股现金净流量的倍数	市现率=（股票在指定交易日期的收盘价 * 当日人民币外汇牌价 * 截至当日公司总股本）/现金及现金等价物净增加额TTM
"""
from ibats_utils.mess import decorator_timer
from jqdatasdk import valuation
from tasks.jqdata.stock.finance_report import FundamentalTableDailySaver
from tasks import app
from sqlalchemy.types import String, Date, Integer
from sqlalchemy.dialects.mysql import DOUBLE

DTYPE = {
    "id": Integer,
    "code": String(12),
    "day": Date,
    "capitalization": DOUBLE,
    "circulating_cap": DOUBLE,
    "market_cap": DOUBLE,
    "circulating_market_cap": DOUBLE,
    "turnover_ratio": DOUBLE,
    "pe_ratio": DOUBLE,
    "pe_ratio_lyr": DOUBLE,
    "pb_ratio": DOUBLE,
    "ps_ratio": DOUBLE,
    "pcf_ratio": DOUBLE,
}
TABLE_NAME = 'jq_stock_valuation'


@app.task
@decorator_timer
def import_jq_stock_valuation(chain_param=None):
    """
    插入股票日线数据到最近一个工作日-1。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    :return:
    """
    saver = FundamentalTableDailySaver(TABLE_NAME, DTYPE, valuation)
    saver.save()


if __name__ == "__main__":
    import_jq_stock_valuation()
