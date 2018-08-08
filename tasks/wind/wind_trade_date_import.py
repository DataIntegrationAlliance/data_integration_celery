# -*- coding: utf-8 -*-
"""
Created on 2017/4/20
@author: MG
"""
from datetime import date, datetime, timedelta
import pandas as pd
import numpy as np
from config_fh import get_db_engine, get_db_session, STR_FORMAT_DATE, UN_AVAILABLE_DATE, WIND_REST_URL
from fh_tools.windy_utils_rest import WindRest
import logging
logger = logging.getLogger()


def import_trade_date():
    """
    增量导入交易日数据导数据库表 wind_trade_date，默认导入未来300天的交易日数据
    2018-01-17 增加港股交易日数据，眼前考虑对减少对已有代码的冲击，新建一张 wind_trade_date_hk表
    日后将会考虑将两张表进行合并
    :return: 
    """
    w = WindRest(WIND_REST_URL)
    engine = get_db_engine()
    trade_date_start = None
    with get_db_session(engine) as session:
        try:
            table = session.execute('select exch_code,max(trade_date) from wind_trade_date_all group by exch_code')
            exch_code_trade_date_dic = {exch_code: trade_date for exch_code, trade_date in table.fetchall()}
        except Exception as exp:
            logger.exception("交易日获取异常")
        if trade_date_start is None:
            trade_date_start = '1980-01-01'

    #
    exchange_code_dict = {
        "HKEX":"香港",
        "NYSE":"纽约",
        "SZSE":"深圳",
        "TWSE":"台湾",
        "NASDAQ":"纳斯达克",
        "AMEX":"美国证券交易所",
        "TSE":"东京",
        "LSE":"伦敦",
        "SGX":"新加坡"
    }
    exchange_code_list = list(exchange_code_dict.keys())
    for exchange_code in exchange_code_list:
        if exchange_code in exch_code_trade_date_dic:
            trade_date_max = exch_code_trade_date_dic[exchange_code]
            trade_date_start = (trade_date_max + timedelta(days=1)).strftime(STR_FORMAT_DATE)
        else:
            trade_date_start = '1980-01-01'
        end_date_str = (date.today() + timedelta(days=310)).strftime(STR_FORMAT_DATE)
        if exchange_code is None or exchange_code == "":
            trade_date_list = w.tdays(trade_date_start, end_date_str)
        else:
            trade_date_list = w.tdays(trade_date_start, end_date_str, "TradingCalendar=%s" % exchange_code)
        if trade_date_list is None:
            logger.warning("没有查询到交易日期")
        date_count = len(trade_date_list)
        if date_count > 0:
            logger.info("%d 条交易日数据将被导入", date_count)
            with get_db_session() as session:
                session.execute("insert into wind_trade_date_all (trade_date,exch_code) VALUE (:trade_date,:exch_code)" ,
                                params=[{'trade_date': trade_date,'exch_code':exchange_code} for trade_date in trade_date_list])
            logger.info('%d 条交易日数据导入 %s 完成', date_count,'wind_trade_date_all')


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s [%(name)s:%(funcName)s] %(message)s')
    # import_trade_date()
    import_trade_date()
