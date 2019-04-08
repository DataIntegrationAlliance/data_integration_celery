# -*- coding: utf-8 -*-
"""
Created on 2017/4/20
@author: MG
@desc    : 2018-08-21 已经正式运行测试完成，可以正常使用
"""
from datetime import date, datetime, timedelta
from tasks.backend import engine_md
from ibats_utils.mess import STR_FORMAT_DATE
from ibats_utils.db import with_db_session, alter_table_2_myisam, bunch_insert_on_duplicate_update
from tasks.wind import invoker
import pandas as pd
import logging
from sqlalchemy.types import String, Date
from tasks.config import config

logger = logging.getLogger()


def import_trade_date():
    """
    增量导入交易日数据导数据库表 wind_trade_date，默认导入未来300天的交易日数据
    2018-01-17 增加港股交易日数据，眼前考虑对减少对已有代码的冲击，新建一张 wind_trade_date_hk表
    日后将会考虑将两张表进行合并
    :return: 
    """
    table_name = 'wind_trade_date'
    has_table = engine_md.has_table(table_name)
    if has_table:
        with with_db_session(engine_md) as session:
            try:
                table = session.execute('SELECT exch_code,max(trade_date) FROM {table_name} GROUP BY exch_code'.format(
                    table_name=table_name))
                exch_code_trade_date_dic = {exch_code: trade_date for exch_code, trade_date in table.fetchall()}
            except Exception:
                logger.exception("交易日获取异常")
    else:
        exch_code_trade_date_dic = {}

    exchange_code_dict = {
        "HKEX": "香港",
        "NYSE": "纽约",
        "SZSE": "深圳",
        "TWSE": "台湾",
        "NASDAQ": "纳斯达克",
        "AMEX": "美国证券交易所",
        "TSE": "东京",
        "LSE": "伦敦",
        "SGX": "新加坡"
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
            trade_date_list = invoker.tdays(trade_date_start, end_date_str)
        else:
            trade_date_list = invoker.tdays(trade_date_start, end_date_str, "TradingCalendar=%s" % exchange_code)
        if trade_date_list is None:
            logger.warning("没有查询到交易日期")
        date_count = len(trade_date_list)
        if date_count > 0:
            logger.info("%d 条交易日数据将被导入", date_count)
            trade_date_df = pd.DataFrame({'trade_date': trade_date_list})
            trade_date_df['exch_code'] = exchange_code
            bunch_insert_on_duplicate_update(trade_date_df, table_name, engine_md,
                                             dtype={'trade_date': Date, 'exch_code': String(20)},
                                             myisam_if_create_table=True,
                                             primary_keys=['trade_date', 'exch_code'], schema=config.DB_SCHEMA_MD)
            logger.info('%s %d 条交易日数据导入 %s 完成', exchange_code, date_count, table_name)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s [%(name)s:%(funcName)s] %(message)s')
    import_trade_date()
