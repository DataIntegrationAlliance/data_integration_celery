# -*- coding: utf-8 -*-
"""
Created on 2017/4/20
@author: MG
"""
from datetime import date, datetime, timedelta
from sqlalchemy.types import String, Date, Integer
from tasks import app
from tasks.ifind import invoker
from tasks.utils.fh_utils import STR_FORMAT_DATE
from tasks.utils.db_utils import with_db_session
from tasks.backend import engine_md
import logging
logger = logging.getLogger()


@app.task
def import_trade_date():
    """
    增量导入交易日数据导数据库表 wind_trade_date，默认导入未来300天的交易日数据
    2018-01-17 增加港股交易日数据，眼前考虑对减少对已有代码的冲击，新建一张 wind_trade_date_hk表
    日后将会考虑将两张表进行合并
    :return: 
    """
    exch_code_trade_date_dic = {}
    with with_db_session(engine_md) as session:
        try:
            table = session.execute('SELECT exch_code,max(trade_date) FROM ifind_trade_date GROUP BY exch_code')
            exch_code_trade_date_dic = {exch_code: trade_date for exch_code, trade_date in table.fetchall()}
        except Exception as exp:
            logger.exception("交易日获取异常")

    exchange_code_dict = {
        # "HKEX": "香港",
        # "NYSE": "纽约",
        "SZSE": "深圳",
        # "TWSE": "台湾",
        # "NASDAQ": "纳斯达克",
        # "AMEX": "美国证券交易所",
        # "TSE": "东京",
        # "LSE": "伦敦",
        # "SGX": "新加坡"
    }
    exchange_code_list = list(exchange_code_dict.keys())
    for exchange_code in exchange_code_list:
        if exchange_code in exch_code_trade_date_dic:
            trade_date_max = exch_code_trade_date_dic[exchange_code]
            start_date_str = (trade_date_max + timedelta(days=1)).strftime(STR_FORMAT_DATE)
        else:
            start_date_str = '1980-01-01'

        end_date_str = (date.today() + timedelta(days=310)).strftime(STR_FORMAT_DATE)
        trade_date_df = invoker.THS_DateQuery(exchange_code, 'dateType:0,period:D,dateFormat:0', start_date_str, end_date_str)
        if trade_date_df is None or trade_date_df.shape[0] == 0:
            logger.warning("%s [%s - %s] 没有查询到交易日期", exchange_code, start_date_str, end_date_str)
            continue
        date_count = trade_date_df.shape[0]
        logger.info("%d 条交易日数据将被导入", date_count)
        # with with_db_session(engine_md) as session:
        #     session.execute("INSERT INTO ifind_trade_date (trade_date,exch_code) VALUE (:trade_date,:exch_code)",
        #                     params=[{'trade_date': trade_date, 'exch_code': exchange_code} for trade_date in
        #                             trade_date_df['time']])
        trade_date_df['exch_code'] = exchange_code
        trade_date_df.rename(columns={'time': 'trade_date'}, inplace=True)
        trade_date_df.to_sql('ifind_trade_date', engine_md, if_exists='append', index=False, dtype={
            'exch_code': String(10),
            'trade_date': Date,
        })
        logger.info('%d 条交易日数据导入 %s 完成', date_count, 'ifind_trade_date')


if __name__ == "__main__":
    # 导入日期数据
    import_trade_date()
