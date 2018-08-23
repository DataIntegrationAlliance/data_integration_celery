# -*- coding: utf-8 -*-
"""
Created on 2017/4/20
@author: MG
@desc    : 2018-08-21 已经正式运行测试完成，可以正常使用
"""
from datetime import date, datetime, timedelta
from tasks.backend import engine_md
from tasks.utils.fh_utils import STR_FORMAT_DATE
from tasks.utils.db_utils import with_db_session, alter_table_2_myisam
from tasks.wind import invoker

import logging

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
    with with_db_session(engine_md) as session:
        try:
            table = session.execute('SELECT exch_code,max(trade_date) FROM {table_name} GROUP BY exch_code'.format(
                table_name=table_name))
            exch_code_trade_date_dic = {exch_code: trade_date for exch_code, trade_date in table.fetchall()}
        except Exception:
            logger.exception("交易日获取异常")

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
            with with_db_session(engine_md) as session:
                session.execute(
                    "INSERT INTO {table_name} (trade_date,exch_code) VALUE (:trade_date,:exch_code)".format(
                        table_name=table_name),
                    params=[{'trade_date': trade_date, 'exch_code': exchange_code} for trade_date in
                            trade_date_list])
            # bunch_insert_on_duplicate_update(trade_date_list, table_name, engine_md, dtype=dtype)
            logger.info('%s %d 条交易日数据导入 %s 完成', exchange_code, date_count, table_name)
            if not has_table and engine_md.has_table(table_name):
                alter_table_2_myisam(engine_md, [table_name])
                create_pk_str = """ALTER TABLE {TABLE_NAME}
                CHANGE COLUMN 'KEY' 'KEY' VARCHAR(20) NOT NULL FIRST ,
                CHANGE COLUMN 'trade_date' 'trade_date' DATA NOT NULL AFTER 'KEY',
                ADD PRIMARY KEY ('key','trade_date')""".format(table_name=table_name)
                with with_db_session(engine_md) as session:
                    session.execute(create_pk_str)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s [%(name)s:%(funcName)s] %(message)s')
    import_trade_date()
