#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/10/22 17:41
@File    : stock_tick_2_bar.py
@contact : mmmaaaggg@163.com
@desc    : 将tick数据合并成为分钟线数据
"""

from tasks.backend import engine_md
from tasks.utils.db_utils import with_db_session
import logging
import pandas as pd
logger = logging.getLogger()


def merge_tick_2_bar():
    """
    将tick数据合并成为1分钟数据
    :return: 
    """""
    table_name = 'pytdx_stock_min1'
    tick_table_name = 'pytdx_tick_temp'
    has_table = engine_md.has_table(table_name)
    sql_create_table = f"""CREATE TABLE `{table_name}` (
          `ts_code` varchar(12) NOT NULL,
          `trade_date` datetime DEFAULT NULL,
          `date` date NOT NULL,
          `time` time  NOT NULL,
          `open` DOUBLE DEFAULT NULL,
          `high` DOUBLE DEFAULT NULL,
          `low` DOUBLE DEFAULT NULL,
          `close` DOUBLE DEFAULT NULL,
          `vol` int(11) DEFAULT NULL,
          `amount` DOUBLE DEFAULT NULL,
          PRIMARY KEY (`ts_code`,`date`,`time`)
        ) ENGINE=MyISAM DEFAULT CHARSET=utf8"""
    sql_str = f"""insert ignore {table_name}
        select bar.ts_code, bar.trade_date, bar.date, bar.time, 
            tick_open.price open, bar.high, bar.low, tick_close.price close, bar.vol, bar.amount
        from 
        (
            select ts_code, trade_date, time, date, min(`index`) index_min, max(`index`) index_max, 
                max(price) high, min(price) low, sum(vol) vol, sum(price*vol) amount 
            from {tick_table_name} group by ts_code, date, time
        ) bar
        left join {tick_table_name} tick_open
            on bar.ts_code = tick_open.ts_code
            and bar.date = tick_open.date
            and bar.index_min = tick_open.index
        left join {tick_table_name} tick_close
            on bar.ts_code = tick_close.ts_code
            and bar.date = tick_close.date
            and bar.index_max = tick_close.index"""
    with with_db_session(engine_md) as session:
        # 创建表
        if not has_table:
            session.execute(sql_create_table)
            logger.info('创建表 %s ', table_name)
        # tick 数据合并成 min1 数据
        rslt = session.execute(sql_str)
        insert_count = rslt.rowcount
        logger.info('合并分钟数据 %d 条', insert_count)


if __name__ == "__main__":
    merge_tick_2_bar()

# sql_str="""insert ignore pytdx_tick_temp select * from pytdx_stock_tick where ts_code in ('000001.SZ','000050.SZ')"""
# # data=pd.read_sql(sql_str,engine_md)
# # rslt = session.execute(sql_str)
# insert_count = rslt.rowcount