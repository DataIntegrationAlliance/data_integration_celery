#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 19-4-12 下午6:00
@File    : block_trade.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
import logging
from datetime import date, datetime, timedelta
from ibats_utils.mess import try_2_date, STR_FORMAT_DATE, datetime_2_str, split_chunk, try_n_times
from tasks import app, config
from sqlalchemy.types import String, Date, Integer
from sqlalchemy.dialects.mysql import DOUBLE
from tasks.backend import engine_md
from ibats_utils.db import with_db_session, bunch_insert_on_duplicate_update
from tasks.backend import bunch_insert
from tasks.tushare.ts_pro_api import pro
from tasks.utils.to_sqlite import bunch_insert_sqlite

DEBUG = False
logger = logging.getLogger()
DATE_BASE = datetime.strptime('2005-01-01', STR_FORMAT_DATE).date()
ONE_DAY = timedelta(days=1)
# 标示每天几点以后下载当日行情数据
BASE_LINE_HOUR = 16
STR_FORMAT_DATE_TS = '%Y%m%d'


@try_n_times(times=5, sleep_time=0, exception_sleep_time=60)
def invoke_block_trade(trade_date):
    df = pro.block_trade(trade_date=trade_date)
    return df


@app.task
def import_tushare_block_trade(chain_param=None):
    """
    插入股票日线数据到最近一个工作日-1。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    :return:
    """
    table_name = 'tushare_block_trade'
    logging.info("更新 %s 开始", table_name)
    param_list = [
        ('trade_date', Date),
        ('ts_code', String(20)),
        ('price', DOUBLE),
        ('vol', DOUBLE),
        ('amount', DOUBLE),
        ('buyer', String(100)),
        ('seller', String(100)),
    ]

    has_table = engine_md.has_table(table_name)
    # 进行表格判断，确定是否含有 table_name

    if has_table:
        sql_str = f"""select cal_date            
                 FROM
                  (
                   select * from tushare_trade_date trddate 
                   where( cal_date>(SELECT max(trade_date) FROM  {table_name}))
                 )tt
                 where (is_open=1 
                        and cal_date <= if(hour(now())<16, subdate(curdate(),1), curdate()) 
                        and exchange='SSE') """
    else:
        # 2003-08-02 大宗交易制度开始实施
        sql_str = """SELECT cal_date FROM tushare_trade_date trddate WHERE (trddate.is_open=1 
                  AND cal_date <= if(hour(now())<16, subdate(curdate(),1), curdate()) 
                  AND exchange='SSE'  AND cal_date>='2003-08-02') ORDER BY cal_date"""
        logger.warning('%s 不存在，仅使用 tushare_trade_date 表进行计算日期范围', table_name)

    with with_db_session(engine_md) as session:
        # 获取交易日数据
        table = session.execute(sql_str)
        trade_date_list = list(row[0] for row in table.fetchall())
    # 设置 dtype
    dtype = {key: val for key, val in param_list}

    try:
        trade_date_list_len = len(trade_date_list)
        for num, trade_date in enumerate(trade_date_list, start=1):
            trade_date = datetime_2_str(trade_date, STR_FORMAT_DATE_TS)
            data_df = invoke_block_trade(trade_date=trade_date)
            if len(data_df) > 0:
                # 当前表不设置主键，由于存在重复记录，因此无法设置主键
                # 例如：002325.SZ 2014-11-17 华泰证券股份有限公司沈阳光荣街证券营业部 两笔完全相同的大宗交易
                data_count = bunch_insert(
                    data_df, table_name=table_name, dtype=dtype)
                if config.ENABLE_EXPORT_2_SQLITE:
                    bunch_insert_sqlite(data_df, mysql_table_name=table_name)
                logging.info("%d/%d) %s更新 %s 结束 %d 条信息被更新",
                             num, trade_date_list_len, trade_date, table_name, data_count)
            else:
                logging.info("%d/%d) %s 无数据信息可被更新", num, trade_date_list_len, trade_date)
    except:
        logger.exception('更新 %s 表异常', table_name)


if __name__ == "__main__":
    # DEBUG = True
    import_tushare_block_trade()
