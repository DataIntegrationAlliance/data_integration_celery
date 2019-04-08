#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 19-4-8 上午9:36
@File    : check.py
@contact : mmmaaaggg@163.com
@desc    : 获取 jqdatasdk 日级别数据（前复权）
"""
import pandas as pd
import logging
from ibats_utils.mess import try_2_date, datetime_2_str, split_chunk, try_n_times, get_first, get_last, date_2_str
from tasks import app
from sqlalchemy.types import String, Date, Integer, SMALLINT
from sqlalchemy.dialects.mysql import DOUBLE
from tasks.backend import engine_md
from ibats_utils.db import with_db_session, bunch_insert_on_duplicate_update
from tasks.jqdata.stock_info import TABLE_NAME as TABLE_NAME_INFO
from tasks.backend import bunch_insert
from jqdatasdk import get_price

DEBUG = False
logger = logging.getLogger()
# 标示每天几点以后下载当日行情数据
BASE_LINE_HOUR = 16  # 目前该参数无用，当期功能在sql语句中实现
# ['open', 'close', 'low', 'high', 'volume', 'money', 'factor', 'high_limit','low_limit', 'avg', 'pre_close', 'paused']
DTYPE_QUERY = [
    ('open', DOUBLE),
    ('high', DOUBLE),
    ('low', DOUBLE),
    ('close', DOUBLE),
    ('pre_close', DOUBLE),
    ('volume', DOUBLE),
    ('money', DOUBLE),
    ('factor', DOUBLE),
    ('high_limit', DOUBLE),
    ('low_limit', DOUBLE),
    ('avg', DOUBLE),
    ('paused', SMALLINT),
]
# 设置 dtype
DTYPE = {key: val for key, val in DTYPE_QUERY}
DTYPE['jq_code'] = String(20)
DTYPE['trade_date'] = Date
TABLE_NAME = 'jq_stock_daily_md_pre'


@try_n_times(times=5, sleep_time=1, logger=logger, exception_sleep_time=60)
def invoke_daily(key_code, start_date, end_date):
    """
    调用接口获取行情数据
    :param key_code:
    :param start_date:
    :param end_date:
    :return:
    """
    fields = [_[0] for _ in DTYPE_QUERY]
    df = get_price(key_code, start_date=start_date, end_date=end_date, frequency='daily', fields=fields, fq='pre')
    if df.shape[0] > 0:
        df['jq_code'] = key_code
        df.index.rename('trade_date', inplace=True)
        df.reset_index(drop=False, inplace=True)
    return df


@app.task
def import_jq_stock_daily(chain_param=None, code_set=None):
    """
    插入股票日线数据到最近一个工作日-1。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    :return:
    """
    table_name_info = TABLE_NAME_INFO
    table_name = TABLE_NAME
    logging.info("更新 %s 开始", table_name)

    has_table = engine_md.has_table(table_name)
    # 进行表格判断，确定是否含有 jq_stock_daily_md
    if has_table:
        sql_str = f"""
            SELECT jq_code, date_frm, if(date_to<end_date, date_to, end_date) date_to
            FROM
            (
            SELECT info.jq_code, ifnull(trade_date, start_date) date_frm, end_date date_to,
            if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
            FROM 
                {table_name_info} info 
            LEFT OUTER JOIN
                (SELECT jq_code, adddate(max(trade_date),1) trade_date FROM {table_name} GROUP BY jq_code) daily
            ON info.jq_code = daily.jq_code
            ) tt
            WHERE date_frm <= if(date_to<end_date, date_to, end_date) 
            ORDER BY jq_code"""
    else:
        sql_str = f"""
            SELECT jq_code, date_frm, if(date_to<end_date, date_to, end_date) date_to
            FROM
              (
                SELECT info.jq_code, start_date date_frm, end_date date_to,
                if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
                FROM {table_name_info} info
              ) tt
            WHERE date_frm <= if(date_to<end_date, date_to, end_date)
            ORDER BY jq_code"""
        logger.warning('%s 不存在，仅使用 %s 表进行计算日期范围', table_name, table_name_info)

    sql_trade_date_str = """SELECT trade_date FROM jq_trade_date trddate 
           WHERE trade_date <= if(hour(now())<16, subdate(curdate(),1), curdate()) ORDER BY trade_date"""

    with with_db_session(engine_md) as session:
        # 获取截至当期全部交易日前
        table = session.execute(sql_trade_date_str)
        trade_date_list = [row[0] for row in table.fetchall()]
        trade_date_list.sort()
        # 获取每只股票日线数据的日期区间
        table = session.execute(sql_str)
        # 计算每只股票需要获取日线数据的日期区间
        # 获取date_from,date_to，将date_from,date_to做为value值
        code_date_range_dic = {
            key_code: (date_from, date_to)
            for key_code, date_from, date_to in table.fetchall() if
            code_set is None or key_code in code_set}

    # data_len = len(code_date_range_dic)
    data_df_list, data_count, all_data_count, data_len = [], 0, 0, len(code_date_range_dic)
    logger.info('%d stocks will been import into %s', data_len, table_name)
    # 将data_df数据，添加到data_df_list

    try:
        for num, (key_code, (date_from_tmp, date_to_tmp)) in enumerate(code_date_range_dic.items(), start=1):
            # 根据交易日数据取交集，避免不用的请求耽误时间
            date_from = get_first(trade_date_list, lambda x: x >= date_from_tmp)
            date_to = get_last(trade_date_list, lambda x: x <= date_to_tmp)
            if date_from is None or date_to is None or date_from > date_to:
                logger.debug('%d/%d) %s [%s - %s] 跳过', num, data_len, key_code, date_from, date_to)
                continue
            logger.debug('%d/%d) %s [%s - %s]', num, data_len, key_code, date_from, date_to)
            data_df = invoke_daily(key_code=key_code, start_date=date_2_str(date_from),
                                   end_date=date_2_str(date_to))
            # 把数据攒起来
            if data_df is not None and data_df.shape[0] > 0:
                data_count += data_df.shape[0]
                data_df_list.append(data_df)

            # 大于阀值有开始插入
            if data_count >= 500:
                data_df_all = pd.concat(data_df_list)
                bunch_insert(data_df_all, table_name, dtype=DTYPE, primary_keys=['jq_code', 'trade_date'])
                all_data_count += data_count
                data_df_list, data_count = [], 0

            if DEBUG and num >= 2:
                break

    finally:
        # 导入数据库
        if len(data_df_list) > 0:
            data_df_all = pd.concat(data_df_list)
            data_count = bunch_insert(data_df_all, table_name, dtype=DTYPE, primary_keys=['jq_code', 'trade_date'])
            all_data_count = all_data_count + data_count
            logging.info("更新 %s 结束 %d 条信息被更新", table_name, all_data_count)


if __name__ == "__main__":

    import_jq_stock_daily(code_set=None)
