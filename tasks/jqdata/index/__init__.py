#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 19-4-19 下午3:02
@File    : __init__.py.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
import logging
import pandas as pd
from datetime import date, datetime, timedelta
from ibats_utils.mess import try_2_date, STR_FORMAT_DATE, get_first_idx, get_last_idx
from tasks.backend import engine_md, bunch_insert
from ibats_utils.db import with_db_session

logger = logging.getLogger(__name__)
DATE_BASE = datetime.strptime('2005-01-01', STR_FORMAT_DATE).date()
ONE_DAY = timedelta(days=1)
# 标示每天几点以后下载当日行情数据
BASE_LINE_HOUR = 16


def import_data(table_name, dtype, invoke_api,
                primary_keys=["index_symbol", "trade_date", "jq_code"], ts_code_set=None, is_debug=False, is_monthly=False):
    """
    插入股票日线数据到最近一个工作日-1。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    :return:
    """
    info_table = 'jq_index_info'
    # table_name = 'jq_index_stocks'
    # primary_keys = ["index_symbol", "trade_date", "jq_code"]
    logging.info("更新 %s 开始", table_name)
    has_table = engine_md.has_table(table_name)
    # 进行表格判断，确定是否含有tushare_stock_daily
    if has_table:
        sql_str = f"""
            SELECT jq_code, date_from, if(date_to<end_date, date_to, end_date) date_to
            FROM
            (
            SELECT info.jq_code, ifnull(trade_date, start_date) date_from, end_date date_to,
            if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
            FROM 
                {info_table} info 
            LEFT OUTER JOIN
                (SELECT index_symbol, adddate(max(trade_date),1) trade_date 
                FROM {table_name} GROUP BY index_symbol) daily
            ON info.jq_code = daily.index_symbol
            ) tt
            WHERE date_from <= if(date_to<end_date, date_to, end_date) 
            ORDER BY jq_code"""
    else:
        sql_str = f"""
            SELECT jq_code, date_from, if(date_to<end_date, date_to, end_date) date_to
            FROM
              (
                SELECT info.jq_code, start_date date_from, end_date date_to,
                if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
                FROM {info_table} info 
              ) tt
            WHERE date_from <= if(date_to<end_date, date_to, end_date) 
            ORDER BY jq_code"""
        logger.warning('%s 不存在，仅使用 tushare_stock_info 表进行计算日期范围', table_name)

    sql_trade_date_str = """
       SELECT trade_date FROM jq_trade_date trddate 
       WHERE trade_date <= if(hour(now())<16, subdate(curdate(),1), curdate()) 
       ORDER BY trade_date"""

    with with_db_session(engine_md) as session:
        table = session.execute(sql_trade_date_str)
        trade_date_list = [row[0] for row in table.fetchall()]
        trade_date_list.sort()
        # 获取每只股票需要获取日线数据的日期区间
        table = session.execute(sql_str)
        begin_time = None
        # 获取date_from,date_to，将date_from,date_to做为value值
        code_date_range_dic = {
            ts_code: (date_from if begin_time is None else min([date_from, begin_time]), date_to)
            for ts_code, date_from, date_to in table.fetchall() if
            ts_code_set is None or ts_code in ts_code_set}

    # data_len = len(code_date_range_dic)
    data_df_list, data_count, all_data_count, data_len = [], 0, 0, len(code_date_range_dic)
    logger.info('%d records will been import into %s', data_len, table_name)
    # 将data_df数据，添加到data_df_list

    try:
        for num, (index_symbol, (date_from_tmp, date_to_tmp)) in enumerate(code_date_range_dic.items(), start=1):
            date_from_idx = get_first_idx(trade_date_list, lambda x: x >= date_from_tmp)
            date_to_idx = get_last_idx(trade_date_list, lambda x: x <= date_to_tmp)
            if date_from_idx is None or date_to_idx is None or date_from_idx > date_to_idx:
                logger.debug('%d/%d) %s [%s - %s] 跳过', num, data_len, index_symbol,
                             trade_date_list[date_from_idx] if date_from_idx is not None else None,
                             trade_date_list[date_to_idx] if date_to_idx is not None else None)
                continue
            if is_monthly:
                date_sample = trade_date_list[date_from_idx: (date_to_idx + 1)]
                date_sample = list(
                    pd.Series(date_sample, index=pd.DatetimeIndex(date_sample)
                              ).resample(rule='M', convention='end').last()
                )
            else:
                date_sample = trade_date_list[date_from_idx: (date_to_idx + 1)]

            date_from, date_to = date_sample[0], date_sample[-1]
            trade_date_count = len(date_sample)
            logger.debug('%d/%d) 开始导入 %s [%s - %s] %d 个交易日的数据 %s',
                         num, data_len, index_symbol, date_from, date_to, trade_date_count,
                         '月度更新' if is_monthly else '')
            for trade_date in date_sample:
                data_df = invoke_api(index_symbol=index_symbol, trade_date=trade_date)

                # 把数据攒起来
                if data_df is not None and data_df.shape[0] > 0:
                    data_count += data_df.shape[0]
                    data_df_list.append(data_df)

                # 大于阀值有开始插入
                if data_count >= 1000:
                    data_count = bunch_insert(data_df_list, table_name=table_name, dtype=dtype,
                                              primary_keys=primary_keys)
                    all_data_count += data_count
                    data_df_list, data_count = [], 0

                if is_debug and len(data_df_list) > 1:
                    break
    except:
        logger.exception("%s 获取数据异常", table_name)
    finally:
        # 导入数据库
        if len(data_df_list) > 0:
            data_count = bunch_insert(data_df_list, table_name=table_name, dtype=dtype, primary_keys=primary_keys)
            all_data_count += data_count
            logging.info("更新 %s 结束 %d 条信息被更新", table_name, all_data_count)


if __name__ == "__main__":
    pass
