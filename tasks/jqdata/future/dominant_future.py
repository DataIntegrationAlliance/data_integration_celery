#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 19-4-22 上午10:37
@File    : dominant_future.py
@contact : mmmaaaggg@163.com
@desc    : 获取主力合约名称
"""
import pandas as pd
import logging
from ibats_utils.mess import get_first, get_last, date_2_str, str_2_date, get_first_idx, get_last_idx
from tasks import app
from sqlalchemy.types import String, Date, SMALLINT
from sqlalchemy.dialects.mysql import DOUBLE
from tasks.backend import engine_md, bunch_insert, execute_sql_commit
from ibats_utils.db import with_db_session
from tasks.jqdata.future.future_info import TABLE_NAME as TABLE_NAME_INFO
from tasks.jqdata import get_price, get_dominant_future

DEBUG = False
logger = logging.getLogger()
# 标示每天几点以后下载当日行情数据
BASE_LINE_HOUR = 16  # 目前该参数无用，当期功能在sql语句中实现
# ['open', 'close', 'low', 'high', 'volume', 'money', 'factor', 'high_limit','low_limit', 'avg', 'pre_close', 'paused']
DTYPE_QUERY = [
    ('jq_code', String(20)),
    ('trade_date', Date),
    ('underlying_symbol', String(6)),
]
# 设置 dtype
DTYPE = {key: val for key, val in DTYPE_QUERY}
TABLE_NAME = 'jq_future_dominant_future'


def invoke_api(underlying_symbol, date_list):
    """
    调用接口获取行情数据
    :param underlying_symbol:
    :param date_list:
    :return:
    """
    ret_dic_list = []
    for num, date_str in enumerate(date_list):
        jq_code = get_dominant_future(underlying_symbol, date_2_str(date_str))
        if jq_code is None or len(jq_code) == 0:
            continue
        ret_dic = {
            'trade_date': str_2_date(date_str),
            'jq_code': jq_code,
            'underlying_symbol': underlying_symbol,
        }
        ret_dic_list.append(ret_dic)
        if DEBUG and num >= 2:
            break

    df = pd.DataFrame(ret_dic_list)
    return df


@app.task
def import_jq_dominant_future_daily(chain_param=None, code_set=None):
    """
    插入股票日线数据到最近一个工作日-1。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    :return:
    """
    table_name_info = TABLE_NAME_INFO
    table_name = TABLE_NAME
    logging.info("更新 %s 开始", table_name)

    # 根据 info table 查询每只股票日期区间
    has_table = engine_md.has_table(table_name)
    # 进行表格判断，确定是否含有 jq_stock_daily_md
    if has_table:
        # 这里对原始的 sql语句进行了调整
        # 以前的逻辑：每只股票最大的一个交易日+1天作为起始日期
        # 现在的逻辑：每只股票最大一天的交易日作为起始日期
        # 主要原因在希望通过接口获取到数据库中现有最大交易日对应的 factor因子以进行比对
        sql_trade_date_range_str = f"""
            SELECT underlying_symbol, date_frm, if(date_to<end_date, date_to, end_date) date_to
            FROM
            (
                SELECT info.underlying_symbol, ifnull(trade_date, info.date_frm) date_frm, info.date_to date_to,
                if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
                FROM 
                    (
                        SELECT info.underlying_symbol, min(start_date) date_frm, max(end_date) date_to,
                        if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
                        FROM {table_name_info} info group by underlying_symbol
                    ) info 
                LEFT OUTER JOIN
                    (
                        SELECT underlying_symbol, max(trade_date) trade_date 
                        FROM {table_name} GROUP BY underlying_symbol
                    ) daily
                ON info.underlying_symbol = daily.underlying_symbol
            ) tt
            WHERE date_frm < if(date_to<end_date, date_to, end_date) 
            ORDER BY underlying_symbol"""

    else:
        sql_trade_date_range_str = f"""
            SELECT underlying_symbol, date_frm, if(date_to<end_date, date_to, end_date) date_to
            FROM
              (
                SELECT info.underlying_symbol, min(start_date) date_frm, max(end_date) date_to,
                if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
                FROM {table_name_info} info group by underlying_symbol
              ) tt
            WHERE date_frm <= if(date_to<end_date, date_to, end_date)
            ORDER BY underlying_symbol"""
        logger.warning('%s 不存在，仅使用 %s 表进行计算日期范围', table_name, table_name_info)

    sql_trade_date_str = """SELECT trade_date FROM jq_trade_date trddate 
       WHERE trade_date <= if(hour(now())<16, subdate(curdate(),1), curdate()) ORDER BY trade_date"""

    with with_db_session(engine_md) as session:
        # 获取截至当期全部交易日前
        table = session.execute(sql_trade_date_str)
        trade_date_list = [row[0] for row in table.fetchall()]
        trade_date_list.sort()
        # 获取每只股票日线数据的日期区间
        table = session.execute(sql_trade_date_range_str)
        # 计算每只股票需要获取日线数据的日期区间
        # 获取date_from,date_to，将date_from,date_to做为value值
        code_date_range_dic = {
            key_code: (date_from, date_to)
            for key_code, date_from, date_to in table.fetchall() if
            code_set is None or key_code in code_set}

    # data_len = len(code_date_range_dic)
    data_df_list, data_count, all_data_count, data_len = [], 0, 0, len(code_date_range_dic)
    logger.info('%d 数据将被导入 %s', data_len, table_name)
    # 将data_df数据，添加到data_df_list

    try:
        for num, (key_code, (date_from_tmp, date_to_tmp)) in enumerate(code_date_range_dic.items(), start=1):
            try:

                # 根据交易日数据取交集，避免不用的请求耽误时间
                date_from_idx = get_first_idx(trade_date_list, lambda x: x >= date_from_tmp)
                date_to_idx = get_last_idx(trade_date_list, lambda x: x <= date_to_tmp)
                if date_from_idx is None or date_to_idx is None or date_from_idx >= date_to_idx:
                    logger.debug('%d/%d) %s [%s - %s] 跳过',
                                 num, data_len, key_code,
                                 trade_date_list[date_from_idx] if date_from_idx is not None else None,
                                 trade_date_list[date_to_idx] if date_to_idx is not None else None,
                                 )
                    break
                logger.debug('%d/%d) %s [%s - %s]',
                             num, data_len, key_code,
                             trade_date_list[date_from_idx],
                             trade_date_list[date_to_idx])
                data_df = invoke_api(key_code, trade_date_list[date_from_idx:(date_to_idx + 1)])

            except Exception as exp:
                data_df = None
                logger.exception('%s [%s - %s]', key_code, date_2_str(date_from_tmp), date_2_str(date_to_tmp))
                if exp.args[0].find('超过了每日最大查询限制'):
                    break

            # 把数据攒起来
            if data_df is not None and data_df.shape[0] > 0:
                data_count += data_df.shape[0]
                data_df_list.append(data_df)

            # 大于阀值有开始插入
            if data_count >= 500:
                data_df_all = pd.concat(data_df_list)
                bunch_insert(data_df_all, table_name, dtype=DTYPE,
                             primary_keys=['underlying_symbol', 'trade_date'])
                all_data_count += data_count
                data_df_list, data_count = [], 0

            if DEBUG and num >= 2:
                break

    finally:
        # 导入数据库
        if len(data_df_list) > 0:
            data_df_all = pd.concat(data_df_list)
            data_count = bunch_insert(data_df_all, table_name, dtype=DTYPE,
                                      primary_keys=['underlying_symbol', 'trade_date'])
            all_data_count = all_data_count + data_count
            logging.info("更新 %s 结束 %d 条信息被更新", table_name, all_data_count)


if __name__ == "__main__":
    # DEBUG = True
    import_jq_dominant_future_daily(code_set=None)
