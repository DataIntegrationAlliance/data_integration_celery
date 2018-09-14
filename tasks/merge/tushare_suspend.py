#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/9/13 9:37
@File    : tushare_suspend.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
import pandas as pd
import itertools
from collections import defaultdict
from tasks.backend import engine_md
from tasks.utils.db_utils import with_db_session
from tasks.utils.fh_utils import is_any
from tasks.tushare.tushare_stock_daily.adj_factor import DTYPE_TUSHARE_STOCK_DAILY_ADJ_FACTOR
from tasks.tushare.tushare_stock_daily.daily_basic import DTYPE_TUSHARE_STOCK_DAILY_BASIC
from tasks.tushare.tushare_stock_daily.stock import DTYPE_TUSHARE_STOCK_DAILY_MD


def get_tushare_daily_merged_df(date_from=None) -> pd.DataFrame:
    col_names = [col_name for col_name in itertools.chain(
        DTYPE_TUSHARE_STOCK_DAILY_BASIC, DTYPE_TUSHARE_STOCK_DAILY_MD, DTYPE_TUSHARE_STOCK_DAILY_ADJ_FACTOR)
                 if col_name not in ('ts_code', 'trade_date', 'close')]
    if len(col_names) == 0:
        col_names_str = ""
    else:
        col_names_str = ",\n  `" + "`, `".join(col_names) + "`"

    if date_from is None:
        sql_str = """SELECT md.ts_code, md.trade_date, ifnull(md.close, basic.close) close {col_names}
            FROM
            (
                SELECT * FROM tushare_stock_daily_md
            ) md
            LEFT OUTER JOIN
            (
                SELECT * FROM tushare_stock_daily_basic 
            ) basic
            ON md.ts_code = basic.ts_code
            AND md.trade_date = basic.trade_date
            LEFT OUTER JOIN
            (
                SELECT * FROM tushare_stock_daily_adj_factor
            ) adj_factor
            ON md.ts_code = adj_factor.ts_code
            AND md.trade_date = adj_factor.trade_date""".format(col_names=col_names_str)
        data_df = pd.read_sql(sql_str, engine_md)
    else:
        sql_str = """SELECT md.ts_code, md.trade_date, ifnull(md.close, basic.close) close {col_names}
            FROM
            (
                SELECT * FROM tushare_stock_daily_md WHERE trade_date >= %s
            ) md
            LEFT OUTER JOIN
            (
                SELECT * FROM tushare_stock_daily_basic WHERE trade_date >= %s
            ) basic
            ON md.ts_code = basic.ts_code
            AND md.trade_date = basic.trade_date
            LEFT OUTER JOIN
            (
                SELECT * FROM tushare_stock_daily_adj_factor WHERE trade_date >= %s
            ) adj_factor
            ON md.ts_code = adj_factor.ts_code
            AND md.trade_date = adj_factor.trade_date""".format(col_names=col_names_str)
        data_df = pd.read_sql(sql_str, engine_md, params=[date_from, date_from, date_from])
    return data_df


def get_suspend_to_dic():
    """将 tushare_stock_daily_suspend 转换成日期范围字典，key：股票代码；value：日期范围"""
    with with_db_session(engine_md) as session:
        sql_str = """SELECT ts_code, suspend_date, resume_date FROM tushare_stock_daily_suspend"""
        table = session.execute(sql_str)
        code_date_range_dic = defaultdict(list)
        for ts_code, suspend_date, resume_date in table.fetchall():
            if suspend_date is None:
                continue
            code_date_range_dic[ts_code].append(
                (suspend_date, suspend_date if resume_date is None else resume_date))

    return code_date_range_dic


def is_suspend(code_date_range_dic, code_trade_date_s):
    ts_code = code_trade_date_s['ts_code']
    date_cur = code_trade_date_s['trade_date']
    data_range_list = code_date_range_dic[ts_code]
    # print(code_trade_date_s)
    return is_any(data_range_list, lambda date_range: date_range[0] <= date_cur <= date_range[1])


if __name__ == "__main__":
    data_df = get_tushare_daily_merged_df()
    code_date_range_dic = get_suspend_to_dic()
    data_df['suspend'] = data_df[['ts_code', 'trade_date']].apply(lambda x: is_suspend(code_date_range_dic, x), axis=1)

