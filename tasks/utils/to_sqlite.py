#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 19-4-11 上午9:49
@File    : to_sqlite.py
@contact : mmmaaaggg@163.com
@desc    : 用于将 mysql 数据库表转换成 sqlite 表
"""
from ibats_utils.db import with_db_session
from ibats_utils.mess import get_folder_path, split_chunk
import os
import sqlite3
import pandas as pd
from tasks.backend import engine_md
import logging


logger = logging.getLogger(__name__)


def tushare_to_sqlite(file_name, table_name):
    logger.info('mysql %s 导入到 sqlite %s 开始', table_name, file_name)
    sqlite_db_folder_path = get_folder_path('sqlite_db', create_if_not_found=False)
    db_file_path = os.path.join(sqlite_db_folder_path, file_name)
    conn = sqlite3.connect(db_file_path)
    sql_str = f"select ts_code from {table_name} group by ts_code"
    with with_db_session(engine_md) as session:
        table = session.execute(sql_str)
        code_list = list([row[0] for row in table.fetchall()])

    code_count, data_count = len(code_list), 0
    for num, (ts_code) in enumerate(code_list, start=1):
        code_exchange = ts_code.split('.')
        sqlite_table_name = f"{code_exchange[1]}{code_exchange[0]}"
        sql_str = f"select * from {table_name} where ts_code=%s"    # where code = '000001.XSHE'
        df = pd.read_sql(sql_str, engine_md, params=[ts_code])      #
        df_len = df.shape[0]
        data_count += df_len
        logger.debug('%4d/%d) %s -> %s %d 条记录', num, code_count, table_name, sqlite_table_name, df_len)
        df.to_sql(sqlite_table_name, conn, index=False, if_exists='replace')

    logger.info('mysql %s 导入到 sqlite %s 结束，导出数据 %d 条', table_name, file_name, data_count)


def tushare_to_sqlite_batch(file_name, table_name, batch_size=500):
    logger.info('mysql %s 导入到 sqlite %s 开始', table_name, file_name)
    sqlite_db_folder_path = get_folder_path('sqlite_db', create_if_not_found=False)
    db_file_path = os.path.join(sqlite_db_folder_path, file_name)
    conn = sqlite3.connect(db_file_path)
    sql_str = f"select ts_code from {table_name} group by ts_code"
    with with_db_session(engine_md) as session:
        table = session.execute(sql_str)
        code_list = list([row[0] for row in table.fetchall()])

    code_count, data_count, num = len(code_list), 0, 0
    for code_sub_list in split_chunk(code_list, batch_size):
        in_clause = ", ".join([r'%s' for _ in code_sub_list])
        sql_str = f"select ts_code from {table_name} where ts_code in ({in_clause})"
        df_tot = pd.read_sql(sql_str, engine_md, params=code_sub_list)
        dfg = df_tot.groupby('ts_code')
        for num, (ts_code, df) in enumerate(dfg, start=num + 1):
            code_exchange = ts_code.split('.')
            sqlite_table_name = f"{code_exchange[1]}{code_exchange[0]}"
            df_len = df.shape[0]
            data_count += df_len
            logger.debug('%4d/%d) %s -> %s %d 条记录', num, code_count, table_name, sqlite_table_name, df_len)
            df.to_sql(sqlite_table_name, conn, index=False, if_exists='replace')

    logger.info('mysql %s 导入到 sqlite %s 结束，导出数据 %d 条', table_name, file_name, data_count)


def tushare_to_sqlite_tot_select(file_name, table_name):
    logger.info('mysql %s 导入到 sqlite %s 开始', table_name, file_name)
    sqlite_db_folder_path = get_folder_path('sqlite_db', create_if_not_found=False)
    db_file_path = os.path.join(sqlite_db_folder_path, file_name)
    conn = sqlite3.connect(db_file_path)
    sql_str = f"select * from {table_name}"
    df_tot = pd.read_sql(sql_str, engine_md)  #
    dfg = df_tot.groupby('ts_code')
    num, code_count, data_count = 0, len(dfg), 0
    for num, (ts_code, df) in enumerate(dfg, start=1):
        code_exchange = ts_code.split('.')
        sqlite_table_name = f"{code_exchange[1]}{code_exchange[0]}"
        df_len = df.shape[0]
        data_count += df_len
        logger.debug('%4d/%d) %s -> %s %d 条记录', num, code_count, table_name, sqlite_table_name, df_len)
        df.to_sql(sqlite_table_name, conn, index=False, if_exists='replace')

    logger.info('mysql %s 导入到 sqlite %s 结束，导出数据 %d 条', table_name, file_name, data_count)


if __name__ == "__main__":
    file_name = 'stock_daily.db'
    table_name = 'tushare_stock_daily_md'
    batch_size = 100
    tushare_to_sqlite_batch(file_name, table_name, batch_size=batch_size)
