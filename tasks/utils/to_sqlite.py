#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 19-4-11 上午9:49
@File    : to_sqlite.py
@contact : mmmaaaggg@163.com
@desc    : 用于将 mysql 数据库表转换成 sqlite 表
"""
from collections import defaultdict

from ibats_utils.db import with_db_session
from ibats_utils.mess import get_folder_path, split_chunk
import os
import sqlite3
import pandas as pd
from tasks.backend import engine_md
import logging

logger = logging.getLogger(__name__)


def tushare_to_sqlite_pre_ts_code(file_name, table_name, field_pair_list):
    """
    将Mysql数据导入到sqlite，全量读取然后导出
    速度慢，出发内存比较少，或需要导出的数据不多，否则不需要使用
    :param file_name:
    :param table_name:
    :return:
    """
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
        sql_str = f"select * from {table_name} where ts_code=%s"  # where code = '000001.XSHE'
        df = pd.read_sql(sql_str, engine_md, params=[ts_code])  #
        if field_pair_list is not None:
            field_list = [_[0] for _ in field_pair_list]
            field_list.append('ts_code')
            df_tot = df_tot[field_list].rename(columns=dict(field_pair_list))

        df_len = df.shape[0]
        data_count += df_len
        logger.debug('%4d/%d) mysql %s -> sqlite %s %s %d 条记录',
                     num, code_count, table_name, file_name, sqlite_table_name, df_len)
        df.to_sql(sqlite_table_name, conn, index=False, if_exists='replace')

    logger.info('mysql %s 导入到 sqlite %s 结束，导出数据 %d 条', table_name, file_name, data_count)


def tushare_to_sqlite_batch(file_name, table_name, field_pair_list, batch_size=500, **kwargs):
    """
    将Mysql数据导入到sqlite，全量读取然后导出
    速度适中，可更加 batch_size 调剂对内存的需求
    :param file_name:
    :param table_name:
    :param field_pair_list:
    :param batch_size:
    :param **kwargs:
    :return:
    """
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
        sql_str = f"select * from {table_name} where ts_code in ({in_clause})"
        df_tot = pd.read_sql(sql_str, engine_md, params=code_sub_list)
        # 对 fields 进行筛选及重命名
        if field_pair_list is not None:
            field_list = [_[0] for _ in field_pair_list]
            field_list.append('ts_code')
            df_tot = df_tot[field_list].rename(columns=dict(field_pair_list))

        dfg = df_tot.groupby('ts_code')
        for num, (ts_code, df) in enumerate(dfg, start=num + 1):
            code_exchange = ts_code.split('.')
            sqlite_table_name = f"{code_exchange[1]}{code_exchange[0]}"
            df_len = df.shape[0]
            data_count += df_len
            logger.debug('%4d/%d) mysql %s -> sqlite %s %s %d 条记录',
                         num, code_count, table_name, file_name, sqlite_table_name, df_len)
            df.drop('ts_code', axis=1, inplace=True)
            df.to_sql(sqlite_table_name, conn, index=False, if_exists='replace')

    logger.info('mysql %s 导入到 sqlite %s 结束，导出数据 %d 条', table_name, file_name, data_count)


def tushare_to_sqlite_tot_select(file_name, table_name, field_pair_list):
    """
    将Mysql数据导入到sqlite，全量读取然后导出
    速度快，对内存要求较高
    :param file_name:
    :param table_name:
    :return:
    """
    logger.info('mysql %s 导入到 sqlite %s 开始', table_name, file_name)
    sqlite_db_folder_path = get_folder_path('sqlite_db', create_if_not_found=False)
    db_file_path = os.path.join(sqlite_db_folder_path, file_name)
    conn = sqlite3.connect(db_file_path)
    sql_str = f"select * from {table_name}"
    df_tot = pd.read_sql(sql_str, engine_md)  #
    # 对 fields 进行筛选及重命名
    if field_pair_list is not None:
        field_list = [_[0] for _ in field_pair_list]
        field_list.append('ts_code')
        df_tot = df_tot[field_list].rename(columns=dict(field_pair_list))

    dfg = df_tot.groupby('ts_code')
    num, code_count, data_count = 0, len(dfg), 0
    for num, (ts_code, df) in enumerate(dfg, start=1):
        code_exchange = ts_code.split('.')
        sqlite_table_name = f"{code_exchange[1]}{code_exchange[0]}"
        df_len = df.shape[0]
        data_count += df_len
        logger.debug('%4d/%d) mysql %s -> sqlite %s %s %d 条记录',
                     num, code_count, table_name, file_name, sqlite_table_name, df_len)
        df.to_sql(sqlite_table_name, conn, index=False, if_exists='replace')

    logger.info('mysql %s 导入到 sqlite %s 结束，导出数据 %d 条', table_name, file_name, data_count)


def transfer_mysql_to_sqlite():
    """
    mysql 转化为 sqlite
    :return:
    """
    transfer_param_list = [
        {
            "doit": True,
            "file_name": 'eDB_Dailybar.db',
            "table_name": 'tushare_stock_daily_md',
            "field_pair_list": [
                ('trade_date', 'Date'),
                ('open', 'Open'),
                ('high', 'High'),
                ('low', 'Low'),
                ('close', 'Close'),
                ('vol', 'Volume'),
                ('amount', 'Amount'),
            ],
            "batch_size": 200,
        },
        {
            "doit": True,
            "file_name": 'eDB_Dailybasic.db',
            "table_name": 'tushare_stock_daily_basic',
            "field_pair_list": [
                ('trade_date', 'Date'),
                ('pe', 'PE'),
                ('pe_ttm', 'PE_TTM'),
                ('pb', 'PB'),
                ('ps', 'PS'),
                ('ps_ttm', 'PS_TTM'),
                ('total_share', 'Total_Share'),
                ('float_share', 'Float_Share'),
                ('total_mv', 'Total_MV'),
                ('circ_mv', 'Circ_MV'),
            ],
            "batch_size": 200,
        },
        {
            "doit": True,
            "file_name": 'eDB_adjfactor.db',
            "table_name": 'tushare_stock_daily_adj_factor',
            "field_pair_list": [
                ('trade_date', 'Date'),
                ('adj_factor', 'adj_factor'),
            ],
            "batch_size": 200,
        },
        {
            "doit": True,
            "file_name": 'eDB_CashFlow.db',
            "table_name": 'tushare_stock_cashflow',
            "field_pair_list": [
                ('ann_date', 'ann_date'),
                ('f_ann_date', 'f_ann_date'),
                ('end_date', 'end_date'),
                ('report_type', 'report_type'),
                ('c_cash_equ_end_period', 'c_cash_equ_end_period'),
                ('n_cashflow_act', 'n_cashflow_act'),
                ('net_profit', 'net_profit'),
            ],
            "batch_size": 200,
        },
        {
            "doit": True,
            "file_name": 'eDB_Balancesheet.db',
            "table_name": 'tushare_stock_balancesheet',
            "field_pair_list": [
                ('ann_date', 'ann_date'),
                ('f_ann_date', 'f_ann_date'),
                ('end_date', 'end_date'),
                ('report_type', 'report_type'),
                ('cap_rese', 'cap_rese'),
                ('undistr_porfit', 'undistr_porfit'),
                ('surplus_rese', 'surplus_rese'),
                ('money_cap', 'money_cap'),
                ('trad_asset', 'trad_asset'),
                ('notes_receiv', 'notes_receiv'),
                ('accounts_receiv', 'accounts_receiv'),
                ('oth_receiv', 'oth_receiv'),
                ('prepayment', 'prepayment'),
                ('div_receiv', 'div_receiv'),
                ('int_receiv', 'int_receiv'),
                ('inventories', 'inventories'),
                ('amor_exp', 'amor_exp'),
                ('nca_within_1y', 'nca_within_1y'),
                ('oth_cur_assets', 'oth_cur_assets'),
                ('total_cur_assets', 'total_cur_assets'),
                ('fa_avail_for_sale', 'fa_avail_for_sale'),
                ('htm_invest', 'htm_invest'),
                ('time_deposits', 'time_deposits'),
                ('oth_assets', 'oth_assets'),
                ('lt_rec', 'lt_rec'),
                ('fix_assets', 'fix_assets'),
                ('intan_assets', 'intan_assets'),
                ('r_and_d', 'r_and_d'),
                ('goodwill', 'goodwill'),
                ('lt_amor_exp', 'lt_amor_exp'),
                ('oth_nca', 'oth_nca'),
                ('total_nca', 'total_nca'),
                ('depos_in_oth_bfi', 'depos_in_oth_bfi'),
                ('invest_as_receiv', 'invest_as_receiv'),
                ('total_assets', 'total_assets'),
                ('lt_borr', 'lt_borr'),
                ('st_borr', 'st_borr'),
                ('loan_oth_bank', 'loan_oth_bank'),
                ('trading_fl', 'trading_fl'),
                ('notes_payable', 'notes_payable'),
                ('acct_payable', 'acct_payable'),
                ('adv_receipts', 'adv_receipts'),
                ('int_payable', 'int_payable'),
                ('div_payable', 'div_payable'),
                ('acc_exp', 'acc_exp'),
                ('deferred_inc', 'deferred_inc'),
                ('st_bonds_payable', 'st_bonds_payable'),
                ('non_cur_liab_due_1y', 'non_cur_liab_due_1y'),
                ('oth_cur_liab', 'oth_cur_liab'),
                ('total_cur_liab', 'total_cur_liab'),
                ('lt_payable', 'lt_payable'),
                ('defer_tax_liab', 'defer_tax_liab'),
                ('oth_ncl', 'oth_ncl'),
                ('total_ncl', 'total_ncl'),
                ('deriv_liab', 'deriv_liab'),
                ('oth_liab', 'oth_liab'),
                ('total_liab', 'total_liab'),
                ('total_liab_hldr_eqy', 'total_liab_hldr_eqy'),
                ('oth_eqt_tools_p_shr', 'oth_eqt_tools_p_shr'),
                ('acc_receivable', 'acc_receivable'),
                ('st_fin_payable', 'st_fin_payable'),
                ('payables', 'payables'),
                ('hfs_assets', 'hfs_assets'),
                ('hfs_sales', 'hfs_sales'),
                ('minority_int', 'minority_int'),
            ],
            "batch_size": 200,
        },
    ]
    # batch_size = 200
    # tushare_to_sqlite_batch(file_name, table_name, field_pair_list, batch_size=batch_size)
    # tushare_to_sqlite_pre_ts_code(file_name, table_name, field_pair_list)
    # tushare_to_sqlite_tot_select(file_name, table_name, field_pair_list)
    transfer_param_list_len = len(transfer_param_list)
    for num, dic in enumerate(transfer_param_list, start=1):
        if dic['doit']:
            logger.info("%d/%d) 转化 %s -> %s", num, transfer_param_list_len, dic["table_name"], dic["file_name"])
            tushare_to_sqlite_batch(**dic)


def get_sqlite_conn(file_name):
    sqlite_db_folder_path = get_folder_path('sqlite_db', create_if_not_found=False)
    db_file_path = os.path.join(sqlite_db_folder_path, file_name)
    conn = sqlite3.connect(db_file_path)
    return conn


def check_match_column(table_name_mysql, file_name_sqlite, match_pairs=None, table_name_sqlite='SH600000'):
    """
    对比两张表字段是否一致，不一致的字段分别列出来
    :param table_name_mysql:
    :param file_name_sqlite:
    :return: [一致字段], [mysql 不一致字段], [sqlite 不一致字段]
    """
    with get_sqlite_conn(file_name_sqlite) as conn:
        sql_str = f"select * from {table_name_sqlite} limit 1"
        sqlite_df = pd.read_sql(sql_str, conn)

    sql_str = f"select * from {table_name_mysql} limit 1"
    mysql_df = pd.read_sql(sql_str, engine_md)

    # 设置匹配清单，包括： match_pairs_internal 内部预设匹配项，以及 match_pairs 匹配项参数
    # 转化成 dict 后，在后续匹配逻辑中进行比较
    match_pairs_internal = [
        ('trade_date', 'Date'),
        ('vol', 'Volume'),
    ]
    match_pairs_dict = defaultdict(set)
    for key1, key2 in match_pairs_internal:
        match_pairs_dict[key1.lower()].add(key2.lower())
        match_pairs_dict[key2.lower()].add(key1.lower())
    if match_pairs is not None:
        for key1, key2 in match_pairs:
            match_pairs_dict[key1.lower()].add(key2.lower())
            match_pairs_dict[key2.lower()].add(key1.lower())

    # 获取 两张表的列名称
    mysql_col_name_list = list(mysql_df.columns)
    sqlite_col_name_list = list(sqlite_df.columns)
    # 开始进行循环比较
    sqlite_col_name_list_dict = dict(enumerate(sqlite_col_name_list))
    match_list, mis_match_mysql = [], []
    for mysql_pos, mysql_col_name in enumerate(mysql_col_name_list):
        mysql_col_name_lower = mysql_col_name.lower()
        col_set_4_match = match_pairs_dict[mysql_col_name_lower] if mysql_col_name_lower in match_pairs_dict else None
        match_pos, match_col_name = None, None
        for sqlite_pos, sqlite_col_name in sqlite_col_name_list_dict.items():
            if mysql_col_name_lower == sqlite_col_name.lower():
                match_pos, match_col_name = sqlite_pos, sqlite_col_name
                break
            # mysql列名 与 sqlite 列名 通过 match_pairs_dict 进行映射匹配
            if col_set_4_match is not None and sqlite_col_name.lower() in col_set_4_match:
                match_pos, match_col_name = sqlite_pos, sqlite_col_name
                break

        # 匹配成功
        if match_pos is not None:
            # 匹配成功
            del sqlite_col_name_list_dict[match_pos]
            match_list.append((mysql_pos, mysql_col_name, match_pos, match_col_name))
        else:
            # 匹配失败
            mis_match_mysql.append((mysql_pos, mysql_col_name))

    mis_match_sqlite = [(sqlite_pos, sqlite_col_name)
                        for sqlite_pos, sqlite_col_name in sqlite_col_name_list_dict.items()]
    return match_list, mis_match_mysql, mis_match_sqlite


def check_table_4_match_cols():
    """
    表字段匹配检查
    :return:
    """
    # file_name_sqlite = 'DB_Dailybar.db'
    # table_name_mysql = 'tushare_stock_daily_md'
    file_name_table_name_pair_list = [
        ('DB_adjfactor.db', 'tushare_stock_daily_adj_factor'),
        ('DB_CashFlow.db', 'tushare_stock_cashflow'),
        ('DB_Balancesheet.db', 'tushare_stock_balancesheet'),
        # ('DB_Dailybar.db', 'tushare_stock_daily_md'),
        # ('DB_Dailybasic.db', 'tushare_stock_daily_basic'),
        # ('DB_EquityIndex.db', 'tushare_stock_index_daily_md'),
        # ('DB_FinaIndicator.db', 'tushare_stock_fin_indicator'),
    ]
    for file_name_sqlite, table_name_mysql in file_name_table_name_pair_list:
        match_list, mis_match_mysql, mis_match_sqlite = check_match_column(
            table_name_mysql=table_name_mysql, file_name_sqlite=file_name_sqlite)
        logger.debug("mysql %s 与 sqlite %s 匹配结果", table_name_mysql, file_name_sqlite)
        logger.debug("match_list %s", match_list)
        field_pair_list = [(sqlite_pos, (mysql_col_name, sqlite_col_name))
                           for mysql_pos, mysql_col_name, sqlite_pos, sqlite_col_name in match_list]
        field_pair_list.sort(key=lambda x: x[0])
        field_pair_list, list_len = [pair for _, pair in field_pair_list], len(field_pair_list)

        # 将数据整理成类似下列格式
        # [
        #         ('trade_date', 'Date'),
        #         ('open', 'Open'),
        #         ('high', 'High'),
        #         ('low', 'Low'),
        #         ('close', 'Close'),
        #         ('vol', 'Volume'),
        #         ('amount', 'Amount'),
        # ]
        field_pair_list_str = "[\n"
        for num, (mysql_col_name, sqlite_col_name) in enumerate(field_pair_list):
            field_pair_list_str += f"\t('{mysql_col_name}', '{sqlite_col_name}'),\n"
        field_pair_list_str += "]"
        # {
        #    "doit": True,
        #    "file_name": 'DB_Dailybasic.db',
        #    "table_name": 'tushare_stock_daily_basic',
        #    "field_pair_list": [
        #        ('trade_date', 'Date'),
        #        ('pe', 'PE'),
        #        ('pe_ttm', 'PE_TTM'),
        #        ('pb', 'PB'),
        #        ('ps', 'PS'),
        #        ('ps_ttm', 'PS_TTM'),
        #        ('total_share', 'Total_Share'),
        #        ('float_share', 'Float_Share'),
        #        ('total_mv', 'Total_MV'),
        #        ('circ_mv', 'Circ_MV'),
        #    ],
        #    "batch_size": 200,
        # },

        logger_str = f"""合成参数代码：
{{
   "doit": True,
   "file_name": '{file_name_sqlite}',
   "table_name": '{table_name_mysql}',
   "field_pair_list": {field_pair_list_str},
   "batch_size": 200,
}},
        """
        logger.debug(logger_str)

        # 显示未匹配项
        if len(mis_match_mysql) == 0:
            logger.debug('  mysql 全部字段均以及找到相应的匹配项')
        else:
            logger.debug("  mis_match_mysql\n%s", mis_match_mysql)

        if len(mis_match_sqlite) == 0:
            logger.debug('  sqlite 全部字段均以及找到相应的匹配项')
        else:
            logger.debug("  mis_match_sqlite\n%s", mis_match_sqlite)


if __name__ == "__main__":
    transfer_mysql_to_sqlite()
    # 2019-04-11 17:20:06,547  -  2019-04-11 17:45:29,555
    # check_table_4_match_cols()
