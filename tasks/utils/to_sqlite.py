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
from concurrent.futures import ProcessPoolExecutor, as_completed
from ibats_utils.db import with_db_session
from ibats_utils.mess import get_folder_path, split_chunk, decorator_timer
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


@decorator_timer
def tushare_to_sqlite_batch(file_name, table_name, field_pair_list, batch_size=500, sort_by='trade_date',
                            clean_old_file_first=True, **kwargs):
    """
    将Mysql数据导入到sqlite，全量读取然后导出
    速度适中，可更加 batch_size 调剂对内存的需求
    :param file_name:
    :param table_name:
    :param field_pair_list:
    :param batch_size:
    :param sort_by:
    :param clean_old_file_first:
    :param kwargs:
    :return:
    """
    logger.info('mysql %s 导入到 sqlite %s 开始', table_name, file_name)
    sqlite_db_folder_path = get_folder_path('sqlite_db', create_if_not_found=False)
    db_file_path = os.path.join(sqlite_db_folder_path, file_name)
    # 删除历史文件——可以提上导入文件速度
    if clean_old_file_first and os.path.exists(db_file_path) and os.path.isfile(db_file_path):
        os.remove(db_file_path)

    conn = sqlite3.connect(db_file_path)
    # 对 fields 进行筛选及重命名
    if field_pair_list is not None:
        field_list = [_[0] for _ in field_pair_list]
        field_list.append('ts_code')
        field_pair_dic = dict(field_pair_list)
        sort_by = field_pair_dic[sort_by] if sort_by is not None else None
    else:
        field_list = None
        field_pair_dic = None

    if table_name == 'tushare_stock_index_daily_md':
        # tushare_stock_index_daily_md 表处理方式有些特殊
        ts_code_sqlite_table_name_dic = {
            # "": "CBIndex",  #
            "h30024.CSI": "CYBZ",  # 中证800保险
            "399300.SZ": "HS300",  # 沪深300
            "000016.SH": "HS50",  # 上证50
            "399905.SZ": "HS500",  # 中证500
            "399678.SZ": "SCXG",  # 深次新股
            "399101.SZ": "ZXBZ",  # 中小板综
        }
        code_list = [_ for _ in ts_code_sqlite_table_name_dic.keys()]
        in_clause = ", ".join([r'%s' for _ in code_list])
        sql_str = f"select * from {table_name} where ts_code in ({in_clause})"
        df_tot = pd.read_sql(sql_str, engine_md, params=code_list)
        # 对 fields 进行筛选及重命名
        if field_pair_dic is not None:
            df_tot = df_tot[field_list].rename(columns=field_pair_dic)

        dfg = df_tot.groupby('ts_code')
        code_count, data_count = len(code_list), 0
        for num, (ts_code, df) in enumerate(dfg, start=1):
            sqlite_table_name = ts_code_sqlite_table_name_dic[ts_code]
            df_len = df.shape[0]
            data_count += df_len
            logger.debug('%2d/%d) mysql %s -> sqlite %s %s %d 条记录',
                         num, code_count, table_name, file_name, sqlite_table_name, df_len)
            df = df.drop('ts_code', axis=1)
            # 排序
            if sort_by is not None:
                df = df.sort_values(sort_by)

            df.to_sql(sqlite_table_name, conn, index=False, if_exists='replace')
    else:
        # 非 tushare_stock_index_daily_md 表
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
            if field_pair_dic is not None:
                df_tot = df_tot[field_list].rename(columns=field_pair_dic)

            dfg = df_tot.groupby('ts_code')
            for num, (ts_code, df) in enumerate(dfg, start=num + 1):
                code_exchange = ts_code.split('.')
                sqlite_table_name = f"{code_exchange[1]}{code_exchange[0]}"
                df_len = df.shape[0]
                data_count += df_len
                logger.debug('%4d/%d) mysql %s -> sqlite %s %s %d 条记录',
                             num, code_count, table_name, file_name, sqlite_table_name, df_len)
                df = df.drop('ts_code', axis=1)
                # 排序
                if sort_by is not None:
                    df = df.sort_values(sort_by)

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


@decorator_timer
def transfer_mysql_to_sqlite(pool_job=True):
    """
    mysql 转化为 sqlite
    :return:
    """
    transfer_param_list = [
        {
            "doit": True,
            "file_name": 'eDB_adjfactor.db',
            "table_name": 'tushare_stock_daily_adj_factor',
            "field_pair_list": [
                ('trade_date', 'Date'),
                ('adj_factor', 'adj_factor'),
            ],
            "batch_size": 100,
            "sort_by": "trade_date",
            "clean_old_file_first": "True",
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
            "batch_size": 100,
            "sort_by": "ann_date",
        },
        {
            "doit": True,
            "file_name": 'eDB_BlockTrade.db',
            "table_name": 'tushare_block_trade',
            "field_pair_list": [
                ('trade_date', 'Date'),
                ('price', 'Price'),
                ('vol', 'Volume'),
                ('amount', 'Amount'),
                ('buyer', 'Buyer'),
                ('seller', 'Seller'),
            ],
            "batch_size": 200,
            "sort_by": "trade_date",
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
            "sort_by": "ann_date",
        },
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
            "sort_by": "trade_date",
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
            "sort_by": "trade_date",
        },
        {
            "doit": True,
            "file_name": 'eDB_EquityIndex.db',
            "table_name": 'tushare_stock_index_daily_md',
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
            "sort_by": "trade_date",
        },
        {
            "doit": True,
            "file_name": 'eDB_FinaIndicator.db',
            "table_name": 'tushare_stock_fin_indicator',
            "field_pair_list": [
                ('ann_date', 'ann_date'),
                ('end_date', 'end_date'),
                ('eps', 'eps'),
                ('dt_eps', 'dt_eps'),
                ('total_revenue_ps', 'total_revenue_ps'),
                ('revenue_ps', 'revenue_ps'),
                ('capital_rese_ps', 'capital_rese_ps'),
                ('surplus_rese_ps', 'surplus_rese_ps'),
                ('undist_profit_ps', 'undist_profit_ps'),
                ('extra_item', 'extra_item'),
                ('profit_dedt', 'profit_dedt'),
                ('op_income', 'op_income'),
                ('ebit', 'ebit'),
                ('ebitda', 'ebitda'),
                ('fcff', 'fcff'),
                ('fcfe', 'fcfe'),
                ('current_exint', 'current_exint'),
                ('noncurrent_exint', 'noncurrent_exint'),
                ('interestdebt', 'interestdebt'),
                ('netdebt', 'netdebt'),
                ('tangible_asset', 'tangible_asset'),
                ('working_capital', 'working_capital'),
                ('networking_capital', 'networking_capital'),
                ('invest_capital', 'invest_capital'),
                ('retained_earnings', 'retained_earnings'),
                ('diluted2_eps', 'diluted2_eps'),
                ('bps', 'bps'),
                ('ocfps', 'ocfps'),
                ('retainedps', 'retainedps'),
                ('cfps', 'cfps'),
                ('ebit_ps', 'ebit_ps'),
                ('fcff_ps', 'fcff_ps'),
                ('fcfe_ps', 'fcfe_ps'),
                ('netprofit_margin', 'netprofit_margin'),
                ('grossprofit_margin', 'grossprofit_margin'),
                ('cogs_of_sales', 'cogs_of_sales'),
                ('expense_of_sales', 'expense_of_sales'),
                ('profit_to_gr', 'profit_to_gr'),
                ('saleexp_to_gr', 'saleexp_to_gr'),
                ('adminexp_of_gr', 'adminexp_of_gr'),
                ('finaexp_of_gr', 'finaexp_of_gr'),
                ('impai_ttm', 'impai_ttm'),
                ('gc_of_gr', 'gc_of_gr'),
                ('op_of_gr', 'op_of_gr'),
                ('ebit_of_gr', 'ebit_of_gr'),
                ('roe', 'roe'),
                ('roe_waa', 'roe_waa'),
                ('roe_dt', 'roe_dt'),
                ('roa', 'roa'),
                ('npta', 'npta'),
                ('roic', 'roic'),
                ('roe_yearly', 'roe_yearly'),
                ('roa2_yearly', 'roa2_yearly'),
                ('debt_to_assets', 'debt_to_assets'),
                ('assets_to_eqt', 'assets_to_eqt'),
                ('dp_assets_to_eqt', 'dp_assets_to_eqt'),
                ('ca_to_assets', 'ca_to_assets'),
                ('nca_to_assets', 'nca_to_assets'),
                ('tbassets_to_totalassets', 'tbassets_to_totalassets'),
                ('int_to_talcap', 'int_to_talcap'),
                ('eqt_to_talcapital', 'eqt_to_talcapital'),
                ('currentdebt_to_debt', 'currentdebt_to_debt'),
                ('longdeb_to_debt', 'longdeb_to_debt'),
                ('ocf_to_shortdebt', 'ocf_to_shortdebt'),
                ('debt_to_eqt', 'debt_to_eqt'),
                ('eqt_to_debt', 'eqt_to_debt'),
                ('eqt_to_interestdebt', 'eqt_to_interestdebt'),
                ('tangibleasset_to_debt', 'tangibleasset_to_debt'),
                ('tangasset_to_intdebt', 'tangasset_to_intdebt'),
                ('tangibleasset_to_netdebt', 'tangibleasset_to_netdebt'),
                ('ocf_to_debt', 'ocf_to_debt'),
                ('turn_days', 'turn_days'),
                ('roa_yearly', 'roa_yearly'),
                ('roa_dp', 'roa_dp'),
                ('fixed_assets', 'fixed_assets'),
                ('profit_to_op', 'profit_to_op'),
                ('q_saleexp_to_gr', 'q_saleexp_to_gr'),
                ('q_gc_to_gr', 'q_gc_to_gr'),
                ('q_roe', 'q_roe'),
                ('q_dt_roe', 'q_dt_roe'),
                ('q_npta', 'q_npta'),
                ('q_ocf_to_sales', 'q_ocf_to_sales'),
                ('basic_eps_yoy', 'basic_eps_yoy'),
                ('dt_eps_yoy', 'dt_eps_yoy'),
                ('cfps_yoy', 'cfps_yoy'),
                ('op_yoy', 'op_yoy'),
                ('ebt_yoy', 'ebt_yoy'),
                ('netprofit_yoy', 'netprofit_yoy'),
                ('dt_netprofit_yoy', 'dt_netprofit_yoy'),
                ('ocf_yoy', 'ocf_yoy'),
                ('roe_yoy', 'roe_yoy'),
                ('bps_yoy', 'bps_yoy'),
                ('assets_yoy', 'assets_yoy'),
                ('eqt_yoy', 'eqt_yoy'),
                ('tr_yoy', 'tr_yoy'),
                ('or_yoy', 'or_yoy'),
                ('q_sales_yoy', 'q_sales_yoy'),
                ('q_op_qoq', 'q_op_qoq'),
                ('equity_yoy', 'equity_yoy'),
            ],
            "batch_size": 100,
            "sort_by": "ann_date",
        },
        {
            "doit": True,
            "file_name": 'eDB_Income.db',
            "table_name": 'tushare_stock_income',
            "field_pair_list": [
                ('ann_date', 'ann_date'),
                ('f_ann_date', 'f_ann_date'),
                ('end_date', 'end_date'),
                ('report_type', 'report_type'),
                ('basic_eps', 'basic_eps'),
                ('diluted_eps', 'diluted_eps'),
                ('total_revenue', 'total_revenue'),
                ('revenue', 'revenue'),
                ('int_income', 'int_income'),
                ('n_oth_income', 'n_oth_income'),
                ('n_oth_b_income', 'n_oth_b_income'),
                ('fv_value_chg_gain', 'fv_value_chg_gain'),
                ('invest_income', 'invest_income'),
                ('ass_invest_income', 'ass_invest_income'),
                ('total_cogs', 'total_cogs'),
                ('oper_cost', 'oper_cost'),
                ('int_exp', 'int_exp'),
                ('biz_tax_surchg', 'biz_tax_surchg'),
                ('assets_impair_loss', 'assets_impair_loss'),
                ('operate_profit', 'operate_profit'),
                ('nca_disploss', 'nca_disploss'),
                ('total_profit', 'total_profit'),
                ('income_tax', 'income_tax'),
                ('n_income', 'n_income'),
                ('n_income_attr_p', 'n_income_attr_p'),
                ('minority_gain', 'minority_gain'),
                ('t_compr_income', 't_compr_income'),
                ('compr_inc_attr_p', 'compr_inc_attr_p'),
                ('compr_inc_attr_m_s', 'compr_inc_attr_m_s'),
                ('ebit', 'ebit'),
                ('ebitda', 'ebitda'),
                ('undist_profit', 'undist_profit'),
                ('distable_profit', 'distable_profit'),
            ],
            "batch_size": 100,
            "sort_by": "ann_date",
        },
    ]
    # batch_size = 200
    # tushare_to_sqlite_batch(file_name, table_name, field_pair_list, batch_size=batch_size)
    # tushare_to_sqlite_pre_ts_code(file_name, table_name, field_pair_list)
    # tushare_to_sqlite_tot_select(file_name, table_name, field_pair_list)
    transfer_param_list_len = len(transfer_param_list)
    if pool_job:
        logger.info('建立进程池进行SQLite导出')
        with ProcessPoolExecutor(4) as executor:
            futures_dic = {executor.submit(tushare_to_sqlite_batch, **dic): num
                           for num, dic in enumerate(transfer_param_list) if dic['doit']}
            for future in as_completed(futures_dic):
                num = futures_dic[future]
                exp = future.exception()
                if exp is None:
                    logger.info('tushare_to_sqlite_batch %s -> %s 完成',
                                transfer_param_list[num]['table_name'], transfer_param_list[num]['file_name'])
                else:
                    logger.exception('tushare_to_sqlite_batch %s -> %s 执行异常',
                                     transfer_param_list[num]['table_name'], transfer_param_list[num]['file_name'],
                                     exc_info=exp)

    else:
        logger.info('循环执行SQLite导出')
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
        # ('DB_adjfactor.db', 'tushare_stock_daily_adj_factor', 'trade_date'),
        # ('DB_Balancesheet.db', 'tushare_stock_balancesheet', 'ann_date'),
        # ('DB_BlockTrade.db', 'tushare_block_trade', 'trade_date'),
        # ('DB_CashFlow.db', 'tushare_stock_cashflow', 'ann_date'),
        # ('DB_Dailybar.db', 'tushare_stock_daily_md', 'trade_date'),
        # ('DB_Dailybasic.db', 'tushare_stock_daily_basic', 'trade_date'),
        # ('DB_EquityIndex.db', 'tushare_stock_index_daily_md', 'trade_date'),
        ('DB_FinaIndicator.db', 'tushare_stock_fin_indicator', 'ann_date'),
        ('DB_Income.db', 'tushare_stock_income', 'ann_date'),
    ]
    for file_name_sqlite, table_name_mysql, sort_by in file_name_table_name_pair_list:
        logger.debug("mysql %s 与 sqlite %s 开始匹配", table_name_mysql, file_name_sqlite)
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
        #    "batch_size": 100,
        #    "sort_by": "ann_date",
        # },

        logger_str = f"""合成参数代码：
{{
   "doit": True,
   "file_name": '{file_name_sqlite}',
   "table_name": '{table_name_mysql}',
   "field_pair_list": {field_pair_list_str},
   "batch_size": 100,
   "sort_by": "{sort_by}",
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


@decorator_timer
def drop_duplicate():
    """
    重建立主键，删除表中重复数据
    :return:
    """
    from ibats_utils.db import drop_duplicate_data_from_table
    drop_duplicate_data_from_table('tushare_stock_index_basic', engine_md, ['ts_code'])


if __name__ == "__main__":
    pass
    # logging.getLogger(__name__).setLevel(logging.INFO)
    # 对比王淳 sqlite 与 mysql 数据库字段差距并合成相应的参数供 transfer_mysql_to_sqlite 使用
    # check_table_4_match_cols()
    # mysql 转化为 sqlite
    transfer_mysql_to_sqlite(pool_job=True)

    # 重建立主键，删除表中重复数据
    # drop_duplicate()
