#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/8/16 10:13
@File    : stock.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
import pandas as pd
import logging

from direstinvoker.utils.fh_utils import date_2_str

from tasks import bunch_insert_on_duplicate_update, alter_table_2_myisam, build_primary_key
from tasks.backend import engine_md
from tasks.utils.db_utils import with_db_session
from tasks.merge import mean_value, prefer_left, prefer_right, merge_data, get_value
from sqlalchemy.types import String, Date, Integer, Text
from sqlalchemy.dialects.mysql import DOUBLE

logger = logging.getLogger()


def merge_stock_info():
    """
    合并 wind，ifind 数据到对应名称的表中
    :return:
    """
    table_name = 'stock_info'
    logging.info("更新 %s 开始", table_name)
    has_table = engine_md.has_table(table_name)
    ifind_table_name = 'ifind_{table_name}'.format(table_name=table_name)
    wind_table_name = 'wind_{table_name}'.format(table_name=table_name)
    # ifind_model = TABLE_MODEL_DIC[ifind_table_name]
    # wind_model = TABLE_MODEL_DIC[wind_table_name]
    # with with_db_session(engine_md) as session:
    #     session.query(ifind_model, wind_model).filter(ifind_model.c.ths_code == wind_model.c.wind_code)
    ifind_sql_str = "select * from {table_name}".format(table_name=ifind_table_name)
    wind_sql_str = "select * from {table_name}".format(table_name=wind_table_name)
    ifind_df = pd.read_sql(ifind_sql_str, engine_md)  # , index_col='ths_code'
    wind_df = pd.read_sql(wind_sql_str, engine_md)  # , index_col='wind_code'
    joined_df = pd.merge(ifind_df, wind_df, how='outer',
                         left_on='ths_code', right_on='wind_code', indicator='indicator_column')
    col_merge_dic = {
        'unique_code': (String(20), prefer_left, {'left_key': 'ths_code', 'right_key': 'wind_code'}),
        'sec_name': (String(20), prefer_left, {'left_key': 'ths_stock_short_name_stock', 'right_key': 'sec_name'}),
        'cn_name': (String(100), get_value, {'key': 'ths_corp_cn_name_stock'}),
        'en_name': (String(100), get_value, {'key': 'ths_corp_name_en_stock'}),
        'delist_date': (Date, prefer_left, {'left_key': 'ths_delist_date_stock', 'right_key': 'delist_date'}),
        'ipo_date': (Date, prefer_left, {'left_key': 'ths_ipo_date_stock', 'right_key': 'ipo_date'}),
        'pre_name': (Text, prefer_left, {'left_key': 'ths_corp_name_en_stock', 'right_key': 'prename'}),
        'established_date': (Date, get_value, {'key': 'ths_established_date_stock'}),
        'exch_city': (String(20), get_value, {'key': 'exch_city'}),
        'exch_cn': (String(20), get_value, {'key': 'ths_listing_exchange_stock'}),
        'exch_eng': (String(20), get_value, {'key': 'exch_eng'}),
        'stock_code': (String(20), prefer_left, {'left_key': 'ths_stock_code_stock', 'right_key': 'trade_code'}),
        'mkt': (String(20), get_value, {'key': 'mkt'}),
    }

    col_merge_rule_dic = {
        key: (val[1], val[2]) for key, val in col_merge_dic.items()
    }
    dtype = {
        key: val[0] for key, val in col_merge_dic.items()
    }
    data_df = merge_data(joined_df, col_merge_rule_dic)
    data_count = bunch_insert_on_duplicate_update(data_df, table_name, engine_md, dtype)
    logger.info('%s 新增或更新记录 %d 条', table_name, data_count)
    if not has_table and engine_md.has_table(table_name):
        alter_table_2_myisam(engine_md, [table_name])
        build_primary_key([table_name])

    return data_df


def merge_stock_daily(date_from=None):
    """
    合并 wind，ifind 数据到对应名称的表中
    :param date_from:
    :return:
    """
    table_name = 'stock_daily'
    logging.info("更新 %s 开始", table_name)
    has_table = engine_md.has_table(table_name)
    if date_from is None and has_table:
        sql_str = "select adddate(max(trade_date),1) from {table_name}".format(table_name=table_name)
        with with_db_session(engine_md) as session:
            date_from = date_2_str(session.execute(sql_str).scalar())
    ifind_table_ds_name = 'ifind_{table_name}_ds'.format(table_name=table_name)
    ifind_table_his_name = 'ifind_{table_name}_his'.format(table_name=table_name)
    wind_table_name = 'wind_{table_name}'.format(table_name=table_name)
    if date_from is None:
        ifind_his_sql_str = "select * from {table_name}".format(table_name=ifind_table_ds_name)
        ifind_ds_sql_str = "select * from {table_name}".format(table_name=ifind_table_his_name)
        wind_sql_str = "select * from {table_name}".format(table_name=wind_table_name)
        ifind_his_df = pd.read_sql(ifind_his_sql_str, engine_md)  # , index_col='ths_code'
        ifind_ds_df = pd.read_sql(ifind_ds_sql_str, engine_md)  # , index_col='ths_code'
        wind_df = pd.read_sql(wind_sql_str, engine_md)  # , index_col='wind_code'
    else:
        ifind_his_sql_str = "select * from {table_name} where time >= %s".format(table_name=ifind_table_ds_name)
        ifind_ds_sql_str = "select * from {table_name} where time >= %s".format(table_name=ifind_table_his_name)
        wind_sql_str = "select * from {table_name} where trade_date >= %s".format(table_name=wind_table_name)
        ifind_his_df = pd.read_sql(ifind_his_sql_str, engine_md, params=[date_from])  # , index_col='ths_code'
        ifind_ds_df = pd.read_sql(ifind_ds_sql_str, engine_md, params=[date_from])  # , index_col='ths_code'
        wind_df = pd.read_sql(wind_sql_str, engine_md, params=[date_from])  # , index_col='wind_code'

    ifind_df = pd.merge(ifind_his_df, ifind_ds_df, how='outer',
                        on=['ths_code', 'time'])
    joined_df = pd.merge(ifind_df, wind_df, how='outer',
                         left_on=['ths_code', 'time'], right_on=['wind_code', 'trade_date'],
                         indicator='indicator_column')
    col_merge_dic = {
        'unique_code': (String(20), prefer_left, {'left_key': 'ths_code', 'right_key': 'wind_code'}),
        'trade_date': (Date, prefer_left, {'left_key': 'time', 'right_key': 'trade_date'}),
        'open': (DOUBLE, mean_value, {
            'left_key': 'open_x', 'right_key': 'open_y',
            'warning_accuracy': 0.01, 'primary_keys': ('ths_code', 'time')}),
        'high': (DOUBLE, mean_value, {
            'left_key': 'high_x', 'right_key': 'high_y',
            'warning_accuracy': 0.01, 'primary_keys': ('ths_code', 'time')}),
        'low': (DOUBLE, mean_value, {
            'left_key': 'low_x', 'right_key': 'low_y',
            'warning_accuracy': 0.01, 'primary_keys': ('ths_code', 'time')}),
        # TODO: 原因不详，wind接口取到的部分 close 数据不准确
        'close': (DOUBLE, prefer_left, {
            'left_key': 'close_x', 'right_key': 'close_y',
            'warning_accuracy': 0.01, 'primary_keys': ('ths_code', 'time')}),
        'volume': (DOUBLE, mean_value, {
            'left_key': 'volume_x', 'right_key': 'volume_y',
            'warning_accuracy': 1, 'primary_keys': ('ths_code', 'time')}),
        'amount': (DOUBLE, mean_value, {
            'left_key': 'amount', 'right_key': 'amt',
            'warning_accuracy': 1, 'primary_keys': ('ths_code', 'time')}),
        # 总股本字段：同花顺的 totalShares 字段以变动日期为准，wind total_shares 以公告日为准
        # 因此出现冲突时应该以 wind 为准
        'total_shares': (DOUBLE, prefer_right, {
            'left_key': 'totalShares', 'right_key': 'total_shares'}),
        # 'susp_days': (Integer, '***', {
        #     'left_key': 'ths_up_and_down_status_stock', 'right_key': 'susp_days', 'other_key': 'trade_status',
        #  'primary_keys': ('ths_code', 'time')}),
        'max_up_or_down': (Integer, max_up_or_down, {
            'ths_key': 'ths_up_and_down_status_stock', 'wind_key': 'maxupordown',
            'primary_keys': ('ths_code', 'time')}),
        'total_capital': (DOUBLE, get_value, {'key': 'totalCapital'}),
        'float_capital': (DOUBLE, get_value, {'key': 'floatCapitalOfAShares'}),
        'pct_chg': (DOUBLE, mean_value, {
            'left_key': 'changeRatio', 'right_key': 'pct_chg',
            'warning_accuracy': 0.01, 'primary_keys': ('ths_code', 'time')}),
        'float_a_shares': (DOUBLE, get_value, {'key': 'floatSharesOfAShares'}),  # 对应wind float_a_shares
        'free_float_shares': (DOUBLE, get_value, {'key': 'free_float_shares'}),  # 对应 ths ths_free_float_shares_stock
        # PE_TTM 对应 ths ths_pe_ttm_stock 以财务报告期为基准日，对应 wind pe_ttm 以报告期为准
        # 因此应该如有不同应该以 wind 为准
        'pe_ttm': (DOUBLE, prefer_right, {
            'left_key': 'ths_pe_ttm_stock', 'right_key': 'pe_ttm',
            'warning_accuracy': 0.01, 'primary_keys': ('ths_code', 'time')}),
        'pe': (DOUBLE, get_value, {'key': 'pe'}),
        'pb': (DOUBLE, get_value, {'key': 'pb'}),
        'ps': (DOUBLE, get_value, {'key': 'ps'}),
        'pcf': (DOUBLE, get_value, {'key': 'pcf'}),
    }

    col_merge_rule_dic = {
        key: (val[1], val[2]) for key, val in col_merge_dic.items()
    }
    dtype = {
        key: val[0] for key, val in col_merge_dic.items()
    }
    data_df = merge_data(joined_df, col_merge_rule_dic)
    data_count = bunch_insert_on_duplicate_update(data_df, table_name, engine_md, dtype)
    logger.info('%s 新增或更新记录 %d 条', table_name, data_count)
    if not has_table and engine_md.has_table(table_name):
        alter_table_2_myisam(engine_md, [table_name])
        build_primary_key([table_name])

    return data_df


def max_up_or_down(data_s: pd.Series, ths_key, wind_key, primary_keys=None, **kwargs):
    ths_val = data_s[ths_key]
    if ths_val == '跌停':
        ret_code_ths = -1
    elif ths_val == '涨停':
        ret_code_ths = 1
    elif ths_val == ('非涨跌停', '停牌'):
        ret_code_ths = 0
    else:
        ret_code_ths = None

    wind_val = data_s[wind_key]
    if wind_val in (1, -1, 0):
        ret_code_wind = wind_val
    else:
        ret_code_wind = None

    if ret_code_ths is None and ret_code_wind is None:
        pk_str = ','.join([str(data_s[key]) for key in primary_keys]) if primary_keys is not None else None
        if pk_str is None:
            msg = '%s = %s; %s = %s; 状态不明' % (ths_key, str(ths_val), wind_key, str(wind_val))
        else:
            msg = '[%s] %s = %s; %s = %s; 状态不明' % (pk_str, ths_key, str(ths_val), wind_key, str(wind_val))
        logger.debug(msg)
        ret_code = -2
    elif ret_code_ths is None:
        ret_code = ret_code_wind
    elif ret_code_wind is None:
        ret_code = ret_code_ths
    elif ret_code_ths == ret_code_wind:
        ret_code = ret_code_wind
    else:
        pk_str = ','.join([str(data_s[key]) for key in primary_keys]) if primary_keys is not None else None
        if pk_str is None:
            msg = '%s = %s; %s = %s; 状态冲突' % (ths_key, str(ths_val), wind_key, str(wind_val))
        else:
            msg = '[%s] %s = %s; %s = %s; 状态冲突' % (pk_str, ths_key, str(ths_val), wind_key, str(wind_val))
        logger.warning(msg)
        ret_code = -3

    return ret_code


if __name__ == "__main__":
    data_df = merge_stock_info()
    # print(data_df)
