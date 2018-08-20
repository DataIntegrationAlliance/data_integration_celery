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
from tasks import bunch_insert_on_duplicate_update, alter_table_2_myisam, build_primary_key
from tasks.backend import engine_md
from tasks.merge import mean_value, prefer_left, prefer_right, merge_data
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
        'unique_code': (prefer_left, {'left_key': 'ths_code', 'right_key': 'wind_code'}, String(20)),
        'sec_name': (prefer_left, {'left_key': 'ths_stock_short_name_stock', 'right_key': 'sec_name'}, String(20)),
        'cn_name': (prefer_left, {'left_key': 'ths_corp_cn_name_stock', 'right_key': None}, String(100)),
        'en_name': (prefer_left, {'left_key': 'ths_corp_name_en_stock', 'right_key': None}, String(100)),
        'delist_date': (prefer_left, {'left_key': 'ths_delist_date_stock', 'right_key': 'delist_date'}, Date),
        'ipo_date': (prefer_left, {'left_key': 'ths_ipo_date_stock', 'right_key': 'ipo_date'}, Date),
        'pre_name': (prefer_left, {'left_key': 'ths_corp_name_en_stock', 'right_key': 'prename'}, Text),
        'established_date': (prefer_left, {'left_key': 'ths_established_date_stock', 'right_key': None}, Date),
        'exch_city': (prefer_right, {'left_key': None, 'right_key': 'exch_city'}, String(20)),
        'exch_cn': (prefer_left, {'left_key': 'ths_listing_exchange_stock', 'right_key': None}, String(20)),
        'exch_eng': (prefer_right, {'left_key': None, 'right_key': 'exch_eng'}, String(20)),
        'stock_code': (prefer_left, {'left_key': 'ths_stock_code_stock', 'right_key': 'trade_code'}, String(20)),
        'mkt': (prefer_right, {'left_key': None, 'right_key': 'mkt'}, String(20)),
    }

    col_merge_rule_dic = {
        key: (val[0], val[1]) for key, val in col_merge_dic.items()
    }
    dtype = {
        key: val[2] for key, val in col_merge_dic.items()
    }
    data_df = merge_data(joined_df, col_merge_rule_dic)
    data_count = bunch_insert_on_duplicate_update(data_df, table_name, engine_md, dtype)
    logger.info('%s 新增或更新记录 %d 条', table_name, data_count)
    if not has_table and engine_md.has_table(table_name):
        alter_table_2_myisam(engine_md, [table_name])
        build_primary_key([table_name])

    return data_df


if __name__ == "__main__":
    data_df = merge_stock_info()
    # print(data_df)
