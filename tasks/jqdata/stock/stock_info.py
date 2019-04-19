#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2019/2/27 11:31
@File    : stock_info.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from tasks.jqdata import get_all_securities
import pandas as pd
import logging
from tasks import app
from sqlalchemy.types import String, Date, Integer
from sqlalchemy.dialects.mysql import DOUBLE
from tasks.backend import engine_md
from tasks.merge.code_mapping import update_from_info_table
from tasks.config import config
from ibats_utils.db import with_db_session, add_col_2_table, alter_table_2_myisam, \
    bunch_insert_on_duplicate_update


TABLE_NAME = 'jq_stock_info'


@app.task
def import_jq_stock_info(chain_param=None, refresh=False):
    """ 获取全市场股票代码及名称
    """
    table_name = TABLE_NAME
    logging.info("更新 %s 开始", table_name)
    # has_table = engine_md.has_table(table_name)
    param_list = [
        ('jq_code', String(20)),
        ('display_name', String(20)),
        ('name', String(20)),
        ('start_date', Date),
        ('end_date', Date),
    ]
    # 设置 dtype
    dtype = {key: val for key, val in param_list}

    # 数据提取
    # types: list: 用来过滤securities的类型, list元素可选:
    # 'stock', 'fund', 'index', 'futures', 'etf', 'lof', 'fja', 'fjb'。types为空时返回所有股票, 不包括基金,指数和期货
    # date: 日期, 一个字符串或者 [datetime.datetime]/[datetime.date] 对象,
    # 用于获取某日期还在上市的股票信息. 默认值为 None, 表示获取所有日期的股票信息
    stock_info_all_df = get_all_securities()
    stock_info_all_df.index.rename('jq_code', inplace=True)
    stock_info_all_df.reset_index(inplace=True)

    logging.info('%s stock data will be import', stock_info_all_df.shape[0])
    data_count = bunch_insert_on_duplicate_update(
        stock_info_all_df, table_name, engine_md, dtype=dtype,
        myisam_if_create_table=True, primary_keys=['jq_code'], schema=config.DB_SCHEMA_MD)
    logging.info("更新 %s 完成 存量数据 %d 条", table_name, data_count)
    # 更新 map 表
    update_from_info_table(table_name)


if __name__ == "__main__":
    import_jq_stock_info()
