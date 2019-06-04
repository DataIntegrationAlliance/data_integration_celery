#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 19-5-21 下午2:21
@File    : export.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from functools import lru_cache
import pandas as pd
from tasks.backend import with_db_session_p, engine_md
from tasks.wind.future_reorg.reorg_md_2_db import wind_future_continuous_md
from ibats_utils.mess import get_folder_path, date_2_str
import os
import re


module_root_path = get_folder_path(re.compile(r'^tasks$'), create_if_not_found=False)  # 'tasks'
root_parent_path = os.path.abspath(os.path.join(module_root_path, os.path.pardir))


@lru_cache()
def get_export_path(file_name, create_folder_if_no_exist=True):
    folder_path = os.path.join(root_parent_path, 'export_files')
    if create_folder_if_no_exist and not os.path.exists(folder_path):
        os.makedirs(folder_path)

    return os.path.join(folder_path, file_name)


def trade_date_list(file_path=None, future_or_stock='future'):
    if file_path is None:
        file_path = get_export_path('trade_date.csv')

    with with_db_session_p() as session:
        if future_or_stock is 'future':
            sql_str = f"select cal_date from tushare_future_trade_cal where exchange='DCE' and is_open=1"
        else:
            sql_str = f"select cal_date from tushare_trade_date where exchange='SSE' and is_open=1"

        table = session.execute(sql_str)
        ret_list = [date_2_str(_[0]) for _ in table.fetchall()]

    pd.DataFrame({'trade_date': ret_list}).to_csv(file_path, index=False)


def future_info(file_path=None):
    if file_path is None:
        file_path = get_export_path('future_info.csv')

    sql_str = "select * from tushare_future_basic"
    pd.read_sql(sql_str, engine_md).to_csv(file_path, index=False)


def future_continuous_md(instrument_type_list=None, export_2_csv=True) -> (pd.DataFrame, pd.DataFrame):
    """
    将每一个交易日主次合约行情信息进行展示
    adj_chg_main, adj_chg_secondary 为前复权调整因子
    :param instrument_type_list:
    :param export_2_csv: 是否导出csv
    :return:
    """
    from tasks.tushare.tushare_future.continuse_contract_md import tushare_future_continuous_md
    folder_path = os.path.join(root_parent_path, 'export_files', 'future_md')
    tushare_future_continuous_md(
        instrument_type_list=instrument_type_list, export_2_csv=export_2_csv, export_folder=folder_path)


if __name__ == "__main__":
    # trade_date_list()
    future_continuous_md()
