#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/9/20 17:49
@File    : ts_pro_api.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
import os
import tushare as ts
import logging
from tasks.config import config
from tasks.utils.to_sqlite import TABLE_NAME_SQLITE_FILE_NAME_DIC, get_sqlite_file_path, \
    TABLE_NAME_MYSQL_COL_2_SQLITE_COL_DIC, add_all_table_primary_keys

logger = logging.getLogger()
try:
    pro = ts.pro_api(config.TUSHARE_TOKEN)
except Exception:
    logger.exception('獲取pro_api失敗,但是不影響合並')
    pro = None


def check_sqlite_db_primary_keys(table_name, primary_keys, ignore_if_exist=True):
    """
    检查 对应sqlite表，建立主键，默认已经有主键的表不重复建立
    :param table_name:
    :param primary_keys:
    :param ignore_if_exist:
    :return:
    """
    sqlite_file_name = TABLE_NAME_SQLITE_FILE_NAME_DIC[table_name]
    if config.ENABLE_EXPORT_2_SQLITE:
        file_path = get_sqlite_file_path(sqlite_file_name)
        if os.path.exists(file_path):
            mysql_col_2_sqlite_col_dic = dict(TABLE_NAME_MYSQL_COL_2_SQLITE_COL_DIC[table_name])
            sqlite_primary_keys = [mysql_col_2_sqlite_col_dic[_]
                                   for _ in primary_keys if _ in mysql_col_2_sqlite_col_dic]
            add_all_table_primary_keys(sqlite_primary_keys, file_name=sqlite_file_name, ignore_if_exist=ignore_if_exist)
