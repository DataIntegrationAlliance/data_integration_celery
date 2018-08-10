#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/8/6 13:21
@File    : code_mapping.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
import re
# from tasks.backend.orm import CodeMapping
from tasks.utils.db_utils import with_db_session
from tasks.backend import engine_md
import logging
logger = logging.getLogger()
ifind_info_table_pattern = re.compile(r"(?<=ifind_).+(?=_info)")
wind_info_table_pattern = re.compile(r"(?<=wind_).+(?=_info)")


def search_cap_type(pattern, table_name):
    """
    更加 Pattern 查找 对应的 资产类型，不匹配则返回 None
    :param pattern:
    :param table_name:
    :return:
    """
    m = pattern.search(table_name)
    if m is not None:
        cap_type = m.group()
    else:
        cap_type = None
    return cap_type


def update_from_info_table(table_name):
    ifind_cap_type = search_cap_type(ifind_info_table_pattern, table_name)
    wind_cap_type = search_cap_type(wind_info_table_pattern, table_name)

    if ifind_cap_type is not None:
        sql_str = """insert into code_mapping(unique_code, ths_code, market, type) 
            select ths_code, ths_code, 
                substring(ths_code, locate('.', ths_code) + 1, length(ths_code)) market, '{cap_type}' 
            from {table_name} 
            on duplicate key update ths_code=values(ths_code), market=values(market), type=values(type)
            """.format(table_name=table_name, cap_type=ifind_cap_type)
        with with_db_session(engine_md) as session:
            rst = session.execute(sql_str)
            logger.debug('从 %s 表中更新 code_mapping 记录 %d 条', table_name, rst.rowcount)
    elif wind_cap_type is not None:
        sql_str = """insert into code_mapping(unique_code, wind_code, market, type) 
            select wind_code, wind_code, 
                substring(wind_code, locate('.', wind_code) + 1, length(wind_code)) market, '{cap_type}' 
            from {table_name} 
            on duplicate key update wind_code=values(wind_code), market=values(market), type=values(type)
            """.format(table_name=table_name, cap_type=wind_cap_type)
        with with_db_session(engine_md) as session:
            rst = session.execute(sql_str)
            logger.debug('从 %s 表中更新 code_mapping 记录 %d 条', table_name, rst.rowcount)
    else:
        raise ValueError('不支持 %s 更新 code_mapping 数据' % table_name)


if __name__ == '__main__':
    table_name = 'ifind_future_info'
    update_from_info_table(table_name)
