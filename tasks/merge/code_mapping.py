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


def update_from_ifind_fund(table_name, cap_type):
    """
    同花顺 ifind 表中 pub_fund private_fund ths_code 整合到 code_mapping 表
    规则：
    如果没有 wind 相关表，则直接映射到 code_mapping 表 unique_code = i.[ths_code]
    如果有 wind 相关表，
    1）将 名称相同部分的记录 更新 code_mapping 表 ths_code 字段
    2）将 名称不同的新增记录 插入到 code_mapping 表 unique_code = i.[ths_code]
    :param table_name:
    :param cap_type:
    :return:
    """
    wind_table_name = table_name.replace('ifind_', 'wind_')
    has_wind_table = engine_md.has_table(wind_table_name)
    if not has_wind_table:
        logger.warning('%s 表不存在，直接插入新数据', wind_table_name)
        sql_str = """insert into code_mapping(unique_code, ths_code, market, type)
            select concat('i.', info.ths_code) unique_code, info.ths_code, 
                substring(info.ths_code, locate('.', info.ths_code) + 1, 
                length(info.ths_code)) market, '{cap_type}' type
            from {table_name} info
            left join code_mapping cm
            on cm.ths_code = info.ths_code 
            where cm.ths_code is null""".format(table_name=table_name, cap_type=cap_type)
        with with_db_session(engine_md) as session:
            rslt = session.execute(sql_str)
            logger.debug('从 %s 表中更新 code_mapping 记录 %d 条', table_name, rslt.rowcount)
            session.commit()
    else:
        # 补充已存在 wind_code 的记录的 ths_code 字段
        update_sql_str = """update code_mapping code_m, 
            (
                select concat('i.', ths_code) unique_code, ths_code,
                    substring(ths_code, locate('.', ths_code) + 1, length(ths_code)) market, 
                    '{cap_type}' type
                from (
                    select info.ths_code, info.ths_product_full_name_sp sec_name 
                    from {table_name} info
                    left join code_mapping cm
                    on cm.ths_code = info.ths_code 
                    where cm.ths_code is null
                    ) ifi
                inner join {wind_table_name} wfi
                on ifi.sec_name = wfi.sec_name
            ) t
            set code_m.unique_code = t.unique_code, 
                code_m.ths_code = t.ths_code, 
                code_m.market = t.market, 
                code_m.type = t.type
            where code_m.wind_code = t.wind_code""".format(
            table_name=table_name, cap_type=cap_type, wind_table_name=wind_table_name)
        # 插入新增 ths_code
        insert_sql_str = """insert into code_mapping(unique_code, ths_code, market, type) 
            select concat('i.', ths_code) unique_code, ths_code, 
                substring(ths_code, locate('.', ths_code) + 1, length(ths_code)) market, 
                '{cap_type}' type 
            from (
                select info.ths_code, info.ths_product_full_name_sp sec_name 
                from {table_name} info
                left join code_mapping cm
                on cm.ths_code = info.ths_code 
                where cm.ths_code is null
            ) ifi
            left join {wind_table_name} wfi
            on ifi.sec_name = wfi.sec_name
            where wfi.wind_code is null""".format(
            table_name=table_name, cap_type=cap_type, wind_table_name=wind_table_name)

        with with_db_session(engine_md) as session:
            rslt = session.execute(update_sql_str)
            logger.debug('从 %s 表中更新 code_mapping 记录 %d 条', table_name, rslt.rowcount)
            rslt = session.execute(insert_sql_str)
            logger.debug('从 %s 表中插入 code_mapping 记录 %d 条', table_name, rslt.rowcount)
            session.commit()


def update_from_wind_fund(table_name, cap_type):
    """
    同花顺 wind 表中 pub_fund private_fund ths_code 整合到 code_mapping 表
    规则：
    如果没有 ifind 相关表，则直接映射到 code_mapping 表 unique_code = w.[wind_code]
    如果有 ifind 相关表，
    1）将 名称相同部分的记录 更新 code_mapping 表 ths_code 字段
    2）将 名称不同的新增记录 插入到 code_mapping 表 unique_code = w.[wind_code]
    :param table_name:
    :param cap_type:
    :return:
    """
    wind_table_name = table_name.replace('wind_', 'ifind_')
    has_wind_table = engine_md.has_table(wind_table_name)
    if not has_wind_table:
        logger.warning('%s 表不存在，直接插入新数据', wind_table_name)
        sql_str = """insert into code_mapping(unique_code, wind_code, market, type)
            select concat('w.', info.wind_code) unique_code, info.wind_code, 
                substring(info.wind_code, locate('.', info.wind_code) + 1, 
                length(info.wind_code)) market, '{cap_type}' type
            from {table_name} info
            left join code_mapping cm
            on cm.wind_code = info.wind_code 
            where cm.wind_code is null""".format(table_name=table_name, cap_type=cap_type)
        with with_db_session(engine_md) as session:
            rslt = session.execute(sql_str)
            logger.debug('从 %s 表中更新 code_mapping 记录 %d 条', table_name, rslt.rowcount)
            session.commit()
    else:
        # 补充已存在 wind_code 的记录的 ths_code 字段
        update_sql_str = """update code_mapping code_m, 
            (
                select concat('w.', wind_code) unique_code, wind_code,
                    substring(wind_code, locate('.', wind_code) + 1, length(wind_code)) market, 
                    '{cap_type}' type
                from (
                    select info.wind_code, info.sec_name sec_name 
                    from {table_name} info
                    left join code_mapping cm
                    on cm.wind_code = info.wind_code 
                    where cm.wind_code is null
                    ) ifi
                inner join {wind_table_name} wfi
                on ifi.sec_name = wfi.sec_name
            ) t
            set code_m.unique_code = t.unique_code, 
                code_m.wind_code = t.wind_code, 
                code_m.market = t.market, 
                code_m.type = t.type
            where code_m.ths_code = t.ths_code""".format(
            table_name=table_name, cap_type=cap_type, wind_table_name=wind_table_name)
        # 插入新增 ths_code
        insert_sql_str = """insert into code_mapping(unique_code, wind_code, market, type) 
            select concat('w.', wind_code) unique_code, wind_code, 
                substring(wind_code, locate('.', wind_code) + 1, length(wind_code)) market, 
                '{cap_type}' type 
            from (
                select info.ths_code, info.sec_name sec_name 
                from {table_name} info
                left join code_mapping cm
                on cm.wind_code = info.wind_code 
                where cm.wind_code is null
            ) main_fi
            left join {wind_table_name} sub_fi
            on main_fi.sec_name = sub_fi.sec_name
            where sub_fi.ths_code is null""".format(
            table_name=table_name, cap_type=cap_type, wind_table_name=wind_table_name)

        with with_db_session(engine_md) as session:
            rslt = session.execute(update_sql_str)
            logger.debug('从 %s 表中更新 code_mapping 记录 %d 条', table_name, rslt.rowcount)
            rslt = session.execute(insert_sql_str)
            logger.debug('从 %s 表中插入 code_mapping 记录 %d 条', table_name, rslt.rowcount)
            session.commit()


def update_from_info_table(table_name):
    """
    更新 code_mapping 表，根据对应的 info表中的 ths_code 或 wind_code 更新对应的 code_mapping表中对应字段
    :param table_name:
    :return:
    """
    ifind_cap_type = search_cap_type(ifind_info_table_pattern, table_name)
    wind_cap_type = search_cap_type(wind_info_table_pattern, table_name)

    if ifind_cap_type is not None:
        if ifind_cap_type.find('fund') >= 0:
            update_from_ifind_fund(table_name, ifind_cap_type)
        else:
            sql_str = """insert into code_mapping(unique_code, ths_code, market, type) 
                select ths_code, ths_code, 
                    substring(ths_code, locate('.', ths_code) + 1, length(ths_code)) market, '{cap_type}' 
                from {table_name} 
                on duplicate key update 
                ths_code=values(ths_code), market=values(market), type=values(type)
                """.format(table_name=table_name, cap_type=ifind_cap_type)
            with with_db_session(engine_md) as session:
                rslt = session.execute(sql_str)
                logger.debug('从 %s 表中更新 code_mapping 记录 %d 条', table_name, rslt.rowcount)
                session.commit()
    elif wind_cap_type is not None:
        if ifind_cap_type.find('fund') >= 0:
            update_from_wind_fund(table_name, ifind_cap_type)
        else:
            sql_str = """insert into code_mapping(unique_code, wind_code, market, type) 
                select wind_code, wind_code, 
                    substring(wind_code, locate('.', wind_code) + 1, length(wind_code)) market, '{cap_type}' 
                from {table_name} 
                on duplicate key update 
                wind_code=values(wind_code), market=values(market), type=values(type)
                """.format(table_name=table_name, cap_type=wind_cap_type)
            with with_db_session(engine_md) as session:
                rslt = session.execute(sql_str)
                logger.debug('从 %s 表中更新 code_mapping 记录 %d 条', table_name, rslt.rowcount)
                session.commit()
    else:
        raise ValueError('不支持 %s 更新 code_mapping 数据' % table_name)


if __name__ == '__main__':
    table_name = 'ifind_private_fund_info'  # ifind_future_info
    update_from_info_table(table_name)
