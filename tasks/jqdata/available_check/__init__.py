#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 19-4-8 上午11:23
@File    : __init__.py.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from ibats_utils.db import get_table_col
import pandas as pd
from tasks.backend import with_db_session, engine_md
from tasks.config import config
import logging

logger = logging.getLogger(__name__)


def backup_table(table_name):
    """
    对数据库表 table_name 进行备份 新表名 f'{table_name}_bak'
    :param table_name:
    :return:
    """
    has_table = engine_md.has_table(table_name)
    if not has_table:
        return

    table_name_backup = f'{table_name}_bak'
    has_table = engine_md.has_table(table_name_backup)
    with with_db_session() as session:
        if has_table:
            sql_str = f'truncate table {table_name_backup}'
            session.execute(sql_str)
            logger.debug('清空 %s 表', table_name_backup)
        else:
            sql_str = f'create table {table_name_backup} like {table_name}'
            session.execute(sql_str)
            logger.debug('创建 %s 表', table_name_backup)
        # 数据 copy
        sql_str = f'insert into {table_name_backup} select * from {table_name}'
        result = session.execute(sql_str)
        data_count = result.rowcount
        logger.info('%s 导入 %d 条记录', table_name_backup, data_count)


def check_diff(table_name, table_name_backup=None):
    """
    检查数据库两个表数据是否一致
    :param table_name:
    :param table_name_backup:
    :return:
    """
    has_table = engine_md.has_table(table_name)
    if not has_table:
        logger.warning('%s 表不存在无需检查', table_name)
        return None

    if table_name_backup is None:
        table_name_backup = f'{table_name}_bak'
    has_table = engine_md.has_table(table_name_backup)
    if not has_table:
        logger.warning('%s 表不存在无需检查', table_name_backup)
        return None

    # table_name, table_name_backup = 'jq_stock_daily_md_pre', 'jq_stock_daily_md_pre_bak'  # 测试使用
    col_list = get_table_col(table_name, engine_md, config.DB_SCHEMA_MD)
    # 主键字段名称列表
    pk_col_set = {_[0] for _ in col_list if _[2]}
    # 非主键字段名称列表
    col_not_pk_list = [col_name for col_name, col_type, is_primary_key in col_list if not is_primary_key]
    # selection clause
    selection_clause_str = ', '.join([f"t.`{key}` {key}" for key in pk_col_set]) + ',\n'
    selection_clause_str += ',\n'.join([
        f"t.`{col_name}` {col_name}_t, bak.`{col_name}` {col_name}_bak"
        for col_name in col_not_pk_list])
    # join on condition
    # t.jq_code = bak.jq_code and t.trade_date = bak.trade_date
    on_condition_str = '\n and '.join([f"t.`{key}` = bak.`{key}`" for key in pk_col_set])
    # where condition
    # t.open <> bak.open or t.close <> bak.close or ...
    where_condition_str = '\n or '.join([
        f"t.`{col_name}` <> bak.`{col_name}`" for col_name in col_not_pk_list])
    sql_str = f"""select {selection_clause_str} from {table_name} t 
        inner join {table_name_backup} bak
        on {on_condition_str}
        where {where_condition_str}"""
    df = pd.read_sql(sql_str, engine_md)
    data_count = df.shape[0]
    if data_count > 0:
        for num, (_, data_s) in enumerate(df.T.items(), start=1):
            for col_name in col_not_pk_list:
                col_name_t, col_name_bak = f'{col_name}_t', f'{col_name}_bak'
                value_t, value_bak = data_s[col_name_t], data_s[col_name_bak]
                is_nan_on_t, is_nan_on_bak = pd.isna(value_t), pd.isna(value_bak)
                if is_nan_on_t and is_nan_on_bak:
                    continue
                if value_t != value_bak:
                    # 两个字段一个为空，另一个不为空，或者两者不一致
                    logger.warning("%s %d/%d) 字段 %s 数值不一致 %f != %f",
                                   table_name, num, data_count, ', '.join([str(data_s[key]) for key in pk_col_set]),
                                   value_t, value_bak)
                    break

    return df


if __name__ == "__main__":
    table_name = 'jq_stock_daily_md_pre'
    backup_table(table_name)
    check_diff(table_name)
