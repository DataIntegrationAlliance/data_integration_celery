"""
@author  : MG
@Time    : 2020/9/14 12:18
@File    : delete_duplicate_ticks.py
@contact : mmmaaaggg@163.com
@desc    : 用于
"""
import logging
from ibats_utils.db import with_db_session
from tasks.backend import engine_md

logger = logging.getLogger()


def drop_duplicate_data_from_table(table_name, engine, primary_key=None):
    """
    仅针对 vnpy dbtickdata 表进行取重操作
    根据主键删除重复数据
    做法：新建表并建立主键->将原有表数据导入到新表->删除旧表->重命名新表
    :param table_name:
    :param engine:
    :param primary_key:
    :return:
    """
    table_name_bak = f"{table_name}_bak"
    has_table = engine.has_table(table_name)
    if not has_table:
        return
    has_table = engine.has_table(table_name_bak)
    with with_db_session(engine) as session:
        if has_table:
            sql_str = f"drop table {table_name_bak}"
            session.execute(sql_str)
            logger.debug('删除现有 %s 表', table_name_bak)

        sql_str = f"create table {table_name_bak} like {table_name}"
        session.execute(sql_str)
        sql_str = f"ALTER TABLE {table_name_bak} CHANGE COLUMN `id` `id` INT NOT NULL , DROP PRIMARY KEY;"
        session.execute(sql_str)
        logger.debug('创建 %s 表', table_name_bak)
        if primary_key is not None:
            key_str = ', '.join(primary_key)
            sql_str = f"""alter table {table_name_bak}
                add constraint {table_name}_pk
                primary key ({key_str})"""
            session.execute(sql_str)
            logger.debug('创建 %s 表 主键 %s：', table_name_bak, key_str)

        sql_str = f"replace into {table_name_bak} select * from {table_name}"
        logger.debug('执行数据插入动作 %s', sql_str)
        session.execute(sql_str)
        session.commit()
        logger.debug('插入数据 %s -> %s', table_name, table_name_bak)
        sql_str = f"drop table {table_name}"
        session.execute(sql_str)
        logger.debug('删除 %s 表', table_name)
        sql_str = f"rename table {table_name_bak} to {table_name}"
        session.execute(sql_str)
        logger.debug('重命名 %s --> %s', table_name_bak, table_name)


def delete_duplicate_ticks():
    """仅针对 vnpy dbtickdata 表进行取重操作"""
    table_name = 'dbtickdata'
    engine = engine_md
    primary_key = ['symbol', 'exchange', 'datetime']
    drop_duplicate_data_from_table(table_name, engine, primary_key)


if __name__ == "__main__":
    delete_duplicate_ticks()
