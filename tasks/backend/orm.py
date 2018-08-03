#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/6/12 13:02
@File    : orm.py
@contact : mmmaaaggg@163.com
@desc    :
"""
from sqlalchemy import MetaData, Column, Integer, String, UniqueConstraint, TIMESTAMP
from sqlalchemy.dialects.mysql import DOUBLE
from sqlalchemy.ext.declarative import declarative_base
from tasks.utils.db_utils import with_db_session
from tasks.backend import engine_md
from tasks.config import config
import logging
from tasks.utils.db_utils import alter_table_2_myisam
logger = logging.getLogger()


def init(alter_table=False):
    if alter_table:
        alter_table_2_myisam(engine_md)

    # 将info、daily表自动增加主键
    query_pk_str = """SELECT column_name FROM INFORMATION_SCHEMA.`KEY_COLUMN_USAGE` 
      WHERE table_name='sys_user' AND CONSTRAINT_SCHEMA=:DB_NAME_MD AND constraint_name='PRIMARY'"""
    create_daily_pk_str = """ALTER TABLE %s
        CHANGE COLUMN `ths_code` `ths_code` VARCHAR(20) NOT NULL ,
        CHANGE COLUMN `time` `time` DATE NOT NULL ,
        ADD PRIMARY KEY (`ths_code`, `time`)"""
    create_info_pk_str = """ALTER TABLE %s
        CHANGE COLUMN `ths_code` `ths_code` VARCHAR(20) NOT NULL ,
        ADD PRIMARY KEY (`ths_code`)"""
    table_name_list = engine_md.table_names()
    table_count = len(table_name_list)
    with with_db_session(engine_md) as session:
        for num, table_name in enumerate(table_name_list, start=1):
            if table_name.find('_daily') != -1:
                col_name = session.execute(query_pk_str, params={'schema': config.DB_NAME_MD,
                                             'table_name': table_name}).scalar()
                if col_name is None:
                    # 如果没有记录则 创建主键
                    session.execute(create_daily_pk_str % table_name)
                    logger.info('%d/%d) %s 建立主键 (ths_code, time)', num, table_count, table_name)

            elif table_name.find('_info') != -1:
                col_name = session.execute(query_pk_str, params={'schema': config.DB_NAME_MD,
                                             'table_name': table_name})
                if col_name is None:
                    # 如果没有记录则 创建主键
                    session.execute(create_info_pk_str % table_name)
                    logger.info('%d/%d) %s 建立主键 (ths_code)', num, table_count, table_name)

    logger.info("所有表结构建立完成")


if __name__ == "__main__":
    init()
