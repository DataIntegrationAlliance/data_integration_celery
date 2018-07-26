#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/6/12 13:02
@File    : orm.py
@contact : mmmaaaggg@163.com
@desc    :
"""
from sqlalchemy import Column, Integer, String, UniqueConstraint, TIMESTAMP
from sqlalchemy.dialects.mysql import DOUBLE
from sqlalchemy.ext.declarative import declarative_base
from tasks.utils.db_utils import with_db_session
from tasks.backend import engine_md
from tasks.config import config
import logging
logger = logging.getLogger()
BaseModel = declarative_base()


def init(alter_table=False):
    BaseModel.metadata.create_all(engine_md)
    if alter_table:
        with with_db_session(engine=engine_md) as session:
            for table_name, _ in BaseModel.metadata.tables.items():
                sql_str = "show table status from " + config.DB_SCHEMA_MD + " where name=:table_name"
                row_data = session.execute(sql_str, params={'table_name': table_name}).first()
                if row_data is None:
                    continue
                if row_data[1].lower() == 'myisam':
                    continue

                logger.info('修改 %s 表引擎为 MyISAM', table_name)
                sql_str = "ALTER TABLE %s ENGINE = MyISAM" % table_name
                session.execute(sql_str)

            sql_str = """select table_name from information_schema.columns 
              where table_schema = :table_schema and column_name = 'ts_start' and extra <> ''"""
            table_name_list = [row_data[0]
                               for row_data in session.execute(sql_str, params={'table_schema': config.DB_SCHEMA_MD})]

            for table_name in table_name_list:
                logger.info('修改 %s 表 ts_start 默认值，剔除 on update 默认项', table_name)
                # TimeStamp 类型的数据会被自动设置 default: 'CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP'
                # 需要将 “on update CURRENT_TIMESTAMP”剔除，否则在执行更新时可能会引起错误
                session.execute("ALTER TABLE " + table_name + " CHANGE COLUMN `ts_start` `ts_start` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP")

    logger.info("所有表结构建立完成")


if __name__ == "__main__":
    init()
