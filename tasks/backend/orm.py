#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/6/12 13:02
@File    : orm.py
@contact : mmmaaaggg@163.com
@desc    :
"""
from sqlalchemy import MetaData, Column, Integer, String, UniqueConstraint, TIMESTAMP, Table
from sqlalchemy.dialects.mysql import DOUBLE
from sqlalchemy.ext.declarative import declarative_base
from tasks.utils.db_utils import with_db_session
from tasks.backend import engine_md
from tasks.config import config
import logging
from tasks.utils.db_utils import alter_table_2_myisam

logger = logging.getLogger()
Base = declarative_base()
TABLE_MODEL_DIC = {}


class CodeMapping(Base):
    __tablename__ = 'code_mapping'
    __table_args__ = {'mysql_engine': 'MyISAM'}
    unique_code = Column(String(20), primary_key=True, comment='统一编码，级别原则是 code.market，例如：600123.SH')
    wind_code = Column(String(20), comment='万得code')
    ths_code = Column(String(20), comment='同花顺code')
    market = Column(String(20), comment='所在市场：SH, SZ, HK, ')
    type = Column(String(20), comment='资产类型：stock, fund, index, future, option')


def init(alter_table=False):
    # 创建表
    Base.metadata.create_all(engine_md)
    logger.info("所有表结构建立完成")

    if alter_table:
        alter_table_2_myisam(engine_md)

    table_name_list = engine_md.table_names()
    build_primary_key(table_name_list)
    logger.info("所有表结构调整完成")

    for table_name in table_name_list:
        TABLE_MODEL_DIC[table_name] = Table(table_name, Base.metadata, autoload=True)

    logger.info("所有表Model动态加载完成")


def build_primary_key(table_name_list):
    """
    自动判断表名称，将info、daily表自动增加主键
    :param table_name_list:
    :return:
    """
    # 将info、daily表自动增加主键
    query_pk_str = """SELECT column_name FROM INFORMATION_SCHEMA.`KEY_COLUMN_USAGE` 
        WHERE table_name=:table_name AND CONSTRAINT_SCHEMA=:schema AND constraint_name='PRIMARY'"""
    create_ifind_daily_pk_str = """ALTER TABLE %s
        CHANGE COLUMN `ths_code` `ths_code` VARCHAR(20) NOT NULL FIRST,
        CHANGE COLUMN `time` `time` DATE NOT NULL AFTER `ths_code`,
        ADD PRIMARY KEY (`ths_code`, `time`)"""
    create_ifind_info_pk_str = """ALTER TABLE %s
        CHANGE COLUMN `ths_code` `ths_code` VARCHAR(20) NOT NULL FIRST,
        ADD PRIMARY KEY (`ths_code`)"""

    create_wind_daily_pk_str = """ALTER TABLE %s
        CHANGE COLUMN `wind_code` `wind_code` VARCHAR(20) NOT NULL FIRST,
        CHANGE COLUMN `trade_date` `trade_date` DATE NOT NULL AFTER `wind_code`,
        ADD PRIMARY KEY (`wind_code`, `trade_date`)"""
    create_wind_info_pk_str = """ALTER TABLE %s
        CHANGE COLUMN `wind_code` `wind_code` VARCHAR(20) NOT NULL FIRST,
        ADD PRIMARY KEY (`wind_code`)"""

    table_count = len(table_name_list)
    with with_db_session(engine_md) as session:
        for num, table_name in enumerate(table_name_list, start=1):
            if table_name.find('ifind_') != -1:
                if table_name.find('_daily') != -1:
                    col_name = session.execute(query_pk_str,
                                               params={'schema': config.DB_SCHEMA_MD,
                                                       'table_name': table_name}).scalar()
                    if col_name is None:
                        # 如果没有记录则 创建主键
                        session.execute(create_ifind_daily_pk_str % table_name)
                        logger.info('%d/%d) %s 建立主键 [ths_code, time]', num, table_count, table_name)

                elif table_name.find('_info') != -1:
                    col_name = session.execute(query_pk_str,
                                               params={'schema': config.DB_SCHEMA_MD,
                                                       'table_name': table_name}).scalar()
                    if col_name is None:
                        # 如果没有记录则 创建主键
                        session.execute(create_ifind_info_pk_str % table_name)
                        logger.info('%d/%d) %s 建立主键 [ths_code]', num, table_count, table_name)
                else:
                    logger.debug('%d/%d) %s 无需操作', num, table_count, table_name)
            elif table_name.find('wind_') != -1:
                if table_name.find('_daily') != -1:
                    col_name = session.execute(query_pk_str,
                                               params={'schema': config.DB_SCHEMA_MD,
                                                       'table_name': table_name}).scalar()
                    if col_name is None:
                        # 如果没有记录则 创建主键
                        session.execute(create_wind_daily_pk_str % table_name)
                        logger.info('%d/%d) %s 建立主键 [wind_code, trade_date]', num, table_count, table_name)

                elif table_name.find('_info') != -1:
                    col_name = session.execute(query_pk_str,
                                               params={'schema': config.DB_SCHEMA_MD,
                                                       'table_name': table_name}).scalar()
                    if col_name is None:
                        # 如果没有记录则 创建主键
                        session.execute(create_wind_info_pk_str % table_name)
                        logger.info('%d/%d) %s 建立主键 [wind_code]', num, table_count, table_name)
                else:
                    logger.debug('%d/%d) %s 无需操作', num, table_count, table_name)


if __name__ == "__main__":
    init()
