#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/6/12 13:02
@File    : orm.py
@contact : mmmaaaggg@163.com
@desc    :
"""
import logging
from datetime import datetime

from ibats_utils.db import alter_table_2_myisam
from ibats_utils.db import with_db_session
from sqlalchemy import Column, String, Table, DateTime
from sqlalchemy.ext.declarative import declarative_base

from tasks.backend import engine_md
from tasks.config import config

logger = logging.getLogger()
Base = declarative_base(bind=engine_md)
TABLE_MODEL_DIC = {}
_HEART_BEAT_THREAD = None


class CodeMapping(Base):
    __tablename__ = 'code_mapping'
    __table_args__ = {'mysql_engine': 'MyISAM'}
    unique_code = Column(String(20), primary_key=True, comment='统一编码，级别原则是 code.market，例如：600123.SH')
    wind_code = Column(String(20), comment='万得code')
    ths_code = Column(String(20), comment='同花顺code')
    jq_code = Column(String(20), comment='聚宽code')
    market = Column(String(20), comment='所在市场：SH, SZ, HK, ')
    type = Column(String(20), comment='资产类型：stock, fund, index, future, option')


class HeartBeat(Base):
    """心跳信息表"""

    __tablename__ = 'heart_beat'
    update_dt = Column(DateTime, primary_key=True)


def init_data():
    from sqlalchemy.sql import func
    with with_db_session(engine_md) as session:
        count = session.query(func.count(HeartBeat.update_dt)).scalar()
        if count == 0:
            logger.debug('初始化 HeartBeat 数据')
            session.add(HeartBeat(update_dt=datetime.now()))
            session.commit()
        else:
            session.query(HeartBeat).update({HeartBeat.update_dt: datetime.now()})
            session.commit()


def strat_heart_beat_thread():
    import time
    global _HEART_BEAT_THREAD
    if _HEART_BEAT_THREAD is not None and _HEART_BEAT_THREAD.is_alive():
        logger.info('timer_heart_beat thread is running')
        return

    def timer_heart_beat():
        logging.debug('timer_heart_beat thread start')
        is_debug, n = False, 10
        while True:
            if is_debug:
                if n <= 0:
                    break
                else:
                    n -= 1

            time.sleep(300)
            try:
                with with_db_session(engine_md) as session:
                    update_dt = datetime.now()
                    session.query(HeartBeat).update({HeartBeat.update_dt: update_dt})
                    session.commit()
                logger.debug('heart beat at %s', update_dt)
            except:
                logger.exception('heart beat exception')
                break

        logging.debug('timer_heart_beat thread finished')

    import threading
    _HEART_BEAT_THREAD = threading.Thread(target=timer_heart_beat)
    _HEART_BEAT_THREAD.daemon = True
    _HEART_BEAT_THREAD.start()


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
    init_data()


def build_primary_key(table_name_list):
    """
    自动判断表名称，将info、daily表自动增加主键
    目前支持 wind, ifind, tushare 以及合并后 info 及 daily表
    :param table_name_list:
    :return:
    """
    # 将info、daily表自动增加主键
    query_pk_str = """SELECT column_name FROM INFORMATION_SCHEMA.`KEY_COLUMN_USAGE` 
        WHERE table_name=:table_name AND CONSTRAINT_SCHEMA=:schema AND constraint_name='PRIMARY'"""

    table_count = len(table_name_list)
    with with_db_session(engine_md) as session:
        for num, table_name in enumerate(table_name_list, start=1):
            if table_name.find('ifind_') != -1:
                # ifind info daily
                create_ifind_daily_pk_str = """ALTER TABLE %s
                    CHANGE COLUMN `ths_code` `ths_code` VARCHAR(20) NOT NULL FIRST,
                    CHANGE COLUMN `time` `time` DATE NOT NULL AFTER `ths_code`,
                    ADD PRIMARY KEY (`ths_code`, `time`)"""
                create_ifind_info_pk_str = """ALTER TABLE %s
                    CHANGE COLUMN `ths_code` `ths_code` VARCHAR(20) NOT NULL FIRST,
                    ADD PRIMARY KEY (`ths_code`)"""
                if any([table_name.find(tag) != -1 for tag in ('_daily', '_report_date', '_fin')]):
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
                # wind info daily
                create_daily_pk_str = """ALTER TABLE %s
                    CHANGE COLUMN `wind_code` `wind_code` VARCHAR(20) NOT NULL FIRST,
                    CHANGE COLUMN `trade_date` `trade_date` DATE NOT NULL AFTER `wind_code`,
                    ADD PRIMARY KEY (`wind_code`, `trade_date`)"""
                create_info_pk_str = """ALTER TABLE %s
                    CHANGE COLUMN `wind_code` `wind_code` VARCHAR(20) NOT NULL FIRST,
                    ADD PRIMARY KEY (`wind_code`)"""
                create_min_pk_str = """ALTER TABLE %s
                    CHANGE COLUMN `wind_code` `wind_code` VARCHAR(20) NOT NULL FIRST,
                    CHANGE COLUMN `trade_date` `trade_date` DATE NOT NULL AFTER `wind_code`,
                    CHANGE COLUMN `trade_datetime` `trade_datetime` DATETIME NOT NULL AFTER `trade_date`,
                    ADD PRIMARY KEY (`wind_code`, `trade_date`, `trade_datetime`)"""
                if table_name.find('_daily') != -1:
                    col_name = session.execute(query_pk_str,
                                               params={'schema': config.DB_SCHEMA_MD,
                                                       'table_name': table_name}).scalar()
                    if col_name is None:
                        # 如果没有记录则 创建主键
                        session.execute(create_daily_pk_str % table_name)
                        logger.info('%d/%d) %s 建立主键 [wind_code, trade_date]', num, table_count, table_name)

                elif table_name.find('_min') != -1:
                    col_name = session.execute(query_pk_str,
                                               params={'schema': config.DB_SCHEMA_MD,
                                                       'table_name': table_name}).scalar()
                    if col_name is None:
                        # 如果没有记录则 创建主键
                        session.execute(create_min_pk_str % table_name)
                        logger.info('%d/%d) %s 建立主键 [wind_code, trade_date, trade_datetime]', num, table_count, table_name)

                elif table_name.find('_info') != -1:
                    col_name = session.execute(query_pk_str,
                                               params={'schema': config.DB_SCHEMA_MD,
                                                       'table_name': table_name}).scalar()
                    if col_name is None:
                        # 如果没有记录则 创建主键
                        session.execute(create_info_pk_str % table_name)
                        logger.info('%d/%d) %s 建立主键 [wind_code]', num, table_count, table_name)
                else:
                    logger.debug('%d/%d) %s 无需操作', num, table_count, table_name)

            elif table_name.find('tushare_') != -1:
                # tushare info daily
                create_daily_pk_str = """ALTER TABLE %s
                        CHANGE COLUMN `ts_code` `ts_code` VARCHAR(20) NOT NULL FIRST,
                        CHANGE COLUMN `trade_date` `trade_date` DATE NOT NULL AFTER `ts_code`,
                        ADD PRIMARY KEY (`ts_code`, `trade_date`)"""
                create_info_pk_str = """ALTER TABLE %s
                        CHANGE COLUMN `ts_code` `ts_code` VARCHAR(20) NOT NULL FIRST,
                        ADD PRIMARY KEY (`ts_code`)"""
                if table_name.find('_daily') != -1:
                    col_name = session.execute(query_pk_str,
                                               params={'schema': config.DB_SCHEMA_MD,
                                                       'table_name': table_name}).scalar()
                    if col_name is None:
                        # 如果没有记录则 创建主键
                        session.execute(create_daily_pk_str % table_name)
                        logger.info('%d/%d) %s 建立主键 [ts_code, trade_date]', num, table_count, table_name)

                elif table_name.find('_info') != -1:
                    col_name = session.execute(query_pk_str,
                                               params={'schema': config.DB_SCHEMA_MD,
                                                       'table_name': table_name}).scalar()
                    if col_name is None:
                        # 如果没有记录则 创建主键
                        session.execute(create_info_pk_str % table_name)
                        logger.info('%d/%d) %s 建立主键 [ts_code]', num, table_count, table_name)
                else:
                    logger.debug('%d/%d) %s 无需操作', num, table_count, table_name)

            elif table_name.find('rqdatac_') != -1:
                # wind info daily
                create_daily_pk_str = """ALTER TABLE %s
                    CHANGE COLUMN `order_book_id` `order_book_id` VARCHAR(20) NOT NULL FIRST,
                    CHANGE COLUMN `trade_date` `trade_date` DATE NOT NULL AFTER `order_book_id`,
                    ADD PRIMARY KEY (`order_book_id`, `trade_date`)"""
                # wind info min
                create_min_pk_str = """ALTER TABLE %s
                    CHANGE COLUMN `order_book_id` `order_book_id` VARCHAR(20) NOT NULL FIRST,
                    CHANGE COLUMN `trade_date` `trade_date` datetime NOT NULL AFTER `order_book_id`,
                    ADD PRIMARY KEY (`order_book_id`, `trade_date`)"""
                create_info_pk_str = """ALTER TABLE %s
                    CHANGE COLUMN `order_book_id` `order_book_id` VARCHAR(20) NOT NULL FIRST,
                    ADD PRIMARY KEY (`order_book_id`)"""
                if table_name.find('_daily') != -1:
                    col_name = session.execute(query_pk_str,
                                               params={'schema': config.DB_SCHEMA_MD,
                                                       'table_name': table_name}).scalar()
                    if col_name is None:
                        # 如果没有记录则 创建主键
                        session.execute(create_daily_pk_str % table_name)
                        logger.info('%d/%d) %s 建立主键 [order_book_id, trade_date]', num, table_count, table_name)

                elif table_name.find('_min') != -1:
                    col_name = session.execute(query_pk_str,
                                               params={'schema': config.DB_SCHEMA_MD,
                                                       'table_name': table_name}).scalar()
                    if col_name is None:
                        # 如果没有记录则 创建主键
                        session.execute(create_min_pk_str % table_name)
                        logger.info('%d/%d) %s 建立主键 [order_book_id, trade_date]', num, table_count, table_name)

                elif table_name.find('_info') != -1:
                    col_name = session.execute(query_pk_str,
                                               params={'schema': config.DB_SCHEMA_MD,
                                                       'table_name': table_name}).scalar()
                    if col_name is None:
                        # 如果没有记录则 创建主键
                        session.execute(create_info_pk_str % table_name)
                        logger.info('%d/%d) %s 建立主键 [order_book_id]', num, table_count, table_name)
                else:
                    logger.debug('%d/%d) %s 无需操作', num, table_count, table_name)

            else:
                # 合并后的 info daily
                create_daily_pk_str = """ALTER TABLE %s
                    CHANGE COLUMN `unique_code` `unique_code` VARCHAR(20) NOT NULL FIRST,
                    CHANGE COLUMN `trade_date` `trade_date` DATE NOT NULL AFTER `unique_code`,
                    ADD PRIMARY KEY (`unique_code`, `trade_date`)"""
                create_info_pk_str = """ALTER TABLE %s
                    CHANGE COLUMN `unique_code` `unique_code` VARCHAR(20) NOT NULL FIRST,
                    ADD PRIMARY KEY (`unique_code`)"""
                if table_name.find('_daily') != -1:
                    col_name = session.execute(query_pk_str,
                                               params={'schema': config.DB_SCHEMA_MD,
                                                       'table_name': table_name}).scalar()
                    if col_name is None:
                        # 如果没有记录则 创建主键
                        session.execute(create_daily_pk_str % table_name)
                        logger.info('%d/%d) %s 建立主键 [unique_code, trade_date]', num, table_count, table_name)

                elif table_name.find('_info') != -1:
                    col_name = session.execute(query_pk_str,
                                               params={'schema': config.DB_SCHEMA_MD,
                                                       'table_name': table_name}).scalar()
                    if col_name is None:
                        # 如果没有记录则 创建主键
                        session.execute(create_info_pk_str % table_name)
                        logger.info('%d/%d) %s 建立主键 [unique_code]', num, table_count, table_name)
                else:
                    logger.debug('%d/%d) %s 无需操作', num, table_count, table_name)


strat_heart_beat_thread()

if __name__ == "__main__":
    init()
