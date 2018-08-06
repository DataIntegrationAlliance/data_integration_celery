# -*- coding: utf-8 -*-
"""
Created on 2017/12/9
@author: MG
"""
from datetime import datetime, date
import pandas as pd
import math
import sqlite3
from config import Config
import logging
from backend.fh_utils import date_2_str

logger = logging.getLogger()


def full_migration(table_name, from_schema, to_schema):
    """
    全量迁移
    :param table_name: 
    :param from_schema: 
    :param to_schema: 
    :return: 
    """
    logger.warning("%s 迁移数据 开始", table_name)
    datetime_start = datetime.now()
    engine_from = Config.get_db_engine(schema_name=from_schema)
    engine_to = Config.get_db_engine(schema_name=to_schema)
    sql_str = "select * from %s" % table_name
    from_df = pd.read_sql(sql_str, engine_from)
    if from_df.shape[0] == 0:
        logger.warning("%s 没有可迁移数据", table_name)
    with Config.with_db_session(engine_to) as session:
        session.execute("truncate table %s" % table_name)
    from_df.to_sql(table_name, engine_to, if_exists="append", index=False)
    datetime_end = datetime.now()
    logger.warning("%s 迁移数据 完成，用时 %s", table_name, (datetime_end - datetime_start).total_seconds())


def increment_migration_4_2pk_with_date(table_name, pk1_name, pk2_date_name, from_schema, to_schema):
    """
    按主键增量迁移
    :param table_name: 
    :param pk1_name: 
    :param pk2_date_name: 
    :param from_schema: 
    :param to_schema: 
    :return: 
    """
    logger.warning("%s 迁移数据 开始", table_name)
    datetime_start = datetime.now()
    engine_from = Config.get_db_engine(schema_name=from_schema)
    engine_to = Config.get_db_engine(schema_name=to_schema)
    sql_str = "select %s, max(%s) as '%s' from %s group by %s" % (
    pk1_name, pk2_date_name, pk2_date_name, table_name, pk1_name)
    pk_from_df = pd.read_sql(sql_str, engine_from, index_col=pk1_name)
    if pk_from_df.shape[0] == 0:
        logger.warning("%s 没有可迁移数据")

    pk_to_df = pd.read_sql(sql_str, engine_to, index_col=pk1_name)
    pk_join_df = pk_from_df.join(pk_to_df, lsuffix="_source", rsuffix="_to")
    pk_migrate_df = pk_join_df[(pk_join_df["%s_source" % pk2_date_name] > pk_join_df["%s_to" % pk2_date_name]) | (
    pk_join_df["%s_to" % pk2_date_name].apply(lambda x: False if isinstance(x, date) else math.isnan(x)))]
    pk1_date_dic = pk_migrate_df.to_dict("index")
    data_count = len(pk1_date_dic)
    transfer_count = 0
    for num, (pk1, date_pair_dic) in enumerate(pk1_date_dic.items()):
        date_to = date_pair_dic["%s_to" % pk2_date_name]
        logger.debug("%d/%d) %s %s", num, data_count, pk1, date_pair_dic)
        if isinstance(date_to, date):
            date_to_str = date_to.strftime(Config.DATE_FORMAT_STR)
            sql_str = "select * from %s where %s = '%s' and %s > '%s'" % (
                table_name, pk1_name, pk1, pk2_date_name, date_to_str
            )
        else:
            sql_str = "select * from %s where %s = '%s'" % (
                table_name, pk1_name, pk1
            )
        from_df = pd.read_sql(sql_str, engine_from)
        from_df.to_sql(table_name, engine_to, if_exists="append", index=False)
        transfer_count += from_df.shape[0]
    datetime_end = datetime.now()
    logger.warning("%s 迁移数据 完成，%d 条记录 %d 组 完成迁移，用时 %s",
                   table_name, transfer_count, data_count, (datetime_end - datetime_start).total_seconds())


def wind_stock_daily_wch_2_sqlite(schema_name):
    file_name = "stock_daily_wch.db"
    logger.warning("%s 数据生成 开始", file_name)
    datetime_start = datetime.now()
    engine = Config.get_db_engine(schema_name=schema_name)
    sql_str = "select * from wind_stock_daily_wch"
    stock_daily_df = pd.read_sql(sql_str, engine)
    stock_daily_gdf = stock_daily_df.groupby("wind_code")
    engine_to = sqlite3.connect(file_name, )
    for n, (wind_code, daily_df) in enumerate(stock_daily_gdf):
        logger.debug("%d) %s %d", n, wind_code, daily_df.shape[0])
        daily_df.drop(columns='wind_code').to_sql(wind_code, engine_to, index=False)
    datetime_end = datetime.now()
    logger.warning("%s 数据生成 完成，用时 %s", file_name, (datetime_end - datetime_start).total_seconds())


def migration_tables(table_name_list, from_schema=Config.DB_SCHEMA_PROD, to_schema=Config.DB_SCHEMA_DEFAULT,
                     force_full_migration=False):
    """
    将两个数据库中指定表进行同步
    :param table_name_list: 
    :param from_schema: 
    :param to_schema: 
    :return: 
    """
    full_migration_table_name_set = [
        "wind_future_info", 'wind_future_daily',
        "wind_index_daily", "wind_trade_date",
        "wind_stock_info", "wind_stock_info_hk", "wind_stock_quertarly",
        "wind_pub_fund_info", "wind_pub_fund_daily",
        # "wind_stock_daily", "wind_stock_daily_wch",  # 增量同步方式
        "wind_smfund_info", "wind_smfund_daily",
        "fund_info", "fund_nav",
        "fund_core_info", "fund_essential_info", 'fund_event', "fund_mgrcomp_info",
        "fund_sec_pct", "fund_stg_pct", "fund_transaction",
        "scheme_fund_pct", "scheme_info",
        "fof_fund_pct", "fof_scheme_fund_pct",
    ]
    for table_name in table_name_list:
        if table_name in full_migration_table_name_set or force_full_migration:
            full_migration(table_name, from_schema, to_schema)
        elif table_name == "wind_stock_daily":
            table_name, pk1_name, pk2_date_name = "wind_stock_daily", "wind_code", "trade_date"
            increment_migration_4_2pk_with_date(table_name, pk1_name, pk2_date_name, from_schema, to_schema)
        elif table_name == "wind_stock_daily_hk":
            table_name, pk1_name, pk2_date_name = "wind_stock_daily_hk", "wind_code", "trade_date"
            increment_migration_4_2pk_with_date(table_name, pk1_name, pk2_date_name, from_schema, to_schema)
        elif table_name == "wind_stock_daily_wch":
            table_name, pk1_name, pk2_date_name = "wind_stock_daily_wch", "wind_code", "Date"
            increment_migration_4_2pk_with_date(table_name, pk1_name, pk2_date_name, from_schema, to_schema)
        elif table_name == "wind_stock_daily_wch_2_sqlite":
            wind_stock_daily_wch_2_sqlite(to_schema)


def check_max_date(from_schema=Config.DB_SCHEMA_PROD, to_schema=Config.DB_SCHEMA_DEFAULT):
    """
    检查数据库中各个表的最新日期
    :return: 
    """
    sql_str = """select 'future_daily' table_name, max(trade_date) trade_date_max from wind_future_daily
union
select 'stock_daily' table_name, max(trade_date) trade_date_max from wind_stock_daily
union
select 'stock_daily_wch' table_name, max(Date) trade_date_max from wind_stock_daily_wch
UNION
select 'pub_fund_daily' table_name, max(nav_date) trade_date_max from wind_pub_fund_daily
union
select 'fund_daily' table_name, max(nav_date) trade_date_max from wind_fund_nav
union
select 'sm_fund_daily' table_name, max(trade_date) trade_date_max from wind_smfund_daily
union
select 'convertible_bond_daily' table_name, max(trade_date) trade_date_max from wind_convertible_bond_daily
union
select 'index_daily' table_name, max(trade_date) trade_date_max from wind_index_daily
union
select 'stock_daily_hk' table_name, max(trade_date) trade_date_max from wind_stock_daily_hk
"""
    engine = Config.get_db_engine(Config.DB_SCHEMA_PROD)
    data_prod_df = pd.read_sql(sql_str, engine, index_col='table_name')
    engine = Config.get_db_engine(Config.DB_SCHEMA_DEFAULT)
    data_local_df = pd.read_sql(sql_str, engine, index_col='table_name')
    data_df = pd.merge(data_local_df.rename(columns={"trade_date_max": "max_local"}),
                       data_prod_df.rename(columns={"trade_date_max": "max_prod"}),
                       left_index=True, right_index=True)
    data_df['equal'] = data_df["max_local"] == data_df["max_prod"]
    return data_df


def check_migration(do_merge=True, key_list=None, direction='prod->dev'):
    """
    检查各个表的最新日期，将不一致的表进行同步
    :return: 
    """
    if direction == 'prod->dev':
        from_schema = Config.DB_SCHEMA_PROD
        to_schema = Config.DB_SCHEMA_DEFAULT
    elif direction == 'dev->prod':
        from_schema = Config.DB_SCHEMA_DEFAULT
        to_schema = Config.DB_SCHEMA_PROD
    elif direction == 'prod->prophets':
        from_schema = Config.DB_SCHEMA_PROD
        to_schema = Config.DB_SCHEMA_PROPHETS_MD
    else:
        raise ValueError('direction %s 无效' % direction)

    data_df = check_max_date(from_schema=from_schema, to_schema=to_schema)
    logger.info("\n%s", data_df)
    if do_merge:
        table_name_dic = {
            "future_daily": ["wind_future_info", 'wind_future_daily'],
            "stock_daily": ["wind_stock_daily", "wind_stock_info", "wind_stock_quertarly", ],
            "stock_daily_wch": ["wind_stock_daily_wch"],
            "pub_fund_daily": ["wind_pub_fund_info", "wind_pub_fund_daily", ],
            "fund_daily": ["fund_info", "fund_nav"],
            "sm_fund_daily": ["wind_smfund_info", "wind_smfund_daily"],
            "convertible_bond_daily": ["wind_convertible_bond_info", "wind_convertible_bond_daily"],
            "index_daily": ["wind_index_daily", "wind_trade_date"],
            "stock_daily_hk": ["wind_stock_daily_hk", "wind_stock_info_hk"],
            "foftech": [
                "fund_core_info", "fund_essential_info", 'fund_event', "fund_mgrcomp_info",
                "fund_sec_pct", "fund_stg_pct", "fund_transaction",
                "scheme_fund_pct", "scheme_info",
                "fof_fund_pct", "fof_scheme_fund_pct", "file_type"
            ],
        }
        data_count = data_df.shape[0]
        for data_num, (key_name, date_dic) in enumerate(data_df.to_dict("index").items(), start=1):
            if key_list is None or key_name in key_list:
                if date_dic["max_local"] is not None and date_dic["max_prod"] is not None and \
                        date_dic["max_local"] >= date_dic["max_prod"]:
                    logger.info("%d/%d %s 生产端：%s 本地：%s 无需同步",
                                data_num, data_count, key_name,
                                date_2_str(date_dic["max_prod"]), date_2_str(date_dic["max_local"]))
                else:
                    logger.info("%d/%d %s 生产端：%s 本地：%s 将与服务器进行同步",
                                data_num, data_count, key_name,
                                date_2_str(date_dic["max_prod"]), date_2_str(date_dic["max_local"]))
                    if key_name not in table_name_dic:
                        logger.error("%d/%d %s 没有找到相应的同步列表", data_num, data_count, key_name)
                        continue
                    table_name_list = table_name_dic[key_name]
                    migration_tables(table_name_list, from_schema=from_schema, to_schema=to_schema)
        else:
            data_df = check_max_date(from_schema=from_schema, to_schema=to_schema)
            logger.info("同步完成后对比结果：\n%s", data_df)
        # 其他数据同步
        if "foftech" in key_list:
            table_name_list = table_name_dic["foftech"]
            migration_tables(table_name_list, from_schema=from_schema, to_schema=to_schema)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format=Config.LOG_FORMAT)

    # 检查两套环境的数据最新日期
    # data_df = check_max_date()
    # logger.info("\n%s", data_df)

    # 自动迁移 生产环境 到 本地数据库
    key_list = [
        'convertible_bond_daily',
        'index_daily',
        'sm_fund_daily',
        'future_daily',
        'pub_fund_daily',
        'stock_daily',
        'stock_daily_wch',
        'stock_daily_hk',
        'foftech',
        'fund_daily',
    ]
    check_migration(do_merge=True, key_list=key_list, direction='prod->dev')  # 'dev->prod' 'prod->dev'

    # 自动迁移 生产环境 到 prophets
    key_list = [
        # 'convertible_bond_daily',
        'index_daily',
        'sm_fund_daily',
        'future_daily',
        'pub_fund_daily',
        'stock_daily',
        # 'stock_daily_wch',
        'stock_daily_hk',
        # 'fund_daily',
    ]
    check_migration(do_merge=True, key_list=key_list, direction='prod->prophets')  # 'dev->prod' 'prod->dev'

    # 手动全量迁移
    # table_name_list = ['em_stock_pub_date',]
    # migration_tables(table_name_list, from_schema=Config.DB_SCHEMA_PROD, to_schema=Config.DB_SCHEMA_DEFAULT,
    #                  force_full_migration=True)
