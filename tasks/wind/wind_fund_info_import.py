# -*- coding: utf-8 -*-
"""
Created on Fri Feb 17 10:17:38 2017

@author: Administrator
"""
import pandas as pd
from sqlalchemy.types import String, Date
from datetime import date
import logging
from tasks.backend import engine_md
from tasks.wind import invoker
# from logging.handlers import RotatingFileHandler
from tasks.backend.orm import build_primary_key
from tasks.merge.code_mapping import update_from_info_table
from tasks.utils.db_utils import bunch_insert_on_duplicate_update
from tasks.utils.db_utils import alter_table_2_myisam


# formatter = logging.Formatter('[%(asctime)s:  %(levelname)s  %(name)s %(message)s')
# file_handle = RotatingFileHandler("app.log",mode='a',maxBytes=5 * 1024 * 1024,backupCount=2,encoding=None,delay=0)
# file_handle.setFormatter(formatter)
# console_handle = logging.StreamHandler()
# console_handle.setFormatter(formatter)
# loger = logging.getLogger()
# loger.setLevel(logging.INFO)
# loger.addHandler(file_handle)
# loger.addHandler(console_handle)
# STR_FORMAT_DATETIME = '%Y-%m-%d'

def wind_fund_info_import(table_name, get_df=False):
    """
    :param table_name:
    :param get_df:
    :return:
    """
    table_name = 'fund_info'
    has_table = engine_md.has_table(table_name)
    # 初始化服务器接口，用于下载万得数据
    types = {u'股票多头策略': 1000023122000000,
             u'股票多空策略': 1000023123000000,
             u'其他股票策略': 1000023124000000,
             u'阿尔法策略': 1000023125000000,
             u'其他市场中性策略': 1000023126000000,
             u'事件驱动策略': 1000023113000000,
             u'债券策略': 1000023114000000,
             u'套利策略': 1000023115000000,
             u'宏观策略': 1000023116000000,
             u'管理期货': 1000023117000000,
             u'组合基金策略': 1000023118000000,
             u'货币市场策略': 1000023119000000,
             u'多策略': 100002312000000,
             u'其他策略': 1000023121000000}
    df = pd.DataFrame()
    today = date.today().strftime('%Y-%m-%d')

    col_param_list = [
        ('FUND_SETUPDATE', Date),
        ('FUND_MATURITYDATE', Date),
        ('FUND_MGRCOMP', String(200))
        ('FUND_EXISTINGYEAR', String(200))
        ('FUND_PTMYEAR', String(200))
        ('FUND_TYPE', String(200))
        ('FUND_FUNDMANAGER', String(200))
    ]
    col_param_dic = {key.upper(): key.lower() for key, _ in col_param_list}
    param_list = ",".join([col_name.lower() for col_name, _ in col_param_list])
    # 设置dtype类型
    dtype = {key.lower(): val for key, val in param_list}
    dtype['wind_code'] = String(20)
    dtype['sec_name'] = String(200),
    dtype['strategy_type'] = String(200),
    for i in types.keys():
        temp = invoker.wset("sectorconstituent", "date=%s;sectorid=%s" % (today, str(types[i])))
        temp['strategy_type'] = i
        df = pd.concat([df, temp], axis=0)

    # 插入数据库
    # 初始化数据库engine
    # 整理数据
    fund_types_df = df[['wind_code', 'sec_name', 'strategy_type']]
    fund_types_df.set_index('wind_code', inplace=True)

    # 获取基金基本面信息
    code_list = list(fund_types_df.index)  # df['wind_code']
    code_count = len(code_list)
    seg_count = 5000
    info_df = pd.DataFrame()
    for n in range(int(code_count / seg_count) + 1):
        num_start = n * seg_count
        num_end = (n + 1) * seg_count
        num_end = num_end if num_end <= code_count else code_count
        if num_start <= code_count:
            codes = ','.join(code_list[num_start:num_end])
            # 分段获取基金成立日期数据
            info2_df = invoker.wss(codes, param_list)
            logging.info('%05d ) [%d %d]' % (n, num_start, num_end))
            info_df = info_df.append(info2_df)
        else:
            break
            # 整理数据插入数据库
    info_df['FUND_SETUPDATE'] = info_df['FUND_SETUPDATE'].apply(lambda x: x.date())
    info_df['FUND_MATURITYDATE'] = info_df['FUND_MATURITYDATE'].apply(lambda x: x.date())
    info_df = fund_types_df.join(info_df, how='right')

    info_df.rename(columns=col_param_dic, inplace=True)

    info_df.index.names = ['wind_code']
    info_df.drop_duplicates(inplace=True)
    # info_df.to_sql(table_name, engine_md, if_exists='append',
    #                dtype={
    #                    'wind_code': String(200),
    #                    'sec_name': String(200),
    #                    'strategy_type': String(200),
    #                    'fund_setupdate': Date,
    #                    'fund_maturitydate': Date
    #                })
    bunch_insert_on_duplicate_update(info_df, table_name, engine_md, dtype=dtype)
    logging.info('%d funds inserted' % len(info_df))
    if not has_table and engine_md.has_table(table_name):
        alter_table_2_myisam(engine_md, [table_name])
        build_primary_key([table_name])

    # 更新 code_mapping 表
    update_from_info_table(table_name)

    if get_df:
        return info_df
