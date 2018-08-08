# -*- coding: utf-8 -*-
"""
Created on Fri Feb 17 10:17:38 2017

@author: Administrator
"""

from fh_tools.windy_utils_rest import WindRest
import pandas as pd
from sqlalchemy.types import String, Date
from datetime import date
import logging
# from logging.handlers import RotatingFileHandler
from config_fh import get_db_engine, WIND_REST_URL


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
    rest = WindRest(WIND_REST_URL)  # 初始化服务器接口，用于下载万得数据
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
    for i in types.keys():
        temp = rest.wset("sectorconstituent", "date=%s;sectorid=%s" % (today, str(types[i])))
        temp['strategy_type'] = i
        df = pd.concat([df, temp], axis=0)

    # 插入数据库
    # 初始化数据库engine
    engine = get_db_engine()

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
            info2_df = rest.wss(codes, "fund_setupdate,fund_maturitydate,fund_mgrcomp,\
                                  fund_existingyear,fund_ptmyear,fund_type,fund_fundmanager")
            logging.info('%05d ) [%d %d]' % (n, num_start, num_end))
            info_df = info_df.append(info2_df)
        else:
            break
            # 整理数据插入数据库
    info_df['FUND_SETUPDATE'] = info_df['FUND_SETUPDATE'].apply(lambda x: x.date())
    info_df['FUND_MATURITYDATE'] = info_df['FUND_MATURITYDATE'].apply(lambda x: x.date())
    info_df = fund_types_df.join(info_df, how='right')

    info_df.rename(columns={'FUND_SETUPDATE': 'fund_setupdate',
                            'FUND_MATURITYDATE': 'fund_maturitydate',
                            'FUND_MGRCOMP': 'fund_mgrcomp',
                            'FUND_EXISTINGYEAR': 'fund_existingyear',
                            'FUND_PTMYEAR': 'fund_ptmyear',
                            'FUND_TYPE': 'fund_type',
                            'FUND_FUNDMANAGER': 'fund_fundmanager'
                            }, inplace=True)

    info_df.index.names = ['wind_code']
    info_df.drop_duplicates(inplace=True)
    info_df.to_sql(table_name, engine, if_exists='append',
                   dtype={
                       'wind_code': String(200),
                       'sec_name': String(200),
                       'strategy_type': String(200),
                       'fund_setupdate': Date,
                       'fund_maturitydate': Date
                   })
    logging.info('%d funds inserted' % len(info_df))
    if get_df:
        return info_df
