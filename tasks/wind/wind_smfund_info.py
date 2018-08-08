# -*- coding: utf-8 -*-
"""
Created on Thu Mar 30 10:17:12 2017

@author: Yupeng Guo
"""

from WindPy import w
from fh_tools.windy_utils import load_cache, wset_cache, wsd_cache, wss_cache, dump_cache
from fh_tools.fh_utils import clean_datetime_remove_time_data
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import String, Date
# import pymysql
from datetime import datetime, date
from config_fh import get_db_engine, get_db_session


def import_smfund_info():
    w.start()

    types = {u'主动股票型分级母基金': 1000007766000000,
             u'被动股票型分级母基金': 1000007767000000,
             u'纯债券型分级母基金': 1000007768000000,
             u'混合债券型分级母基金': 1000007769000000,
             u'混合型分级母基金': 1000026143000000,
             u'QDII分级母基金': 1000019779000000
             }

    # 获取各个历史时段的分级基金列表，并汇总全部基金代码
    today = date.today().strftime('%Y-%m-%d')
    dates = ['2011-01-01', '2013-01-01', '2015-01-01', '2017-01-01', '2018-01-01']  # 分三个时间点获取市场上所有分级基金产品
    df = pd.DataFrame()
    for date_p in dates:
        temp_df = wset_cache(w, "sectorconstituent", "date=%s;sectorid=1000006545000000" % date_p)
        df = df.append(temp_df)
    wind_code_all = df['wind_code'].unique()

    # 查询数据库，剔除已存在的基金代码
    with get_db_session() as session:
        table = session.execute("select wind_code from wind_smfund_info")
        wind_code_existed = set([content[0] for content in table.fetchall()])
    wind_code_new = list(set(wind_code_all) - wind_code_existed)
    wind_code_new = [code for code in wind_code_new if code.find('!') < 0]
    if len(wind_code_new) == 0:
        print('no sm fund imported')
    else:
        info_df = wss_cache(w, wind_code_new, 'fund_setupdate, fund_maturitydate')
        if info_df is None:
            raise Exception('no data')
        info_df['FUND_SETUPDATE'] = info_df['FUND_SETUPDATE'].apply(lambda x: x.date())
        info_df['FUND_MATURITYDATE'] = info_df['FUND_MATURITYDATE'].apply(lambda x: x.date())
        info_df.rename(columns={'FUND_SETUPDATE': 'fund_setupdate', 'FUND_MATURITYDATE': 'fund_maturitydate'}, inplace=True)
        field = "fund_type,wind_code,sec_name,class_a_code,class_a_name,class_b_code,class_b_name,a_pct,b_pct,upcv_nav,\
        downcv_nav,track_indexcode,track_indexname,max_purchasefee,max_redemptionfee"

        df = pd.DataFrame()
        for code in info_df.index:
            beginDate = info_df.loc[code, 'fund_setupdate'].strftime('%Y-%m-%d')
            temp_df = wset_cache(w, "leveragedfundinfo", "date=%s;windcode=%s;field=%s" % (beginDate, code, field))
            df = df.append(temp_df)
        df.set_index('wind_code', inplace=True)
        df['tradable'] = df.index.map(lambda x: x if 'S' in x else None)
        # df.index = df.index.map(lambda x: x[:-2] + 'OF')
        info_df = info_df.join(df, how='outer')
        info_df.rename(columns={'a_nav': 'nav_a', 'b_nav': 'nav_b', 'a_fs_inc': 'fs_inc_a', 'b_fs_inc': 'fs_inc_b'})
        info_df.index.rename('wind_code', inplace=True)
        engine = get_db_engine()
        info_df.to_sql('wind_smfund_info', engine, if_exists='append', index_label='wind_code',
                       dtype={
                           'wind_code': String(20),
                           'fund_setupdate': Date,
                           'fund_maturitydate': Date,
                           'fund_type': String(20),
                           'sec_name': String(50),
                           'class_a_code': String(20),
                           'class_a_name': String(50),
                           'class_b_code': String(20),
                           'class_b_name': String(50),
                           'track_indexcode': String(20),
                           'track_indexname': String(50),
                           'tradable': String(20),
                       })
    w.close()

if __name__ == "__main__":
    import_smfund_info()
