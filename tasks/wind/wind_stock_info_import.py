# -*- coding: utf-8 -*-
"""
Created on 2017/4/14
@author: MG
"""
import logging
import math
from sqlalchemy.types import String, Date
from fh_tools.windy_utils_rest import WindRest
import pandas as pd
from datetime import datetime, date, timedelta
from config_fh import WIND_REST_URL, STR_FORMAT_DATE, get_db_engine, get_db_session

w = WindRest(WIND_REST_URL)


def get_stock_code_set(date_fetch):
    date_fetch_str = date_fetch.strftime(STR_FORMAT_DATE)
    stock_df = w.wset("sectorconstituent", "date=%s;sectorid=a001010100000000" % date_fetch_str)
    if stock_df is None:
        logging.warning('%s 获取股票代码失败', date_fetch_str)
        return None
    stock_count = stock_df.shape[0]
    logging.info('get %d stocks on %s', stock_count, date_fetch_str)
    return set(stock_df['wind_code'])


def import_wind_stock_info(refresh=False):

    # 获取全市场股票代码及名称
    logging.info("更新 wind_stock_info 开始")
    if refresh:
        date_fetch = datetime.strptime('2005-1-1', STR_FORMAT_DATE).date()
    else:
        date_fetch = date.today()
    date_end = date.today()
    stock_code_set = set()
    while date_fetch < date_end:
        stock_code_set_sub = get_stock_code_set(date_fetch)
        if stock_code_set_sub is not None:
            stock_code_set |= stock_code_set_sub
        date_fetch += timedelta(days=365)
    stock_code_set_sub = get_stock_code_set(date_end)
    if stock_code_set_sub is not None:
        stock_code_set |= stock_code_set_sub

    # 获取股票对应上市日期，及摘牌日期
    # w.wss("300005.SZ,300372.SZ,000003.SZ", "ipo_date,trade_code,mkt,exch_city,exch_eng")
    stock_code_list = list(stock_code_set)
    stock_code_count = len(stock_code_list)
    seg_count = 1000
    loop_count = math.ceil(float(stock_code_count) / seg_count)
    stock_info_df_list = []
    for n in range(loop_count):
        num_start = n * seg_count
        num_end = (n + 1) * seg_count
        num_end = num_end if num_end <= stock_code_count else stock_code_count
        stock_code_list_sub = stock_code_list[num_start:num_end]
        # 尝试将 stock_code_list_sub 直接传递给wss，是否可行
        stock_info_df = w.wss(stock_code_list_sub, "sec_name,trade_code,ipo_date,delist_date,mkt,exch_city,exch_eng,prename")
        stock_info_df_list.append(stock_info_df)

    stock_info_all_df = pd.concat(stock_info_df_list)
    stock_info_all_df.index.rename('WIND_CODE', inplace=True)
    logging.info('%s stock data will be import', stock_info_all_df.shape[0])
    engine = get_db_engine()
    stock_info_all_df.reset_index(inplace=True)
    data_list = list(stock_info_all_df.T.to_dict().values())
    sql_str = "REPLACE INTO wind_stock_info (wind_code, trade_code, sec_name, ipo_date, delist_date, mkt, exch_city, exch_eng, prename) values (:WIND_CODE, :TRADE_CODE, :SEC_NAME, :IPO_DATE, :DELIST_DATE, :MKT, :EXCH_CITY, :EXCH_ENG, :PRENAME)"
    # sql_str = "insert INTO wind_stock_info (wind_code, trade_code, sec_name, ipo_date, delist_date, mkt, exch_city, exch_eng, prename) values (:WIND_CODE, :TRADE_CODE, :SEC_NAME, :IPO_DATE, :DELIST_DATE, :MKT, :EXCH_CITY, :EXCH_ENG, :PRENAME)"
    with get_db_session(engine) as session:
        session.execute(sql_str, data_list)
        stock_count = session.execute('select count(*) from wind_stock_info').first()[0]
    logging.info("更新 wind_stock_info 完成 存量数据 %d 条", stock_count)


if __name__ == "__main__":
    import_wind_stock_info(refresh=False)
