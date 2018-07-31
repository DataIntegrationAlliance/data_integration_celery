# -*- coding: utf-8 -*-
"""
Created on 2018/1/17
@author: MG
"""

import logging
import math
from datetime import date, datetime, timedelta
import pandas as pd
from tasks.ifind import invoker
from direstinvoker.ifind import APIError
from tasks.utils.fh_utils import get_last, get_first, date_2_str, STR_FORMAT_DATE
from sqlalchemy.types import String, Date, Integer
from sqlalchemy.dialects.mysql import DOUBLE
from tasks.utils.fh_utils import unzip_join
from tasks.utils.db_utils import with_db_session
from tasks.backend import engine_md
from tasks import app

logger = logging.getLogger()
DATE_BASE = datetime.strptime('1990-01-01', STR_FORMAT_DATE).date()
ONE_DAY = timedelta(days=1)
# 标示每天几点以后下载当日行情数据
BASE_LINE_HOUR = 20


def get_stock_code_set(date_fetch):
    date_fetch_str = date_fetch.strftime(STR_FORMAT_DATE)
    stock_df = invoker.THS_DataPool('block', date_fetch_str + ';001005010', 'thscode:Y,security_name:Y')
    if stock_df is None:
        logging.warning('%s 获取股票代码失败', date_fetch_str)
        return None
    stock_count = stock_df.shape[0]
    logging.info('get %d stocks on %s', stock_count, date_fetch_str)
    return set(stock_df['THSCODE'])


@app.task
def import_stock_info(ths_code=None, refresh=False):
    """

    :param ths_code:
    :param refresh:
    :return:
    """
    logging.info("更新 wind_stock_info 开始")
    if ths_code is None:
        # 获取全市场股票代码及名称
        if refresh:
            date_fetch = datetime.strptime('1991-02-01', STR_FORMAT_DATE).date()
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

        ths_code = ','.join(stock_code_set)

    indicator_param_list = [
        ('ths_stock_short_name_stock', '', String(10)),
        ('ths_stock_code_stock', '', String(10)),
        ('ths_stock_varieties_stock', '', String(10)),
        ('ths_ipo_date_stock', '', Date),
        ('ths_listing_exchange_stock', '', String(10)),
        ('ths_delist_date_stock', '', Date),
        ('ths_corp_cn_name_stock', '', String(40)),
        ('ths_corp_name_en_stock', '', String(100)),
        ('ths_established_date_stock', '', Date),
    ]
    # jsonIndicator='ths_stock_short_name_stock;ths_stock_code_stock;ths_thscode_stock;ths_stock_varieties_stock;ths_ipo_date_stock;ths_listing_exchange_stock;ths_delist_date_stock;ths_corp_cn_name_stock;ths_corp_name_en_stock;ths_established_date_stock'
    # jsonparam=';;;;;;;;;'
    indicator, param = unzip_join([(key, val) for key, val, _ in indicator_param_list], sep=';')
    data_df = invoker.THS_BasicData(ths_code, indicator, param)
    if data_df is None or data_df.shape[0] == 0:
        logging.info("没有可用的 stock info 可以更新")
        return
    # 删除历史数据，更新数据
    with with_db_session(engine_md) as session:
        session.execute(
            "DELETE FROM ifind_stock_info WHERE ths_code IN (" + ','.join(
                [':code%d' % n for n in range(len(stock_code_set))]
            ) + ")",
            params={'code%d' % n: val for n, val in enumerate(stock_code_set)})
        session.commit()
    dtype = {key: val for key, _, val in indicator_param_list}
    dtype['ths_code'] = String(20)
    data_count = data_df.shape[0]
    data_df.to_sql('ifind_stock_info', engine_md, if_exists='append', index=False, dtype=dtype)
    logging.info("更新 ifind_stock_info 完成 存量数据 %d 条", data_count)


@app.task
def import_stock_daily_ds(ths_code_set: set = None, begin_time=None):
    """

    :param ths_code_set:
    :param begin_time:
    :return:
    """
    if ths_code_set is None:
        pass
    indicator_param_list = [
        ('ths_pre_close_stock', '100', DOUBLE),
        ('ths_open_price_stock', '100', DOUBLE),
        ('ths_high_price_stock', '100', DOUBLE),
        ('ths_low_stock', '100', DOUBLE),
        ('ths_close_price_stock', '100', DOUBLE),
        ('ths_chg_ratio_stock', '', DOUBLE),
        ('ths_chg_stock', '100', DOUBLE),
        ('ths_vol_stock', '100', DOUBLE),
        ('ths_trans_num_stock', '', Integer),
        ('ths_amt_stock', '', DOUBLE),
        ('ths_turnover_ratio_stock', '', DOUBLE),
        ('ths_vaild_turnover_stock', '', DOUBLE),
        ('ths_af_stock', '', DOUBLE),
        ('ths_up_and_down_status_stock', '', String(10)),
        ('ths_trading_status_stock', '', String(50)),
        ('ths_suspen_reason_stock', '', String(50)),
        ('ths_last_td_date_stock', '', Date),
    ]
    # jsonIndicator='ths_pre_close_stock;ths_open_price_stock;ths_high_price_stock;ths_low_stock;ths_close_price_stock;ths_chg_ratio_stock;ths_chg_stock;ths_vol_stock;ths_trans_num_stock;ths_amt_stock;ths_turnover_ratio_stock;ths_vaild_turnover_stock;ths_af_stock;ths_up_and_down_status_stock;ths_trading_status_stock;ths_suspen_reason_stock;ths_last_td_date_stock'
    # jsonparam='100;100;100;100;100;;100;100;;;;;;;;;'
    json_indicator, json_param = unzip_join([(key, val) for key, val, _ in indicator_param_list], sep=';')
    sql_str = """select ths_code, date_frm, if(ths_delist_date_stock<end_date, ths_delist_date_stock, end_date) date_to
FROM
(
    select info.ths_code, ifnull(trade_date_max_1, ths_ipo_date_stock) date_frm, ths_delist_date_stock,
    if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
    from 
        ifind_stock_info info 
    left outer join
        (select ths_code, adddate(max(time),1) trade_date_max_1 from ifind_stock_daily group by ths_code) daily
    on info.ths_code = daily.ths_code
) tt
where date_frm <= if(ths_delist_date_stock<end_date, ths_delist_date_stock, end_date) 
order by ths_code"""
    if begin_time is None:
        with with_db_session(engine_md) as session:
            # 获取每只股票需要获取日线数据的日期区间
            table = session.execute(sql_str)
            code_date_range_dic = {ths_code: (date_from, date_to)
                                   for ths_code, date_from, date_to in table.fetchall() if
                                   ths_code_set is None or ths_code in ths_code_set}
    # 设置 dtype
    dtype = {key: val for key, _, val in indicator_param_list}
    dtype['ths_code'] = String(20)
    dtype['time'] = Date

    data_df_list, data_count, tot_data_count, code_count = [], 0, 0, len(code_date_range_dic)
    try:
        for num, (ths_code, (begin_time, end_time)) in enumerate(code_date_range_dic.items(), start=1):
            logger.debug('%d/%d) %s [%s - %s]', num, code_count, ths_code, begin_time, end_time)
            data_df = invoker.THS_DateSerial(
                ths_code,
                json_indicator,
                json_param,
                'Days:Tradedays,Fill:Previous,Interval:D',
                begin_time, end_time
            )
            if data_df is not None or data_df.shape[0] > 0:
                data_count += data_df.shape[0]
                data_df_list.append(data_df)
            # 大于阀值有开始插入
            if data_count >= 10000:
                tot_data_df = pd.concat(data_df_list)
                tot_data_df.to_sql('ifind_stock_daily', engine_md, if_exists='append', index=False, dtype=dtype)
                tot_data_count += data_count
                data_df_list, data_count = [], 0
    finally:
        if data_count > 0:
            tot_data_df = pd.concat(data_df_list)
            tot_data_df.to_sql('ifind_stock_daily', engine_md, if_exists='append', index=False, dtype=dtype)
            tot_data_count += data_count

        logging.info("更新 ifind_stock_daily 完成 新增数据 %d 条", tot_data_count)


@app.task
def import_stock_daily_his(ths_code_set: set = None, begin_time=None):
    """
    通过history接口将历史数据保存到 ifind_stock_daily_his
    :param ths_code_set:
    :param begin_time:
    :return:
    """
    if ths_code_set is None:
        pass
    indicator_param_list = [
        ('preClose', '', DOUBLE),
        ('open', '', DOUBLE),
        ('high', '', DOUBLE),
        ('low', '', DOUBLE),
        ('close', '', DOUBLE),
        ('avgPrice', '', DOUBLE),
        ('changeRatio', '', DOUBLE),
        ('volume', '', DOUBLE),
        ('amount', '', Integer),
        ('turnoverRatio', '', DOUBLE),
        ('transactionAmount', '', DOUBLE),
        ('totalShares', '', DOUBLE),
        ('totalCapital', '', DOUBLE),
        ('floatSharesOfAShares', '', DOUBLE),
        ('floatSharesOfBShares', '', DOUBLE),
        ('floatCapitalOfAShares', '', DOUBLE),
        ('floatCapitalOfBShares', '', DOUBLE),
        ('pe_ttm', '', DOUBLE),
        ('pe', '', DOUBLE),
        ('pb', '', DOUBLE),
        ('ps', '', DOUBLE),
        ('pcf', '', DOUBLE),
    ]
    # THS_HistoryQuotes('600006.SH,600010.SH',
    # 'preClose,open,high,low,close,avgPrice,changeRatio,volume,amount,turnoverRatio,transactionAmount,totalShares,totalCapital,floatSharesOfAShares,floatSharesOfBShares,floatCapitalOfAShares,floatCapitalOfBShares,pe_ttm,pe,pb,ps,pcf',
    # 'Interval:D,CPS:1,baseDate:1900-01-01,Currency:YSHB,fill:Previous',
    # '2018-06-30','2018-07-30')
    json_indicator, _ = unzip_join([(key, val) for key, val, _ in indicator_param_list], sep=';')
    sql_str = """select ths_code, date_frm, if(ths_delist_date_stock<end_date, ths_delist_date_stock, end_date) date_to
        FROM
        (
            select info.ths_code, ifnull(trade_date_max_1, ths_ipo_date_stock) date_frm, ths_delist_date_stock,
            if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
            from 
                ifind_stock_info info 
            left outer join
                (select ths_code, adddate(max(time),1) trade_date_max_1 from ifind_stock_daily_his group by ths_code) daily
            on info.ths_code = daily.ths_code
        ) tt
        where date_frm <= if(ths_delist_date_stock<end_date, ths_delist_date_stock, end_date) 
        order by ths_code"""
    if begin_time is None:
        with with_db_session(engine_md) as session:
            # 获取每只股票需要获取日线数据的日期区间
            table = session.execute(sql_str)
            code_date_range_dic = {ths_code: (date_from, date_to)
                                   for ths_code, date_from, date_to in table.fetchall() if
                                   ths_code_set is None or ths_code in ths_code_set}
    # 设置 dtype
    dtype = {key: val for key, _, val in indicator_param_list}
    dtype['ths_code'] = String(20)
    dtype['time'] = Date

    data_df_list, data_count, tot_data_count, code_count = [], 0, 0, len(code_date_range_dic)
    try:
        for num, (ths_code, (begin_time, end_time)) in enumerate(code_date_range_dic.items(), start=1):
            logger.debug('%d/%d) %s [%s - %s]', num, code_count, ths_code, begin_time, end_time)
            data_df = invoker.THS_HistoryQuotes(
                ths_code,
                json_indicator,
                'Interval:D,CPS:1,baseDate:1900-01-01,Currency:YSHB,fill:Previous',
                begin_time, end_time
            )
            if data_df is not None or data_df.shape[0] > 0:
                data_count += data_df.shape[0]
                data_df_list.append(data_df)
            # 大于阀值有开始插入
            if data_count >= 10000:
                tot_data_df = pd.concat(data_df_list)
                tot_data_df.to_sql('ifind_stock_daily_his', engine_md, if_exists='append', index=False, dtype=dtype)
                tot_data_count += data_count
                data_df_list, data_count = [], 0
    finally:
        if data_count > 0:
            tot_data_df = pd.concat(data_df_list)
            tot_data_df.to_sql('ifind_stock_daily_his', engine_md, if_exists='append', index=False, dtype=dtype)
            tot_data_count += data_count

        logging.info("更新 ifind_stock_daily_his 完成 新增数据 %d 条", tot_data_count)


if __name__ == "__main__":
    ths_code = None  # '600006.SH,600009.SH'
    # 股票基本信息数据加载
    # import_stock_info(ths_code)
    # 股票日K数据行情加载
    import_stock_daily_his(ths_code)
