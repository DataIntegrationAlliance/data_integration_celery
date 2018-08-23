# -*- coding: utf-8 -*-
"""
Created on 2017/12/5
@author: MG
"""
from datetime import date, datetime, timedelta
import math
import pandas as pd
import numpy as np
from direstinvoker.utils.fh_utils import get_first, get_last
from tasks import app
from tasks.backend.orm import build_primary_key
from tasks.merge.code_mapping import update_from_info_table
from tasks.utils.db_utils import alter_table_2_myisam
from tasks.wind import invoker, bunch_insert_on_duplicate_update
from tasks.backend import engine_md
from tasks.utils.db_utils import with_db_session
from tasks.utils.fh_utils import STR_FORMAT_DATE
from direstinvoker import APIError, UN_AVAILABLE_DATE
import logging
from sqlalchemy.types import String, Date, Float, Integer
DEBUG = False
logger = logging.getLogger()
DATE_BASE = datetime.strptime('1998-01-01', STR_FORMAT_DATE).date()
ONE_DAY = timedelta(days=1)
# 标示每天几点以后下载当日行情数据
BASE_LINE_HOUR = 16


def get_wind_code_set(date_fetch):
    date_fetch_str = date_fetch.strftime(STR_FORMAT_DATE)
    # 纯股票型基金 混合型
    sectorid_list = ['2001010101000000', '2001010200000000']
    for sector_id in sectorid_list:
        data_df = invoker.wset("sectorconstituent", "date=%s;sectorid=%s" % (date_fetch_str, sector_id))
        if data_df is None:
            logging.warning('%s 获取股票代码失败', date_fetch_str)
            return None
        data_count = data_df.shape[0]
        logging.info('get %d public offering fund on %s', data_count, date_fetch_str)
        wind_code_s = data_df['wind_code']
        wind_code_s = wind_code_s[wind_code_s.apply(lambda x: x.find('!') == -1)]

    return set(wind_code_s)


@app.task
def import_pub_fund_info(first_time=False):
    """
    获取全市场可转债基本信息
    :param first_time: 第一次执行时将从2004年开始查找全部公募基金数据
    :return: 
    """
    table_name = "wind_pub_fund_info"
    has_table = engine_md.has_table(table_name)
    if first_time:
        date_since = datetime.strptime('2004-01-01', STR_FORMAT_DATE).date()
        date_list = []
        one_year = timedelta(days=365)
        while date_since < date.today() - ONE_DAY:
            date_list.append(date_since)
            date_since += one_year
        else:
            date_list.append(date.today() - ONE_DAY)
    else:
        date_list = [date.today() - ONE_DAY]
    # 获取 wind_code 集合
    wind_code_set = set()
    for fetch_date in date_list:
        data_set = get_wind_code_set(fetch_date)
        if data_set is not None:
            wind_code_set |= data_set
        if DEBUG and fetch_date > 6:
            break

    with with_db_session(engine_md) as session:
        sql_str = "select wind_code from {table_name }".format(table_name=table_name)
        table = session.execute(sql_str)
        wind_code_set_existed = {content[0] for content in table.fetchall()}
    wind_code_set -= wind_code_set_existed
    # 获取股票对应上市日期，及摘牌日期
    # w.wss("300005.SZ,300372.SZ,000003.SZ", "ipo_date,trade_code,mkt,exch_city,exch_eng")
    wind_code_list = list(wind_code_set)
    wind_code_count = len(wind_code_list)
    seg_count = 1000
    # loop_count = math.ceil(float(wind_code_count) / seg_count)
    data_info_df_list = []
    fund_info_field_col_name_list = [
        ('FULL_FULLNAME', String(50)),
        ('FULL_EXCHANGESHORTNAME', String(30)),
        ('FULL_BENCHMARK', String(30)),
        ('FULL_SETUPDATE', Date),
        ('FULL_MATURITYDATE', Date),
        ('FULL_FUNDMANAGER', String(30)),
        ('FULL_PREDFUNDMANAGER', String(30)),
        ('FULL_MGRCOMP', String(30)),
        ('FULL_CUSTODIANBANK', String(30)),
        ('FULL_TYPE', String(30)),
        ('FULL_FIRSTINVESTTYPE', String(30)),
        ('FULL_INVESTTYPE', String(30)),
        ('FULL_STRUCTREDFUNDOENOT', String(20)),
    ]
    col_name_dic = {col_name.upper(): col_name.lower() for col_name, _ in fund_info_field_col_name_list}
    col_name_list = [col_name.lower() for col_name in col_name_dic.keys()]
    dtype = {key.lower(): val for key, val in col_name_list}
    dtype['wind_code'] = String(20)
    # fund_info_field_col_name_dic = {'fund_fullname': "full_name",
    #                                 'fund_exchangeshortname': "short_name",
    #                                 'fund_benchmark': "bench_mark",
    #                                 'fund_benchindexcode': "bench_index_code",
    #                                 'fund_setupdate': "setup_date",
    #                                 'fund_maturitydate': "maturity_date",
    #                                 'fund_fundmanager': "fund_fundmanager",
    #                                 'fund_predfundmanager': "fund_predfundmanager",
    #                                 'fund_mgrcomp': "fund_mgrcomp",
    #                                 'fund_custodianbank': "custodian_bank",
    #                                 'fund_type': "fund_type",
    #                                 'fund_firstinvesttype': "invest_type_level1",
    #                                 'fund_investtype': "invest_type",
    #                                 'fund_structuredfundornot': "structured_fund_or_not",
    #                                 'fund_investstyle': "invest_style",
    #                                 }
    for n in range(0, wind_code_count, seg_count):
        sub_list = wind_code_list[n:(n + seg_count)]
        # 尝试将 stock_code_list_sub 直接传递给wss，是否可行
        # w.wss("000309.OF", "fund_fullname,fund_exchangeshortname,fund_benchmark,fund_benchindexcode,fund_setupdate,fund_maturitydate,fund_fundmanager,fund_mgrcomp,fund_custodianbank,fund_type,fund_firstinvesttype,fund_investtype,fund_structuredfundornot,fund_investstyle")
        field_str = ",".join(col_name_dic.keys())
        stock_info_df = invoker.wss(sub_list, field_str)
        data_info_df_list.append(stock_info_df)
    if len(data_info_df_list) == 0:
        logger.info("wind_pub_fund_info 没有数据可以导入")
        return
    data_info_all_df = pd.concat(data_info_df_list)
    data_info_all_df.index.rename('wind_code', inplace=True)
    data_info_all_df.rename(
        columns=col_name_dic, inplace=True)
    logging.info('%s stock data will be import', data_info_all_df.shape[0])
    data_info_all_df.reset_index(inplace=True)
    bunch_insert_on_duplicate_update(data_info_all_df, table_name, engine_md, dtype=dtype)
    if not has_table and engine_md.has_table(table_name):
        alter_table_2_myisam(engine_md, [table_name])
        build_primary_key([table_name])
    update_from_info_table(table_name)


@app.task
def import_pub_fund_daily():
    """
    导入公募基金日线数据
    :return: 
    """
    logging.info("更新 wind_pub_fund_daily 开始")
    table_name = "wind_pub_fund_daily"
    has_table = engine_md.has_table(table_name)
    if has_table:
        sql_str = """
        SELECT wind_code, date_frm, if(maturity_date<end_date, maturity_date, end_date) date_to
            FROM
            (
            SELECT info.wind_code, ifnull(nav_date, ipo_date) date_frm, maturity_date,
            if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
            FROM 
                wind_pub_fund_info info 
            LEFT OUTER JOIN
                (SELECT wind_code, adddate(max(nav_date),1) nav_date FROM {table_name} GROUP BY wind_code) daily
            ON info.wind_code = daily.wind_code
            ) tt
            WHERE date_frm <= if(maturity_date<end_date, maturity_date, end_date) 
            ORDER BY wind_code""".format(table_name=table_name)
    else:
        logger.warning('wind_pub_fund_daily 不存在，仅使用 wind_pub_fund_info 表进行计算日期范围')
        sql_str = """
            SELECT wind_code, date_frm, if(maturity_date<end_date, maturity_date, end_date) date_to
            FROM
              (
                SELECT info.wind_code, setup_date date_frm, maturity_date,
                if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
                FROM wind_pub_fund_info info 
              ) tt
            WHERE date_frm <= if(maturity_date<end_date, maturity_date, end_date) 
            ORDER BY wind_code
        """
    # with with_db_session(engine_md) as session:
    #     # 获取每只股票最新交易日数据
    #     sql_str = 'select wind_code, max(nav_date) from wind_pub_fund_daily group by wind_code'
    #     table = session.execute(sql_str)
    #     trade_date_latest_dic = dict(table.fetchall())
    #     # 获取市场有效交易日数据
    #     sql_str = "select trade_date from wind_trade_date where trade_date > '1997-1-1'"
    #     table = session.execute(sql_str)
    #     trade_date_sorted_list = [t[0] for t in table.fetchall()]
    #     trade_date_sorted_list.sort()
    #     # 获取每只股票上市日期、退市日期
    #     table = session.execute('SELECT wind_code, setup_date, maturity_date FROM wind_pub_fund_info')
    #
    #
    #     wind_code_date_dic = {
    #     wind_code: (setup_date, maturity_date if maturity_date is None or maturity_date > UN_AVAILABLE_DATE else None)
    #     for
    #     wind_code, setup_date, maturity_date in table.fetchall()}
    # date_ending = date.today() - ONE_DAY if datetime.now().hour < BASE_LINE_HOUR else date.today()
    with with_db_session(engine_md) as session:
        # 获取每只股票需要获取日线数据的日期区间
        table = session.execute(sql_str)
        # 计算每只股票需要获取日线数据的日期区间
        begin_time = None
        # 获取date_from,date_to，将date_from,date_to做为value值
        trade_date_latest_dic = {
            wind_code: (date_from if begin_time is None else min([date_from, begin_time]), date_to)
            for wind_code, date_from, date_to in table.fetchall() if
            wind_code_set is None or wind_code in wind_code_set}
    data_df_list = []
    wind_code_date_count = len(trade_date_latest_dic)

    logger.info('%d pub fund will been import into wind_pub_fund_daily', wind_code_date_count)
    # 获取股票量价等行情数据
    field_col_name_dic = [
        ('NAV_date', Date),
        ('NAV_acc', String(20)),
        ('netasset_total', String(20)),
    ]
    wind_indictor_str = ",".join(key.lower() for key, _ in field_col_name_dic)
    upper_col_2_name_dic = {name.upper(): name.lower() for name, _ in field_col_name_dic.items()}
    dtype = {key.lower(): val for key, val in field_col_name_dic}
    try:
        data_tot = 0
        for data_num, (wind_code, (date_from, date_to)) in enumerate(trade_date_latest_dic.items()):
            # 初次加载阶段全量载入，以后 ipo_date为空的情况，直接warning跳过
            # if setup_date is None:
            #     # date_ipo = DATE_BASE
            #     logging.warning("%d/%d) %s 缺少 ipo date", data_num, wind_code_date_count, wind_code)
            #     continue
            # # 获取 date_from
            # if wind_code in trade_date_latest_dic:
            #     date_latest_t1 = trade_date_latest_dic[wind_code] + ONE_DAY
            #     date_from = max([date_latest_t1, DATE_BASE, setup_date])
            # else:
            #     date_from = max([DATE_BASE, setup_date])
            # date_from = get_first(trade_date_sorted_list, lambda x: x >= date_from)
            # # 获取 date_to
            # if maturity_date is None:
            #     date_to = date_ending
            # else:
            #     date_to = min([maturity_date, date_ending])
            # date_to = get_last(trade_date_sorted_list, lambda x: x <= date_to)
            # if date_from is None or date_to is None or date_from > date_to:
            #     continue
            try:
                data_df = invoker.wsd(wind_code, wind_indictor_str, date_from, date_to, "unit=1;Days=Weekdays")
            except APIError as exp:
                logger.exception("%d/%d) %s 执行异常", data_num, wind_code_date_count, wind_code)
                if exp.ret_dic.setdefault('error_code', 0) in (
                        -40520007,  # 没有可用数据
                        -40521009,  # 数据解码失败。检查输入参数是否正确，如：日期参数注意大小月月末及短二月
                ):
                    continue
                else:
                    break

            if data_df is None:
                logger.warning('%d/%d) %s has no ohlc data during %s %s', data_num, wind_code_date_count, wind_code,
                               date_from, date_to)
                continue
            data_df = data_df.drop_duplicates().dropna()
            data_df.rename(columns=upper_col_2_name_dic, inplace=True)
            logger.info('%d/%d) %d data of %s between %s and %s', data_num, wind_code_date_count, data_df.shape[0],
                        wind_code, date_from,
                        date_to)
            data_df['wind_code'] = wind_code
            data_tot += data_df.shape[0]
            data_df_list.append(data_df)
            if data_tot > 10000:
                bunch_insert_on_duplicate_update(data_df, table_name, engine_md, dtype=dtype)
                data_df_list = []
                data_tot = 0

    finally:
        # 导入数据库
        if len(data_df_list) > 0:
            bunch_insert_on_duplicate_update(data_df, table_name, engine_md, dtype=dtype)
            logging.info("更新 wind_pub_fund_daily 结束 %d 条信息被更新", len(data_df_list))
            if not has_table and engine_md.has_table(table_name):
                alter_table_2_myisam(engine_md, [table_name])
                build_primary_key([table_name])


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s [%(name)s:%(funcName)s] %(message)s')
    DEBUG = True
    wind_code_set = None
    import_pub_fund_info(first_time=True)
    # import_pub_fund_daily()
