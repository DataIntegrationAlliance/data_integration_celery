# -*- coding: utf-8 -*-
"""
Created on 2017/5/2
@author: MG
@desc    : 2018-08-23 info daily 已经正式运行测试完成，可以正常使用
"""
import itertools
import logging
import os
import re
from datetime import datetime, date, timedelta

import pandas as pd
from direstinvoker import APIError
from ibats_utils.db import alter_table_2_myisam
from ibats_utils.db import bunch_insert_on_duplicate_update
from ibats_utils.db import with_db_session
from ibats_utils.mess import STR_FORMAT_DATE, STR_FORMAT_DATETIME, datetime_2_str, str_2_date
from ibats_utils.mess import date_2_str
from sqlalchemy.dialects.mysql import DOUBLE
from sqlalchemy.types import String, Date, DateTime

from tasks import app
from tasks.backend import engine_md
from tasks.backend.orm import build_primary_key
from tasks.merge.code_mapping import update_from_info_table
from tasks.wind import invoker

logger = logging.getLogger()
RE_PATTERN_MFPRICE = re.compile(r'\d*\.*\d*')
ONE_DAY = timedelta(days=1)
WIND_VNPY_EXCHANGE_DIC = {
    'SHF': 'SHFE',
    'CZC': 'CZCE',
    'CFE': 'CFFEX',
    'DCE': 'DCE',
    'INE': 'INE'
}
PATTERN_INSTRUMENT_TYPE = re.compile(r'\D+(?=\d{2,4})', re.IGNORECASE)
# 标示每天几点以后下载当日行情数据
BASE_LINE_HOUR = 17
DEBUG = False


def mfprice_2_num(input_str):
    if input_str is None:
        return 0
    m = RE_PATTERN_MFPRICE.search(input_str)
    if m is not None:
        return m.group()
    else:
        return 0


def get_date_since(wind_code_ipo_date_dic, regex_str, date_establish):
    """
    获取最新的合约日期，如果没有对应合约日期则返回该品种起始日期
    2020-12-10 开始按照交易所获取合约列表，因此该函数不再使用，仅留存供参考
    :param wind_code_ipo_date_dic: 
    :param regex_str: 
    :param date_establish: 
    :return: 
    """
    date_since = date_establish
    ndays_per_update = 60
    for wind_code, ipo_date in wind_code_ipo_date_dic.items():
        m = re.match(regex_str, wind_code)
        if m is not None and date_since < ipo_date:
            date_since = ipo_date
    # if date_since != date_establish:
    #     date_since += timedelta(days=ndays_per_update)
    return date_since


def get_exchange_latest_data():
    sql_str = 'SELECT exch_eng, max(ipo_date) FROM wind_future_info group by exch_eng'
    with with_db_session(engine_md) as session:
        table = session.execute(sql_str)
        exchange_latest_ipo_date_dic = dict(table.fetchall())
    return exchange_latest_ipo_date_dic


def get_main_secondary_contract_by_instrument_types(instrument_types=None):
    if instrument_types is None or len(instrument_types) == 0:
        sql_str = """SELECT t.instrument_type, t.Contract, t.ContractNext 
            FROM wind_future_continuous_adj t
            inner join 
            (
                SELECT instrument_type, max(trade_date) trade_date_max 
                FROM wind_future_continuous_adj group by instrument_type
            ) latest
            on t.instrument_type = latest.instrument_type
            and t.trade_date = latest.trade_date_max"""
    else:
        sql_str = f"""SELECT t.instrument_type, t.Contract, t.ContractNext 
            FROM wind_future_continuous_adj t
            inner join 
            (
                SELECT instrument_type, max(trade_date) trade_date_max 
                FROM wind_future_continuous_adj
                where instrument_type in ('{"','".join(instrument_types)}')
                group by instrument_type
            ) latest
            on t.instrument_type = latest.instrument_type
            and t.trade_date = latest.trade_date_max"""

    with with_db_session(engine_md) as session:
        table = session.execute(sql_str)
        wind_code_list = []
        for instrument_type, main, secondary in table.fetchall():
            if main is not None:
                wind_code_list.append(main)
            if secondary is not None:
                wind_code_list.append(secondary)

        return wind_code_list


def _test_get_main_secondary_contract_by_instrument_types():
    instrument_types = None
    wind_code_list = get_main_secondary_contract_by_instrument_types(instrument_types)
    print(f"instrument_types is {instrument_types}\n", wind_code_list)

    instrument_types = ['rb', 'hc', 'i', 'j', 'jm', 'jd', 'ap', 'a', 'p', 'm', 'y']
    wind_code_list = get_main_secondary_contract_by_instrument_types(instrument_types)
    print(f"instrument_types is {instrument_types}\n", wind_code_list)


@app.task
def import_future_info(chain_param=None):
    """
    更新期货合约列表信息
    :param chain_param:  在celery 中將前面結果做爲參數傳給後面的任務
    :return:
    """
    table_name = "wind_future_info"
    has_table = engine_md.has_table(table_name)
    logger.info("更新 %s 开始", table_name)
    # 获取已存在合约列表
    if has_table:
        sql_str = 'select wind_code, ipo_date from {table_name}'.format(table_name=table_name)
        with with_db_session(engine_md) as session:
            table = session.execute(sql_str)
            wind_code_ipo_date_dic = dict(table.fetchall())
    else:
        wind_code_ipo_date_dic = {}

    # 按交易所获取合约列表
    # 上期所
    # w.wset("sectorconstituent","date=1995-05-10;sectorid=a599010201000000")
    # 金交所
    # w.wset("sectorconstituent","date=2013-09-10;sectorid=a599010101000000")
    # 大商所
    # w.wset("sectorconstituent","date=1999-01-10;sectorid=a599010301000000")
    # 郑商所
    # w.wset("sectorconstituent","date=1999-01-10;sectorid=a599010401000000")
    exchange_sectorid_dic_list = [
        {'exch_eng': 'SHFE', 'exchange_name': '上期所',
         'sectorid': 'a599010201000000', 'date_establish': '1995-05-10'},
        {'exch_eng': 'CFFEX', 'exchange_name': '金交所',
         'sectorid': 'a599010101000000', 'date_establish': '2013-09-10'},
        {'exch_eng': 'DCE', 'exchange_name': '大商所',
         'sectorid': 'a599010301000000', 'date_establish': '1999-01-10'},
        {'exch_eng': 'CZCE', 'exchange_name': '郑商所',
         'sectorid': 'a599010401000000', 'date_establish': '1999-01-10'},
    ]
    exchange_latest_ipo_date_dic = get_exchange_latest_data()
    wind_code_set = set()
    ndays_per_update = 90
    # 获取接口参数以及参数列表
    col_name_param_list = [
        ("ipo_date", Date),
        ("sec_name", String(50)),
        ("sec_englishname", String(200)),
        ("exch_eng", String(200)),
        ("lasttrade_date", Date),
        ("lastdelivery_date", Date),
        ("dlmonth", String(20)),
        ("lprice", DOUBLE),
        ("sccode", String(20)),
        ("margin", DOUBLE),
        ("punit", String(200)),
        ("changelt", DOUBLE),
        ("mfprice", DOUBLE),
        ("contractmultiplier", DOUBLE),
        ("ftmargins", String(100)),
        ("trade_code", String(200)),
    ]
    wind_indictor_str = ",".join(col_name for col_name, _ in col_name_param_list)
    dtype = {key: val for key, val in col_name_param_list}
    dtype['wind_code'] = String(20)
    # 获取历史期货合约列表信息
    logger.info("获取历史期货合约列表信息")
    for exchange_sectorid_dic in exchange_sectorid_dic_list:
        exchange_name = exchange_sectorid_dic['exchange_name']
        exch_eng = exchange_sectorid_dic['exch_eng']
        sector_id = exchange_sectorid_dic['sectorid']
        date_establish = exchange_sectorid_dic['date_establish']
        date_since = str_2_date(exchange_latest_ipo_date_dic.setdefault(exch_eng, date_establish))
        date_yestoday = date.today() - timedelta(days=1)
        logger.info("%s[%s][%s] %s ~ %s", exchange_name, exch_eng, sector_id, date_since, date_yestoday)
        while date_since <= date_yestoday:
            date_since_str = date_since.strftime(STR_FORMAT_DATE)
            future_info_df = invoker.wset("sectorconstituent", "date=%s;sectorid=%s" % (date_since_str, sector_id))
            data_count = 0 if future_info_df is None else future_info_df.shape[0]
            logger.info("subject_name=%s[%s] %s 返回 %d 条数据",
                        exchange_name, sector_id, date_since_str, data_count)
            if data_count > 0:
                wind_code_set |= set(future_info_df['wind_code'])

            if date_since >= date_yestoday:
                break
            else:
                date_since += timedelta(days=ndays_per_update)
                if date_since > date_yestoday:
                    date_since = date_yestoday

    # 获取合约列表
    wind_code_list = [wc for wc in wind_code_set if wc not in wind_code_ipo_date_dic]
    # 获取合约基本信息
    # w.wss("AU1706.SHF,AG1612.SHF,AU0806.SHF", "ipo_date,sec_name,sec_englishname,exch_eng,lasttrade_date,lastdelivery_date,dlmonth,lprice,sccode,margin,punit,changelt,mfprice,contractmultiplier,ftmargins,trade_code")
    if len(wind_code_list) > 0:
        logger.info("%d wind_code will be invoked by wss, wind_code_list=%s",
                    len(wind_code_list), wind_code_list)
        future_info_df = invoker.wss(wind_code_list, wind_indictor_str)
        future_info_df['MFPRICE'] = future_info_df['MFPRICE'].apply(mfprice_2_num)
        future_info_count = future_info_df.shape[0]

        future_info_df.rename(columns={c: str.lower(c) for c in future_info_df.columns}, inplace=True)
        future_info_df.index.rename('wind_code', inplace=True)
        future_info_df.reset_index(inplace=True)
        data_count = bunch_insert_on_duplicate_update(future_info_df, table_name, engine_md, dtype=dtype)
        logging.info("更新 %s 结束 %d 条信息被更新", table_name, data_count)
        if not has_table and engine_md.has_table(table_name):
            # alter_table_2_myisam(engine_md, [table_name])
            build_primary_key([table_name])

        logger.info("更新 wind_future_info 结束 %d 条记录被更新", future_info_count)
        update_from_info_table(table_name)


@app.task
def import_future_daily(chain_param=None, wind_code_set=None, begin_time=None):
    """
    更新期货合约日级别行情信息
    :param chain_param:  在celery 中將前面結果做爲參數傳給後面的任務
    :return:
    """
    table_name = "wind_future_daily"
    logger.info("更新 %s 开始", table_name)
    has_table = engine_md.has_table(table_name)
    param_list = [
        ("open", DOUBLE),
        ("high", DOUBLE),
        ("low", DOUBLE),
        ("close", DOUBLE),
        ("volume", DOUBLE),
        ("amt", DOUBLE),
        ("dealnum", DOUBLE),
        ("settle", DOUBLE),
        ("oi", DOUBLE),
        ("st_stock", DOUBLE),
        ('position', DOUBLE),
        ('instrument_id', String(20)),
        ('trade_date', Date,)
    ]
    wind_indictor_str = ",".join([key for key, _ in param_list[:10]])

    if has_table:
        sql_str = """
            select wind_code, date_frm, if(lasttrade_date<end_date, lasttrade_date, end_date) date_to
            FROM
            (
            select fi.wind_code, ifnull(trade_date_max_1, ipo_date) date_frm, 
                lasttrade_date,
                if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
            from wind_future_info fi 
            left outer join
                (select wind_code, adddate(max(trade_date),1) trade_date_max_1 from {table_name} group by wind_code) wfd
            on fi.wind_code = wfd.wind_code
            ) tt
            where date_frm <= if(lasttrade_date<end_date, lasttrade_date, end_date) 
            -- and subdate(curdate(), 360) < if(lasttrade_date<end_date, lasttrade_date, end_date) 
            order by wind_code""".format(table_name=table_name)
    else:
        sql_str = """
            SELECT wind_code, date_frm,
                if(lasttrade_date<end_date,lasttrade_date, end_date) date_to
            FROM
            (
                SELECT info.wind_code,ipo_date date_frm, lasttrade_date,
                if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
                FROM wind_future_info info
            ) tt
            WHERE date_frm <= if(lasttrade_date<end_date, lasttrade_date, end_date)
            ORDER BY wind_code;
         """
        logger.warning('%s 不存在，仅使用 wind_future_info 表进行计算日期范围', table_name)

    with with_db_session(engine_md) as session:
        table = session.execute(sql_str)
        # 获取date_from,date_to，将date_from,date_to做为value值
        future_date_dic = {
            wind_code: (date_from if begin_time is None else min([date_from, begin_time]), date_to)
            for wind_code, date_from, date_to in table.fetchall() if
            wind_code_set is None or wind_code in wind_code_set}

    # 设置 dtype
    dtype = {key: val for key, val in param_list}
    dtype['wind_code'] = String(20)
    dtype['trade_date'] = Date

    data_df_list = []
    data_len = len(future_date_dic)
    try:
        logger.info("%d future instrument will be handled", data_len)
        for num, (wind_code, (date_frm, date_to)) in enumerate(future_date_dic.items(), start=1):
            # 暂时只处理 RU 期货合约信息
            # if wind_code.find('RU') == -1:
            #     continue
            if date_frm > date_to:
                continue
            date_frm_str = date_frm.strftime(STR_FORMAT_DATE)
            date_to_str = date_to.strftime(STR_FORMAT_DATE)
            logger.info('%d/%d) get %s between %s and %s', num, data_len, wind_code, date_frm_str, date_to_str)
            # data_df = wsd_cache(w, wind_code, "open,high,low,close,volume,amt,dealnum,settle,oi,st_stock",
            #                         date_frm, date_to, "")
            try:
                data_df = invoker.wsd(wind_code, wind_indictor_str, date_frm_str, date_to_str, "")
            except APIError as exp:
                from tasks.wind import ERROR_CODE_MSG_DIC
                error_code = exp.ret_dic.setdefault('error_code', 0)
                if error_code in ERROR_CODE_MSG_DIC:
                    logger.error("%d/%d) %s 执行异常 error_code=%d, %s",
                                 num, data_len, wind_code, error_code, ERROR_CODE_MSG_DIC[error_code])
                else:
                    logger.exception("%d/%d) %s 执行异常 error_code=%d",
                                     num, data_len, wind_code, error_code)

                if exp.ret_dic.setdefault('error_code', 0) in (
                        -40520007,  # 没有可用数据
                        -40521009,  # 数据解码失败。检查输入参数是否正确，如：日期参数注意大小月月末及短二月
                        -40520004,  # 错误码是“登陆失败”其实就是没有数据了
                        -40520017,  # 没有数据
                ):
                    continue
                else:
                    break
            if data_df is None:
                logger.warning('%d/%d) %s has no data during %s %s', num, data_len, wind_code, date_frm_str, date_to)
                continue
            logger.info('%d/%d) %d data of %s between %s and %s', num, data_len, data_df.shape[0], wind_code,
                        date_frm_str,
                        date_to)
            data_df['wind_code'] = wind_code
            data_df.index.rename('trade_date', inplace=True)
            data_df.reset_index(inplace=True)
            data_df.rename(columns={c: str.lower(c) for c in data_df.columns}, inplace=True)
            data_df.rename(columns={'oi': 'position'}, inplace=True)  # oi 应该是 open_interest
            data_df['instrument_id'] = wind_code.split('.')[0]
            data_df_list.append(data_df)
            # 仅仅调试时使用
            if DEBUG and len(data_df_list) >= 1:
                break
    finally:
        data_df_count = len(data_df_list)
        if data_df_count > 0:
            logger.info('merge data with %d df', data_df_count)
            data_df = pd.concat(data_df_list)
            data_count = bunch_insert_on_duplicate_update(data_df, table_name, engine_md, dtype=dtype)
            logger.info("更新 %s 结束 %d 条记录被更新", table_name, data_count)
            if not has_table and engine_md.has_table(table_name):
                alter_table_2_myisam(engine_md, [table_name])
                build_primary_key([table_name])
        else:
            logger.info("更新 %s 结束 0 条记录被更新", table_name)


@app.task
def import_future_min(chain_param=None, wind_code_set=None, begin_time=None, recent_n_years=3):
    """
    更新期货合约分钟级别行情信息
    请求语句类似于：
    w.wsi("CU2012.SHF", "open,high,low,close,volume,amt,oi,begin_time,end_time", "2020-11-11 09:00:00", "2020-11-11 11:18:27", "")
    :param chain_param:  在celery 中將前面結果做爲參數傳給後面的任務
    :param wind_code_set:  只道 wind_code 集合
    :param begin_time:  最早的起始日期
    :param recent_n_years:  忽略n年前的合约，wind不提供更早的历史数据
    :return:
    """
    # global DEBUG
    # DEBUG = True
    table_name = "wind_future_min"
    logger.info("更新 %s 开始", table_name)
    has_table = engine_md.has_table(table_name)
    param_list = [
        ("open", DOUBLE),
        ("high", DOUBLE),
        ("low", DOUBLE),
        ("close", DOUBLE),
        ("volume", DOUBLE),
        ("amt", DOUBLE),
        ("oi", DOUBLE),
        # wind 返回的该字段为一个float值，且 begin_time 与 end_time 数值一样，没有意义。
        # 该值与 trade_datetime 字段相同，因此无需获取
        # ('begin_time', DateTime),
        # ('end_time', DateTime),
        # ('instrument_id', String(20)),
        # ('trade_date', Date,)
    ]
    wind_indictor_str = ",".join([key for key, _ in param_list[:10]])

    if has_table:
        sql_str = f"""
        select wind_code, date_frm, if(lasttrade_date<end_date, lasttrade_date, end_date) date_to
        FROM
        (
            select fi.wind_code, 
                ifnull(trade_date_max_1, addtime(ipo_date,'09:00:00')) date_frm, 
                addtime(lasttrade_date,'15:00:00') lasttrade_date,
                case 
                    when hour(now())>=23 then DATE_FORMAT(now(),'%Y-%m-%d 23:00:00') 
                    when hour(now())>=15 then DATE_FORMAT(now(),'%Y-%m-%d 15:00:00') 
                    when hour(now())>=12 then DATE_FORMAT(now(),'%Y-%m-%d 12:00:00') 
                    else DATE_FORMAT(now(),'%Y-%m-%d 03:00:00') 
                end end_date
            from wind_future_info fi 
            left outer join
            (
                select wind_code, addtime(max(trade_datetime),'00:00:01') trade_date_max_1 
                from {table_name} group by wind_code
            ) wfd
            on fi.wind_code = wfd.wind_code
        ) tt
        where date_frm <= if(lasttrade_date<end_date, lasttrade_date, end_date) 
        -- and subdate(curdate(), 360) < if(lasttrade_date<end_date, lasttrade_date, end_date) 
        order by date_to desc, date_frm"""
    else:
        sql_str = """
        SELECT wind_code, date_frm,
            if(lasttrade_date<end_date,lasttrade_date, end_date) date_to
        FROM
        (
            SELECT info.wind_code,
            addtime(ipo_date,'09:00:00') date_frm, 
            addtime(lasttrade_date,'15:00:00')  lasttrade_date,
            case 
                when hour(now())>=23 then DATE_FORMAT(now(),'%Y-%m-%d 23:00:00') 
                when hour(now())>=15 then DATE_FORMAT(now(),'%Y-%m-%d 15:00:00') 
                when hour(now())>=12 then DATE_FORMAT(now(),'%Y-%m-%d 12:00:00') 
                else DATE_FORMAT(now(),'%Y-%m-%d 03:00:00') 
            end end_date
            FROM wind_future_info info
        ) tt
        WHERE date_frm <= if(lasttrade_date<end_date, lasttrade_date, end_date)
        ORDER BY date_to desc, date_frm"""
        logger.warning('%s 不存在，仅使用 wind_future_info 表进行计算日期范围', table_name)

    with with_db_session(engine_md) as session:
        table = session.execute(sql_str)
        # 获取date_from,date_to，将date_from,date_to做为value值
        future_date_dic = {
            wind_code: (date_from if begin_time is None else min([date_from, begin_time]), date_to)
            for wind_code, date_from, date_to in table.fetchall() if
            wind_code_set is None or wind_code in wind_code_set}

    # 设置 dtype
    dtype = {key: val for key, val in param_list}
    dtype['wind_code'] = String(20)
    dtype['instrument_id'] = String(20)
    dtype['trade_date'] = Date
    dtype['trade_datetime'] = DateTime
    dtype['open_interest'] = dtype.pop('oi')

    # 定义统一的插入函数
    def insert_db(df: pd.DataFrame):
        nonlocal has_table
        insert_data_count = bunch_insert_on_duplicate_update(df, table_name, engine_md, dtype=dtype)
        if not has_table and engine_md.has_table(table_name):
            # mysql 8 开始 myisam 不再支持 partition，因此只能使用 innodb
            # alter_table_2_myisam(engine_md, [table_name])
            build_primary_key([table_name])
            has_table = True

        return insert_data_count

    data_df_list = []
    future_count = len(future_date_dic)
    bulk_data_count, tot_data_count = 0, 0
    # 忽略更早的历史合约
    ignore_before = pd.to_datetime(
        date.today() - timedelta(days=int(365 * recent_n_years))) if recent_n_years is not None else None
    try:
        logger.info("%d future instrument will be handled", future_count)
        for num, (wind_code, (date_frm, date_to)) in enumerate(future_date_dic.items(), start=1):
            # 暂时只处理 RU 期货合约信息
            # if wind_code.find('RU') == -1:
            #     continue
            if date_frm > date_to:
                continue

            if ignore_before is not None and pd.to_datetime(date_frm) < ignore_before:
                # 忽略掉 n 年前的合约
                continue
            if isinstance(date_frm, datetime):
                date_frm_str = date_frm.strftime(STR_FORMAT_DATETIME)
            elif isinstance(date_frm, str):
                date_frm_str = date_frm
            else:
                date_frm_str = date_frm.strftime(STR_FORMAT_DATE) + ' 09:00:00'

            # 结束时间到次日的凌晨5点
            if isinstance(date_to, str):
                date_to_str = date_to
            else:
                date_to += timedelta(days=1)
                date_to_str = date_to.strftime(STR_FORMAT_DATE) + ' 03:00:00'

            logger.info('%d/%d) get %s between %s and %s', num, future_count, wind_code, date_frm_str, date_to_str)
            # data_df = wsd_cache(w, wind_code, "open,high,low,close,volume,amt,dealnum,settle,oi,st_stock",
            #                         date_frm, date_to, "")
            try:
                data_df = invoker.wsi(wind_code, wind_indictor_str, date_frm_str, date_to_str, "")
            except APIError as exp:
                from tasks.wind import ERROR_CODE_MSG_DIC
                error_code = exp.ret_dic.setdefault('error_code', 0)
                if error_code in ERROR_CODE_MSG_DIC:
                    logger.error("%d/%d) %s 执行异常 error_code=%d, %s",
                                 num, future_count, wind_code, error_code, ERROR_CODE_MSG_DIC[error_code])
                else:
                    logger.exception("%d/%d) %s 执行异常 error_code=%d",
                                     num, future_count, wind_code, error_code)

                if error_code in (
                        -40520007,  # 没有可用数据
                        -40521009,  # 数据解码失败。检查输入参数是否正确，如：日期参数注意大小月月末及短二月
                        -40520004,  # 错误码是“登陆失败”其实就是没有数据了
                        -40520017,  # 没有数据
                ):
                    continue
                else:
                    break
            if data_df is None:
                logger.warning('%d/%d) %s has no data during %s %s',
                               num, future_count, wind_code, date_frm_str, date_to)
                continue
            logger.info('%d/%d) %d data of %s between %s and %s',
                        num, future_count, data_df.shape[0], wind_code, date_frm_str, date_to)
            data_df['wind_code'] = wind_code
            data_df.index.rename('trade_datetime', inplace=True)
            data_df.reset_index(inplace=True)
            data_df['trade_date'] = pd.to_datetime(data_df['trade_datetime']).apply(lambda x: x.date())
            data_df.rename(columns={c: str.lower(c) for c in data_df.columns}, inplace=True)
            data_df.rename(columns={'oi': 'open_interest'}, inplace=True)
            data_df['instrument_id'] = wind_code.split('.')[0]
            data_df_list.append(data_df)
            bulk_data_count += data_df.shape[0]
            # 仅仅调试时使用
            if DEBUG and len(data_df_list) >= 1:
                break
            if bulk_data_count > 50000:
                logger.info('merge data with %d df %d data', len(data_df_list), bulk_data_count)
                data_df = pd.concat(data_df_list)
                tot_data_count = insert_db(data_df)
                logger.info("更新 %s，累计 %d 条记录被更新", table_name, tot_data_count)
                data_df_list = []
                bulk_data_count = 0
    finally:
        data_df_count = len(data_df_list)
        if data_df_count > 0:
            logger.info('merge data with %d df %d data', len(data_df_list), bulk_data_count)
            data_df = pd.concat(data_df_list)
            tot_data_count += insert_db(data_df)

        logger.info("更新 %s 结束 累计 %d 条记录被更新", table_name, tot_data_count)


@app.task
def update_future_info_hk(chain_param=None):
    """
    更新 香港股指 期货合约列表信息
    香港恒生指数期货，香港国企指数期货合约只有07年2月开始的合约，且无法通过 wset 进行获取
    :param chain_param:  在celery 中將前面結果做爲參數傳給後面的任務
    :return:
    """
    table_name = "wind_future_info_hk"
    has_table = engine_md.has_table(table_name)
    param_list = [
        ("ipo_date", Date),
        ("sec_name", String(50)),
        ("sec_englishname", String(200)),
        ("exch_eng", String(200)),
        ("lasttrade_date", Date),
        ("lastdelivery_date", Date),
        ("dlmonth", String(20)),
        ("lprice", DOUBLE),
        ("sccode", String(20)),
        ("margin", DOUBLE),
        ("punit", String(200)),
        ("changelt", DOUBLE),
        ("mfprice", DOUBLE),
        ("contractmultiplier", DOUBLE),
        ("ftmargins", String(100)),
        ("trade_code", String(200)),
    ]
    wind_indictor_str = ",".join([key for key, _ in param_list])
    dtype = {key: val for key, val in param_list}
    dtype['wind_code'] = String(20)
    logger.info("更新 wind_future_info_hk 开始")
    # 获取已存在合约列表
    # sql_str = 'select wind_code, ipo_date from wind_future_info_hk'
    # with with_db_session(engine_md) as session:
    #     table = session.execute(sql_str)
    #     wind_code_ipo_date_dic = dict(table.fetchall())

    # 获取合约列表
    # 手动生成合约列表
    # 香港恒生指数期货，香港国企指数期货合约只有07年2月开始的合约，且无法通过 wset 进行获取
    wind_code_list = ['%s%02d%02d.HK' % (name, year, month)
                      for name, year, month in itertools.product(['HSIF', 'HHIF'], range(7, 19), range(1, 13))
                      if not (year == 7 and month == 1)]

    # 获取合约基本信息
    # w.wss("AU1706.SHF,AG1612.SHF,AU0806.SHF", "ipo_date,sec_name,sec_englishname,exch_eng,lasttrade_date,lastdelivery_date,dlmonth,lprice,sccode,margin,punit,changelt,mfprice,contractmultiplier,ftmargins,trade_code")
    # future_info_df = wss_cache(w, wind_code_list,
    #                            "ipo_date,sec_name,sec_englishname,exch_eng,lasttrade_date,lastdelivery_date,dlmonth,lprice,sccode,margin,punit,changelt,mfprice,contractmultiplier,ftmargins,trade_code")
    if len(wind_code_list) > 0:
        future_info_df = invoker.wss(wind_code_list, wind_indictor_str)
        future_info_df['MFPRICE'] = future_info_df['MFPRICE'].apply(mfprice_2_num)
        future_info_df.rename(columns={c: str.lower(c) for c in future_info_df.columns}, inplace=True)
        future_info_df.index.rename('wind_code', inplace=True)
        future_info_df = future_info_df[~(future_info_df['ipo_date'].isna() | future_info_df['lasttrade_date'].isna())]
        future_info_df.reset_index(inplace=True)
        future_info_count = future_info_df.shape[0]
        bunch_insert_on_duplicate_update(future_info_df, table_name, engine_md, dtype=dtype)
        logger.info("更新 wind_future_info_hk 结束 %d 条记录被更新", future_info_count)


def get_wind_code_list_by_types(instrument_types: list, all_if_none=True,
                                lasttrade_date_lager_than_n_days_before=30) -> list:
    """
    输入 instrument_type 列表，返回对应的所有合约列表
    :param instrument_types: 可以使 instrument_type 列表 也可以是 （instrument_type，exchange）列表
    :param all_if_none 如果 instrument_types 为 None 则返回全部合约代码
    :param lasttrade_date_lager_than_n_days_before 仅返回最后一个交易日 大于 N 日前日期的合约
    :return: wind_code_list
    """
    wind_code_list = []
    if all_if_none and instrument_types is None:
        sql_str = f"select wind_code from wind_future_info"
        with with_db_session(engine_md) as session:
            if lasttrade_date_lager_than_n_days_before is not None:
                date_from_str = date_2_str(date.today() - timedelta(days=lasttrade_date_lager_than_n_days_before))
                sql_str += " where lasttrade_date > :lasttrade_date"
                table = session.execute(sql_str, params={"lasttrade_date": date_from_str})
            else:
                table = session.execute(sql_str)

            # 获取date_from,date_to，将date_from,date_to做为value值
            for row in table.fetchall():
                wind_code = row[0]
                wind_code_list.append(wind_code)
    else:
        for instrument_type in instrument_types:
            if isinstance(instrument_type, tuple):
                instrument_type, exchange = instrument_type
            else:
                exchange = None
            # re.search(r"(?<=RB)\d{4}(?=\.SHF)", 'RB2101.SHF')
            # pattern = re.compile(r"(?<=" + instrument_type + r")\d{4}(?=\." + exchange + ")")
            # MySql: REGEXP 'rb[:digit:]+.[:alpha:]+'
            # 参考链接： https://blog.csdn.net/qq_22238021/article/details/80929518

            sql_str = f"select wind_code from wind_future_info where wind_code " \
                      f"REGEXP 'rb[:digit:]+.{'[:alpha:]+' if exchange is None else exchange}'"
            with with_db_session(engine_md) as session:
                if lasttrade_date_lager_than_n_days_before is not None:
                    date_from_str = date_2_str(date.today() - timedelta(days=lasttrade_date_lager_than_n_days_before))
                    sql_str += " and lasttrade_date > :lasttrade_date"
                    table = session.execute(sql_str, params={"lasttrade_date": date_from_str})
                else:
                    table = session.execute(sql_str)

                # 获取date_from,date_to，将date_from,date_to做为value值
                for row in table.fetchall():
                    wind_code = row[0]
                    wind_code_list.append(wind_code)

    return wind_code_list


def load_by_wind_code_desc(instrument_types):
    wind_code_set, year_month_set = set(), set()
    for instrument_type, exchange in instrument_types:
        # re.search(r"(?<=RB)\d{4}(?=\.SHF)", 'RB2101.SHF')
        pattern = re.compile(r"(?<=" + instrument_type + r")\d{4}(?=\." + exchange + ")")
        sql_str = f"""select wind_code from wind_future_info where wind_code like '{instrument_type}%.{exchange}'"""
        with with_db_session(engine_md) as session:
            table = session.execute(sql_str)
            # 获取date_from,date_to，将date_from,date_to做为value值
            for row in table.fetchall():
                wind_code = row[0]
                match = pattern.search(wind_code)
                if match is None:
                    continue

                wind_code_set.add(wind_code)
                year_month_set.add(match.group())

    year_month_list = list(year_month_set)
    year_month_list.sort(reverse=True)
    wind_code_list = [
        [
            f'{instrument_type}{_}.{exchange}' for instrument_type, exchange in instrument_types
            if f'{instrument_type}{_}.{exchange}' in wind_code_set
        ]
        for _ in year_month_list]
    for _ in wind_code_list:
        import_future_daily(None, wind_code_set=set(_))


@app.task
def wind_future_daily_2_vnpy(chain_param=None, instrument_types=None):
    from tasks.config import config
    from tasks.backend import engine_dic
    table_name = 'dbbardata'
    interval = '1d'
    engine_vnpy = engine_dic[config.DB_SCHEMA_VNPY]
    has_table = engine_vnpy.has_table(table_name)
    if not has_table:
        logger.error('当前数据库 %s 没有 %s 表，建议使用 vnpy先建立相应的数据库表后再进行导入操作', engine_vnpy, table_name)
        return

    wind_code_list = get_wind_code_list_by_types(instrument_types)
    wind_code_count = len(wind_code_list)
    for n, wind_code in enumerate(wind_code_list, start=1):
        symbol, exchange = wind_code.split('.')
        if exchange in WIND_VNPY_EXCHANGE_DIC:
            exchange_vnpy = WIND_VNPY_EXCHANGE_DIC[exchange]
        else:
            logger.warning('exchange: %s 在交易所列表中不存在', exchange)
            exchange_vnpy = exchange

        # 读取日线数据
        sql_str = "select trade_date `datetime`, `open` open_price, high high_price, " \
                  "`low` low_price, `close` close_price, volume, position as open_interest " \
                  "from wind_future_daily where wind_code = %s and `close` <> 0"
        df = pd.read_sql(sql_str, engine_md, params=[wind_code]).dropna()
        df_len = df.shape[0]
        if df_len == 0:
            continue

        df['symbol'] = symbol
        df['exchange'] = exchange_vnpy
        df['interval'] = interval

        sql_str = f"select count(1) from {table_name} where symbol=:symbol and `interval`='1d'"
        del_sql_str = f"delete from {table_name} where symbol=:symbol and `interval`='1d'"
        with with_db_session(engine_vnpy) as session:
            existed_count = session.scalar(sql_str, params={'symbol': symbol})
            if existed_count == df_len:
                continue
            if existed_count > 0:
                session.execute(del_sql_str, params={'symbol': symbol})
                session.commit()

        df.to_sql(table_name, engine_vnpy, if_exists='append', index=False)
        logger.info("%d/%d) %s %d data have been insert into table %s interval %s",
                    n, wind_code_count, symbol, df.shape[0], table_name, interval)


@app.task
def wind_future_daily_2_model_server(chain_param=None, instrument_types=None):
    from tasks.config import config
    from tasks.backend import engine_dic
    table_name = 'wind_future_daily'
    engine_model_server = engine_dic[config.DB_SCHEMA_MODEL]
    has_table = engine_model_server.has_table(table_name)
    if not has_table:
        logger.error('当前数据库 %s 没有 %s 表，建议使用先建立相应的数据库表后再进行导入操作',
                     engine_model_server, table_name)
        return

    wind_code_list = get_wind_code_list_by_types(instrument_types)
    wind_code_count = len(wind_code_list)
    for n, wind_code in enumerate(wind_code_list, start=1):
        symbol, exchange = wind_code.split('.')
        if exchange in WIND_VNPY_EXCHANGE_DIC:
            exchange_vnpy = WIND_VNPY_EXCHANGE_DIC[exchange]
        else:
            logger.warning('exchange: %s 在交易所列表中不存在', exchange)
            exchange_vnpy = exchange

        sql_str = f"select max(trade_date) from wind_future_daily where wind_code = :symbol"
        with with_db_session(engine_model_server) as session:
            trade_date_max = session.scalar(sql_str, params={'symbol': symbol})

        # 读取日线数据
        if trade_date_max is None:
            sql_str = "select * from wind_future_daily where wind_code = %s and `close` <> 0"
            df = pd.read_sql(sql_str, engine_md, params=[wind_code]).dropna()
        else:
            sql_str = "select * from wind_future_daily where wind_code = %s and trade_date > %s and `close` <> 0"
            df = pd.read_sql(sql_str, engine_md, params=[wind_code, trade_date_max]).dropna()

        df_len = df.shape[0]
        if df_len == 0:
            continue

        df.to_sql(table_name, engine_model_server, if_exists='append', index=False)
        logger.info("%d/%d) %s %d data have been insert into table %s",
                    n, wind_code_count, symbol, df.shape[0], table_name)


def daily_to_model_server_db(chain_param=None, instrument_types=None):
    from tasks.config import config
    from tasks.backend import engine_dic
    from tasks.wind.future_reorg.reorg_md_2_db import data_reorg_daily, update_data_reorg_df_2_db
    engine_model_db = engine_dic[config.DB_SCHEMA_MODEL]
    wind_code_list = get_wind_code_list_by_types(instrument_types)
    instrument_types = {get_instrument_type(wind_code.split('.')[0]) for wind_code in wind_code_list}
    instrument_type_count = len(instrument_types)

    # 同步主力连续合约数据
    for num, instrument_type in enumerate(instrument_types, start=1):
        logger.info("%d/%d) 开始将 %s 前复权数据插入到数据库 %s", num, instrument_type_count, instrument_type, engine_model_db)
        data_no_adj_df, data_adj_df = data_reorg_daily(instrument_type=instrument_type)
        table_name = 'wind_future_continuous_adj'
        update_data_reorg_df_2_db(instrument_type, table_name, data_adj_df, engine=engine_model_db)
        table_name = 'wind_future_continuous_no_adj'
        update_data_reorg_df_2_db(instrument_type, table_name, data_no_adj_df, engine=engine_model_db)

    # 同步每日行情数据
    wind_future_daily_2_model_server(None, instrument_types=instrument_types)


@app.task
def min_to_vnpy_whole(chain_param=None, instrument_types=None):
    from tasks.config import config
    from tasks.backend import engine_dic
    table_name = 'dbbardata'
    interval = '1m'
    engine_vnpy = engine_dic[config.DB_SCHEMA_VNPY]
    has_table = engine_vnpy.has_table(table_name)
    if not has_table:
        logger.error('当前数据库 %s 没有 %s 表，建议使用 vnpy先建立相应的数据库表后再进行导入操作', engine_vnpy, table_name)
        return

    wind_code_list = get_wind_code_list_by_types(instrument_types)
    wind_code_count = len(wind_code_list)
    for n, wind_code in enumerate(wind_code_list, start=1):
        symbol, exchange = wind_code.split('.')
        if exchange in WIND_VNPY_EXCHANGE_DIC:
            exchange_vnpy = WIND_VNPY_EXCHANGE_DIC[exchange]
        else:
            logger.warning('%s exchange: %s 在交易所列表中不存在', wind_code, exchange)
            exchange_vnpy = exchange
        # 读取日线数据
        sql_str = "select trade_datetime `datetime`, `open` open_price, high high_price, " \
                  "`low` low_price, `close` close_price, volume, position as open_interest " \
                  "from wind_future_min where wind_code = %s and `close` is not null and `close` <> 0"
        df = pd.read_sql(sql_str, engine_md, params=[wind_code]).dropna()
        df_len = df.shape[0]
        if df_len == 0:
            continue

        df['symbol'] = symbol
        df['exchange'] = exchange_vnpy
        df['interval'] = interval
        datetime_latest = df['datetime'].max().to_pydatetime()
        sql_str = f"select max(`datetime`) from {table_name} where symbol=:symbol and `interval`='{interval}'"
        del_sql_str = f"delete from {table_name} where symbol=:symbol and `interval`='{interval}'"
        with with_db_session(engine_vnpy) as session:
            datetime_exist = session.scalar(sql_str, params={'symbol': symbol})
            if datetime_exist is not None:
                if datetime_exist >= datetime_latest:
                    continue
                else:
                    session.execute(del_sql_str, params={'symbol': symbol})
                    session.commit()

        df.to_sql(table_name, engine_vnpy, if_exists='append', index=False)
        logger.info("%d/%d) %s %s -> %s %d data have been insert into table %s interval %s",
                    n, wind_code_count, symbol,
                    datetime_2_str(datetime_exist), datetime_2_str(datetime_latest),
                    df_len, table_name, interval)


@app.task
def min_to_vnpy_increment(chain_param=None, instrument_types=None):
    from tasks.config import config
    from tasks.backend import engine_dic
    table_name = 'dbbardata'
    interval = '1m'
    engine_vnpy = engine_dic[config.DB_SCHEMA_VNPY]
    has_table = engine_vnpy.has_table(table_name)
    if not has_table:
        logger.error('当前数据库 %s 没有 %s 表，建议使用 vnpy先建立相应的数据库表后再进行导入操作', engine_vnpy, table_name)
        return

    sql_increment_str = "select trade_datetime `datetime`, `open` open_price, high high_price, " \
                        "`low` low_price, `close` close_price, volume, position as open_interest " \
                        "from wind_future_min where wind_code = %s and " \
                        "trade_datetime > %s and `close` is not null and `close` <> 0"
    sql_whole_str = "select trade_datetime `datetime`, `open` open_price, high high_price, " \
                    "`low` low_price, `close` close_price, volume, position as open_interest " \
                    "from wind_future_min where wind_code = %s and " \
                    "`close` is not null and `close` <> 0"
    wind_code_list = get_wind_code_list_by_types(instrument_types)
    wind_code_count = len(wind_code_list)
    for n, wind_code in enumerate(wind_code_list, start=1):
        symbol, exchange = wind_code.split('.')
        if exchange in WIND_VNPY_EXCHANGE_DIC:
            exchange_vnpy = WIND_VNPY_EXCHANGE_DIC[exchange]
        else:
            logger.warning('%s exchange: %s 在交易所列表中不存在', wind_code, exchange)
            exchange_vnpy = exchange
        sql_str = f"select max(`datetime`) from {table_name} where symbol=:symbol and `interval`='{interval}'"
        with with_db_session(engine_vnpy) as session:
            datetime_exist = session.scalar(sql_str, params={'symbol': symbol})
        if datetime_exist is not None:
            # 读取日线数据
            df = pd.read_sql(sql_increment_str, engine_md, params=[wind_code, datetime_exist]).dropna()
        else:
            df = pd.read_sql(sql_whole_str, engine_md, params=[wind_code]).dropna()

        df_len = df.shape[0]
        if df_len == 0:
            continue

        df['symbol'] = symbol
        df['exchange'] = exchange_vnpy
        df['interval'] = interval
        datetime_latest = df['datetime'].max().to_pydatetime()
        df.to_sql(table_name, engine_vnpy, if_exists='append', index=False)
        logger.info("%d/%d) %s %s -> %s %d data have been insert into table %s interval %s",
                    n, wind_code_count, symbol,
                    datetime_2_str(datetime_exist), datetime_2_str(datetime_latest),
                    df_len, table_name, interval)


def _run_daily_to_vnpy():
    # instrument_types = ['RB']
    instrument_types = None
    wind_future_daily_2_vnpy(None, instrument_types)


def get_instrument_type(symbol):
    match = PATTERN_INSTRUMENT_TYPE.search(symbol)
    if match is not None:
        instrument_type = match.group()
    else:
        logger.error("当前合约 %s 无法判断期货品种", symbol)
        instrument_type = None

    return instrument_type.upper()


def output_future_multiplier():
    """保存每个期货品种的乘数"""
    df = pd.read_sql("SELECT trade_code, contractmultiplier FROM wind_future_info", engine_md)
    df.rename(columns={'contractmultiplier': "multiplier"}, inplace=True)
    df['instrument_type'] = df['trade_code'].apply(get_instrument_type)
    df = df[['instrument_type', 'multiplier']].drop_duplicates()
    df.to_csv(os.path.join("output", "instrument_type_multiplier.csv"), index=False)
    import json
    logger.info(json.dumps({row['instrument_type']: row['multiplier'] for _, row in df.iterrows()}, indent=4))


def output_instrument_type_daily_bar_count():
    """保存每个期货品种的每日分钟数"""
    sql_str = """select inst_type, max(bar_count) daily_bar_count
    from (
        SELECT REGEXP_SUBSTR(wind_code, '^[[:alpha:]]+') inst_type, trade_date, count(1) bar_count 
        FROM wind_future_min 
        group by REGEXP_SUBSTR(wind_code, '^[[:alpha:]]+'), trade_date
    ) t
    group by inst_type"""
    df = pd.read_sql(sql_str, engine_md)
    df.to_csv(os.path.join("output", "instrument_type_daily_bar_count.csv"), index=False)
    import json
    logger.info(json.dumps({row['inst_type']: row['daily_bar_count'] for _, row in df.iterrows()}, indent=4))


def _run_task():
    from tasks.wind.future_reorg.reversion_rights_factor import task_save_adj_factor
    # DEBUG = True
    wind_code_set = None
    # import_future_info_hk(chain_param=None)
    # update_future_info_hk(chain_param=None)
    # import_future_info(chain_param=None)
    # 导入期货每日行情数据
    import_future_daily(None, wind_code_set)
    try:
        # 同步到 阿里云 RDS 服务器
        daily_to_model_server_db()
    except:
        logger.exception("同步到 阿里云 RDS 服务器失败")
    # 根据商品类型将对应日线数据插入到 vnpy dbbardata 表中
    _run_daily_to_vnpy()
    # 重新计算复权数据
    task_save_adj_factor()
    # 导入期货分钟级行情数据
    import_future_min(None, wind_code_set, recent_n_years=1)
    min_to_vnpy_increment(None)

    # 按品种合约倒叙加载每日行情
    # load_by_wind_code_desc(instrument_types=[
    #     ('RB', r"SHF"),
    #     ('I', r"DCE"),
    #     ('HC', r"SHF"),
    # ])


def run_daily_only():
    wind_code_set = None
    # 导入期货每日行情数据
    import_future_daily(None, wind_code_set)
    try:
        # 同步到 阿里云 RDS 服务器
        daily_to_model_server_db()
    except:
        logger.exception("同步到 阿里云 RDS 服务器失败")
    # 根据商品类型将对应日线数据插入到 vnpy dbbardata 表中
    _run_daily_to_vnpy()


def run_daily_to_model_server_db():
    daily_to_model_server_db(instrument_types=['rb', 'hc', 'i', 'j', 'jm'])


if __name__ == "__main__":
    _run_task()
    # run_daily_only()
    # _test_get_main_secondary_contract_by_instrument_types()
    # run_daily_to_model_server_db()
