# -*- coding: utf-8 -*-
"""
Created on 2017/5/2
@author: MG
"""
import logging
from datetime import datetime, date, timedelta
from fh_tools.windy_utils_rest import WindRest, APIError
from sqlalchemy.types import String, Date, Float
import re, itertools
import pandas as pd
from config_fh import WIND_REST_URL, STR_FORMAT_DATE, get_db_engine, get_db_session
logger = logging.getLogger()
RE_PATTERN_MFPRICE = re.compile(r'\d*\.*\d*')
ONE_DAY = timedelta(days=1)
# 标示每天几点以后下载当日行情数据
BASE_LINE_HOUR = 16


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


def import_wind_future_info():
    """
    更新期货合约列表信息
    :return: 
    """
    logger.info("更新 wind_future_info 开始")
    # 获取已存在合约列表
    sql_str = 'select wind_code, ipo_date from wind_future_info'
    engine = get_db_engine()
    with get_db_session(engine) as session:
        table = session.execute(sql_str)
        wind_code_ipo_date_dic = dict(table.fetchall())

    # 通过wind获取合约列表
    # w.start()
    rest = WindRest(WIND_REST_URL)  # 初始化服务器接口，用于下载万得数据
    future_sectorid_dic_list = [
        {'subject_name': 'CFE 沪深300', 'regex': r"IF\d{4}\.CFE",
         'sectorid': 'a599010102000000', 'date_establish': '2010-4-16'},
        {'subject_name': 'CFE 上证50', 'regex': r"IH\d{4}\.CFE",
         'sectorid': '1000014871000000', 'date_establish': '2015-4-16'},
        {'subject_name': 'CFE 中证500', 'regex': r"IC\d{4}\.CFE",
         'sectorid': '1000014872000000', 'date_establish': '2015-4-16'},
        {'subject_name': 'SHFE 黄金', 'regex': r"AU\d{4}\.SHF",
         'sectorid': 'a599010205000000', 'date_establish': '2008-01-09'},
        {'subject_name': 'SHFE 沪银', 'regex': r"AG\d{4}\.SHF",
         'sectorid': '1000006502000000', 'date_establish': '2012-05-10'},
        {'subject_name': 'SHFE 螺纹钢', 'regex': r"RB\d{4}\.SHF",
         'sectorid': 'a599010206000000', 'date_establish': '2009-03-27'},
        {'subject_name': 'SHFE 热卷', 'regex': r"HC\d{4}\.SHF",
         'sectorid': '1000011455000000', 'date_establish': '2014-03-21'},
        {'subject_name': 'DCE 焦炭', 'regex': r"J\d{4}\.SHF",
         'sectorid': '1000002976000000', 'date_establish': '2011-04-15'},
        {'subject_name': 'DCE 焦煤', 'regex': r"JM\d{4}\.SHF",
         'sectorid': '1000009338000000', 'date_establish': '2013-03-22'},
        {'subject_name': '铁矿石', 'regex': r"I\d{4}\.SHF",
         'sectorid': '1000006502000000', 'date_establish': '2013-10-18'},
        {'subject_name': '天然橡胶', 'regex': r"RU\d{4}\.SHF",
         'sectorid': 'a599010208000000', 'date_establish': '1995-06-01'},
        {'subject_name': '铜', 'regex': r"CU\d{4}\.SHF",
         'sectorid': 'a599010202000000', 'date_establish': '1995-05-01'},
        {'subject_name': '铝', 'regex': r"AL\d{4}\.SHF",
         'sectorid': 'a599010203000000', 'date_establish': '1995-05-01'},
        {'subject_name': '锌', 'regex': r"ZN\d{4}\.SHF",
         'sectorid': 'a599010204000000', 'date_establish': '2007-03-26'},
        {'subject_name': '铅', 'regex': r"PB\d{4}\.SHF",
         'sectorid': '1000002892000000', 'date_establish': '2011-03-24'},
        {'subject_name': '镍', 'regex': r"NI\d{4}\.SHF",
         'sectorid': '1000011457000000', 'date_establish': '2015-03-27'},
        {'subject_name': '锡', 'regex': r"SN\d{4}\.SHF",
         'sectorid': '1000011458000000', 'date_establish': '2015-03-27'},
        {'subject_name': '白糖', 'regex': r"SR\d{4}\.CZC",
         'sectorid': 'a599010405000000', 'date_establish': '2006-01-06'},
        {'subject_name': '棉花', 'regex': r"CF\d{4}\.CZC",
         'sectorid': 'a599010404000000', 'date_establish': '2004-06-01'},
        {'subject_name': '棉花', 'regex': r"CF\d{4}\.CZC",
         'sectorid': 'a599010404000000', 'date_establish': '2004-06-01'},
    ]
    wind_code_set = set()
    ndays_per_update = 60
    # 获取历史期货合约列表信息
    for future_sectorid_dic in future_sectorid_dic_list:
        subject_name = future_sectorid_dic['subject_name']
        sector_id = future_sectorid_dic['sectorid']
        regex_str = future_sectorid_dic['regex']
        date_establish = datetime.strptime(future_sectorid_dic['date_establish'], STR_FORMAT_DATE).date()
        date_since = get_date_since(wind_code_ipo_date_dic, regex_str, date_establish)
        date_yestoday = date.today() - timedelta(days=1)
        while date_since <= date_yestoday:
            date_since_str = date_since.strftime(STR_FORMAT_DATE)
            # w.wset("sectorconstituent","date=2017-05-02;sectorid=a599010205000000")
            # future_info_df = wset_cache(w, "sectorconstituent", "date=%s;sectorid=%s" % (date_since_str, sector_id))
            future_info_df = rest.wset("sectorconstituent", "date=%s;sectorid=%s" % (date_since_str, sector_id))
            wind_code_set |= set(future_info_df['wind_code'])
            # future_info_df = future_info_df[['wind_code', 'sec_name']]
            # future_info_dic_list = future_info_df.to_dict(orient='records')
            # for future_info_dic in future_info_dic_list:
            #     wind_code = future_info_dic['wind_code']
            #     if wind_code not in wind_code_future_info_dic:
            #         wind_code_future_info_dic[wind_code] = future_info_dic
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
    # future_info_df = wss_cache(w, wind_code_list,
    #                            "ipo_date,sec_name,sec_englishname,exch_eng,lasttrade_date,lastdelivery_date,dlmonth,lprice,sccode,margin,punit,changelt,mfprice,contractmultiplier,ftmargins,trade_code")
    if len(wind_code_list) > 0:
        future_info_df = rest.wss(wind_code_list,
                                  "ipo_date,sec_name,sec_englishname,exch_eng,lasttrade_date,lastdelivery_date,dlmonth,lprice,sccode,margin,punit,changelt,mfprice,contractmultiplier,ftmargins,trade_code")

        future_info_df['MFPRICE'] = future_info_df['MFPRICE'].apply(mfprice_2_num)
        future_info_count = future_info_df.shape[0]

        future_info_df.rename(columns={c: str.lower(c) for c in future_info_df.columns}, inplace=True)
        future_info_df.index.rename('wind_code', inplace=True)
        future_info_df.to_sql('wind_future_info', engine, if_exists='append',
                              dtype={
                                  'wind_code': String(20),
                                  'trade_code': String(20),
                                  'sec_name': String(50),
                                  'sec_englishname': String(50),
                                  'exch_eng': String(50),
                                  'ipo_date': Date,
                                  'lasttrade_date': Date,
                                  'lastdelivery_date': Date,
                                  'dlmonth': String(20),
                                  'lprice': Float,
                                  'sccode': String(20),
                                  'margin': Float,
                                  'punit': String(20),
                                  'changelt': Float,
                                  'mfprice': Float,
                                  'contractmultiplier': Float,
                                  'ftmargins': String(100),
                              })
        logger.info("更新 wind_future_info 结束 %d 条记录被更新", future_info_count)
        # w.close()


def import_wind_future_daily():
    """
    更新期货合约日级别行情信息
    :return: 
    """
    logger.info("更新 wind_future_daily 开始")
    date_ending = date.today() - ONE_DAY if datetime.now().hour < BASE_LINE_HOUR else date.today()
    # w.wsd("AG1612.SHF", "open,high,low,close,volume,amt,dealnum,settle,oi,st_stock", "2016-11-01", "2016-12-21", "")
    # sql_str = """select fi.wind_code, ifnull(trade_date_max_1, ipo_date) date_frm,
    # lasttrade_date
    # from wind_future_info fi left outer join
    # (select wind_code, adddate(max(trade_date),1) trade_date_max_1 from wind_future_daily group by wind_code) wfd
    # on fi.wind_code = wfd.wind_code"""
    # 16 点以后 下载当天收盘数据，16点以前只下载前一天的数据
    # 对于 date_to 距离今年超过1年的数据不再下载：发现有部分历史过于久远的数据已经无法补全，
    # 如：AL0202.SHF AL9902.SHF CU0202.SHF
    sql_str = """select wind_code, date_frm, if(lasttrade_date<end_date, lasttrade_date, end_date) date_to
FROM
(
select fi.wind_code, ifnull(trade_date_max_1, ipo_date) date_frm, 
    lasttrade_date,
		if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
    from wind_future_info fi left outer join
    (select wind_code, adddate(max(trade_date),1) trade_date_max_1 from wind_future_daily group by wind_code) wfd
    on fi.wind_code = wfd.wind_code
) tt
where date_frm <= if(lasttrade_date<end_date, lasttrade_date, end_date) 
and subdate(curdate(), 360) < if(lasttrade_date<end_date, lasttrade_date, end_date) 
order by wind_code"""
    engine = get_db_engine()
    future_date_dic = {}
    with get_db_session(engine) as session:
        table = session.execute(sql_str)
        for wind_code, date_frm, lasttrade_date in table.fetchall():
            if date_frm is None:
                continue
            if isinstance(date_frm, str):
                date_frm = datetime.strptime(date_frm, STR_FORMAT_DATE).date()
            if isinstance(lasttrade_date, str):
                lasttrade_date = datetime.strptime(lasttrade_date, STR_FORMAT_DATE).date()
            date_to = date_ending if date_ending < lasttrade_date else lasttrade_date
            if date_frm > date_to:
                continue
            future_date_dic[wind_code] = (date_frm, date_to)
    data_df_list = []
    # w.start()
    rest = WindRest(WIND_REST_URL)  # 初始化服务器接口，用于下载万得数据
    data_len = len(future_date_dic)
    try:
        logger.info("%d future instrument will be handled", data_len)
        for data_num, (wind_code, (date_frm, date_to)) in enumerate(future_date_dic.items()):
            # 暂时只处理 RU 期货合约信息
            # if wind_code.find('RU') == -1:
            #     continue
            if date_frm > date_to:
                continue
            date_frm_str = date_frm.strftime(STR_FORMAT_DATE)
            date_to_str = date_to.strftime(STR_FORMAT_DATE)
            logger.info('%d/%d) get %s between %s and %s', data_num, data_len, wind_code, date_frm_str, date_to_str)
            # data_df_tmp = wsd_cache(w, wind_code, "open,high,low,close,volume,amt,dealnum,settle,oi,st_stock",
            #                         date_frm, date_to, "")
            try:
                data_df_tmp = rest.wsd(wind_code, "open,high,low,close,volume,amt,dealnum,settle,oi,st_stock",
                                   date_frm_str, date_to_str, "")
            except APIError as exp:
                logger.exception("%d/%d) %s 执行异常", data_num, data_len, wind_code)
                if exp.ret_dic.setdefault('error_code', 0) in (
                        -40520007,  # 没有可用数据
                        -40521009,  # 数据解码失败。检查输入参数是否正确，如：日期参数注意大小月月末及短二月
                ):
                    continue
                else:
                    break
            data_df_tmp['wind_code'] = wind_code
            data_df_list.append(data_df_tmp)
            # if len(data_df_list) >= 50:
            #     break
    finally:
        data_df_count = len(data_df_list)
        if data_df_count > 0:
            logger.info('merge data with %d df', data_df_count)
            data_df = pd.concat(data_df_list)
            data_df.index.rename('trade_date', inplace=True)
            data_df = data_df.reset_index()
            data_df['instrument_id'] = data_df['wind_code'].apply(lambda x: x.split('.')[0])
            data_df = data_df.set_index(['wind_code', 'trade_date'])
            data_df.rename(columns={c: str.lower(c) for c in data_df.columns}, inplace=True)
            data_df.rename(columns={'oi': 'position'}, inplace=True)
            data_count = data_df.shape[0]
            data_df.to_sql('wind_future_daily', engine, if_exists='append',
                           index_label=['wind_code', 'trade_date'],
                           dtype={
                               'wind_code': String(20),
                               'instrument_id': String(20),
                               'trade_date': Date,
                               'open': Float,
                               'high': Float,
                               'low': Float,
                               'close': Float,
                               'volume': Float,
                               'amt': Float,
                               'dealnum': Float,
                               'settle': Float,
                               'position': Float,
                               'st_stock': Float,
                           })
            logger.info("更新 wind_future_daily 结束 %d 条记录被更新", data_count)
        else:
            logger.info("更新 wind_future_daily 结束 0 条记录被更新")
        # w.close()


def import_wind_future_info_hk():
    """
    更新 香港股指 期货合约列表信息
    香港恒生指数期货，香港国企指数期货合约只有07年2月开始的合约，且无法通过 wset 进行获取
    :return: 
    """
    logger.info("更新 wind_future_info 开始")
    # 获取已存在合约列表
    sql_str = 'select wind_code, ipo_date from wind_future_info'
    engine = get_db_engine()
    with get_db_session(engine) as session:
        table = session.execute(sql_str)
        wind_code_ipo_date_dic = dict(table.fetchall())

    # 通过wind获取合约列表
    # w.start()
    rest = WindRest(WIND_REST_URL)  # 初始化服务器接口，用于下载万得数据
    # future_sectorid_dic_list = [
    #     {'subject_name': 'CFE 沪深300', 'regex': r"IF\d{4}\.CFE",
    #      'sectorid': 'a599010102000000', 'date_establish': '2010-4-16'},
    #     {'subject_name': 'CFE 上证50', 'regex': r"IH\d{4}\.CFE",
    #      'sectorid': '1000014871000000', 'date_establish': '2015-4-16'},
    #     {'subject_name': 'CFE 中证500', 'regex': r"IC\d{4}\.CFE",
    #      'sectorid': '1000014872000000', 'date_establish': '2015-4-16'},
    #     {'subject_name': 'SHFE 黄金', 'regex': r"AU\d{4}\.SHF",
    #      'sectorid': 'a599010205000000', 'date_establish': '2008-01-09'},
    #     {'subject_name': 'SHFE 沪银', 'regex': r"AG\d{4}\.SHF",
    #      'sectorid': '1000006502000000', 'date_establish': '2012-05-10'},
    #     {'subject_name': 'SHFE 螺纹钢', 'regex': r"RB\d{4}\.SHF",
    #      'sectorid': 'a599010206000000', 'date_establish': '2009-03-27'},
    #     {'subject_name': 'SHFE 热卷', 'regex': r"HC\d{4}\.SHF",
    #      'sectorid': '1000011455000000', 'date_establish': '2014-03-21'},
    #     {'subject_name': 'DCE 焦炭', 'regex': r"J\d{4}\.SHF",
    #      'sectorid': '1000002976000000', 'date_establish': '2011-04-15'},
    #     {'subject_name': 'DCE 焦煤', 'regex': r"JM\d{4}\.SHF",
    #      'sectorid': '1000009338000000', 'date_establish': '2013-03-22'},
    #     {'subject_name': '铁矿石', 'regex': r"I\d{4}\.SHF",
    #      'sectorid': '1000006502000000', 'date_establish': '2013-10-18'},
    #     {'subject_name': '天然橡胶', 'regex': r"RU\d{4}\.SHF",
    #      'sectorid': 'a599010208000000', 'date_establish': '1995-06-01'},
    #     {'subject_name': '铜', 'regex': r"CU\d{4}\.SHF",
    #      'sectorid': 'a599010202000000', 'date_establish': '1995-05-01'},
    #     {'subject_name': '铝', 'regex': r"AL\d{4}\.SHF",
    #      'sectorid': 'a599010203000000', 'date_establish': '1995-05-01'},
    #     {'subject_name': '锌', 'regex': r"ZN\d{4}\.SHF",
    #      'sectorid': 'a599010204000000', 'date_establish': '2007-03-26'},
    #     {'subject_name': '铅', 'regex': r"PB\d{4}\.SHF",
    #      'sectorid': '1000002892000000', 'date_establish': '2011-03-24'},
    #     {'subject_name': '镍', 'regex': r"NI\d{4}\.SHF",
    #      'sectorid': '1000011457000000', 'date_establish': '2015-03-27'},
    #     {'subject_name': '锡', 'regex': r"SN\d{4}\.SHF",
    #      'sectorid': '1000011458000000', 'date_establish': '2015-03-27'},
    #     {'subject_name': '白糖', 'regex': r"SR\d{4}\.CZC",
    #      'sectorid': 'a599010405000000', 'date_establish': '2006-01-06'},
    #     {'subject_name': '棉花', 'regex': r"CF\d{4}\.CZC",
    #      'sectorid': 'a599010404000000', 'date_establish': '2004-06-01'},
    #     {'subject_name': '棉花', 'regex': r"CF\d{4}\.CZC",
    #      'sectorid': 'a599010404000000', 'date_establish': '2004-06-01'},
    # ]
    # wind_code_set = set()
    # ndays_per_update = 60
    # # 获取历史期货合约列表信息
    # for future_sectorid_dic in future_sectorid_dic_list:
    #     subject_name = future_sectorid_dic['subject_name']
    #     sector_id = future_sectorid_dic['sectorid']
    #     regex_str = future_sectorid_dic['regex']
    #     date_establish = datetime.strptime(future_sectorid_dic['date_establish'], STR_FORMAT_DATE).date()
    #     date_since = get_date_since(wind_code_ipo_date_dic, regex_str, date_establish)
    #     date_yestoday = date.today() - timedelta(days=1)
    #     while date_since <= date_yestoday:
    #         date_since_str = date_since.strftime(STR_FORMAT_DATE)
    #         # w.wset("sectorconstituent","date=2017-05-02;sectorid=a599010205000000")
    #         # future_info_df = wset_cache(w, "sectorconstituent", "date=%s;sectorid=%s" % (date_since_str, sector_id))
    #         future_info_df = rest.wset("sectorconstituent", "date=%s;sectorid=%s" % (date_since_str, sector_id))
    #         wind_code_set |= set(future_info_df['wind_code'])
    #         # future_info_df = future_info_df[['wind_code', 'sec_name']]
    #         # future_info_dic_list = future_info_df.to_dict(orient='records')
    #         # for future_info_dic in future_info_dic_list:
    #         #     wind_code = future_info_dic['wind_code']
    #         #     if wind_code not in wind_code_future_info_dic:
    #         #         wind_code_future_info_dic[wind_code] = future_info_dic
    #         if date_since >= date_yestoday:
    #             break
    #         else:
    #             date_since += timedelta(days=ndays_per_update)
    #             if date_since > date_yestoday:
    #                 date_since = date_yestoday

    # 获取合约列表
    # 手动生成合约列表
    # 香港恒生指数期货，香港国企指数期货合约只有07年2月开始的合约，且无法通过 wset 进行获取
    wind_code_list = ['%s%02d%02d.HK' % (name, year, month)
                      for name, year, month in itertools.product(['HSIF', 'HHIF'], range(7,19), range(1,13))
                      if not(year==7 and month==1)]

    # 获取合约基本信息
    # w.wss("AU1706.SHF,AG1612.SHF,AU0806.SHF", "ipo_date,sec_name,sec_englishname,exch_eng,lasttrade_date,lastdelivery_date,dlmonth,lprice,sccode,margin,punit,changelt,mfprice,contractmultiplier,ftmargins,trade_code")
    # future_info_df = wss_cache(w, wind_code_list,
    #                            "ipo_date,sec_name,sec_englishname,exch_eng,lasttrade_date,lastdelivery_date,dlmonth,lprice,sccode,margin,punit,changelt,mfprice,contractmultiplier,ftmargins,trade_code")
    if len(wind_code_list) > 0:
        future_info_df = rest.wss(wind_code_list,
                                  "ipo_date,sec_name,sec_englishname,exch_eng,lasttrade_date,lastdelivery_date,dlmonth,lprice,sccode,margin,punit,changelt,mfprice,contractmultiplier,ftmargins,trade_code")

        future_info_df['MFPRICE'] = future_info_df['MFPRICE'].apply(mfprice_2_num)

        future_info_df.rename(columns={c: str.lower(c) for c in future_info_df.columns}, inplace=True)
        future_info_df.index.rename('wind_code', inplace=True)
        future_info_df = future_info_df[~(future_info_df['ipo_date'].isna() | future_info_df['lasttrade_date'].isna())]
        future_info_count = future_info_df.shape[0]
        future_info_df.to_sql('wind_future_info', engine, if_exists='append',
                              dtype={
                                  'wind_code': String(20),
                                  'trade_code': String(20),
                                  'sec_name': String(50),
                                  'sec_englishname': String(50),
                                  'exch_eng': String(50),
                                  'ipo_date': Date,
                                  'lasttrade_date': Date,
                                  'lastdelivery_date': Date,
                                  'dlmonth': String(20),
                                  'lprice': Float,
                                  'sccode': String(20),
                                  'margin': Float,
                                  'punit': String(20),
                                  'changelt': Float,
                                  'mfprice': Float,
                                  'contractmultiplier': Float,
                                  'ftmargins': String(100),
                              })
        logger.info("更新 wind_future_info 结束 %d 条记录被更新", future_info_count)
        # w.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s [%(name)s] %(message)s')
    # windy_utils.CACHE_ENABLE = False
    # import_wind_future_info()
    # import_wind_future_daily()
    import_wind_future_info_hk()
