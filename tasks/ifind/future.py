# -*- coding: utf-8 -*-
"""
Created on 2017/5/2
@author: MG
"""
import logging
from sqlalchemy.exc import ProgrammingError
from datetime import datetime, date, timedelta
from tasks.utils.db_utils import with_db_session
from tasks.backend import engine_md
from tasks.ifind import invoker
from tasks import app
from direstinvoker import APIError
from sqlalchemy.dialects.mysql import DOUBLE
from sqlalchemy.types import String, Date, Integer
import re
import pandas as pd
from tasks.utils.fh_utils import STR_FORMAT_DATE, unzip_join
logger = logging.getLogger()
RE_PATTERN_MFPRICE = re.compile(r'\d*\.*\d*')
ONE_DAY = timedelta(days=1)
# 标示每天几点以后下载当日行情数据
BASE_LINE_HOUR = 16


# def mfprice_2_num(input_str):
#     if input_str is None:
#         return 0
#     m = RE_PATTERN_MFPRICE.search(input_str)
#     if m is not None:
#         return m.group()
#     else:
#         return 0


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


def import_variety_info():
    """保存期货交易所品种信息，一次性数据导入，以后基本上不需要使用了"""
    exchange_list = [
        '上海期货交易所',
        '大连商品交易所',
        '郑州商品交易所',
        '中国金融期货交易所',
        '纽约商业期货交易所(NYMEX)',
        '纽约商品交易所(COMEX)',
        # '纽约期货交易所(NYBOT)',  # 暂无数据
        '芝加哥期货交易所(CBOT)',
        # '洲际交易所(ICE)',  # 暂无数据
        '伦敦金属交易所(LME)',
        '马来西亚衍生品交易所（BMD）',
        '新加坡证券交易所（SGX）',
    ]
    data_df_list = []
    data_count = len(exchange_list)
    try:
        for num, exchange in enumerate(exchange_list):
            logger.debug("%d/%d) %s 获取交易品种信息", num, data_count, exchange)
            data_df = invoker.THS_DataPool('variety', exchange, 'variety:Y,id:Y')
            data_df['exchange'] = exchange
            data_df_list.append(data_df)
    finally:
        if len(data_df_list) > 0:
            tot_data_df = pd.concat(data_df_list)
            tot_data_df.to_sql('ifind_variety_info', engine_md, index=False, if_exists='append', dtype={
                'exchange': String(20),
                'ID': String(20),
                'SECURITY_NAME': String(20),
            })
            tot_data_count = tot_data_df.shape[0]
        else:
            tot_data_count = 0

        logger.info('保存交易所品种信息完成， %d条数据被保存', tot_data_count)


@app.task
def import_future_info():
    """更新期货合约列表信息"""
    logger.info("更新 ifind_future_info 开始")
    # 获取已存在合约列表
    try:
        sql_str = 'select ths_code, ths_start_trade_date_future from ifind_future_info'
        with with_db_session(engine_md) as session:
            table = session.execute(sql_str)
            code_ipo_date_dic = dict(table.fetchall())
    except:
        code_ipo_date_dic = {}

    # 通过wind获取合约列表
    future_sectorid_dic_list = [
        {'subject_name': 'CFE 沪深300', 'regex': r"IF\d{4}\.CFE",
         'sectorid': '091004001', 'date_establish': '2010-4-16'},
        {'subject_name': 'CFE 上证50', 'regex': r"IH\d{4}\.CFE",
         'sectorid': '091004003', 'date_establish': '2015-4-16'},
        {'subject_name': 'CFE 中证500', 'regex': r"IC\d{4}\.CFE",
         'sectorid': '091004004', 'date_establish': '2015-4-16'},
        {'subject_name': 'CFE 5年国债期货', 'regex': r"TF\d{4}\.CFE",
         'sectorid': '091004002', 'date_establish': '2013-09-06'},
        {'subject_name': 'CFE 10年期国债期货', 'regex': r"T\d{4}\.CFE",
         'sectorid': '091004005', 'date_establish': '2015-03-20'},
        {'subject_name': 'SHFE 黄金', 'regex': r"AU\d{4}\.SHF",
         'sectorid': '091002002', 'date_establish': '2008-01-09'},
        {'subject_name': 'SHFE 沪银', 'regex': r"AG\d{4}\.SHF",
         'sectorid': '091002010', 'date_establish': '2012-05-10'},
        {'subject_name': 'SHFE 螺纹钢', 'regex': r"RB\d{4}\.SHF",
         'sectorid': '091002006', 'date_establish': '2009-03-27'},
        {'subject_name': 'SHFE 热卷', 'regex': r"HC\d{4}\.SHF",
         'sectorid': '091002012', 'date_establish': '2014-03-21'},
        {'subject_name': 'DCE 焦炭', 'regex': r"J\d{4}\.SHF",
         'sectorid': '091001004', 'date_establish': '2011-04-15'},
        {'subject_name': 'DCE 焦煤', 'regex': r"JM\d{4}\.SHF",
         'sectorid': '091001010', 'date_establish': '2013-03-22'},
        {'subject_name': '铁矿石', 'regex': r"I\d{4}\.SHF",
         'sectorid': '091001011', 'date_establish': '2013-10-18'},
        {'subject_name': '天然橡胶', 'regex': r"RU\d{4}\.SHF",
         'sectorid': '091002007', 'date_establish': '1997-02-01'},
        {'subject_name': '铜', 'regex': r"CU\d{4}\.SHF",
         'sectorid': '091002003', 'date_establish': '1997-02-01'},
        {'subject_name': '铝', 'regex': r"AL\d{4}\.SHF",
         'sectorid': '091002001', 'date_establish': '1997-02-01'},
        {'subject_name': '锌', 'regex': r"ZN\d{4}\.SHF",
         'sectorid': '091002009', 'date_establish': '2007-03-26'},
        {'subject_name': '铅', 'regex': r"PB\d{4}\.SHF",
         'sectorid': '091002005', 'date_establish': '2011-03-24'},
        {'subject_name': '镍', 'regex': r"NI\d{4}\.SHF",
         'sectorid': '091002014', 'date_establish': '2015-03-27'},
        {'subject_name': '锡', 'regex': r"SN\d{4}\.SHF",
         'sectorid': '091002013', 'date_establish': '2015-03-27'},
        {'subject_name': '白糖', 'regex': r"SR\d{4}\.CZC",
         'sectorid': '091003004', 'date_establish': '2006-01-06'},
        {'subject_name': '棉花', 'regex': r"CF\d{4}\.CZC",
         'sectorid': '091003001', 'date_establish': '2004-06-01'},
        {'subject_name': '鲜苹果', 'regex': r"AP\d{4}\.CZC",
         'sectorid': '091003019', 'date_establish': '2017-12-22'},
    ]

    # 字段列表及参数
    indicator_param_list = [
        ('ths_future_short_name_future', '', String(20)),
        ('ths_future_code_future', '', String(20)),
        ('ths_sec_type_future', '', String(20)),
        ('ths_td_variety_future', '', String(20)),
        ('ths_td_unit_future', '', DOUBLE),
        ('ths_pricing_unit_future', '', String(20)),
        ('ths_mini_chg_price_future', '', DOUBLE),
        ('ths_chg_ratio_lmit_future', '', DOUBLE),
        ('ths_td_deposit_future', '', DOUBLE),
        ('ths_start_trade_date_future', '', Date),
        ('ths_last_td_date_future', '', Date),
        ('ths_last_delivery_date_future', '', Date),
        ('ths_delivery_month_future', '', Integer),
        ('ths_listing_benchmark_price_future', '', DOUBLE),
        ('ths_initial_td_deposit_future', '', DOUBLE),
        ('ths_contract_month_explain_future', '', String(60)),
        ('ths_td_time_explain_future', '', String(60)),
        ('ths_last_td_date_explian_future', '', String(60)),
        ('ths_delivery_date_explain_future', '', String(60)),
        ('ths_exchange_short_name_future', '', String(20)),
        ('ths_contract_en_short_name_future', '', String(20)),
        ('ths_contract_en_name_future', '', String(20)),
    ]
    json_indicator, json_param = unzip_join([(key, val) for key, val, _ in indicator_param_list], sep=';')

    # 设置 dtype
    dtype = {key: val for key, _, val in indicator_param_list}
    dtype['ths_code'] = String(20)

    # 获取合约列表
    code_set = set()
    ndays_per_update = 60
    # 获取历史期货合约列表信息
    sector_count = len(future_sectorid_dic_list)
    for num, future_sectorid_dic in enumerate(future_sectorid_dic_list, start=1):
        subject_name = future_sectorid_dic['subject_name']
        sector_id = future_sectorid_dic['sectorid']
        regex_str = future_sectorid_dic['regex']
        date_establish = datetime.strptime(future_sectorid_dic['date_establish'], STR_FORMAT_DATE).date()
        # 计算获取合约列表的起始日期
        date_since = get_date_since(code_ipo_date_dic, regex_str, date_establish)
        date_yestoday = date.today() - timedelta(days=1)
        logger.debug('%d/%d) 获取 %s %s [%s - %s] 合约列表',
                     num, sector_count, subject_name, sector_id, date_since, date_yestoday)
        while date_since <= date_yestoday:
            date_since_str = date_since.strftime(STR_FORMAT_DATE)
            # 获取合约列表
            # w.wset("sectorconstituent","date=2017-05-02;sectorid=a599010205000000")
            # future_info_df = rest.wset("sectorconstituent", "date=%s;sectorid=%s" % (date_since_str, sector_id))
            try:
                future_info_df = invoker.THS_DataPool('block', '%s;%s' % (date_since_str, sector_id), 'date:Y,thscode:Y,security_name:Y')
            except APIError:
                logger.exception('THS_DataPool %s 获取失败', '%s;%s' % (date_since_str, sector_id))
                break
            if future_info_df is None or future_info_df.shape[0] == 0:
                break

            code_set |= set(future_info_df['THSCODE'])
            if date_since >= date_yestoday:
                break
            else:
                date_since += timedelta(days=ndays_per_update)
                if date_since > date_yestoday:
                    date_since = date_yestoday

    # 获取合约列表
    code_list = [wc for wc in code_set if wc not in code_ipo_date_dic]
    # 获取合约基本信息
    if len(code_list) > 0:
        future_info_df = invoker.THS_BasicData(code_list, json_indicator, json_param)
        if future_info_df is None or future_info_df.shape[0] == 0:
            future_info_count = 0
            logger.warning("更新 ifind_future_info 结束 %d 条记录被更新", future_info_count)
        else:
            future_info_count = future_info_df.shape[0]
            future_info_df.to_sql('ifind_future_info', engine_md, if_exists='append', index=False, dtype=dtype)
            logger.info("更新 ifind_future_info 结束 %d 条记录被更新", future_info_count)


def save_future_daily_df_list(data_df_list):
    """将期货历史数据保存的数据库"""
    data_df_count = len(data_df_list)
    if data_df_count > 0:
        logger.info('merge data with %d df', data_df_count)
        data_df = pd.concat(data_df_list)
        data_count = data_df.shape[0]
        data_df.to_sql('ifind_future_daily', engine_md, if_exists='append', index=False,
                       dtype={
                           'ths_code': String(20),
                           'time': Date,
                           'preClose': String(20),
                           'open': DOUBLE,
                           'high': DOUBLE,
                           'low': DOUBLE,
                           'close': DOUBLE,
                           'volume': DOUBLE,
                           'amount': DOUBLE,
                           'avgPrice': DOUBLE,
                           'change': DOUBLE,
                           'changeRatio': DOUBLE,
                           'preSettlement': DOUBLE,
                           'settlement': DOUBLE,
                           'change_settlement': DOUBLE,
                           'chg_settlement': DOUBLE,
                           'openInterest': DOUBLE,
                           'positionChange': DOUBLE,
                           'amplitude': DOUBLE,
                       })
        logger.info("更新 wind_future_daily 结束 %d 条记录被更新", data_count)
    else:
        logger.info("更新 wind_future_daily 结束 0 条记录被更新")


@app.task
def import_future_daily():
    """
    更新期货合约日级别行情信息
    :return: 
    """
    logger.info("更新 ifind_future_daily 开始")
    date_ending = date.today() - ONE_DAY if datetime.now().hour < BASE_LINE_HOUR else date.today()
    # 16 点以后 下载当天收盘数据，16点以前只下载前一天的数据
    # 对于 date_to 距离今年超过1年的数据不再下载：发现有部分历史过于久远的数据已经无法补全，
    # 如：AL0202.SHF AL9902.SHF CU0202.SHF
    # TODO: ths_ksjyr_future 字段需要替换为 ths_contract_listed_date_future 更加合理
    sql_str = """select ths_code, date_frm, if(lasttrade_date<end_date, lasttrade_date, end_date) date_to
        FROM
        (
        select fi.ths_code, ifnull(trade_date_max_1, ths_start_trade_date_future) date_frm, 
            ths_last_td_date_future lasttrade_date,
                if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
            from ifind_future_info fi left outer join
            (select ths_code, adddate(max(time),1) trade_date_max_1 from ifind_future_daily group by ths_code) wfd
            on fi.ths_code = wfd.ths_code
        ) tt
        where date_frm <= if(lasttrade_date<end_date, lasttrade_date, end_date) 
        and subdate(curdate(), 360) < if(lasttrade_date<end_date, lasttrade_date, end_date) 
        order by ths_code"""
    future_date_dic = {}
    with with_db_session(engine_md) as session:
        try:
            table = session.execute(sql_str)
        except ProgrammingError:
            logger.exception('获取历史数据最新交易日期失败，尝试仅适用 ifind_future_info 表进行计算')
            sql_str = """select ths_code, date_frm, if(lasttrade_date<end_date, lasttrade_date, end_date) date_to
                from 
                (
                select fi.ths_code, ths_start_trade_date_future date_frm, 
                    ths_last_td_date_future lasttrade_date,
                        if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
                    from ifind_future_info fi
                ) tt"""
            table = session.execute(sql_str)
        # 整理 日期范围数据
        for ths_code, date_frm, lasttrade_date in table.fetchall():
            if date_frm is None:
                continue
            if isinstance(date_frm, str):
                date_frm = datetime.strptime(date_frm, STR_FORMAT_DATE).date()
            if isinstance(lasttrade_date, str):
                lasttrade_date = datetime.strptime(lasttrade_date, STR_FORMAT_DATE).date()
            date_to = date_ending if date_ending < lasttrade_date else lasttrade_date
            if date_frm > date_to:
                continue
            future_date_dic[ths_code] = (date_frm, date_to)

    data_df_list, data_count = [], 0
    data_len = len(future_date_dic)
    try:
        logger.info("%d future instrument will be handled", data_len)
        for data_num, (ths_code, (date_frm, date_to)) in enumerate(future_date_dic.items()):
            if date_frm > date_to:
                continue
            # TODO: 试用账号仅能获取近5年数据，正式账号时将此语句剔除
            if date_frm <= (date.today() - timedelta(days=(365 * 5))):
                continue
            date_frm_str = date_frm.strftime(STR_FORMAT_DATE)
            date_to_str = date_to.strftime(STR_FORMAT_DATE)
            logger.info('%d/%d) get %s between %s and %s', data_num, data_len, ths_code, date_frm_str, date_to_str)
            try:
                data_df_tmp = invoker.THS_HistoryQuotes(
                    ths_code,
                    'preClose,open,high,low,close,avgPrice,change,changeRatio,volume,amount,preSettlement,settlement,change_settlement,chg_settlement,openInterest,positionChange,amplitude',
                    'Interval:D,CPS:1,baseDate:1900-01-01,Currency:YSHB,fill:Previous', date_frm_str, date_to_str)
            except APIError as exp:
                logger.exception("%d/%d) %s 执行异常", data_num, data_len, ths_code)
                break
            if data_df_tmp is not None or data_df_tmp.shape[0] > 0:
                data_df_list.append(data_df_tmp)
                data_count += data_df_tmp.shape[0]
                if data_count >= 10000:
                    save_future_daily_df_list(data_df_list)
                    data_df_list, data_count = [], 0
            # if len(data_df_list) >= 5:
            #     break
    finally:
        save_future_daily_df_list(data_df_list)


# def import_wind_future_info_hk():
#     """
#     更新 香港股指 期货合约列表信息
#     香港恒生指数期货，香港国企指数期货合约只有07年2月开始的合约，且无法通过 wset 进行获取
#     :return:
#     """
#     logger.info("更新 wind_future_info 开始")
#     # 获取已存在合约列表
#     sql_str = 'select wind_code, ipo_date from wind_future_info'
#     engine = get_db_engine()
#     with get_db_session(engine) as session:
#         table = session.execute(sql_str)
#         wind_code_ipo_date_dic = dict(table.fetchall())
#
#     # 通过wind获取合约列表
#     # w.start()
#     rest = WindRest(WIND_REST_URL)  # 初始化服务器接口，用于下载万得数据
#     # future_sectorid_dic_list = [
#     #     {'subject_name': 'CFE 沪深300', 'regex': r"IF\d{4}\.CFE",
#     #      'sectorid': 'a599010102000000', 'date_establish': '2010-4-16'},
#     #     {'subject_name': 'CFE 上证50', 'regex': r"IH\d{4}\.CFE",
#     #      'sectorid': '1000014871000000', 'date_establish': '2015-4-16'},
#     #     {'subject_name': 'CFE 中证500', 'regex': r"IC\d{4}\.CFE",
#     #      'sectorid': '1000014872000000', 'date_establish': '2015-4-16'},
#     #     {'subject_name': 'SHFE 黄金', 'regex': r"AU\d{4}\.SHF",
#     #      'sectorid': 'a599010205000000', 'date_establish': '2008-01-09'},
#     #     {'subject_name': 'SHFE 沪银', 'regex': r"AG\d{4}\.SHF",
#     #      'sectorid': '1000006502000000', 'date_establish': '2012-05-10'},
#     #     {'subject_name': 'SHFE 螺纹钢', 'regex': r"RB\d{4}\.SHF",
#     #      'sectorid': 'a599010206000000', 'date_establish': '2009-03-27'},
#     #     {'subject_name': 'SHFE 热卷', 'regex': r"HC\d{4}\.SHF",
#     #      'sectorid': '1000011455000000', 'date_establish': '2014-03-21'},
#     #     {'subject_name': 'DCE 焦炭', 'regex': r"J\d{4}\.SHF",
#     #      'sectorid': '1000002976000000', 'date_establish': '2011-04-15'},
#     #     {'subject_name': 'DCE 焦煤', 'regex': r"JM\d{4}\.SHF",
#     #      'sectorid': '1000009338000000', 'date_establish': '2013-03-22'},
#     #     {'subject_name': '铁矿石', 'regex': r"I\d{4}\.SHF",
#     #      'sectorid': '1000006502000000', 'date_establish': '2013-10-18'},
#     #     {'subject_name': '天然橡胶', 'regex': r"RU\d{4}\.SHF",
#     #      'sectorid': 'a599010208000000', 'date_establish': '1995-06-01'},
#     #     {'subject_name': '铜', 'regex': r"CU\d{4}\.SHF",
#     #      'sectorid': 'a599010202000000', 'date_establish': '1995-05-01'},
#     #     {'subject_name': '铝', 'regex': r"AL\d{4}\.SHF",
#     #      'sectorid': 'a599010203000000', 'date_establish': '1995-05-01'},
#     #     {'subject_name': '锌', 'regex': r"ZN\d{4}\.SHF",
#     #      'sectorid': 'a599010204000000', 'date_establish': '2007-03-26'},
#     #     {'subject_name': '铅', 'regex': r"PB\d{4}\.SHF",
#     #      'sectorid': '1000002892000000', 'date_establish': '2011-03-24'},
#     #     {'subject_name': '镍', 'regex': r"NI\d{4}\.SHF",
#     #      'sectorid': '1000011457000000', 'date_establish': '2015-03-27'},
#     #     {'subject_name': '锡', 'regex': r"SN\d{4}\.SHF",
#     #      'sectorid': '1000011458000000', 'date_establish': '2015-03-27'},
#     #     {'subject_name': '白糖', 'regex': r"SR\d{4}\.CZC",
#     #      'sectorid': 'a599010405000000', 'date_establish': '2006-01-06'},
#     #     {'subject_name': '棉花', 'regex': r"CF\d{4}\.CZC",
#     #      'sectorid': 'a599010404000000', 'date_establish': '2004-06-01'},
#     #     {'subject_name': '棉花', 'regex': r"CF\d{4}\.CZC",
#     #      'sectorid': 'a599010404000000', 'date_establish': '2004-06-01'},
#     # ]
#     # wind_code_set = set()
#     # ndays_per_update = 60
#     # # 获取历史期货合约列表信息
#     # for future_sectorid_dic in future_sectorid_dic_list:
#     #     subject_name = future_sectorid_dic['subject_name']
#     #     sector_id = future_sectorid_dic['sectorid']
#     #     regex_str = future_sectorid_dic['regex']
#     #     date_establish = datetime.strptime(future_sectorid_dic['date_establish'], STR_FORMAT_DATE).date()
#     #     date_since = get_date_since(wind_code_ipo_date_dic, regex_str, date_establish)
#     #     date_yestoday = date.today() - timedelta(days=1)
#     #     while date_since <= date_yestoday:
#     #         date_since_str = date_since.strftime(STR_FORMAT_DATE)
#     #         # w.wset("sectorconstituent","date=2017-05-02;sectorid=a599010205000000")
#     #         # future_info_df = wset_cache(w, "sectorconstituent", "date=%s;sectorid=%s" % (date_since_str, sector_id))
#     #         future_info_df = rest.wset("sectorconstituent", "date=%s;sectorid=%s" % (date_since_str, sector_id))
#     #         wind_code_set |= set(future_info_df['wind_code'])
#     #         # future_info_df = future_info_df[['wind_code', 'sec_name']]
#     #         # future_info_dic_list = future_info_df.to_dict(orient='records')
#     #         # for future_info_dic in future_info_dic_list:
#     #         #     wind_code = future_info_dic['wind_code']
#     #         #     if wind_code not in wind_code_future_info_dic:
#     #         #         wind_code_future_info_dic[wind_code] = future_info_dic
#     #         if date_since >= date_yestoday:
#     #             break
#     #         else:
#     #             date_since += timedelta(days=ndays_per_update)
#     #             if date_since > date_yestoday:
#     #                 date_since = date_yestoday
#
#     # 获取合约列表
#     # 手动生成合约列表
#     # 香港恒生指数期货，香港国企指数期货合约只有07年2月开始的合约，且无法通过 wset 进行获取
#     wind_code_list = ['%s%02d%02d.HK' % (name, year, month)
#                       for name, year, month in itertools.product(['HSIF', 'HHIF'], range(7,19), range(1,13))
#                       if not(year==7 and month==1)]
#
#     # 获取合约基本信息
#     # w.wss("AU1706.SHF,AG1612.SHF,AU0806.SHF", "ipo_date,sec_name,sec_englishname,exch_eng,lasttrade_date,lastdelivery_date,dlmonth,lprice,sccode,margin,punit,changelt,mfprice,contractmultiplier,ftmargins,trade_code")
#     # future_info_df = wss_cache(w, wind_code_list,
#     #                            "ipo_date,sec_name,sec_englishname,exch_eng,lasttrade_date,lastdelivery_date,dlmonth,lprice,sccode,margin,punit,changelt,mfprice,contractmultiplier,ftmargins,trade_code")
#     if len(wind_code_list) > 0:
#         future_info_df = rest.wss(wind_code_list,
#                                   "ipo_date,sec_name,sec_englishname,exch_eng,lasttrade_date,lastdelivery_date,dlmonth,lprice,sccode,margin,punit,changelt,mfprice,contractmultiplier,ftmargins,trade_code")
#
#         future_info_df['MFPRICE'] = future_info_df['MFPRICE'].apply(mfprice_2_num)
#
#         future_info_df.rename(columns={c: str.lower(c) for c in future_info_df.columns}, inplace=True)
#         future_info_df.index.rename('wind_code', inplace=True)
#         future_info_df = future_info_df[~(future_info_df['ipo_date'].isna() | future_info_df['lasttrade_date'].isna())]
#         future_info_count = future_info_df.shape[0]
#         future_info_df.to_sql('wind_future_info', engine, if_exists='append',
#                               dtype={
#                                   'wind_code': String(20),
#                                   'trade_code': String(20),
#                                   'sec_name': String(50),
#                                   'sec_englishname': String(50),
#                                   'exch_eng': String(50),
#                                   'ipo_date': Date,
#                                   'lasttrade_date': Date,
#                                   'lastdelivery_date': Date,
#                                   'dlmonth': String(20),
#                                   'lprice': Float,
#                                   'sccode': String(20),
#                                   'margin': Float,
#                                   'punit': String(20),
#                                   'changelt': Float,
#                                   'mfprice': Float,
#                                   'contractmultiplier': Float,
#                                   'ftmargins': String(100),
#                               })
#         logger.info("更新 wind_future_info 结束 %d 条记录被更新", future_info_count)
#         # w.close()


if __name__ == "__main__":
    # 保存期货交易所品种信息，一次性数据导入，以后基本上不需要使用了
    # import_variety_info()
    # 导入期货基础信息数据
    # import_future_info()
    # 导入期货历史行情数据
    import_future_daily()
    # import_future_info_hk()
