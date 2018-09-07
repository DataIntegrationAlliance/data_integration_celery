# -*- coding: utf-8 -*-
"""
Created on 2017/5/2
@author: MG
@desc    : 2018-08-23 his 函数 已经正式运行测试完成，可以正常使用，其他函数稍后验证
"""
import logging
from datetime import datetime, date, timedelta
from tasks.utils.db_utils import with_db_session, bunch_insert_on_duplicate_update, alter_table_2_myisam
from tasks.backend import engine_md
from tasks.backend.orm import build_primary_key
from tasks.ifind import invoker
from tasks import app
from direstinvoker import APIError
from sqlalchemy.dialects.mysql import DOUBLE
from sqlalchemy.types import String, Date, Integer
import re
import pandas as pd
from tasks.utils.fh_utils import STR_FORMAT_DATE, unzip_join

DEBUG = False
TRIAL = True
logger = logging.getLogger()
RE_PATTERN_MFPRICE = re.compile(r'\d*\.*\d*')
ONE_DAY = timedelta(days=1)
# 标示每天几点以后下载当日行情数据
BASE_LINE_HOUR = 16


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
        if ipo_date is None:
            continue
        m = re.match(regex_str, wind_code)
        if m is not None and date_since < ipo_date:
            date_since = ipo_date
    # if date_since != date_establish:
    #     date_since += timedelta(days=ndays_per_update)
    return date_since


def import_variety_info():
    """保存期货交易所品种信息，一次性数据导入，以后基本上不需要使用了"""
    table_name = 'ifind_future_variety_info'
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
    # 设置 dtype
    dtype = {
        'exchange': String(20),
        'ID': String(20),
        'SECURITY_NAME': String(20),
    }
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
            # tot_data_df.to_sql('ifind_variety_info', engine_md, index=False, if_exists='append', dtype=dtype)
            tot_data_count = bunch_insert_on_duplicate_update(tot_data_df, table_name, engine_md, dtype=dtype)
        else:
            tot_data_count = 0

        logger.info('保存交易所品种信息完成， %d条数据被保存', tot_data_count)


@app.task
def import_future_info(chain_param=None):
    """
    更新期货合约列表信息
    :param chain_param: 该参数仅用于 task.chain 串行操作时，上下传递参数使用
    :return:
    """
    table_name = 'ifind_future_info'
    logger.info("更新 %s 开始", table_name)
    # 获取已存在合约列表
    try:
        sql_str = 'SELECT ths_code, ths_start_trade_date_future FROM {table_name}'.format(table_name=table_name)
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
        ('ths_delivery_month_future', '', String(10)),
        ('ths_listing_benchmark_price_future', '', DOUBLE),
        ('ths_initial_td_deposit_future', '', DOUBLE),
        ('ths_contract_month_explain_future', '', String(60)),
        ('ths_td_time_explain_future', '', String(80)),
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
                future_info_df = invoker.THS_DataPool('block', '%s;%s' % (date_since_str, sector_id),
                                                      'date:Y,thscode:Y,security_name:Y')
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

        if DEBUG:
            break

    # 获取合约列表
    code_list = [wc for wc in code_set if wc not in code_ipo_date_dic]
    # 获取合约基本信息
    if len(code_list) > 0:
        future_info_df = invoker.THS_BasicData(code_list, json_indicator, json_param)
        if future_info_df is None or future_info_df.shape[0] == 0:
            data_count = 0
            logger.warning("更新 %s 结束 %d 条记录被更新", table_name, data_count)
        else:
            data_count = future_info_df.shape[0]

            # future_info_df.to_sql(table_name, engine_md, if_exists='append', index=False, dtype=dtype)
            data_count = bunch_insert_on_duplicate_update(future_info_df, table_name, engine_md, dtype)
            logger.info("更新 %s 结束 %d 条记录被更新", table_name, data_count)


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
def import_future_daily_his(chain_param=None, ths_code_set: set = None, begin_time=None):
    """
    更新期货合约日级别行情信息
    :param chain_param: 该参数仅用于 task.chain 串行操作时，上下传递参数使用
    :param ths_code_set:
    :param begin_time:
    :return:
    """
    table_name = 'ifind_future_daily'
    info_table_name = 'ifind_future_info'
    logger.info("更新 %s 开始", table_name)
    has_table = engine_md.has_table(table_name)
    indicator_param_list = [
        ('preClose', String(20)),
        ('open', DOUBLE),
        ('high', DOUBLE),
        ('low', DOUBLE),
        ('close', DOUBLE),
        ('volume', DOUBLE),
        ('amount', DOUBLE),
        ('avgPrice', DOUBLE),
        ('change', DOUBLE),
        ('changeRatio', DOUBLE),
        ('preSettlement', DOUBLE),
        ('settlement', DOUBLE),
        ('change_settlement', DOUBLE),
        ('chg_settlement', DOUBLE),
        ('openInterest', DOUBLE),
        ('positionChange', DOUBLE),
        ('amplitude', DOUBLE),
    ]
    json_indicator = ','.join([key for key, _ in indicator_param_list])
    if has_table:
        # 16 点以后 下载当天收盘数据，16点以前只下载前一天的数据
        # 对于 date_to 距离今年超过1年的数据不再下载：发现有部分历史过于久远的数据已经无法补全，
        # 如：AL0202.SHF AL9902.SHF CU0202.SHF
        # TODO: ths_ksjyr_future 字段需要替换为 ths_contract_listed_date_future 更加合理
        sql_str = """SELECT ths_code, date_frm, 
                if(lasttrade_date<end_date, lasttrade_date, end_date) date_to
            FROM
            (
            SELECT fi.ths_code, ifnull(trade_date_max_1, ths_start_trade_date_future) date_frm, 
                ths_last_td_date_future lasttrade_date,
                    if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
                FROM {info_table_name} fi LEFT OUTER JOIN
                (SELECT ths_code, adddate(max(time),1) trade_date_max_1 FROM {table_name} GROUP BY ths_code) wfd
                ON fi.ths_code = wfd.ths_code
            ) tt
            WHERE date_frm <= if(lasttrade_date<end_date, lasttrade_date, end_date) 
            AND subdate(curdate(), 360) < if(lasttrade_date<end_date, lasttrade_date, end_date) 
            ORDER BY ths_code""".format(table_name=table_name, info_table_name=info_table_name)
    else:
        sql_str = """SELECT ths_code, date_frm, if(lasttrade_date<end_date, lasttrade_date, end_date) date_to
            FROM 
            (
            SELECT fi.ths_code, ths_start_trade_date_future date_frm, 
                ths_last_td_date_future lasttrade_date,
                    if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
                FROM {info_table_name} fi
            ) tt""".format(info_table_name=info_table_name)
        logger.warning('%s 不存在，仅使用 %s 表进行计算日期范围', table_name, info_table_name)

    with with_db_session(engine_md) as session:
        # 获取每只股票需要获取日线数据的日期区间
        table = session.execute(sql_str)
        # 获取每只股票需要获取日线数据的日期区间
        code_date_range_dic = {
            ths_code: (date_from if begin_time is None else min([date_from, begin_time]), date_to)
            for ths_code, date_from, date_to in table.fetchall() if
            ths_code_set is None or ths_code in ths_code_set}

    if TRIAL:
        date_from_min = date.today() - timedelta(days=(365 * 5))
        # 试用账号只能获取近5年数据
        code_date_range_dic = {
            ths_code: (max([date_from, date_from_min]), date_to)
            for ths_code, (date_from, date_to) in code_date_range_dic.items() if date_from_min <= date_to}

    # 设置 dtype
    dtype = {key: val for key, val in indicator_param_list}
    dtype['ths_code'] = String(20)
    dtype['time'] = Date

    data_df_list, data_count, tot_data_count, code_count = [], 0, 0, len(code_date_range_dic)
    try:
        logger.info("%d future instrument will be handled", code_count)
        for num, (ths_code, (begin_time, end_time)) in enumerate(code_date_range_dic.items(), start=1):
            logger.debug('%d/%d) %s [%s - %s]', num, code_count, ths_code, begin_time, end_time)
            data_df = invoker.THS_HistoryQuotes(
                ths_code, json_indicator,
                'Interval:D,CPS:1,baseDate:1900-01-01,Currency:YSHB,fill:Previous', begin_time, end_time)
            if data_df is not None and data_df.shape[0] > 0:
                data_count += data_df.shape[0]
                data_df_list.append(data_df)
            # 大于阀值有开始插入
            if data_count >= 10000:
                data_df_all = pd.concat(data_df_list)
                # data_df_all.to_sql(table_name, engine_md, if_exists='append', index=False, dtype=dtype)
                data_count = bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md, dtype)
                tot_data_count += data_count
                data_df_list, data_count = [], 0
                logging.info("%s 新增数据 %d 条", table_name, data_count)

            # 仅调试使用
            if DEBUG and len(data_df_list) > 1:
                break
    finally:
        if data_count > 0:
            data_df_all = pd.concat(data_df_list)
            # data_df_all.to_sql(table_name, engine_md, if_exists='append', index=False, dtype=dtype)
            data_count = bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md, dtype)
            tot_data_count += data_count
            logging.info("%s 新增数据 %d 条", table_name, data_count)

        if not has_table and engine_md.has_table(table_name):
            alter_table_2_myisam(engine_md, [table_name])
            build_primary_key([table_name])

        logging.info("更新 %s 完成 新增数据 %d 条", table_name, tot_data_count)


if __name__ == "__main__":
    TRIAL = True
    # DEBUG = True
    # 保存期货交易所品种信息，一次性数据导入，以后基本上不需要使用了
    import_variety_info()
    # 导入期货基础信息数据
    import_future_info()
    # 导入期货历史行情数据
    import_future_daily_his()
    # import_future_info_hk()
