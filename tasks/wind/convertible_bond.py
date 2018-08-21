# -*- coding: utf-8 -*-
"""
Created on 2017/12/5
@author: MG
稍后需要通过wind或者ts补充一下可转债交易日期间 OHLCV
另外，信息表里面需要补充“兑付日” redemption_beginning 数据 行情数据兑付日后就没有交易数据更新了。不必每日获取数据
import tushare as ts
ts.get_k_data('sz128011')

"""
from datetime import date, datetime, timedelta
import math
import pandas as pd
import logging
from sqlalchemy.dialects.mysql import DOUBLE
from tasks import app
from tasks.wind import invoker
from tasks.backend import engine_md
from tasks.utils.fh_utils import STR_FORMAT_DATE
from direstinvoker.iwind import UN_AVAILABLE_DATE
from tasks.backend.orm import build_primary_key
from sqlalchemy.types import String, Date, Integer
from tasks.utils.db_utils import alter_table_2_myisam
from tasks.merge.code_mapping import update_from_info_table
from tasks.utils.fh_utils import get_last, get_first, str_2_date
from tasks.utils.db_utils import with_db_session, bunch_insert_on_duplicate_update
DEBUG = False
logger = logging.getLogger()
DATE_BASE = datetime.strptime('1998-01-01', STR_FORMAT_DATE).date()
ONE_DAY = timedelta(days=1)


def get_cb_set(date_fetch):
    date_fetch_str = date_fetch.strftime(STR_FORMAT_DATE)
    data_df = invoker.wset("sectorconstituent", "date=%s;sectorid=1000021892000000" % date_fetch_str)
    if data_df is None:
        logging.warning('%s 获取股票代码失败', date_fetch_str)
        return None
    data_count = data_df.shape[0]
    logging.info('get %d convertible bond on %s', data_count, date_fetch_str)
    return set(data_df['wind_code'])


@app.task
def import_cb_info(first_time=False):
    """
    获取全市场可转债数据
    :param first_time: 第一次执行时将从 1999 年开始查找全部基本信息
    :return: 
    """
    table_name = 'wind_convertible_bond_info'
    has_table = engine_md.has_table(table_name)
    name_param_list = [
        ('trade_code', DOUBLE),
        ('full_name', String(45)),
        ('sec_name', String(45)),
        ('issue_announcement_date', Date),
        ('ipo_date', Date),
        ('start_date', Date),
        ('end_date', Date),
        ('conversion_code', DOUBLE),
        ('is_floating_rate', String(8)),
        ('is_interest_compensation', String(8)),
        ('interest_compensation_desc', String(200)),
        ('interest_compensation', DOUBLE),
        ('term', DOUBLE),
        ('underlying_code', DOUBLE),
        ('underlying_name', DOUBLE),
        ('redemption_beginning', Date),
    ]
    param = ",".join([key for key, _ in name_param_list])
    name_param_dic = {col_name.lower(): col_name.upper() for col_name, _ in name_param_list}
    # 设置dtype类型
    dtype = {key: val for key, val in name_param_list}
    dtype['wind_code'] = DOUBLE
    if first_time:
        date_since = datetime.strptime('1999-01-01', STR_FORMAT_DATE).date()
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
        data_set = get_cb_set(fetch_date)
        if data_set is not None:
            wind_code_set |= data_set

    # 获取股票对应上市日期，及摘牌日期
    # w.wss("300005.SZ,300372.SZ,000003.SZ", "ipo_date,trade_code,mkt,exch_city,exch_eng")
    wind_code_list = list(wind_code_set)
    wind_code_count = len(wind_code_list)
    seg_count = 1000
    # loop_count = math.ceil(float(wind_code_count) / seg_count)
    data_info_df_list = []
    for n in range(0, wind_code_count, seg_count):
        num_start = n * seg_count
        num_end = (n + 1) * seg_count
        # num_end = num_end if num_end <= wind_code_count else wind_code_count
        sub_list = wind_code_list[n:(n + seg_count)]
        # 尝试将 stock_code_list_sub 直接传递给wss，是否可行
        stock_info_df = invoker.wss(sub_list, param, "unit=1")
        data_info_df_list.append(stock_info_df)
        # 仅仅调试时使用
        # if DEBUG and len(data_info_df_list) > 5000:
        #     break

    data_info_all_df = pd.concat(data_info_df_list)
    data_info_all_df.index.rename('WIND_CODE', inplace=True)
    logging.info('%s stock data will be import', data_info_all_df.shape[0])
    data_info_all_df.reset_index(inplace=True)
    bunch_insert_on_duplicate_update(data_info_all_df, table_name, engine_md, dtype=dtype)
    logging.info("%d stocks have been in wind_convertible_bond_info", len(data_info_all_df))
    if not has_table and engine_md.has_table(table_name):
        alter_table_2_myisam(engine_md, [table_name])
        build_primary_key([table_name])
    # 更新 code_mapping 表
    update_from_info_table(table_name)


@app.task
def import_cb_daily():
    """
    导入可转债日线数据
    需要补充 转股价格
    :return: 
    """
    col_name_param_list = [
        ('outstandingbalance', DOUBLE),
        ('clause_conversion2_bondlot', DOUBLE),
        ('clause_conversion2_bondproportion', DOUBLE),
        ('clause_conversion2_swapshareprice', DOUBLE),
        ('clause_conversion2_conversionproportion', DOUBLE),
        ('convpremium', DOUBLE),
        ('convpremiumratio', DOUBLE),
        ('convvalue', DOUBLE),
        ('convpe', DOUBLE),
        ('convpb', DOUBLE),
        ('underlyingpe', DOUBLE),
        ('underlyingpb', DOUBLE),
        ('diluterate', DOUBLE),
        ('ldiluterate', DOUBLE),
        ('open', DOUBLE),
        ('high', DOUBLE),
        ('low', DOUBLE),
        ('close', DOUBLE),
        ('volume', DOUBLE),
    ]
    wind_indictor_str = ",".join(col_name for col_name, _ in col_name_param_list)
    logging.info("更新 wind_convertible_bond_daily 开始")
    table_name = "wind_convertible_bond_daily"
    has_table = engine_md.has_table(table_name)

    if has_table:
        sql_str = """
            SELECT wind_code, date_frm, if(issue_announcement_date<end_date, issue_announcement_date, end_date) date_to
            FROM
            (
            SELECT info.wind_code, ifnull(trade_date, ipo_date) date_frm, issue_announcement_date,
            if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
            FROM 
                wind_convertible_bond_info info 
            LEFT OUTER JOIN
                (SELECT wind_code, adddate(max(trade_date),1) trade_date FROM {table_name} GROUP BY wind_code) daily
            ON info.wind_code = daily.wind_code
            ) tt
            WHERE date_frm <= if(issue_announcement_date<end_date, issue_announcement_date, end_date) 
            ORDER BY wind_code""".format(table_name=table_name)
    else:
        logger.warning('wind_convertible_bond_daily 不存在，仅使用 wind_convertible_bond_info 表进行计算日期范围')
        sql_str = """
            SELECT wind_code, date_frm, if(issue_announcement_date<end_date, issue_announcement_date, end_date) date_to
            FROM
              (
                SELECT info.wind_code, ipo_date date_frm, issue_announcement_date,
                if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
                FROM wind_convertible_bond_info info 
              ) tt
            WHERE date_frm <= if(issue_announcement_date<end_date, issue_announcement_date, end_date) 
            ORDER BY wind_code"""

    with with_db_session(engine_md) as session:
        # 获取每只股票需要获取日线数据的日期区间
        table = session.execute(sql_str)
        # 计算每只股票需要获取日线数据的日期区间
        begin_time = None
        # 获取date_from,date_to，将date_from,date_to做为value值
        stock_date_dic = {
            wind_code: (date_from if begin_time is None else min([date_from, begin_time]), date_to)
            for wind_code, date_from, date_to in table.fetchall() if
            wind_code_set is None or wind_code in wind_code_set}
    # 设置dtype
    dtype = {key: val for key, val in col_name_param_list}
    dtype['wind_code'] = String(20)
    data_df_list = []
    data_count = len(stock_date_dic)
    logger.info('%d stocks will been import into wind_convertible_bond_daily', data_count)
    # 获取股票量价等行情数据
    upper_col_2_name_dic = {name.upper(): name for name, _ in col_name_param_list}

    try:
        for data_num, (wind_code, (date_from, date_to)) in enumerate(stock_date_dic.items(), start=1):
            data_df = invoker.wsd(wind_code, wind_indictor_str, date_from, date_to, "unit=1")
            if data_df is None:
                logger.warning('%d/%d) %s has no data during %s %s',
                               data_num, data_count, wind_code, date_from, date_to)
                continue
            data_df.rename(columns=upper_col_2_name_dic, inplace=True)
            data_df = data_df[~data_df['close'].isna()]
            if data_df.shape[0] == 0:
                logger.warning('%d/%d) %s has no available data during %s %s',
                               data_num, data_count, wind_code, date_from, date_to)
                continue
            logger.info('%d/%d) %d data of %s between %s and %s',
                        data_num, data_count, data_df.shape[0], wind_code, date_from, date_to)
            data_df['wind_code'] = wind_code
            data_df_list.append(data_df)
            # 仅调试时使用
            # if len(data_df_list) > 10:
            #     break
    finally:
        # 导入数据库
        if len(data_df_list) > 0:
            data_df_all = pd.concat(data_df_list)
            data_df_all.index.rename('trade_date', inplace=True)
            data_df_all.reset_index(inplace=True)
            data_df_all.set_index(['wind_code', 'trade_date'], inplace=True)
            bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md, dtype=dtype)
            logging.info("更新 wind_convertible_bond_daily 结束 %d 条信息被更新", data_df_all.shape[0])
            if not has_table and engine_md.has_table(table_name):
                alter_table_2_myisam(engine_md, [table_name])
                build_primary_key([table_name])


def fill_col_by_wss(col_name_dic, table_name):
    """补充历史col数据
    :param col_name_dic:
    :param table_name:
    :return:
    """
    # 股票列表
    col_name_list = [col_name.lower() for col_name in col_name_dic.keys()]
    # 获取每只股票ipo 日期 及 最小的交易日前一天
    sql_str = """select wind_code from %s""" % table_name
    with with_db_session(engine_md) as session:
        table = session.execute(sql_str)
        wind_code_set = {content[0] for content in table.fetchall()}
    data_count = len(wind_code_set)
    data_df_list = []
    try:
        # for n, (wind_code, (date_from, date_to)) in enumerate(stock_trade_date_range_dic.items()):
        for data_num, wind_code in enumerate(wind_code_set, start=1):
            if wind_code not in wind_code_set:
                continue
            # 获取股票量价等行情数据
            wind_indictor_str = col_name_list
            data_df = invoker.wss(wind_code, wind_indictor_str)
            if data_df is None:
                logger.warning('%d) %s has no data during %s %s', data_num, wind_code)
                continue
            logger.debug('%d/%d) 获取 %s', data_num, data_count, wind_code)
            # data_df['wind_code'] = wind_code
            data_df_list.append(data_df)
            # 仅供调试使用
            # if data_num > 10:
            #     break
    finally:
        # 导入数据库
        if len(data_df_list) > 0:
            data_df_all = pd.concat(data_df_list)
            data_df_all.index.rename('wind_code', inplace=True)
            data_df_all.reset_index(inplace=True)
            # 只有全部列为空的项才需要剔除
            is_na_s = None
            for col_name in col_name_dic.keys():
                col_name = col_name.upper()
                if is_na_s is None:
                    is_na_s = data_df_all[col_name].isna()
                else:
                    is_na_s = is_na_s & data_df_all[col_name].isna()
            data_df_not_null = data_df_all[~is_na_s]
            data_df_not_null.fillna('null', inplace=True)
            data_dic_list = data_df_not_null.to_dict(orient='records')
            sql_str = "update %s set " % table_name + \
                      ",".join(["%s=:%s" % (db_col_name, col_name.upper()) for col_name, db_col_name in
                                col_name_dic.items()]) + \
                      " where wind_code=:wind_code"
            with with_db_session(engine_md) as session:
                table = session.execute(sql_str, params=data_dic_list)
            logger.info('%d data updated', data_df_not_null.shape[0])
        else:
            logger.warning('no data for update')


def fill_col_by_wsd(col_name_dic: dict, table_name, top_n=None):
    """补充历史col数据
    :param col_name_dic:
    :param table_name:
    :param top_n:
    :return:
    """
    # 股票列表
    # db_col_name_list = [col_name.lower() for col_name in col_name_dic.values()]
    col_name_list = [col_name.lower() for col_name in col_name_dic.keys()]
    # 获取每只股票ipo 日期 及 最小的交易日前一天
    #     sql_str = """select si.wind_code, td_from, td_to
    # from wind_stock_info si,
    # (select wind_code, min(trade_date) td_from, max(trade_date) td_to from wind_stock_daily where ev2_to_ebitda is null group by wind_code) sd
    # where si.wind_code = sd.wind_code"""
    where_sub_str = ' and '.join([col_name + ' is null' for col_name in col_name_dic.values()])
    sql_str = """
        select wsd.wind_code, min_trade_date, max_trade_date
        from
        (
        select wind_code, min(trade_date) min_trade_date, max(trade_date) max_trade_date
        from wind_convertible_bond_daily
        group by wind_code
        ) wsd
        INNER JOIN
        (
        select wind_code
        from """ + table_name + """ where """ + where_sub_str + """
        group by wind_code
        ) wsd_not_null
        on wsd.wind_code = wsd_not_null.wind_code"""
    with with_db_session(engine_md) as session:
        table = session.execute(sql_str)
        stock_trade_date_range_dic = {content[0]: (content[1], content[2]) for content in table.fetchall()}
    data_df_list = []
    data_count = len(stock_trade_date_range_dic)
    try:
        # for n, (wind_code, (date_from, date_to)) in enumerate(stock_trade_date_range_dic.items()):
        for data_num, (wind_code, (date_from, date_to)) in enumerate(stock_trade_date_range_dic.items(), start=1):
            if top_n is not None and data_num > top_n:
                break
            # 获取股票量价等行情数据
            wind_indictor_str = col_name_list
            data_df = invoker.wsd(wind_code, wind_indictor_str, date_from, date_to)
            if data_df is None:
                logger.warning('%d/%d) %s has no data during %s %s',
                               data_num, data_count, wind_code, date_from, date_to)
                continue
            logger.info('%d/%d) %d data of %s between %s and %s',
                        data_num, data_count, data_df.shape[0], wind_code, date_from, date_to)
            data_df['wind_code'] = wind_code
            data_df_list.append(data_df)
            # 仅供调试使用
            # if data_num >= 10:
            #     break
    finally:
        # 导入数据库
        if len(data_df_list) > 0:
            data_df_all = pd.concat(data_df_list)
            data_df_all.index.rename('trade_date', inplace=True)
            data_df_all.reset_index(inplace=True)
            # 只有全部列为空的项才需要剔除
            is_na_s = None
            for col_name in col_name_dic.keys():
                col_name = col_name.upper()
                if is_na_s is None:
                    is_na_s = data_df_all[col_name].isna()
                else:
                    is_na_s = is_na_s & data_df_all[col_name].isna()
            data_df_not_null = data_df_all[~is_na_s]
            data_df_not_null.fillna(0, inplace=True)
            if data_df_not_null.shape[0] > 0:
                data_dic_list = data_df_not_null.to_dict(orient='records')
                sql_str = "update %s set " % table_name + \
                          ",".join(
                              ["%s=:%s" % (db_col_name, col_name.upper()) for col_name, db_col_name in
                               col_name_dic.items()]) + \
                          " where wind_code=:wind_code and trade_date=:trade_date"
                with with_db_session(engine_md) as session:
                    session.execute(sql_str, params=data_dic_list)
            logger.info('%d data updated on %s', data_df_not_null.shape[0], table_name)
        else:
            logger.warning('no data for updating on %s', table_name)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s [%(name)s:%(funcName)s] %(message)s')
    DEBUG = True
    # import_cb_info()
    import_cb_daily()
    wind_code_set = None
    # 更新 wind_convertible_bond_info 信息
    # col_name_dic = {
    #     'redemption_beginning': 'redemption_beginning',
    # }
    # table_name = 'wind_convertible_bond_info'
    # fill_col_by_wss(col_name_dic, table_name)

    # 更新 wind_convertible_bond_daily
    # col_name_dic = {
    #     'open': 'open',
    #     'high': 'high',
    #     'low': 'low',
    #     'volume': 'volume',
    # }
    # table_name = 'wind_convertible_bond_daily'
    # fill_col_by_wsd(col_name_dic, table_name)
