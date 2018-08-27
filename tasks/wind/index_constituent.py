#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/3/30 9:39
@File    : wind_indexconstituent.py
@contact : mmmaaaggg@163.com
@desc    :导入指数成分股及权重
"""
import logging
import pandas as pd
from tasks import app
from tasks.wind import invoker
from tasks.backend import engine_md
from collections import OrderedDict
from datetime import date, datetime, timedelta
from tasks.utils.db_utils import with_db_session, bunch_insert_on_duplicate_update
from direstinvoker import APIError, UN_AVAILABLE_DATE
from tasks.utils.fh_utils import STR_FORMAT_DATE, date_2_str, str_2_date, get_first_idx, get_last_idx
from sqlalchemy.types import String, Date, Integer
from sqlalchemy.dialects.mysql import DOUBLE, TEXT

DEBUG = False
logger = logging.getLogger()
DATE_BASE = datetime.strptime('1980-01-01', STR_FORMAT_DATE).date()
ONE_DAY = timedelta(days=1)
# 标示每天几点以后下载当日行情数据
BASE_LINE_HOUR = 20


def get_trade_date_list_sorted(exch_code='SZSE') -> list:
    """
    獲取交易日列表
    :return:
    """
    sql_str = "select trade_date from wind_trade_date where exch_code = :exch_code order by trade_date"
    with with_db_session(engine_md) as session:
        trade_date_list_sorted = [content[0] for content in
                                  session.execute(sql_str, params={'exch_code': exch_code}).fetchall()]
    return trade_date_list_sorted


def get_latest_constituent_df(index_code):
    """
    获取最新的交易日日期，及相应的成分股数据
    :return:
    """
    table_name = "wind_index_constituent"
    has_table = engine_md.has_table(table_name)
    if not has_table:
        return None, None
    with with_db_session(engine_md) as session:
        date_latest = session.execute(
            "select max(trade_date) from wind_index_constituent where index_code = :index_code",
            params={"index_code": index_code}).scalar()
        if date_latest is None:
            return None, None

    sql_str = "select * from wind_index_constituent where index_code = %s and trade_date = %s"
    sec_df = pd.read_sql(sql_str, engine_md, params=[index_code, date_latest])
    return date_latest, sec_df


def get_sectorconstituent(index_code, index_name, target_date) -> pd.DataFrame:
    """
    通过 wind 获取指数成分股及权重
    :param index_code:
    :param index_name:
    :param target_date:
    :return:
    """
    target_date_str = date_2_str(target_date)
    logger.info('获取 %s %s %s 板块信息', index_code, index_name, target_date)
    sec_df = invoker.wset("indexconstituent", "date=%s;windcode=%s" % (target_date_str, index_code))
    if sec_df is not None and sec_df.shape[0] > 0:
        # 发现部分情况下返回数据的日期与 target_date 日期不匹配
        sec_df = sec_df[sec_df['date'].apply(lambda x: str_2_date(x) == target_date)]
    if sec_df is None or sec_df.shape[0] == 0:
        return None
    sec_df["index_code"] = index_code
    sec_df["index_name"] = index_name
    sec_df.rename(columns={
        'date': 'trade_date',
        'sec_name': 'stock_name',
        'i_weight': 'weight',
    }, inplace=True)
    return sec_df


def get_index_constituent_2_dic(index_code, index_name, date_start, idx_start,
                                date_constituent_df_dict, idx_constituent_set_dic):
    """
    通过 wind 获取指数成分股及权重 存入 date_constituent_df_dict idx_constituent_set_dic
    :param index_code:
    :param index_name:
    :param date_start:
    :param idx_start:
    :param date_constituent_df_dict:
    :param idx_constituent_set_dic:
    :return:
    """
    if date_start in date_constituent_df_dict and idx_start in idx_constituent_set_dic:
        date_start_str = date_2_str(date_start)
        logger.debug('%s %s %s 成分股 已经存在 直接返回', date_start_str, index_name, index_code)
        sec_df = date_constituent_df_dict[date_start]
        constituent_set = idx_constituent_set_dic[idx_start]
    else:
        sec_df = get_sectorconstituent(index_code, index_name, date_start)
        if sec_df is None or sec_df.shape[0] == 0:
            date_start_str = date_2_str(date_start)
            logger.warning('%s 无法获取到 %s %s 成分股', date_start_str, index_name, index_code)
            # raise ValueError('%s 无法获取到 %s %s 成分股' % (date_start_str, index_name, index_code))
            return None, None
        date_constituent_df_dict[date_start] = sec_df
        # constituent_set = set(sec_df['wind_code'])
        constituent_set = {tuple(val) for key, val in sec_df[['wind_code', 'weight']].T.items()}
        idx_constituent_set_dic[idx_start] = constituent_set
    return sec_df, constituent_set


def recursion_dichotomy_get_data(idx_start, idx_end, trade_date_list_sorted, date_constituent_df_dict,
                                 idx_constituent_set_dic, left_or_right, index_code, index_name):
    """
    递归调用，通过 idx_start, idx_end 之间的坐标获取成分股
    比较该成分与 idx_start 及 idx_end 的成分股师傅相同，如果相同则退出，不同则采用二分法进行
    :param idx_start:
    :param idx_end:
    :param trade_date_list_sorted:
    :param date_constituent_df_dict:
    :param idx_constituent_set_dic:
    :param left_or_right: -1 获取 idx_start 成分股数据；1 获取 idx_end 成分股数据
    :param index_code:
    :param index_name:
    :return:
    """
    if left_or_right == -1:
        date_start = trade_date_list_sorted[idx_start]
        # 获取起始点的成分股
        _, constituent_set_left = get_index_constituent_2_dic(index_code, index_name, date_start, idx_start,
                                                              date_constituent_df_dict, idx_constituent_set_dic)
    else:
        constituent_set_left = idx_constituent_set_dic[idx_start]

    if left_or_right == 1:
        date_to = trade_date_list_sorted[idx_end]
        # 获取截止点的成分股
        _, constituent_set_right = get_index_constituent_2_dic(index_code, index_name, date_to, idx_end,
                                                               date_constituent_df_dict, idx_constituent_set_dic)
    else:
        constituent_set_right = idx_constituent_set_dic[idx_end]

    # 如果左右节点结果集相同则退出
    if constituent_set_left == constituent_set_right:
        return constituent_set_left, constituent_set_right
    # 获取中间节点
    idx_mid = int((idx_start + idx_end) / 2)
    # 如果没有中间节点，则退出
    if idx_mid == idx_start:
        return constituent_set_left, constituent_set_right

    left_or_right_right = 1
    _, constituent_set_mid = recursion_dichotomy_get_data(idx_start, idx_mid, trade_date_list_sorted,
                                                          date_constituent_df_dict,
                                                          idx_constituent_set_dic, left_or_right_right, index_code,
                                                          index_name)
    if constituent_set_mid == constituent_set_right:
        return constituent_set_left, constituent_set_right

    left_or_right_left = -1
    _, _ = recursion_dichotomy_get_data(idx_mid, idx_end, trade_date_list_sorted,
                                        date_constituent_df_dict,
                                        idx_constituent_set_dic, left_or_right_left, index_code,
                                        index_name)
    return constituent_set_left, constituent_set_right


def loop_get_data(idx_start, idx_end, trade_date_list_sorted, date_constituent_df_dict,
                  idx_constituent_set_dic, index_code, index_name):
    """
    循环调用，通过 idx_start, idx_end 之间的坐标获取成分股及权重
    :param idx_start:
    :param idx_end:
    :param trade_date_list_sorted:
    :param date_constituent_df_dict:
    :param idx_constituent_set_dic:
    :param index_code:
    :param index_name:
    :return:
    """
    for idx in range(idx_start, idx_end + 1):
        date_curr = trade_date_list_sorted[idx]
        _, _ = get_index_constituent_2_dic(index_code, index_name, date_curr, idx,
                                           date_constituent_df_dict, idx_constituent_set_dic)
        # 仅仅调试时使用
        # if DEBUG and idx >= 3516:
        #     break


@app.task
def import_index_constituent_all():
    param_dic_list = [
        # 股票指数
        {"index_name": '沪深300', "index_code": '000300.SH', 'date_start': '2005-04-08', 'method': 'loop'},
        {"index_name": '中证500', "index_code": '000905.SH', 'date_start': '2007-01-31', 'method': 'loop'},
        {"index_name": '上证50', "index_code": '000016.SH', 'date_start': '2009-04-01', 'method': 'loop'},
        {"index_name": '创业板指', "index_code": '399006.SZ', 'date_start': '2010-06-30', 'method': 'loop'},
        {"index_name": '中小板指', "index_code": '399005.SZ', 'date_start': '2009-09-30', 'method': 'loop'},
    ]

    for param_dic in param_dic_list:
        import_index_constituent(**param_dic)


def import_index_constituent(index_code, index_name, date_start, exch_code='SZSE', date_end=None, method='loop'):
    """
    导入 sector_code 板块的成分股
    :param index_code:默认"SZSE":"深圳"
    :param index_name:
    :param date_start:
    :param exch_code:
    :param date_end:默认为None，到最近交易日的历史数据
    :return:
    """
    table_name = 'wind_index_constituent'
    param_list = [
        ('trade_date', Date),
        ('weight', DOUBLE),
        ('stock_name', String(80)),
        ('index_code', String(20)),
        ('index_name', String(80)),
    ]
    #  sldksldDFGDFGD,Nlfkgldfngldldfngldnzncvxcvnx
    dtype = {key: val for key, val in param_list}
    dtype['wind_cod'] = String(20)
    # 根据 exch_code 获取交易日列表
    trade_date_list_sorted = get_trade_date_list_sorted(exch_code)
    if trade_date_list_sorted is None or len(trade_date_list_sorted) == 0:
        raise ValueError("没有交易日数据")
    trade_date_list_count = len(trade_date_list_sorted)
    # 格式化 日期字段
    date_start = str_2_date(date_start)
    if date_end is not None:
        date_end = str_2_date(date_end)
        idx_end = get_first_idx(trade_date_list_sorted, lambda x: x >= date_end)
        if idx_end is not None:
            trade_date_list_sorted = trade_date_list_sorted[:(idx_end + 1)]

    date_constituent_df_dict = OrderedDict()
    idx_constituent_set_dic = {}
    # 从数据库中获取最近一个交易日的成分股列表，如果为空，则代表新导入数据 date, constituent_df
    date_latest, constituent_df = get_latest_constituent_df(index_code)
    # date_constituent_df_dict[date] = constituent_df
    date_latest = str_2_date(date_latest)
    if date_latest is None or date_latest < date_start:
        idx_start = get_last_idx(trade_date_list_sorted, lambda x: x <= date_start)
        sec_df, _ = get_index_constituent_2_dic(index_code, index_name, date_start, idx_start,
                                                date_constituent_df_dict, idx_constituent_set_dic)
        if sec_df is None or sec_df.shape[0] == 0:
            return
        # 保存板块数据
        # sec_df.to_sql(table_name, engine_md, if_exists='append', index=False)
        bunch_insert_on_duplicate_update(sec_df, table_name, engine_md, dtype=dtype)
    else:
        date_start = date_latest
        idx_start = get_last_idx(trade_date_list_sorted, lambda x: x <= date_start)
        date_constituent_df_dict[date_latest] = constituent_df
        idx_constituent_set_dic[idx_start] = set(constituent_df['wind_code'])

    # 设定日期字段
    # idx_end = idx_start + span if idx_start + span < trade_date_list_count - 1 else trade_date_list_count -1
    yesterday = date.today() - timedelta(days=1)
    idx_end = get_last_idx(trade_date_list_sorted, lambda x: x <= yesterday)
    if idx_start >= idx_end:
        return

    if method == 'loop':
        try:
            idx_end = idx_start + 10  # 调试使用
            loop_get_data(idx_start + 1, idx_end, trade_date_list_sorted, date_constituent_df_dict,
                          idx_constituent_set_dic, index_code, index_name)
        except APIError:
            logger.exception('loop_get_data (idx_start=%d, idx_end=%d, index_code=%s, index_name=%s)'
                             , idx_start, idx_end, index_code, index_name)
    elif method == 'recursion':
        left_or_right = 1
        recursion_dichotomy_get_data(idx_start, idx_end, trade_date_list_sorted, date_constituent_df_dict,
                                     idx_constituent_set_dic, left_or_right, index_code, index_name)
    else:
        raise ValueError('method = %s error' % method)

    # 剔除 date_start 点的数据，该日期数据以及纳入数据库
    del date_constituent_df_dict[date_start]
    # 其他数据导入数据库
    for num, (date_cur, sec_df) in enumerate(date_constituent_df_dict.items(), start=1):
        # sec_df.to_sql(table_name, engine_md, if_exists='append', index=False)
        bunch_insert_on_duplicate_update(sec_df, table_name, engine_md, dtype=dtype)
        logger.info("%d) %s %d 条 %s 成分股数据导入数据库", num, date_cur, sec_df.shape[0], index_name)
        # 仅仅调试时使用
        # if DEBUG and num >= 4:
        #     break


if __name__ == "__main__":
    sector_name = 'HSI恒生综合指数成分'
    sector_code = 'a003090201000000'
    date_str = '2018-02-23'
    DEBUG = True
    import_index_constituent(sector_code, sector_name, date_str)
    import_index_constituent_all()
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s [%(name)s:%(funcName)s] %(message)s')
    logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(logging.WARNING)
    logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)
