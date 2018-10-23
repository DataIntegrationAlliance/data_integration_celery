#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/2/24 11:01
@File    : sectorconstituent.py
@contact : mmmaaaggg@163.com
@desc    : 导入板块成分股
"""

import logging
import pandas as pd
from tasks import app
from tasks.backend import engine_md
from datetime import date, datetime, timedelta
from tasks.utils.db_utils import with_db_session
from tasks.utils.fh_utils import STR_FORMAT_DATE, date_2_str, str_2_date, get_last_idx, get_first, get_last
from tasks.wind import invoker
from sqlalchemy.types import String, Date, Float, Integer
from sqlalchemy.dialects.mysql import DOUBLE

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
    sql_str = "SELECT trade_date FROM wind_trade_date WHERE exch_code = :exch_code ORDER BY trade_date"
    with with_db_session(engine_md) as session:
        trade_date_list_sorted = [content[0] for content in
                                  session.execute(sql_str, params={'exch_code': exch_code}).fetchall()]
    return trade_date_list_sorted


def get_latest_constituent_df(sector_code):
    """
    获取最新的交易日日期，及相应的成分股数据
    :return:
    """
    table_name = "wind_sectorconstituent"
    has_table = engine_md.has_table(table_name)
    if not has_table:
        return None, None

    sql_str = 'SELECT max(trade_date) FROM wind_sectorconstituent WHERE sector_code = :sector_code'

    with with_db_session(engine_md) as session:
        date_latest = session.execute(sql_str, params={"sector_code": sector_code}).scalar()
        if date_latest is None:
            return None, None

    sql_str = "SELECT * FROM wind_sectorconstituent WHERE sector_code = %s AND trade_date = %s"
    sec_df = pd.read_sql(sql_str, engine_md, params=[sector_code, date_latest])
    return date_latest, sec_df


def get_sectorconstituent(sector_code, sector_name, target_date) -> pd.DataFrame:
    """
    通过 wind 获取板块成分股
    :param sector_code:
    :param sector_name:
    :param target_date:
    :return:
    """
    target_date_str = date_2_str(target_date)
    logger.info('获取 %s %s %s 板块信息', sector_code, sector_name, target_date)
    sec_df = invoker.wset("sectorconstituent", "date=%s;sectorid=%s" % (target_date_str, sector_code))
    sec_df["sector_code"] = sector_code
    sec_df["sector_name"] = sector_name
    sec_df.rename(columns={
        'date': 'trade_date',
        'sec_name': 'stock_name',
    }, inplace=True)
    return sec_df


def get_sectorconstituent_2_dic(sector_code, sector_name, date_start, idx_start, trade_date_list_sorted,
                                date_constituent_df_dict, idx_constituent_set_dic):
    """
    通过 wind 获取板块成分股 存入 date_constituent_df_dict idx_constituent_set_dic
    :param sector_code:
    :param sector_name:
    :param date_start:
    :param idx_start:
    :param trade_date_list_sorted:
    :param date_constituent_df_dict:
    :param idx_constituent_set_dic:
    :return:
    """
    if date_start in date_constituent_df_dict and idx_start in idx_constituent_set_dic:
        date_start_str = date_2_str(date_start)
        logger.debug('%s %s %s 成分股 已经存在 直接返回', date_start_str, sector_name, sector_code)
        sec_df = date_constituent_df_dict[date_start]
        constituent_set_left = idx_constituent_set_dic[idx_start]
    else:
        sec_df = get_sectorconstituent(sector_code, sector_name, date_start)
        if sec_df is None or sec_df.shape[0] == 0:
            date_start_str = date_2_str(date_start)
            logger.warning('%s 无法获取到 %s %s 成分股', date_start_str, sector_name, sector_code)
            raise ValueError('%s 无法获取到 %s %s 成分股' % (date_start_str, sector_name, sector_code))
        date_constituent_df_dict[date_start] = sec_df
        constituent_set_left = set(sec_df['wind_code'])
        idx_constituent_set_dic[idx_start] = constituent_set_left
    return sec_df, constituent_set_left


def recursion_get_sectorconstituent(idx_start, idx_end, trade_date_list_sorted, date_constituent_df_dict,
                                    idx_constituent_set_dic, left_or_right, sector_code, sector_name):
    """
    递归调用，通过 idx_start, idx_end 之间的坐标获取成分股
    比较该成分与 idx_start 及 idx_end 的成分股师傅相同，如果相同则退出，不同则采用二分法进行
    :param idx_start:
    :param idx_end:
    :param trade_date_list_sorted:
    :param date_constituent_df_dict:
    :param idx_constituent_set_dic:
    :param left_or_right: -1 获取 idx_start 成分股数据；1 获取 idx_end 成分股数据
    :param sector_code:
    :param sector_name:
    :return:
    """
    if left_or_right == -1:
        date_start = trade_date_list_sorted[idx_start]
        # 获取起始点的成分股
        _, constituent_set_left = get_sectorconstituent_2_dic(sector_code, sector_name, date_start, idx_start,
                                                              trade_date_list_sorted, date_constituent_df_dict,
                                                              idx_constituent_set_dic)
    else:
        constituent_set_left = idx_constituent_set_dic[idx_start]

    if left_or_right == 1:
        date_to = trade_date_list_sorted[idx_end]
        # 获取截止点的成分股
        _, constituent_set_right = get_sectorconstituent_2_dic(sector_code, sector_name, date_to, idx_end,
                                                               trade_date_list_sorted, date_constituent_df_dict,
                                                               idx_constituent_set_dic)
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
    _, constituent_set_mid = recursion_get_sectorconstituent(idx_start, idx_mid, trade_date_list_sorted,
                                                             date_constituent_df_dict,
                                                             idx_constituent_set_dic, left_or_right_right, sector_code,
                                                             sector_name)
    if constituent_set_mid == constituent_set_right:
        return constituent_set_left, constituent_set_right

    left_or_right_left = -1
    _, _ = recursion_get_sectorconstituent(idx_mid, idx_end, trade_date_list_sorted,
                                           date_constituent_df_dict,
                                           idx_constituent_set_dic, left_or_right_left, sector_code,
                                           sector_name)
    return constituent_set_left, constituent_set_right


@app.task
def import_sectorconstituent_all(chain_param=None):
    """
    :param chain_param:  在celery 中將前面結果做爲參數傳給後面的任務
    :return:
    """
    param_dic_list = [
        # {"sector_name": 'SW银行', "sector_code": '1000012612000000', 'date_start': '2018-02-23'},
        # {"sector_name": 'SW非銀金融', "sector_code": '1000012613000000', 'date_start': '2018-02-23'},
        # {"sector_name": 'SW銀行(SW港股通)', "sector_code": '1000028789000000', 'date_start': '2018-02-23'},
        # {"sector_name": 'SW非銀金融(SW港股通)', "sector_code": '1000028790000000', 'date_start': '2018-02-23'},
        #
        # {"sector_name": '恒生指数成份', "sector_code": 'a002010a00000000', 'date_start': '1980-01-02', 'exch_code': 'HKEX'},
        # {"sector_name": 'HSI恒生指数成份类', "sector_code": 'a002030100000000', 'date_start': '1980-01-02',
        #  'exch_code': 'HKEX'},
        # {"sector_name": 'HSI恒生综合指数成份类', "sector_code": 'a003090200000000', 'date_start': '2002-01-02',
        #  'exch_code': 'HKEX'},
        # {"sector_name": 'HSI恒生中国企业指数成份类', "sector_code": 'a003090102000000', 'date_start': '1994-07-08',
        #  'exch_code': 'HKEX'},
        # {"sector_name": '融资融券', "sector_code": '1000011318000000', 'date_start': '2010-03-31'},
        # {"sector_name": 'CS石油石化', "sector_code": 'b101000000000000', 'date_start': '2003-01-02'},
        # {"sector_name": 'CS煤炭', "sector_code": 'b102000000000000', 'date_start': '2003-01-02'},
        # {"sector_name": 'CS电力及公用事业', "sector_code": 'b104000000000000', 'date_start': '2003-01-02'},
        # {"sector_name": 'CS钢铁', "sector_code": 'b105000000000000', 'date_start': '2003-01-02'},
        # {"sector_name": 'CS基础化工', "sector_code": 'b106000000000000', 'date_start': '2003-01-02'},
        # {"sector_name": 'CS建筑', "sector_code": 'b107000000000000', 'date_start': '2003-01-02'},
        # {"sector_name": 'CS建材', "sector_code": 'b108000000000000', 'date_start': '2003-01-02'},
        # {"sector_name": 'CS轻工制造', "sector_code": 'b109000000000000', 'date_start': '2003-01-02'},
        {"sector_name": 'CS机械', "sector_code": 'b10a000000000000', 'date_start': '2003-01-02'},
        # {"sector_name": 'CS电力设备', "sector_code": 'b10b000000000000', 'date_start': '2003-01-02'},
        # {"sector_name": 'CS国防军工', "sector_code": 'b10c000000000000', 'date_start': '2003-01-02'},
        # {"sector_name": 'CS汽车', "sector_code": 'b10d000000000000', 'date_start': '2003-01-02'},
        # {"sector_name": 'CS商贸零售', "sector_code": 'b10e000000000000', 'date_start': '2003-01-02'},
        # {"sector_name": 'CS餐饮旅游', "sector_code": 'b10f000000000000', 'date_start': '2003-01-02'},
        # {"sector_name": 'CS家电', "sector_code": 'b10g000000000000', 'date_start': '2003-01-02'},
        # {"sector_name": 'CS纺织服装', "sector_code": 'b10h000000000000', 'date_start': '2003-01-02'},
        # {"sector_name": 'CS医药', "sector_code": 'b10i000000000000', 'date_start': '2003-01-02'},
        # {"sector_name": 'CS食品饮料', "sector_code": 'b10j000000000000', 'date_start': '2003-01-02'},
        # {"sector_name": 'CS农林牧渔', "sector_code": 'b10k000000000000', 'date_start': '2003-01-02'},
        # {"sector_name": 'CS银行', "sector_code": 'b10l000000000000', 'date_start': '2003-01-02'},
        # {"sector_name": 'CS非银行金融', "sector_code": 'b10m000000000000', 'date_start': '2003-01-02'},
        # {"sector_name": 'CS房地产', "sector_code": 'b10n000000000000', 'date_start': '2003-01-02'},
        # {"sector_name": 'CS交通运输', "sector_code": 'b10o000000000000', 'date_start': '2003-01-02'},
        # {"sector_name": 'CS电子元器件', "sector_code": 'b10p000000000000', 'date_start': '2003-01-02'},
        # {"sector_name": 'CS通信', "sector_code": 'b10q000000000000', 'date_start': '2003-01-02'},
        # {"sector_name": 'CS计算机', "sector_code": 'b10r000000000000', 'date_start': '2003-01-02'},
        # {"sector_name": 'CS传媒', "sector_code": 'b10s000000000000', 'date_start': '2003-01-02'},
        # {"sector_name": 'CS综合', "sector_code": 'b10t000000000000', 'date_start': '2003-01-02'},
        # # 概念类
        # # {"sector_name": '概念:智能电网', "sector_code": '0201a90000000000', 'date_start': '2009-07-23'},
        # {"sector_name": '概念:智能电网', "sector_code": '0201a90000000000', 'date_start': '2009-07-23'},

    ]

    for param_dic in param_dic_list:
        import_sectorconstituent(**param_dic)


@app.task
def import_sectorconstituent(sector_code, sector_name, date_start, chain_param=None, exch_code='SZSE'):
    """
    导入 sector_code 板块的成分股
    :param sector_code:默认"SZSE":"深圳"
    :param sector_name:
    :param date_start:
    :param chain_param:  在celery 中將前面結果做爲參數傳給後面的任務
    :return:
    """
    # 根据 exch_code 获取交易日列表
    trade_date_list_sorted = get_trade_date_list_sorted(exch_code)
    if trade_date_list_sorted is None or len(trade_date_list_sorted) == 0:
        raise ValueError("没有交易日数据")
    trade_date_list_count = len(trade_date_list_sorted)
    # 格式化 日期字段
    date_start = str_2_date(date_start)

    date_constituent_df_dict = {}
    idx_constituent_set_dic = {}
    # 从数据库中获取最近一个交易日的成分股列表，如果为空，则代表新导入数据 date, constituent_df
    date_latest, constituent_df = get_latest_constituent_df(sector_code)
    # date_constituent_df_dict[date] = constituent_df
    date_latest = str_2_date(date_latest)
    if date_latest is None or  date_latest < date_start:
        idx_start = get_last_idx(trade_date_list_sorted, lambda x: x <= date_start)
        sec_df, _ = get_sectorconstituent_2_dic(sector_code, sector_name, date_start, idx_start, trade_date_list_sorted,
                                                date_constituent_df_dict, idx_constituent_set_dic)
        # 保存板块数据
        sec_df.to_sql("wind_sectorconstituent", engine_md, if_exists='append', index=False)
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

    left_or_right = 1
    recursion_get_sectorconstituent(idx_start, idx_end, trade_date_list_sorted, date_constituent_df_dict,
                                    idx_constituent_set_dic, left_or_right, sector_code, sector_name)

    # 剔除 date_start 点的数据，该日期数据以及纳入数据库
    del date_constituent_df_dict[date_start]
    # 其他数据导入数据库
    for num, (date_cur, sec_df) in enumerate(date_constituent_df_dict.items(), start=1):
        sec_df.to_sql("wind_sectorconstituent", engine_md, if_exists='append', index=False)
        logger.info("%d) %s %d 条 %s 成分股数据导入数据库", num, date_cur, sec_df.shape[0], sector_name)
        #仅仅调试时使用
        if DEBUG and num >= 20:
            break


if __name__ == "__main__":
    DEBUG = True
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s [%(name)s:%(funcName)s] %(message)s')
    logging.getLogger('requests.packageimport_sectorconstituent_alls.urllib3.connectionpool').setLevel(logging.WARNING)
    logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)

    # sector_name = 'HSI恒生综合指数成分'
    # sector_code = 'a003090201000000'
    # date_str = '2018-02-23'
    # import_sectorconstituent(sector_code, sector_name, date_str, None)

    import_sectorconstituent_all(chain_param=None)
