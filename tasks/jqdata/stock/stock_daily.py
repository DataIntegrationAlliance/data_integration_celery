#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 19-4-8 上午9:36
@File    : check.py
@contact : mmmaaaggg@163.com
@desc    : 获取 jqdatasdk 日级别数据（前复权）
"""
import pandas as pd
import logging
from ibats_utils.mess import get_first, get_last, date_2_str
from tasks import app
from sqlalchemy.types import String, Date, SMALLINT
from sqlalchemy.dialects.mysql import DOUBLE
from tasks.backend import engine_md, bunch_insert, execute_sql_commit
from ibats_utils.db import with_db_session
from tasks.jqdata.stock.available_check import get_bak_table_name
from tasks.jqdata.stock.stock_info import TABLE_NAME as TABLE_NAME_INFO
from tasks.jqdata import get_price

DEBUG = False
logger = logging.getLogger()
# 标示每天几点以后下载当日行情数据
BASE_LINE_HOUR = 16  # 目前该参数无用，当期功能在sql语句中实现
# ['open', 'close', 'low', 'high', 'volume', 'money', 'factor', 'high_limit','low_limit', 'avg', 'pre_close', 'paused']
DTYPE_QUERY = [
    ('open', DOUBLE),
    ('high', DOUBLE),
    ('low', DOUBLE),
    ('close', DOUBLE),
    ('pre_close', DOUBLE),
    ('volume', DOUBLE),
    ('money', DOUBLE),
    ('factor', DOUBLE),
    ('high_limit', DOUBLE),
    ('low_limit', DOUBLE),
    ('avg', DOUBLE),
    ('paused', SMALLINT),
]
# 设置 dtype
DTYPE = {key: val for key, val in DTYPE_QUERY}
DTYPE['jq_code'] = String(20)
DTYPE['trade_date'] = Date
TABLE_NAME = 'jq_stock_daily_md_pre'


def invoke_daily(key_code, start_date, end_date):
    """
    调用接口获取行情数据
    一个单位时间内的股票的数据

    SecurityUnitData 基本属性
    以下属性也能通过[history]/[attribute_history]/[get_price]获取到

    open: 时间段开始时价格
    close: 时间段结束时价格
    low: 时间段中的最低价
    high: 时间段中的最高价
    volume: 时间段中的成交的股票数量
    money: 时间段中的成交的金额
    factor: 前复权因子, 我们提供的价格都是前复权后的, 但是利用这个值可以算出原始价格, 方法是价格除以factor, 比如:
    close/factor
    high_limit: 时间段中的涨停价
    low_limit: 时间段中的跌停价
    avg: 这段时间的平均价。计算方法：股票是成交额处以成交量；期货是直接从CTP行情获取的，计算方法为成交额除以成交量再除以合约乘数。
    price: 已经过时, 为了向前兼容, 等同于 avg
    pre_close: 前一个单位时间结束时的价格, 按天则是前一天的收盘价, 按分钟则是前一分钟的结束价格
    paused: bool值, 这只股票是否停牌, 停牌时open/close/low/high/pre_close依然有值,都等于停牌前的收盘价, volume=money=0
    额外的属性和方法
    security: 股票代码, 比如'000001.XSHE'
    returns: 股票在这个单位时间的相对收益比例, 等于
    (close-pre_close)/pre_close
    isnan(): 数据是否有效, 当股票未上市或者退市时, 无数据, isnan()返回True
    mavg(days, field='close')
    : 过去days天的每天收盘价的平均值, 把field设成'avg'(等同于已过时的'price')则为每天均价的平均价, 下同
    vwap(days)
    : 过去days天的每天均价的加权平均值, 以days=2为例子, 算法是:
    (avg1 * volume1 + avg2 * volume2) / (volume1 + volume2)
    stddev(days)
    : 过去days天的每天收盘价的标准差
    注：mavg/vwap/stddev:都会跳过停牌日期, 如果历史交易天数不足, 则返回nan

    :param key_code:
    :param start_date:
    :param end_date:
    :return:
    """
    fields = [_[0] for _ in DTYPE_QUERY]
    df = get_price(key_code, start_date=start_date, end_date=end_date, frequency='daily', fields=fields, fq='pre')

    if df is not None and df.shape[0] > 0:
        df['jq_code'] = key_code
        df.index.rename('trade_date', inplace=True)
        df.reset_index(drop=False, inplace=True)
    return df


@app.task
def import_jq_stock_daily(chain_param=None, code_set=None):
    """
    插入股票日线数据到最近一个工作日-1。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    :return:
    """
    table_name_info = TABLE_NAME_INFO
    table_name = TABLE_NAME
    table_name_bak = get_bak_table_name(table_name)
    logging.info("更新 %s 开始", table_name)

    # 根据 info table 查询每只股票日期区间
    sql_info_str = f"""
        SELECT jq_code, date_frm, if(date_to<end_date, date_to, end_date) date_to
        FROM
          (
            SELECT info.jq_code, start_date date_frm, end_date date_to,
            if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
            FROM {table_name_info} info
          ) tt
        WHERE date_frm <= if(date_to<end_date, date_to, end_date)
        ORDER BY jq_code"""

    has_table = engine_md.has_table(table_name)
    has_bak_table = engine_md.has_table(table_name_bak)
    # 进行表格判断，确定是否含有 jq_stock_daily_md
    if has_table:
        # 这里对原始的 sql语句进行了调整
        # 以前的逻辑：每只股票最大的一个交易日+1天作为起始日期
        # 现在的逻辑：每只股票最大一天的交易日作为起始日期
        # 主要原因在希望通过接口获取到数据库中现有最大交易日对应的 factor因子以进行比对
        sql_trade_date_range_str = f"""
            SELECT jq_code, date_frm, if(date_to<end_date, date_to, end_date) date_to
            FROM
            (
                SELECT info.jq_code, ifnull(trade_date, info.start_date) date_frm, info.end_date date_to,
                if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
                FROM 
                    {table_name_info} info 
                LEFT OUTER JOIN
                    (SELECT jq_code, max(trade_date) trade_date FROM {table_name} GROUP BY jq_code) daily
                ON info.jq_code = daily.jq_code
            ) tt
            WHERE date_frm < if(date_to<end_date, date_to, end_date) 
            ORDER BY jq_code"""

    else:
        sql_trade_date_range_str = sql_info_str
        logger.warning('%s 不存在，仅使用 %s 表进行计算日期范围', table_name, table_name_info)

    sql_trade_date_str = """SELECT trade_date FROM jq_trade_date trddate 
       WHERE trade_date <= if(hour(now())<16, subdate(curdate(),1), curdate()) ORDER BY trade_date"""

    with with_db_session(engine_md) as session:
        # 获取截至当期全部交易日前
        table = session.execute(sql_trade_date_str)
        trade_date_list = [row[0] for row in table.fetchall()]
        trade_date_list.sort()
        # 获取每只股票日线数据的日期区间
        table = session.execute(sql_trade_date_range_str)
        # 计算每只股票需要获取日线数据的日期区间
        # 获取date_from,date_to，将date_from,date_to做为value值
        code_date_range_dic = {
            key_code: (date_from, date_to)
            for key_code, date_from, date_to in table.fetchall() if
            code_set is None or key_code in code_set}

        # 从 info 表中查询全部日期区间
        if sql_info_str == sql_trade_date_range_str:
            code_date_range_from_info_dic = code_date_range_dic
        else:
            # 获取每只股票日线数据的日期区间
            table = session.execute(sql_info_str)
            # 计算每只股票需要获取日线数据的日期区间
            # 获取date_from,date_to，将date_from,date_to做为value值
            code_date_range_from_info_dic = {
                key_code: (date_from, date_to)
                for key_code, date_from, date_to in table.fetchall() if
                code_set is None or key_code in code_set}

    # data_len = len(code_date_range_dic)
    data_df_list, data_count, all_data_count, data_len = [], 0, 0, len(code_date_range_dic)
    logger.info('%d stocks will been import into %s', data_len, table_name)
    # 将data_df数据，添加到data_df_list

    try:
        for num, (key_code, (date_from_tmp, date_to_tmp)) in enumerate(code_date_range_dic.items(), start=1):
            data_df = None
            try:
                for loop_count in range(2):
                    # 根据交易日数据取交集，避免不用的请求耽误时间
                    date_from = get_first(trade_date_list, lambda x: x >= date_from_tmp)
                    date_to = get_last(trade_date_list, lambda x: x <= date_to_tmp)
                    if date_from is None or date_to is None or date_from >= date_to:
                        logger.debug('%d/%d) %s [%s - %s] 跳过', num, data_len, key_code, date_from, date_to)
                        break
                    logger.debug('%d/%d) %s [%s - %s] %s',
                                 num, data_len, key_code, date_from, date_to, '第二次查询' if loop_count > 0 else '')
                    data_df = invoke_daily(key_code=key_code, start_date=date_2_str(date_from),
                                           end_date=date_2_str(date_to))

                    # 该判断只在第一次循环时执行
                    if loop_count == 0 and has_table:
                        # 进行 factor 因子判断，如果发现最小的一个交易日的因子不为1,则删除数据库中该股票的全部历史数据，然后重新下载。
                        # 因为当期股票下载的数据为前复权价格，如果股票出现复权调整，则历史数据全部需要重新下载
                        factor_value = data_df.sort_values('trade_date').iloc[0, :]['factor']
                        if factor_value != 1 and (
                                code_date_range_from_info_dic[key_code][0] != code_date_range_dic[key_code][0]):
                            # 删除该股屏历史数据
                            sql_str = f"delete from {table_name} where jq_code=:jq_code"
                            row_count = execute_sql_commit(sql_str, params={'jq_code': key_code})
                            date_from_tmp, date_to_tmp = code_date_range_from_info_dic[key_code]
                            if has_bak_table:
                                sql_str = f"delete from {table_name_bak} where jq_code=:jq_code"
                                row_count = execute_sql_commit(sql_str, params={'jq_code': key_code})
                                date_from_tmp, date_to_tmp = code_date_range_from_info_dic[key_code]
                                logger.info('%d/%d) %s %d 条历史记录被清除，重新加载前复权历史数据 [%s - %s] 同时清除bak表中相应记录',
                                            num, data_len, key_code, row_count, date_from_tmp, date_to_tmp)
                            else:
                                logger.info('%d/%d) %s %d 条历史记录被清除，重新加载前复权历史数据 [%s - %s]',
                                            num, data_len, key_code, row_count, date_from_tmp, date_to_tmp)

                            # 重新设置起止日期，进行第二次循环
                            continue

                    # 退出  for _ in range(2): 循环
                    break
            except Exception as exp:
                data_df = None
                logger.exception('%s [%s - %s]', key_code, date_2_str(date_from_tmp), date_2_str(date_to_tmp))
                if exp.args[0].find('超过了每日最大查询限制'):
                    break

            # 把数据攒起来
            if data_df is not None and data_df.shape[0] > 0:
                data_count += data_df.shape[0]
                data_df_list.append(data_df)

            # 大于阀值有开始插入
            if data_count >= 500:
                data_df_all = pd.concat(data_df_list)
                bunch_insert(data_df_all, table_name, dtype=DTYPE, primary_keys=['jq_code', 'trade_date'])
                all_data_count += data_count
                data_df_list, data_count = [], 0

            if DEBUG and num >= 2:
                break

    finally:
        # 导入数据库
        if len(data_df_list) > 0:
            data_df_all = pd.concat(data_df_list)
            data_count = bunch_insert(data_df_all, table_name, dtype=DTYPE, primary_keys=['jq_code', 'trade_date'])
            all_data_count = all_data_count + data_count
            logging.info("更新 %s 结束 %d 条信息被更新", table_name, all_data_count)


if __name__ == "__main__":

    import_jq_stock_daily(code_set=None)
