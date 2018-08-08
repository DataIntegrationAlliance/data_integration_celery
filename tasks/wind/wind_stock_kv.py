#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/6/4 16:58
@File    : wind_stock_kv.py
@contact : mmmaaaggg@163.com
@desc    : 加载wind 一致预期EPS数据
"""
import logging
import math
import pandas as pd
from datetime import datetime, date, timedelta
from datetime import date, datetime, timedelta
import pandas as pd
import numpy as np
from config_fh import get_db_engine, get_db_session, STR_FORMAT_DATE, UN_AVAILABLE_DATE, WIND_REST_URL
from fh_tools.windy_utils_rest import WindRest, APIError
from fh_tools.fh_utils import get_last, get_first, date_2_str, str_2_date
import logging
from sqlalchemy.types import String, Date, Float, Integer
from sqlalchemy.dialects.mysql import DOUBLE

logger = logging.getLogger()
DATE_BASE = datetime.strptime('2006-02-28', STR_FORMAT_DATE).date()
ONE_DAY = timedelta(days=1)
# 标示每天几点以后下载当日行情数据
BASE_LINE_HOUR = 16

w = WindRest(WIND_REST_URL)


#


def get_wind_kv_per_year(wind_code, wind_indictor_str, date_from, date_to, params):
    """
    \
    :param wind_code:
    :param wind_indictor_str:
    :param date_from:
    :param date_to:
    :param params: "year=%(year)d;westPeriod=180"== > "year=2018;westPeriod=180"
    :return:
    """
    date_from, date_to = str_2_date(date_from), str_2_date(date_to)
    # 以年底为分界线，将日期范围截取成以自然年为分段的日期范围
    date_pair = []
    if date_from <= date_to:
        date_curr = date_from
        while True:
            date_new_year = str_2_date("%d-01-01" % (date_curr.year + 1))
            date_year_end = date_new_year - timedelta(days=1)
            if date_to < date_year_end:
                date_pair.append((date_curr, date_to))
                break
            else:
                date_pair.append((date_curr, date_year_end))
            date_curr = date_new_year
    data_df_list = []
    for date_from_sub, date_to_sub in date_pair:
        params_sub = params % {'year': (date_from_sub.year + 1)}
        try:
            data_df = w.wsd(wind_code, wind_indictor_str, date_from_sub, date_to_sub, params_sub)
        except APIError as exp:
            logger.exception(
                "%s %s [%s ~ %s] %s 执行异常",
                wind_code, wind_indictor_str, date_2_str(date_from_sub), date_2_str(date_to_sub), params_sub)
            if exp.ret_dic.setdefault('error_code', 0) in (
                    -40520007,  # 没有可用数据
                    -40521009,  # 数据解码失败。检查输入参数是否正确，如：日期参数注意大小月月末及短二月
            ):
                continue
            else:
                raise exp
        if data_df is None:
            logger.warning('%s %s [%s ~ %s] has no data',
                           wind_code, wind_indictor_str, date_2_str(date_from_sub), date_2_str(date_to_sub))
            continue
        data_df.dropna(inplace=True)
        if data_df.shape[0] == 0:
            # logger.warning('%s %s [%s ~ %s] has 0 data',
            #                wind_code, wind_indictor_str, date_2_str(date_from_sub), date_2_str(date_to_sub))
            continue
        data_df_list.append(data_df)

    # 合并数据
    data_df_tot = pd.concat(data_df_list) if len(data_df_list) > 0 else None
    return data_df_tot


def import_wind_code_kv(keys: list, wind_code_list=None):
    """
    插入股票相关市场数据。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    w.wsd("600000.SH", "west_eps", "2018-05-05", "2018-06-03", "year=2018;westPeriod=180")
    :param keys:
    :param wind_code_list:
    :return:
    """
    wind_code_set = set(wind_code_list) if wind_code_list is not None else None
    logging.info("更新 wind_stock_daily 开始")
    engine = get_db_engine()
    # 本次获取数据的截止日期
    date_ending = date.today() - ONE_DAY if datetime.now().hour < BASE_LINE_HOUR else date.today()

    for item_key in keys:
        item_key = item_key.lower()
        # 获取每只股票对应key 的最近一个交易日数据 start_date
        # 如果没有，则使用 DATE_BASE ipo_date 中最大的一个
        sql_str = """SELECT info.wind_code, ifnull(trade_date_max_1,start_date) start_date, delist_date FROM 
        (
        SELECT wind_code, if (ipo_date<%s,date(%s),ipo_date) start_date, delist_date FROM wind_stock_info
        ) info
        LEFT JOIN
        (
        SELECT wind_code, adddate(max(Trade_date),1) trade_date_max_1 FROM wind_code_kv 
        WHERE wind_code_kv.key=%s GROUP BY wind_code
        ) kv
        ON info.wind_code = kv.wind_code"""
        date_df = pd.read_sql(sql_str, engine, params=[DATE_BASE, DATE_BASE, item_key], index_col='wind_code')

        with get_db_session(engine) as session:
            # 获取市场有效交易日数据
            sql_str = "SELECT trade_date FROM wind_trade_date WHERE trade_date >= :trade_date"
            table = session.execute(sql_str, params={'trade_date': DATE_BASE})
            trade_date_sorted_list = [t[0] for t in table.fetchall()]
            trade_date_sorted_list.sort()

        data_df_list = []
        data_len = date_df.shape[0]
        logger.info('%d stocks will been import into wind_code_kv', data_len)
        try:
            for data_num, (wind_code, date_s) in enumerate(date_df.T.items(), start=1):
                if wind_code_set is not None and wind_code not in wind_code_set:
                    continue
                # 获取 date_from
                date_from, date_delist = date_s['start_date'], date_s['delist_date']
                # 获取 date_to
                if date_delist is None:
                    date_to = date_ending
                else:
                    date_to = min([date_delist, date_ending])
                date_to = get_last(trade_date_sorted_list, lambda x: x <= date_to)
                if date_from is None or date_to is None or date_from > date_to:
                    continue
                # 获取股票量价等行情数据
                wind_indictor_str = item_key
                data_df = get_wind_kv_per_year(wind_code, wind_indictor_str, date_from, date_to,
                                               "year=%(year)d;westPeriod=180")
                # try:
                #     data_df = w.wsd(wind_code, wind_indictor_str, date_from, date_to, "year=2018;westPeriod=180")
                # except APIError as exp:
                #     logger.exception("%d/%d) %s 执行异常", data_num, data_len, wind_code)
                #     if exp.ret_dic.setdefault('error_code', 0) in (
                #             -40520007,  # 没有可用数据
                #             -40521009,  # 数据解码失败。检查输入参数是否正确，如：日期参数注意大小月月末及短二月
                #     ):
                #         continue
                #     else:
                #         break
                if data_df is None:
                    logger.warning('%d/%d) %s has no data during %s %s', data_num, data_len, wind_code, date_from,
                                   date_to)
                    continue
                data_df.dropna(inplace=True)
                if data_df.shape[0] == 0:
                    logger.warning('%d/%d) %s has 0 data during %s %s', data_num, data_len, wind_code, date_from,
                                   date_to)
                    continue
                logger.info('%d/%d) %d data of %s between %s and %s', data_num, data_len, data_df.shape[0], wind_code,
                            date_from, date_to)
                data_df['wind_code'] = wind_code
                data_df_list.append(data_df)
                # 调试使用
                # if data_num >= 5:
                #     break
        finally:
            # 导入数据库
            if len(data_df_list) > 0:
                data_df_all = pd.concat(data_df_list)
                data_df_all.index.rename('trade_date', inplace=True)
                data_df_all.reset_index(inplace=True)
                data_df_all.rename(columns={item_key.upper(): 'value'}, inplace=True)
                data_df_all['key'] = item_key
                data_df_all.set_index(['wind_code', 'trade_date', 'key'], inplace=True)
                data_df_all.to_sql('wind_code_kv', engine, if_exists='append',
                                   dtype={
                                       'wind_code': String(20),
                                       'trade_date': Date,
                                       'key': String(45),
                                       'value': DOUBLE,
                                   }
                                   )
                logging.info("更新 wind_stock_daily 结束 %d 条信息被更新", data_df_all.shape[0])


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s [%(name)s:%(funcName)s] %(message)s')
    # 全部地产股股票
    wind_code_list = [
        "000002.SZ", "000006.SZ", "000007.SZ", "000011.SZ", "000014.SZ", "000029.SZ", "000031.SZ", "000036.SZ",
        "000042.SZ", "000043.SZ", "000046.SZ", "000056.SZ", "000058.SZ", "000062.SZ", "000069.SZ", "000402.SZ",
        "000502.SZ", "000505.SZ", "000506.SZ", "000514.SZ", "000517.SZ", "000526.SZ", "000534.SZ", "000537.SZ",
        "000540.SZ", "000558.SZ", "000567.SZ", "000573.SZ", "000608.SZ", "000609.SZ", "000615.SZ", "000616.SZ",
        "000620.SZ", "000631.SZ", "000638.SZ", "000656.SZ", "000667.SZ", "000668.SZ", "000671.SZ", "000691.SZ",
        "000718.SZ", "000732.SZ", "000736.SZ", "000797.SZ", "000809.SZ", "000838.SZ", "000863.SZ", "000897.SZ",
        "000918.SZ", "000926.SZ", "000961.SZ", "000965.SZ", "000979.SZ", "000981.SZ", "001979.SZ", "002016.SZ",
        "002077.SZ", "002113.SZ", "002133.SZ", "002146.SZ", "002147.SZ", "002208.SZ", "002244.SZ", "002285.SZ",
        "002305.SZ", "300492.SZ", "300675.SZ", "300732.SZ", "600007.SH", "600048.SH", "600052.SH", "600064.SH",
        "600067.SH", "600077.SH", "600094.SH", "600095.SH", "600113.SH", "600133.SH", "600158.SH", "600159.SH",
        "600162.SH", "600173.SH", "600177.SH", "600185.SH", "600208.SH", "600215.SH", "600223.SH", "600225.SH",
        "600239.SH", "600240.SH", "600246.SH", "600266.SH", "600322.SH", "600325.SH", "600340.SH", "600376.SH",
        "600383.SH", "600393.SH", "600463.SH", "600466.SH", "600503.SH", "600515.SH", "600533.SH", "600555.SH",
        "600556.SH", "600565.SH", "600568.SH", "600576.SH", "600603.SH", "600604.SH", "600606.SH", "600620.SH",
        "600621.SH", "600622.SH", "600638.SH", "600639.SH", "600641.SH", "600647.SH", "600649.SH", "600657.SH",
        "600658.SH", "600663.SH", "600665.SH", "600675.SH", "600683.SH", "600684.SH", "600696.SH", "600708.SH",
        "600716.SH", "600724.SH", "600732.SH", "600733.SH", "600736.SH", "600743.SH", "600748.SH", "600773.SH",
        "600791.SH", "600807.SH", "600817.SH", "600823.SH", "600848.SH", "600890.SH", "600895.SH", "601155.SH",
        "601588.SH", "603357.SH", "603506.SH", "603778.SH", "603955.SH", ]
    import_wind_code_kv(['west_eps'])  # , wind_code_list=wind_code_list
