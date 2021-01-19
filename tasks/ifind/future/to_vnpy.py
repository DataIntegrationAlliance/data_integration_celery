"""
@author  : MG
@Time    : 2021/1/19 14:27
@File    : to_vnpy.py
@contact : mmmaaaggg@163.com
@desc    : 用于
"""

import logging
from datetime import date, timedelta

import pandas as pd
from ibats_utils.db import with_db_session
from ibats_utils.mess import date_2_str
from ibats_utils.mess import datetime_2_str

from tasks import app
from tasks.backend import engine_md
from tasks.ifind import IFIND_VNPY_EXCHANGE_DIC

logger = logging.getLogger()


def get_code_list_by_types(instrument_types: list, all_if_none=True,
                           lasttrade_date_lager_than_n_days_before=30) -> list:
    """
    输入 instrument_type 列表，返回对应的所有合约列表
    :param instrument_types: 可以使 instrument_type 列表 也可以是 （instrument_type，exchange）列表
    :param all_if_none 如果 instrument_types 为 None 则返回全部合约代码
    :param lasttrade_date_lager_than_n_days_before 仅返回最后一个交易日 大于 N 日前日期的合约
    :return: code_list
    """
    code_list = []
    if all_if_none and instrument_types is None:
        sql_str = f"select ths_code from ifind_future_info"
        with with_db_session(engine_md) as session:
            if lasttrade_date_lager_than_n_days_before is not None:
                date_from_str = date_2_str(date.today() - timedelta(days=lasttrade_date_lager_than_n_days_before))
                sql_str += " where ths_last_td_date_future > :last_trade_date"
                table = session.execute(sql_str, params={"last_trade_date": date_from_str})
            else:
                table = session.execute(sql_str)

            # 获取date_from,date_to，将date_from,date_to做为value值
            for row in table.fetchall():
                wind_code = row[0]
                code_list.append(wind_code)
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

            sql_str = f"select ths_code from ifind_future_info where ths_code " \
                      f"REGEXP 'rb[:digit:]+.{'[:alpha:]+' if exchange is None else exchange}'"
            with with_db_session(engine_md) as session:
                if lasttrade_date_lager_than_n_days_before is not None:
                    date_from_str = date_2_str(date.today() - timedelta(days=lasttrade_date_lager_than_n_days_before))
                    sql_str += " and ths_last_td_date_future > :last_trade_date"
                    table = session.execute(sql_str, params={"last_trade_date": date_from_str})
                else:
                    table = session.execute(sql_str)

                # 获取date_from,date_to，将date_from,date_to做为value值
                for row in table.fetchall():
                    wind_code = row[0]
                    code_list.append(wind_code)

    return code_list


@app.task
def min_to_vnpy_increment(chain_param=None, instrument_types=None):
    from tasks.config import config
    from tasks.backend import engine_dic
    original_table_name = 'ifind_future_min'
    table_name = 'dbbardata'
    interval = '1m'
    engine_vnpy = engine_dic[config.DB_SCHEMA_VNPY]
    has_table = engine_vnpy.has_table(table_name)
    if not has_table:
        logger.error('当前数据库 %s 没有 %s 表，建议使用 vnpy先建立相应的数据库表后再进行导入操作', engine_vnpy, table_name)
        return

    sql_increment_str = f"select trade_datetime `datetime`, `open` open_price, high high_price, " \
                        f"`low` low_price, `close` close_price, volume, openinterest as open_interest " \
                        f"from {original_table_name} where ths_code = %s and " \
                        f"trade_datetime > %s and `close` is not null and `close` <> 0"
    sql_whole_str = f"select trade_datetime `datetime`, `open` open_price, high high_price, " \
                    f"`low` low_price, `close` close_price, volume, openinterest as open_interest " \
                    f"from {original_table_name} where ths_code = %s and " \
                    f"`close` is not null and `close` <> 0"
    code_list = get_code_list_by_types(instrument_types)
    code_count = len(code_list)
    for n, code in enumerate(code_list, start=1):
        symbol, exchange = code.split('.')
        if exchange in IFIND_VNPY_EXCHANGE_DIC:
            exchange_vnpy = IFIND_VNPY_EXCHANGE_DIC[exchange]
        else:
            logger.warning('%s exchange: %s 在交易所列表中不存在', code, exchange)
            exchange_vnpy = exchange

        sql_str = f"select max(`datetime`) from {table_name} where symbol=:symbol and `interval`='{interval}'"
        with with_db_session(engine_vnpy) as session:
            datetime_exist = session.scalar(sql_str, params={'symbol': symbol})
        if datetime_exist is not None:
            # 读取日线数据
            df = pd.read_sql(sql_increment_str, engine_md, params=[code, datetime_exist]).dropna()
        else:
            df = pd.read_sql(sql_whole_str, engine_md, params=[code]).dropna()

        df_len = df.shape[0]
        if df_len == 0:
            continue

        df['symbol'] = symbol
        df['exchange'] = exchange_vnpy
        df['interval'] = interval
        datetime_latest = df['datetime'].max().to_pydatetime()
        df.to_sql(table_name, engine_vnpy, if_exists='append', index=False)
        logger.info("%d/%d) %s (%s ~ %s] %d data -> %s interval %s",
                    n, code_count, symbol,
                    datetime_2_str(datetime_exist), datetime_2_str(datetime_latest),
                    df_len, table_name, interval)


def run_task():
    min_to_vnpy_increment()


if __name__ == "__main__":
    run_task()
