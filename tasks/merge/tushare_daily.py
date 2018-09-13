import tushare as ts
import pandas as pd
import logging
from tasks.backend.orm import build_primary_key
from datetime import date, datetime, timedelta

from tasks.tushare.tushare_stock_daily.daily_basic import DTYPE_TUSHARE_DAILY_BASIC
from tasks.tushare.tushare_stock_daily.stock import DTYPE_TUSHARE_DAILY
from tasks.tushare.tushare_stock_daily.suspend import DTYPE_TUSHARE_SUSPEND
from tasks.utils.fh_utils import try_2_date, STR_FORMAT_DATE, datetime_2_str, split_chunk
from tasks import app
from direstinvoker.utils.fh_utils import date_2_str
from sqlalchemy.types import String, Date, Integer
from sqlalchemy.dialects.mysql import DOUBLE
from tasks.backend import engine_md
from tasks.merge.code_mapping import update_from_info_table
from tasks.utils.db_utils import with_db_session, add_col_2_table, alter_table_2_myisam, \
    bunch_insert_on_duplicate_update
from tasks.utils.fh_utils import is_not_nan_or_none

def get_tushare_daily_df(table_name, date_from) -> pd.DataFrame:
    if date_from is None:
        sql_str = "select * from {table_name}".format(table_name=table_name)
        data_df = pd.read_sql(sql_str, engine_md)  # , index_col='ts_code'
    else:
        sql_str = "select * from {table_name} where trade_date >= %s".format(table_name=table_name)
        data_df = pd.read_sql(sql_str, engine_md, params=[date_from])  # , index_col='ts_code'
    return data_df


def get_tushare_daily_suspend_df(table_name, date_from) -> pd.DataFrame:
    if date_from is None:
        sql_str = "select * from {table_name}".format(table_name=table_name)
        data_df = pd.read_sql(sql_str, engine_md)
    return data_df


def get_merge(x):
    if x['close_x'] is not None:
        return x['close_x']
    elif x['close_y'] is not None:
        return x['close_y']
    else:
        return None


def merge_tushare_daily(date_from=None):
    """
    合並 adj_factor,daily_basic,stock,suspend 表格數據
    :param date_from:
    :return:
    """
    table_name = 'tushare_stock_daily'
    has_table = engine_md.has_table(table_name)
    if date_from is None and has_table:
        sql_str = "select adddate(max(trade_date),1) from {table_name}".format(table_name=table_name)
        with with_db_session(engine_md) as session:
            date_from = date_2_str(session.execute(sql_str).scalar())
    tushare_adj_factor_df = get_tushare_daily_df('tushare_stock_daily_adj_factor', date_from)
    tushare_daily_basic_df = get_tushare_daily_df('tushare_stock_daily_basic', date_from)
    tushare_stock_daily_md_df = get_tushare_daily_df('tushare_stock_daily_md', date_from)
    # tushare_stock_daily_suspend_df = get_tushare_daily_df('tushare_stock_daily_suspend', date_from)
    tushare_daily_two_form_df = pd.merge(tushare_adj_factor_df, tushare_daily_basic_df, how='outer',
                                         on=['ts_code', 'trade_date'])
    tushare_daily_df = pd.merge(tushare_daily_two_form_df, tushare_stock_daily_md_df,
                                how='outer', on=['ts_code', 'trade_date'])
    # tushare_daily_df = pd.merge(tushare_daily_three_form_df, tushare_stock_daily_suspend_df, how='outer',
    #                             on=['ts_code'])
    # 设置 dtype
    dtype = {}
    for dic in [DTYPE_TUSHARE_SUSPEND, DTYPE_TUSHARE_DAILY_BASIC, DTYPE_TUSHARE_DAILY]:
        for key, val in dic.items():
            dtype[key] = val
    tushare_daily_df["close"] = tushare_daily_df.apply(get_merge, axis=1)
    tushare_daily_df.drop(['close_x', 'close_y'], axis=1, inplace=True)
    data_count = bunch_insert_on_duplicate_update(tushare_daily_df, table_name, engine_md, dtype)
    logging.info('%s  更新 %d', table_name, data_count)
    # if not has_table and engine_md.has_table(table_name):
    #     alter_table_2_myisam(engine_md, table_name)
    #     build_primary_key([table_name])
    return tushare_daily_df


if __name__ == '__main__':
    merge_tushare_daily()
