"""
Created on 2018/8/14
@author: yby
@desc    : 2018-08-21 已经正式运行测试完成，可以正常使用
"""
from tasks.tushare.ts_pro_api import pro, check_sqlite_db_primary_keys
import pandas as pd
import logging
from datetime import date, datetime, timedelta
from ibats_utils.mess import try_2_date, STR_FORMAT_DATE, datetime_2_str, split_chunk, try_n_times
from tasks import app, config
from sqlalchemy.types import String, Date, Integer
from sqlalchemy.dialects.mysql import DOUBLE
from tasks.backend import engine_md, bunch_insert
from ibats_utils.db import with_db_session, bunch_insert_on_duplicate_update

DEBUG = False
logger = logging.getLogger()
DATE_BASE = datetime.strptime('2005-01-01', STR_FORMAT_DATE).date()
ONE_DAY = timedelta(days=1)
# 标示每天几点以后下载当日行情数据
BASE_LINE_HOUR = 16
STR_FORMAT_DATE_TS = '%Y%m%d'

INDICATOR_PARAM_LIST_TUSHARE_STOCK_INDEX_DAILY_MD = [
    ('ts_code', String(20)),
    ('trade_date', Date),
    ('open', DOUBLE),
    ('high', DOUBLE),
    ('low', DOUBLE),
    ('close', DOUBLE),
    ('pre_close', DOUBLE),
    ('change', DOUBLE),
    ('pct_chg', DOUBLE),
    ('vol', DOUBLE),
    ('amount', DOUBLE),
]
# 设置 dtype
DTYPE_TUSHARE_STOCK_INDEX_DAILY_MD = {key: val for key, val in INDICATOR_PARAM_LIST_TUSHARE_STOCK_INDEX_DAILY_MD}
DTYPE_TUSHARE_STOCK_INDEX_DAILY_MD['ts_code'] = String(20)
DTYPE_TUSHARE_STOCK_INDEX_DAILY_MD['trade_date'] = Date


@try_n_times(times=5, sleep_time=3, logger=logger, exception_sleep_time=60)
def invoke_index_daily(ts_code, start_date, end_date):
    invoke_index_daily = pro.index_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
    return invoke_index_daily


@app.task
def import_tushare_stock_index_daily_mini(
        chain_param=None,
        ts_code_set=["h30024.CSI", "399300.SZ", "000016.SH", "399905.SZ", "399678.SZ", "399101.SZ", "000300.SH"]):
    """
    插入股票日线数据到最近一个工作日-1。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    :return:
    """
    return import_tushare_stock_index_daily(ts_code_set=ts_code_set)


@app.task
def import_tushare_stock_index_daily(chain_param=None, ts_code_set=None):
    """
    插入股票日线数据到最近一个工作日-1。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    :return:
    """
    table_name = 'tushare_stock_index_daily_md'
    primary_keys = ["ts_code", "trade_date"]
    logging.info("更新 %s 开始", table_name)
    check_sqlite_db_primary_keys(table_name, primary_keys)
    has_table = engine_md.has_table(table_name)
    # 进行表格判断，确定是否含有tushare_stock_daily
    if has_table:
        sql_str = """
            SELECT ts_code, date_frm, if(exp_date<end_date, exp_date, end_date) date_to
            FROM
            (
            SELECT info.ts_code, ifnull(trade_date, base_date) date_frm, exp_date,
            if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
            FROM 
                tushare_stock_index_basic info 
            LEFT OUTER JOIN
                (SELECT ts_code, adddate(max(trade_date),1) trade_date FROM {table_name} GROUP BY ts_code) daily
            ON info.ts_code = daily.ts_code
            ) tt
            WHERE date_frm <= if(exp_date<end_date, exp_date, end_date) 
            ORDER BY ts_code""".format(table_name=table_name)
    else:
        sql_str = """
            SELECT ts_code, date_frm, if(exp_date<end_date, exp_date, end_date) date_to
            FROM
              (
                SELECT info.ts_code, base_date date_frm, exp_date,
                if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
                FROM tushare_stock_index_basic info 
              ) tt
            WHERE date_frm <= if(exp_date<end_date, exp_date, end_date) 
            ORDER BY ts_code"""
        logger.warning('%s 不存在，仅使用 tushare_stock_info 表进行计算日期范围', table_name)

    with with_db_session(engine_md) as session:
        # 获取每只股票需要获取日线数据的日期区间
        table = session.execute(sql_str)
        # 计算每只股票需要获取日线数据的日期区间
        begin_time = None
        # 获取date_from,date_to，将date_from,date_to做为value值
        code_date_range_dic = {
            ts_code: (date_from if begin_time is None else min([date_from, begin_time]), date_to)
            for ts_code, date_from, date_to in table.fetchall() if
            ts_code_set is None or ts_code in ts_code_set}

    # data_len = len(code_date_range_dic)
    data_df_list, data_count, all_data_count, data_len = [], 0, 0, len(code_date_range_dic)
    logger.info('%d data will been import into %s', data_len, table_name)
    # 将data_df数据，添加到data_df_list
    try:
        for num, (ts_code, (date_from, date_to)) in enumerate(code_date_range_dic.items(), start=1):
            logger.debug('%d/%d) %s [%s - %s]', num, data_len, ts_code, date_from, date_to)
            data_df = invoke_index_daily(ts_code=ts_code, start_date=datetime_2_str(date_from, STR_FORMAT_DATE_TS),
                                         end_date=datetime_2_str(date_to, STR_FORMAT_DATE_TS))
            # data_df = df
            if data_df is not None and data_df.shape[0] > 0:
                while try_2_date(data_df['trade_date'].iloc[-1]) > date_from:
                    last_date_in_df_last, last_date_in_df_cur = try_2_date(data_df['trade_date'].iloc[-1]), None
                    df2 = invoke_index_daily(ts_code=ts_code, start_date=datetime_2_str(date_from, STR_FORMAT_DATE_TS),
                                             end_date=datetime_2_str(
                                                 try_2_date(data_df['trade_date'].iloc[-1]) - timedelta(days=1),
                                                 STR_FORMAT_DATE_TS))
                    if len(df2 > 0):
                        last_date_in_df_cur = try_2_date(df2['trade_date'].iloc[-1])
                        if last_date_in_df_cur < last_date_in_df_last:
                            data_df = pd.concat([data_df, df2])
                            # df = df2
                        elif last_date_in_df_cur == last_date_in_df_last:
                            break
                        if data_df is None:
                            logger.warning('%d/%d) %s has no data during %s %s', num, data_len, ts_code, date_from,
                                           date_to)
                            continue
                        logger.info('%d/%d) %d data of %s between %s and %s', num, data_len, data_df.shape[0], ts_code,
                                    date_from, date_to)
                    else:
                        break

                # 把数据攒起来
                data_count += data_df.shape[0]
                data_df_list.append(data_df)

            # 仅调试使用
            if DEBUG and len(data_df_list) > 5:
                break

            # 大于阀值有开始插入
            if data_count >= 500:
                data_df_all = pd.concat(data_df_list)
                data_count = bunch_insert(
                    data_df_all, table_name=table_name, dtype=DTYPE_TUSHARE_STOCK_INDEX_DAILY_MD,
                    primary_keys=primary_keys)

                all_data_count += data_count
                data_df_list, data_count = [], 0

    finally:
        # 导入数据库
        if len(data_df_list) > 0:
            data_df_all = pd.concat(data_df_list)
            data_count = bunch_insert(
                data_df_all, table_name=table_name, dtype=DTYPE_TUSHARE_STOCK_INDEX_DAILY_MD,
                primary_keys=primary_keys)

            all_data_count += data_count
            logging.info("更新 %s 结束 %d 条信息被更新", table_name, all_data_count)


def export_tushare_stock_index_daily(index_code='000300.SH', file_path=None):
    import os
    sql_str = """SELECT * from tushare_stock_index_daily_md
        where ts_code = %s
        ORDER BY trade_date"""
    df = pd.read_sql(sql_str, engine_md, params=[index_code])
    if file_path is None:
        folder_path = os.path.join(os.path.abspath('.'), 'output', 'commodity')
        file_name = f"{index_code}.csv"
        file_path = os.path.join(folder_path, file_name)
    else:
        folder_path, file_name = os.path.split(file_path)

    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    df.to_csv(file_path, index=False)


if __name__ == "__main__":
    # DEBUG = True
    # import_tushare_stock_index_daily(ts_code_set=None)
    import_tushare_stock_index_daily_mini()
