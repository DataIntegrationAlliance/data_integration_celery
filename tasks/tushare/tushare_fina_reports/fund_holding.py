"""
Created on 2018/9/3
@author: yby
@desc    : 2018-09-3
contact author:ybychem@gmail.com
"""

import tushare as ts
from tasks.tushare.ts_pro_api import pro
import pandas as pd
import logging
from tasks.backend.orm import build_primary_key
from datetime import date, datetime, timedelta
from ibats_utils.mess import try_2_date, STR_FORMAT_DATE, datetime_2_str, split_chunk, try_n_times
from tasks import app
from sqlalchemy.types import String, Date, Integer
from sqlalchemy.dialects.mysql import DOUBLE
from tasks.backend import engine_md
from tasks.merge.code_mapping import update_from_info_table
from ibats_utils.db import with_db_session, add_col_2_table, alter_table_2_myisam, \
    bunch_insert_on_duplicate_update

DEBUG = False
logger = logging.getLogger()
DATE_BASE = datetime.strptime('2005-01-01', STR_FORMAT_DATE).date()
ONE_DAY = timedelta(days=1)
# 标示每天几点以后下载当日行情数据
BASE_LINE_HOUR = 16


@try_n_times(times=5, sleep_time=0, logger=logger, exception_sleep_time=10)
def invoke_fund_holdings(year, quarter):
    invoke_fund_holdings = ts.fund_holdings(year=year, quarter=quarter)
    return invoke_fund_holdings


@app.task
def import_tushare_stock_fund_holdings():
    table_name = 'tushare_stock_fund_holdings'
    logging.info("更新 %s 开始", table_name)
    has_table = engine_md.has_table(table_name)
    tushare_fund_holdings_indicator_param_list = [
        ('ts_code', String(20)),
        ('sec_name', String(20)),
        ('end_date', Date),
        ('nums', DOUBLE),
        ('nlast', DOUBLE),
        ('count', DOUBLE),
        ('clast', DOUBLE),
        ('amount', DOUBLE),
        ('ratio', DOUBLE),
    ]
    tushare_fund_holdings_dtype = {key: val for key, val in tushare_fund_holdings_indicator_param_list}
    data_df_list, data_count, all_data_count, = [], 0, 0
    years = list(range(2013, 2019))
    try:
        for year in years:
            for quarter in list([1, 2, 3, 4]):
                print((year, quarter))
                data_df = invoke_fund_holdings(year, quarter)
                ts_code_list = []
                for i in data_df.code:
                    if i[0] == '6':
                        sh = i + '.SH'
                        ts_code_list.append(sh)
                    else:
                        sz = i + '.SZ'
                        ts_code_list.append(sz)
                data_df.code = ts_code_list
                data_df = data_df.rename(columns={'code': 'ts_code', 'name': 'sec_name', 'date': 'end_date'})
                # 把数据攒起来
                if data_df is not None and data_df.shape[0] > 0:
                    data_count += data_df.shape[0]
                    data_df_list.append(data_df)
                # 大于阀值有开始插入
                if data_count >= 50:
                    data_df_all = pd.concat(data_df_list)
                    bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md, tushare_fund_holdings_dtype)
                    all_data_count += data_count
                    data_df_list, data_count = [], 0
    finally:
        if len(data_df_list) > 0:
            data_df_all = pd.concat(data_df_list)
            data_count = bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md,
                                                          tushare_fund_holdings_dtype)
            all_data_count = all_data_count + data_count
            logging.info("更新 %s 结束 %d 条信息被更新", table_name, all_data_count)
            if not has_table and engine_md.has_table(table_name):
                alter_table_2_myisam(engine_md, [table_name])
                build_primary_key([table_name])


@app.task
def fresh_tushare_stock_fund_holdings(year, quarter):
    table_name = 'tushare_stock_fund_holdings'
    logging.info("更新 %s 表%s年%s季度基金持股信息开始", table_name, year, quarter)
    has_table = engine_md.has_table(table_name)
    tushare_fund_holdings_indicator_param_list = [
        ('ts_code', String(20)),
        ('sec_name', String(20)),
        ('end_date', Date),
        ('nums', DOUBLE),
        ('nlast', DOUBLE),
        ('count', DOUBLE),
        ('clast', DOUBLE),
        ('amount', DOUBLE),
        ('ratio', DOUBLE),
    ]
    tushare_fund_holdings_dtype = {key: val for key, val in tushare_fund_holdings_indicator_param_list}
    data_df_list, data_count, all_data_count, = [], 0, 0
    data_df = invoke_fund_holdings(year, quarter)
    ts_code_list = []
    for i in data_df.code:
        if i[0] == '6':
            sh = i + '.SH'
            ts_code_list.append(sh)
        else:
            sz = i + '.SZ'
            ts_code_list.append(sz)
    data_df.code = ts_code_list
    data_df = data_df.rename(columns={'code': 'ts_code', 'name': 'sec_name', 'date': 'end_date'})
    bunch_insert_on_duplicate_update(data_df, table_name, engine_md, tushare_fund_holdings_dtype)
    logging.info("%s年%s季度 %s 更新 %d 条基金持股信息", year, quarter, table_name, all_data_count)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s [%(name)s] %(message)s')
    import_tushare_stock_fund_holdings()
    # fresh_tushare_stock_fund_holdings(year=2018,quarter=3)
