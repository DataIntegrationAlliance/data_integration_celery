"""
Created on 2018/9/3
@author: yby
@desc    : 2018-09-3
contact author:ybychem@gmail.com
"""

import pandas as pd
import numpy as np
import logging
from tasks.backend.orm import build_primary_key
from datetime import date, datetime, timedelta
from ibats_utils.mess import try_2_date, STR_FORMAT_DATE, datetime_2_str, split_chunk, try_n_times
from tasks import app
from sqlalchemy.types import String, Date, Integer, Text
from sqlalchemy.dialects.mysql import DOUBLE
from tasks.backend import engine_md
from tasks.merge.code_mapping import update_from_info_table
from ibats_utils.db import with_db_session, add_col_2_table, alter_table_2_myisam, \
    bunch_insert_on_duplicate_update
from tasks.tushare.ts_pro_api import pro

DEBUG = False
logger = logging.getLogger()
DATE_BASE = datetime.strptime('2005-01-01', STR_FORMAT_DATE).date()
ONE_DAY = timedelta(days=1)
# 标示每天几点以后下载当日行情数据
BASE_LINE_HOUR = 16
STR_FORMAT_DATE_TS = '%Y%m%d'

INDICATOR_PARAM_LIST_TUSHARE_STOCK_PLEDGE_DETAIL = [
    ('ts_code', String(20)),
    ('ann_date', Date),
    ('holder_name', String(200)),
    ('pledge_amount', DOUBLE),
    ('start_date', Date),
    ('end_date', Date),
    ('is_release', String(20)),
    ('release_date', Date),
    ('pledgor', String(200)),
    ('holding_amount', DOUBLE),
    ('pledged_amount', DOUBLE),
    ('p_total_ratio', DOUBLE),
    ('h_total_ratio', DOUBLE),
    ('is_buyback', String(20)),
]
# 设置 dtype
DTYPE_TUSHARE_STOCK_PLEDGE_DETAIL = {key: val for key, val in INDICATOR_PARAM_LIST_TUSHARE_STOCK_PLEDGE_DETAIL}


@try_n_times(times=5, sleep_time=2, logger=logger, exception=Exception, exception_sleep_time=5)
def invoke_pledge_detail(ts_code):
    invoke_pledge_detail = pro.pledge_detail(ts_code=ts_code)
    return invoke_pledge_detail


# df=invoke_pledge_detail(ts_code='000014.SZ')

@app.task
def import_tushare_stock_pledge_detail(chain_param=None, ts_code_set=None):
    """
    插入股票日线数据到最近一个工作日-1。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    :return:
    """
    table_name = 'tushare_stock_pledge_detail'
    logging.info("更新 %s 开始", table_name)

    has_table = engine_md.has_table(table_name)
    # 进行表格判断，确定是否含有tushare_stock_daily

    sql_str = """SELECT ts_code FROM tushare_stock_info WHERE ts_code >'002021.SZ'"""
    logger.warning('使用 tushare_stock_info 表确认需要提取股票质押数据的范围')

    with with_db_session(engine_md) as session:
        # 获取交易日数据
        table = session.execute(sql_str)
        ts_code_list = list(row[0] for row in table.fetchall())

    data_df_list, data_count, all_data_count, data_len = [], 0, 0, len(ts_code_list)
    logger.info('%d 只股票的质押信息将被插入 tushare_stock_pledge_detail 表', data_len)
    # 将data_df数据，添加到data_df_list

    Cycles = 1
    try:
        for ts_code in ts_code_list:
            data_df = invoke_pledge_detail(ts_code=ts_code)
            for i in range(len(data_df.start_date)):
                if data_df.start_date[i] is not None and len(data_df.start_date[i]) != 8:
                    data_df.start_date[i] = np.nan
            for i in range(len(data_df.end_date)):
                if data_df.end_date[i] is not None and len(data_df.end_date[i]) != 8:
                    data_df.end_date[i] = np.nan
            for i in range(len(data_df.release_date)):
                if data_df.release_date[i] is not None and len(data_df.release_date[i]) != 8:
                    data_df.release_date[i] = np.nan
            logger.warning('提取 %s 质押信息 %d 条', ts_code, len(data_df))

            # 把数据攒起来
            if data_df.shape[0] > 0:
                data_count += data_df.shape[0]
                data_df_list.append(data_df)
            # 大于阀值有开始插入
            if data_count >= 100 and len(data_df_list) > 0:
                data_df_all = pd.concat(data_df_list)
                data_count = bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md,
                                                              DTYPE_TUSHARE_STOCK_PLEDGE_DETAIL)
                logger.warning('更新股票质押信息 %d 条', data_count)
                all_data_count += data_count
                data_df_list, data_count = [], 0

            # 仅调试使用
            Cycles = Cycles + 1
            if DEBUG and Cycles > 2:
                break
    finally:
        if len(data_df_list) > 0:
            data_df_all = pd.concat(data_df_list)
            data_count = bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md,
                                                          DTYPE_TUSHARE_STOCK_PLEDGE_DETAIL)
            all_data_count = all_data_count + data_count
            logging.info("更新 %s 结束，总共 %d 条信息被更新", table_name, all_data_count)


if __name__ == "__main__":
    # DEBUG = True

    import_tushare_stock_pledge_detail()
