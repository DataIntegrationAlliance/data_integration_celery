"""
Created on 2018/10/22
@author: yby
@desc    : 2018/10/22
contact author:ybychem@gmail.com
"""

import pandas as pd
import logging
from tasks.backend.orm import build_primary_key
from datetime import date, datetime, timedelta
from tasks.utils.fh_utils import try_2_date, STR_FORMAT_DATE, datetime_2_str, split_chunk,try_n_times
from tasks import app
from sqlalchemy.types import String, Date, Integer,Text
from sqlalchemy.dialects.mysql import DOUBLE
from tasks.backend import engine_md
from tasks.merge.code_mapping import update_from_info_table
from tasks.utils.db_utils import with_db_session, add_col_2_table, alter_table_2_myisam, \
    bunch_insert_on_duplicate_update
from tasks.tushare.ts_pro_api import pro

DEBUG = False
logger = logging.getLogger()
DATE_BASE = datetime.strptime('2005-01-01', STR_FORMAT_DATE).date()
ONE_DAY = timedelta(days=1)
# 标示每天几点以后下载当日行情数据
BASE_LINE_HOUR = 16
STR_FORMAT_DATE_TS = '%Y%m%d'

@try_n_times(times=5, sleep_time=2, exception_sleep_time=60)
def invoke_fund_nav(ts_code):
    invoke_fund_nav = pro.fund_nav(ts_code=ts_code)
    return invoke_fund_nav

INDICATOR_PARAM_LIST_TUSHARE_FUND_NAV = [
    ('ts_code', String(20)),
    ('ann_date', Date),
    ('end_date', Date),
    ('unit_nav', DOUBLE),
    ('accum_nav', DOUBLE),
    ('accum_div', DOUBLE),
    ('net_asset', DOUBLE),
    ('total_netasset', DOUBLE),
    ('adj_nav', DOUBLE),
  ]
# 设置 dtype
DTYPE_TUSHARE_FUND_NAV = {key: val for key, val in INDICATOR_PARAM_LIST_TUSHARE_FUND_NAV}

@app.task
def import_tushare_fund_nav(chain_param=None, ):
    """
    插入股票日线数据到最近一个工作日-1。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    :return:
    """
    table_name = 'tushare_fund_nav'
    logging.info("更新 %s 开始", table_name)
    has_table = engine_md.has_table(table_name)
    # 进行表格判断，确定是否含有tushare_stock_daily
    # if has_table is True:
    #     sql_str=""""""
    # else:
    sql_str="""SELECT ts_code FROM md_integration.tushare_fund_basic """

    with with_db_session(engine_md) as session:
        # 获取交易日数据
        table = session.execute(sql_str)
        code_list = list(row[0] for row in table.fetchall())
    data_df_list, data_count, all_data_count, data_len = [], 0, 0, len(code_list)
    try:
        for num, (ts_code) in enumerate(code_list, start=1):
            data_df = invoke_fund_nav(ts_code=ts_code)
            logging.info(" 提取%s 基金行情信息%d 条，整体完成%d/%d", ts_code, len(data_df),num,data_len)
            # 把数据攒起来
            if data_df is not None and data_df.shape[0] > 0:
                data_count += data_df.shape[0]
                data_df_list.append(data_df)
            # 大于阀值有开始插入
            if data_count >= 20000:
                data_df_all = pd.concat(data_df_list)
                bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md, DTYPE_TUSHARE_FUND_NAV)
                logging.info(" %s 表 %d 条净值信息被更新", table_name, data_count)
                all_data_count += data_count
                data_df_list, data_count = [], 0
    finally:
        #插入剩余数据
        if len(data_df_list) > 0:
            data_df_all = pd.concat(data_df_list)
            data_count = bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md, DTYPE_TUSHARE_FUND_NAV)
            all_data_count = all_data_count + data_count
            logging.info("更新 %s 结束 %d 条信息被更新", table_name, all_data_count)

if __name__ == "__main__":
    # DEBUG = True
    import_tushare_fund_nav()