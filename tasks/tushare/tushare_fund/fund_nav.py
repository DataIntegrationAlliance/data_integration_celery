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

@try_n_times(times=5, sleep_time=1, exception_sleep_time=60)
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
    sql_str="""SELECT ts_code FROM md_integration.tushare_fund_basic"""

    with with_db_session(engine_md) as session:
        # 获取交易日数据
        table = session.execute(sql_str)
        code_list = list(row[0] for row in table.fetchall())

    for ts_code in code_list:

        data_df = invoke_fund_nav(ts_code=ts_code)
        if len(data_df) > 0:
            data_count = bunch_insert_on_duplicate_update(data_df, table_name, engine_md, DTYPE_TUSHARE_FUND_NAV)
            logging.info(" %s 表 %s 基金 %d 条净值信息被更新", table_name, ts_code, data_count)
        else:
            logging.info("无数据信息可被更新")



if __name__ == "__main__":
    # DEBUG = True
    import_tushare_fund_nav()