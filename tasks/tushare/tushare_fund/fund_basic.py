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
from ibats_utils.mess import try_2_date, STR_FORMAT_DATE, datetime_2_str, split_chunk
from tasks import app
from sqlalchemy.types import String, Date, Integer,Text
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

INDICATOR_PARAM_LIST_TUSHARE_STOCK_DAILY_ADJ_FACTOR = [
    ('ts_code', String(20)),
    ('name', String(200)),
    ('management', String(60)),
    ('custodian', String(60)),
    ('fund_type', String(20)),
    ('found_date', Date),
    ('due_date', Date),
    ('list_date', Date),
    ('issue_date', Date),
    ('delist_date', Date),
    ('issue_amount', DOUBLE),
    ('m_fee', DOUBLE),
    ('c_fee', DOUBLE),
    ('duration_year', DOUBLE),
    ('p_value', DOUBLE),
    ('min_amount', DOUBLE),
    ('exp_return', DOUBLE),
    ('benchmark', Text),
    ('status', String(20)),
    ('invest_type', String(200)),
    ('type', String(100)),
    ('trustee', String(20)),
    ('purc_startdate', Date),
    ('redm_startdate', Date),
    ('market', String(20)),
]
# 设置 dtype
DTYPE_TUSHARE_STOCK_DAILY_ADJ_FACTOR = {key: val for key, val in INDICATOR_PARAM_LIST_TUSHARE_STOCK_DAILY_ADJ_FACTOR}

@app.task
def import_tushare_fund_basic(chain_param=None, ):
    """
    插入股票日线数据到最近一个工作日-1。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    :return:
    """
    table_name = 'tushare_fund_basic'
    logging.info("更新 %s 开始", table_name)
    has_table = engine_md.has_table(table_name)
    # 进行表格判断，确定是否含有tushare_stock_daily

    for market in list(['E','O']):
        data_df = pro.fund_basic(market=market)
        if len(data_df) > 0:
            data_count = bunch_insert_on_duplicate_update(data_df, table_name, engine_md, DTYPE_TUSHARE_STOCK_DAILY_ADJ_FACTOR)
            logging.info(" %s 表 %d 条基金信息被更新", table_name,  data_count)
        else:
            logging.info("无数据信息可被更新")



if __name__ == "__main__":
    # DEBUG = True
    import_tushare_fund_basic()