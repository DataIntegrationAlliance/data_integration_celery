"""
Created on 2018/10/22
@author: yby
@desc    : 2018/10/22
contact author:ybychem@gmail.com
"""

import pandas as pd
import numpy as np
import logging
from tasks.backend.orm import build_primary_key
from datetime import date, datetime, timedelta
from tasks.utils.fh_utils import try_2_date, STR_FORMAT_DATE, datetime_2_str, split_chunk
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

INDICATOR_PARAM_LIST_TUSHARE_STOCK_DAILY_ADJ_FACTOR = [
    ('name', String(200)),
    ('shortname', String(100)),
    ('short_enname', String(200)),
    ('province', String(200)),
    ('city', String(200)),
    ('address', String(200)),
    ('phone', String(200)),
    ('office', String(200)),
    ('website', String(200)),
    ('chairman', String(200)),
    ('manager', String(200)),
    ('reg_capital', DOUBLE),
    ('setup_date', Date),
    ('end_date', Date),
    ('employees', DOUBLE),
    ('main_business', Text),
    ('org_code', String(200)),
    ('credit_code', String(200)),
]
# 设置 dtype
DTYPE_TUSHARE_STOCK_DAILY_ADJ_FACTOR = {key: val for key, val in INDICATOR_PARAM_LIST_TUSHARE_STOCK_DAILY_ADJ_FACTOR}

@app.task
def import_tushare_fund_company(chain_param=None, ):
    """
    插入股票日线数据到最近一个工作日-1。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    :return:
    """
    table_name = 'tushare_fund_company'
    logging.info("更新 %s 开始", table_name)
    has_table = engine_md.has_table(table_name)
    # 进行表格判断，确定是否含有tushare_stock_daily
    data_df = pro.fund_company()
    for i in range(len(data_df.setup_date)):
        if data_df.setup_date[i] is not None and len(data_df.setup_date[i]) != 8:
            data_df.setup_date[i] = np.nan
    for i in range(len(data_df.end_date)):
        if data_df.end_date[i] is not None and len(data_df.end_date[i]) != 8:
            data_df.end_date[i] = np.nan
        # data_df=data_df[data_df['shortname'].apply(lambda x:x is not None)]
    if len(data_df) > 0:
        data_count = bunch_insert_on_duplicate_update(data_df, table_name, engine_md, DTYPE_TUSHARE_STOCK_DAILY_ADJ_FACTOR)
        logging.info(" %s 表 %d 条公募基金公司信息被更新", table_name,  data_count)
    else:
        logging.info("无数据信息可被更新")



if __name__ == "__main__":
    # DEBUG = True
    import_tushare_fund_company()