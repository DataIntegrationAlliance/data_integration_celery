"""
Created on 2018/10/13
@author: yby
@desc    : 2018-10-13
contact author:ybychem@gmail.com
"""

import pandas as pd
import logging
from tasks.backend.orm import build_primary_key
from datetime import date, datetime, timedelta
from tasks.utils.fh_utils import try_2_date, STR_FORMAT_DATE, datetime_2_str, split_chunk, try_n_times
from tasks import app
from sqlalchemy.types import String, Date, Integer
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

INDICATOR_PARAM_LIST_TUSHARE_STOCK_DAILY_BASIC = [
    ('ts_code', String(20)),
    ('trade_date', Date),
    ('close', DOUBLE),
    ('turnover_rate', DOUBLE),
    ('volume_ratio', DOUBLE),
    ('pe', DOUBLE),
    ('pe_ttm', DOUBLE),
    ('pb', DOUBLE),
    ('ps', DOUBLE),
    ('ps_ttm', DOUBLE),
    ('total_share', DOUBLE),
    ('float_share', DOUBLE),
    ('free_share', DOUBLE),
    ('total_mv', DOUBLE),
    ('circ_mv', DOUBLE),
]
# 设置 dtype
DTYPE_TUSHARE_STOCK_DAILY_BASIC = {key: val for key, val in INDICATOR_PARAM_LIST_TUSHARE_STOCK_DAILY_BASIC}



data = pro.stock_basic(exchange_id='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')