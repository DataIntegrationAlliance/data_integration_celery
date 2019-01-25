"""
Created on 2018-09-14
@author: yby
@desc    : 2018-09-14
contact author:ybychem@gmail.com
"""
import pandas as pd
import logging
from tasks.backend.orm import build_primary_key
from datetime import date, datetime, timedelta
from tasks.utils.fh_utils import try_2_date, STR_FORMAT_DATE, datetime_2_str, split_chunk, try_n_times
from tasks import app
from sqlalchemy.types import String, Date, Text
from sqlalchemy.dialects.mysql import DOUBLE
from tasks.backend import engine_md
from tasks.merge.code_mapping import update_from_info_table
from tasks.utils.db_utils import with_db_session, add_col_2_table, alter_table_2_myisam, \
    bunch_insert_on_duplicate_update
from tasks.tushare.ts_pro_api import pro
from tasks.config import config

DEBUG = False
logger = logging.getLogger()
DATE_BASE = datetime.strptime('2005-01-01', STR_FORMAT_DATE).date()
ONE_DAY = timedelta(days=1)
# 标示每天几点以后下载当日行情数据
BASE_LINE_HOUR = 16
STR_FORMAT_DATE_TS = '%Y%m%d'

INDICATOR_PARAM_LIST_TUSHARE_STOCK_INDEX_BASIC = [
    ('ts_code', String(30)),
    ('name', String(100)),
    ('fullname', String(200)),
    ('market', String(100)),
    ('publisher', String(100)),
    ('index_type', String(100)),
    ('category', String(50)),
    ('base_date', Date),
    ('base_point', DOUBLE),
    ('list_date', Date),
    ('weight_rule', String(200)),
    ('desc', Text),
    ('exp_date', Date),
]
# 设置 dtype
DTYPE_TUSHARE_STOCK_INDEX_BASIC = {key: val for key, val in INDICATOR_PARAM_LIST_TUSHARE_STOCK_INDEX_BASIC}


@try_n_times(times=5, sleep_time=0, logger=logger, exception=Exception, exception_sleep_time=60)
def invoke_index_basic(market,fields):
    invoke_index_basic = pro.index_basic(market=market,fields=fields)
    return invoke_index_basic


@app.task
def import_tushare_index_basic(chain_param=None):
    """
    插入股票日线数据到最近一个工作日-1。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    :return:
    """
    table_name = 'tushare_stock_index_basic'
    logging.info("更新 %s 开始", table_name)

    has_table = engine_md.has_table(table_name)

    fields='ts_code','name','fullname','market','publisher','index_type','category','base_date','base_point','list_date','weight_rule','desc','exp_date'
    market_list=list(['MSCI','CSI','SSE','SZSE','CICC','SW','CNI','OTH'])

    for mkt in market_list:
        # trade_date = datetime_2_str(trddate[i], STR_FORMAT_DATE_TS)
        data_df = invoke_index_basic(market=mkt, fields=fields)
        if len(data_df) > 0:
            data_count = bunch_insert_on_duplicate_update(
                data_df, table_name, engine_md, DTYPE_TUSHARE_STOCK_INDEX_BASIC,
                myisam_if_create_table=True, primary_keys=['ts_code'], schema=config.DB_SCHEMA_MD
            )
            logging.info("%s更新 %s 结束 %d 条信息被更新", mkt, table_name, data_count)
        else:
            logging.info("无数据信息可被更新")


if __name__ == "__main__":
    # DEBUG = True
    import_tushare_index_basic()

#下面代码是生成fields和par的

# sub=pd.read_excel('tasks/tushare/tushare_fina_reports/fina_indicator.xlsx',header=0)[['code','types']]
# for a, b in [tuple(x) for x in sub.values]:
#     print("('%s', %s)," % (a, b))
#     # print("'%s'," % (a))