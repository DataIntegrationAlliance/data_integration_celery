"""
Created on 2018/8/13
@author: yby
@desc    : 2018-08-3
contact author:ybychem@gmail.com
"""
import pandas as pd
import logging
from tasks.backend.orm import build_primary_key
from datetime import date, datetime, timedelta
from ibats_utils.mess import try_2_date, STR_FORMAT_DATE, datetime_2_str, split_chunk, try_n_times
from tasks import app
from sqlalchemy.types import String, Date, Text
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

INDICATOR_PARAM_LIST_TUSHARE_STOCK_NAMECHANGE = [
    ('ts_code', String(20)),
    ('name', String(200)),
    ('start_date', Date),
    ('end_date', Date),
    ('ann_date', Date),
    ('change_reason', Text),
]
# 设置 dtype
DTYPE_TUSHARE_STOCK_NAMECHANGE = {key: val for key, val in INDICATOR_PARAM_LIST_TUSHARE_STOCK_NAMECHANGE}



@app.task
def import_tushare_namechange(chain_param=None):
    """
    插入股票日线数据到最近一个工作日-1。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    :return:
    """
    table_name = 'tushare_stock_namechange'
    logging.info("更新 %s 开始", table_name)

    has_table = engine_md.has_table(table_name)
    if has_table:
        sql_str = """select max(start_date) start_date   FROM md_integration.tushare_stock_namechange"""

    else:
        sql_str = """select min(list_date) start_date   FROM md_integration.tushare_stock_info"""


    with with_db_session(engine_md) as session:
        # 获取交易日数据
        table = session.execute(sql_str)
        start_date = list(row[0] for row in table.fetchall())
        start_date = datetime_2_str(start_date[0], STR_FORMAT_DATE_TS)
        end_date=datetime_2_str(date.today(), STR_FORMAT_DATE_TS)

    try:
        data_df = pro.namechange(start_date=start_date,end_date=end_date, fields='ts_code,name,start_date,end_date,change_reason')
        if len(data_df) > 0:
            data_count = bunch_insert_on_duplicate_update(data_df, table_name, engine_md, DTYPE_TUSHARE_STOCK_NAMECHANGE)
            logging.info("更新 %s 结束 %d 条上市公司更名信息被更新",table_name, data_count)
        else:
            logging.info("无数据信息可被更新")
    finally:
        if not has_table and engine_md.has_table(table_name):
            alter_table_2_myisam(engine_md, [table_name])
            # build_primary_key([table_name])
            create_pk_str = """ALTER TABLE {table_name}
                CHANGE COLUMN `ts_code` `ts_code` VARCHAR(20) NOT NULL FIRST,
                CHANGE COLUMN `start_date` `start_date` DATE NOT NULL AFTER `ts_code`,
                ADD PRIMARY KEY (`ts_code`, `start_date`)""".format(table_name=table_name)
            with with_db_session(engine_md) as session:
                session.execute(create_pk_str)
            logger.info('%s 表 `ts_code`, `start_date` 主键设置完成', table_name)


if __name__ == "__main__":
    # DEBUG = True
    import_tushare_namechange()
