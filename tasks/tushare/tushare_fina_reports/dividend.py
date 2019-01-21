"""
Created on 2018/9/21
@author: yby
@desc    : 2018-09-21
contact author:ybychem@gmail.com
"""

import pandas as pd
import logging
from datetime import date, datetime, timedelta
from tasks.utils.fh_utils import try_2_date, STR_FORMAT_DATE, datetime_2_str, split_chunk,try_n_times
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

@try_n_times(times=5, sleep_time=2, logger=logger, exception_sleep_time=5)
def invoke_dividend(ann_date,fields):
    invoke_dividend = pro.dividend(ann_date=ann_date, fields=fields)
    return invoke_dividend

INDICATOR_PARAM_LIST_TUSHARE_DIVIDEND = [
    ('ts_code', String(20)),
    ('end_date', Date),
    ('ann_date', Date),
    ('div_proc', String(200)),
    ('stk_div', DOUBLE),
    ('stk_bo_rate', DOUBLE),
    ('stk_co_rate', DOUBLE),
    ('cash_div', DOUBLE),
    ('cash_div_tax', DOUBLE),
    ('record_date', Date),
    ('ex_date', Date),
    ('pay_date', Date),
    ('div_listdate', Date),
    ('imp_ann_date', Date),
    ('base_date', Date),
    ('base_share', DOUBLE),
]
# 设置 dtype
DTYPE_TUSHARE_DIVIDEND= {key: val for key, val in INDICATOR_PARAM_LIST_TUSHARE_DIVIDEND}


@app.task
def import_tushare_dividend(chain_param=None ):
    """
    插入股票日线数据到最近一个工作日-1。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    :return:
    """
    table_name = 'tushare_stock_dividend'
    logging.info("更新 %s 开始", table_name)
    has_table = engine_md.has_table(table_name)
    # 进行表格判断，确定是否含有tushare_stock_daily

    if has_table:
        sql_str = """
               select cal_date  ann_date          
               FROM
                (
                 select * from tushare_trade_date trddate 
                 where( cal_date>(SELECT max(ann_date) FROM  {table_name}))
               )tt
               where (is_open=1 
                      and cal_date <= if(hour(now())<16, subdate(curdate(),1), curdate()) 
                      and exchange='SSE') """.format(table_name=table_name)
    else:
        sql_str = """
               SELECT cal_date ann_date FROM tushare_trade_date trddate WHERE (trddate.is_open=1 
            AND cal_date <= if(hour(now())<16, subdate(curdate(),1), curdate()) 
            AND exchange='SSE') ORDER BY cal_date"""
        logger.warning('%s 不存在，仅使用 tushare_stock_info 表进行计算日期范围', table_name)

    with with_db_session(engine_md) as session:
        # 获取交易日数据
        table = session.execute(sql_str)
        trddate = list(row[0] for row in table.fetchall())

    #输出数据字段
    fields='ts_code,end_date,ann_date,div_proc,stk_div,stk_bo_rate,stk_co_rate,cash_div,cash_div_tax,\
           record_date,ex_date,pay_date,div_listdate,imp_ann_date,base_date,base_share'

    data_df_list, data_count, all_data_count, data_len = [], 0, 0, len(trddate)
    try:
        for i in range(len(trddate)):
            ann_date = datetime_2_str(trddate[i], STR_FORMAT_DATE_TS)
            data_df = invoke_dividend(ann_date=ann_date,fields=fields)
            logging.info(" %s 日 提取 %d 条分红送股信息", ann_date, len(data_df))
            # if len(data_df) > 0:
            #     data_count = bunch_insert_on_duplicate_update(data_df, table_name, engine_md, DTYPE_TUSHARE_DIVIDEND)
            #     logging.info(" %s 表 %s 日 %d 条信息被更新", table_name, ann_date, data_count)
            # else:
            #     logging.info("无数据信息可被更新")

            # 把数据攒起来
            if data_df is not None and data_df.shape[0] > 0:
                data_count += data_df.shape[0]
                data_df_list.append(data_df)

            # 仅调试使用
            if DEBUG and len(data_df_list) > 5:
                break
            # 大于阀值开始插入
            if data_count >= 500:
                data_df_all = pd.concat(data_df_list)
                bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md,DTYPE_TUSHARE_DIVIDEND)
                all_data_count += data_count
                data_df_list, data_count = [], 0

    finally:
        # 导入残余数据到数据库
        if len(data_df_list) > 0:
            data_df_all = pd.concat(data_df_list)
            data_count = bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md,DTYPE_TUSHARE_DIVIDEND)
            logging.info("更新 %s 结束 %d 条信息被更新", table_name, data_count)
        if not has_table and engine_md.has_table(table_name):
            alter_table_2_myisam(engine_md, [table_name])
            # build_primary_key([table_name])
            create_pk_str = """ALTER TABLE {table_name}
                CHANGE COLUMN `ts_code` `ts_code` VARCHAR(20) NOT NULL FIRST,
                CHANGE COLUMN `ann_date` `ann_date` DATE NOT NULL AFTER `ts_code`,
                ADD PRIMARY KEY (`ts_code`, `ann_date`)""".format(table_name=table_name)
            with with_db_session(engine_md) as session:
                session.execute(create_pk_str)
            logger.info('%s 表 `ts_code`, `trade_date` 主键设置完成', table_name)


if __name__ == "__main__":
    # DEBUG = True
    import_tushare_dividend()
