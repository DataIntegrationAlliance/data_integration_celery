"""
Created on 2018/11/8
@author: yby
@desc    : 2018-11-8
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

INDICATOR_PARAM_LIST_TUSHARE_STOCK_INDEX_DAILYBASIC = [
    ('ts_code', String(20)),
    ('trade_date', Date),
    ('total_mv', DOUBLE),
    ('float_mv', DOUBLE),
    ('total_share', DOUBLE),
    ('float_share', DOUBLE),
    ('free_share', DOUBLE),
    ('turnover_rate', DOUBLE),
    ('turnover_rate_f', DOUBLE),
    ('pe', DOUBLE),
    ('pe_ttm', DOUBLE),
    ('pb', DOUBLE),
]
# 设置 dtype
DTYPE_TUSHARE_STOCK_INDEX_DAILYBASIC = {key: val for key, val in INDICATOR_PARAM_LIST_TUSHARE_STOCK_INDEX_DAILYBASIC}


@try_n_times(times=3, sleep_time=2, logger=logger, exception=Exception, exception_sleep_time=5)
def invoke_index_dailybasic(trade_date):
    invoke_index_dailybasic = pro.index_dailybasic(trade_date=trade_date)
    return invoke_index_dailybasic


@app.task
def import_tushare_stock_index_dailybasic(chain_param=None):
    """
    插入股票日线数据到最近一个工作日-1。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    :return:
    """
    table_name = 'tushare_stock_index_dailybasic'
    logging.info("更新 %s 开始", table_name)
    has_table = engine_md.has_table(table_name)
    # 进行表格判断，确定是否含有tushare_stock_daily

    if has_table:
        sql_str = """
               select cal_date            
               FROM
                (
                 select * from tushare_trade_date trddate 
                 where( cal_date>(SELECT max(trade_date) FROM  {table_name}))
               )tt
               where (is_open=1 
                      and cal_date <= if(hour(now())<16, subdate(curdate(),1), curdate()) 
                      and exchange_id='SSE') """.format(table_name=table_name)
    else:
        sql_str = """
            SELECT cal_date FROM tushare_trade_date trddate WHERE (trddate.is_open=1 
            AND cal_date <= if(hour(now())<16, subdate(curdate(),1), curdate()) 
            AND exchange_id='SSE')
            AND cal_date>'2004-01-01'ORDER BY cal_date"""
        logger.warning('%s 不存在，仅使用 tushare_stock_info 表进行计算日期范围', table_name)
    # 提取交易日信息
    with with_db_session(engine_md) as session:
        # 获取交易日数据
        table = session.execute(sql_str)
        trddate = list(row[0] for row in table.fetchall())

    # 定义参数
    data_df_list, data_count, all_data_count,num, data_len = [], 0, 0,1, len(trddate)
    try:
        for i in range(len(trddate)):
            trade_date = datetime_2_str(trddate[i], STR_FORMAT_DATE_TS)
            data_df = invoke_index_dailybasic(trade_date=trade_date)

            if data_df is None:
                logger.warning('%s 日无股票指数基本面信息，可能数据还没有更新，稍后再试一次！', trade_date)
            elif data_df is not None:
                logger.info('总体进度%d/%d，提取出 %s日  %d 条大盘指数基础信息', num, data_len, trade_date, data_df.shape[0])
            num+=1
            # 把数据攒起来
            if data_df is not None and data_df.shape[0] > 0:
                data_count += data_df.shape[0]
                data_df_list.append(data_df)
            # 大于阀值有开始插入
            if data_count >= 500 and len(data_df_list) > 0:
                data_df_all = pd.concat(data_df_list)
                bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md,
                                                 DTYPE_TUSHARE_STOCK_INDEX_DAILYBASIC, myisam_if_create_table=True,
                                                 primary_keys = ['ts_code', 'trade_date'])
                all_data_count += data_count
                data_df_list, data_count = [], 0


    finally:
        if len(data_df_list) > 0:
            data_df_all = pd.concat(data_df_list)
            data_count = bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md,
                                                          DTYPE_TUSHARE_STOCK_INDEX_DAILYBASIC)
            all_data_count = all_data_count + data_count
            logging.info("更新 %s 结束 %d 条信息被更新", table_name, all_data_count)


if __name__ == "__main__":
    # DEBUG = True
    import_tushare_stock_index_dailybasic()


