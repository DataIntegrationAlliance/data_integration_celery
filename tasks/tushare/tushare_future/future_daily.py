"""
Created on 2018/9/3
@author: yby
@desc    : 2018-09-3
contact author:ybychem@gmail.com
"""


from tasks.tushare.ts_pro_api import pro
import pandas as pd
import logging
from tasks.backend.orm import build_primary_key
from datetime import date, datetime, timedelta
from ibats_utils.mess import try_2_date, STR_FORMAT_DATE, datetime_2_str, split_chunk, try_n_times
from tasks import app
from sqlalchemy.types import String, Date, Integer
from sqlalchemy.dialects.mysql import DOUBLE
from tasks.backend import engine_md
from tasks.merge.code_mapping import update_from_info_table
from ibats_utils.db import with_db_session, add_col_2_table, alter_table_2_myisam, \
    bunch_insert_on_duplicate_update

DEBUG = False
logger = logging.getLogger()
DATE_BASE = datetime.strptime('2005-01-01', STR_FORMAT_DATE).date()
ONE_DAY = timedelta(days=1)
# 标示每天几点以后下载当日行情数据
BASE_LINE_HOUR = 16
STR_FORMAT_DATE_TS = '%Y%m%d'

INDICATOR_PARAM_LIST_TUSHARE_FUTURE_DAILY_MD = [
    ('ts_code', String(20)),
    ('trade_date', Date),
    ('pre_close', DOUBLE),
    ('pre_settle', DOUBLE),
    ('open', DOUBLE),
    ('high', DOUBLE),
    ('low', DOUBLE),
    ('close', DOUBLE),
    ('settle', DOUBLE),
    ('change1', DOUBLE),
    ('change2', DOUBLE),
    ('vol', DOUBLE),
    ('amount', DOUBLE),
    ('oi', DOUBLE),
    ('oi_chg', DOUBLE),
    ('delv_settle', DOUBLE),
]
# 设置 dtype
DTYPE_TUSHARE_FUTURE_DAILY_MD = {key: val for key, val in INDICATOR_PARAM_LIST_TUSHARE_FUTURE_DAILY_MD}


# df=pro.fut_daily(trade_date='20181113')
@try_n_times(times=5, sleep_time=1, logger=logger, exception_sleep_time=60)
def invoke_future_daily(trade_date,fields):
    invoke_future_daily = pro.fut_daily(trade_date=trade_date,fields=fields)
    return invoke_future_daily


@app.task
def import_tushare_future_daily(chain_param=None, ts_code_set=None):
    """
    插入股票日线数据到最近一个工作日-1。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    :return:
    """
    table_name = 'tushare_future_daily_md'
    logging.info("更新 %s 开始", table_name)

    has_table = engine_md.has_table(table_name)
    # 进行表格判断，确定是否含有tushare_stock_daily
    if has_table:
        sql_str = """
                  select cal_date            
                  FROM
                   (
                    select * from tushare_future_trade_cal trddate 
                    where( cal_date>(SELECT max(trade_date) FROM  {table_name}))
                  )tt
                  where (is_open=1 
                         and cal_date <= if(hour(now())<16, subdate(curdate(),1), curdate()) 
                         ) """.format(table_name=table_name)
    else:
        sql_str = """
                    SELECT cal_date FROM tushare_future_trade_cal trddate WHERE (trddate.is_open=1 
               AND cal_date <= if(hour(now())<16, subdate(curdate(),1), curdate()) 
               AND cal_date>'19950414') ORDER BY cal_date"""
        logger.warning('%s 不存在，仅使用 tushare_stock_info 表进行计算日期范围', table_name)

    with with_db_session(engine_md) as session:
        # 获取交易日数据
        table = session.execute(sql_str)
        trddate = list(row[0] for row in table.fetchall())

    # data_len = len(code_date_range_dic)
    data_df_list, data_count, all_data_count, data_len = [], 0, 0, len(trddate)
    logger.info('%d stocks will been import into tushare_stock_daily_md', data_len)
    # 将data_df数据，添加到data_df_list
    fields='ts_code,trade_date,pre_close,pre_settle,open,high,low,close,settle,change1,change2,vol,amount,oi,oi_chg,delv_settle,delv_settle'
    try:
        for i in range(len(trddate)):
            trade_date = datetime_2_str(trddate[i], STR_FORMAT_DATE_TS)
            data_df = invoke_future_daily(trade_date=trade_date,fields=fields)
            logging.info(" 提取 %s 日 %d 条期货行情数据", trade_date, data_df.shape[0])

            # 把数据攒起来
            if data_df is not None and data_df.shape[0] > 0:
                data_count += data_df.shape[0]
                data_df_list.append(data_df)

            # 大于阀值有开始插入
            if data_count >= 1000:
                data_df_all = pd.concat(data_df_list)
                bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md, DTYPE_TUSHARE_FUTURE_DAILY_MD)
                logging.info(" 更新%s表%d条期货行情数据", table_name, data_count)
                all_data_count += data_count
                data_df_list, data_count = [], 0


    finally:
        # 导入数据库
        if len(data_df_list) > 0:
            data_df_all = pd.concat(data_df_list)
            data_count = bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md,
                                                          DTYPE_TUSHARE_FUTURE_DAILY_MD)
            all_data_count = all_data_count + data_count
            logging.info("更新 %s 结束 %d 条信息被更新", table_name, all_data_count)



if __name__ == "__main__":
    import_tushare_future_daily(ts_code_set=None)

