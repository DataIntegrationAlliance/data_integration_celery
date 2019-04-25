"""
Created on 2018/8/13
@author: yby
@desc    : 2018-08-3
contact author:ybychem@gmail.com
"""
import logging
from datetime import date, datetime, timedelta
from ibats_utils.mess import try_2_date, STR_FORMAT_DATE, datetime_2_str, split_chunk, try_n_times
from tasks import app, config
from sqlalchemy.types import String, Date, Integer
from sqlalchemy.dialects.mysql import DOUBLE
from tasks.backend import engine_md
from ibats_utils.db import with_db_session
from tasks.backend import bunch_insert
from tasks.tushare.ts_pro_api import pro, check_sqlite_db_primary_keys

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
    ('turnover_rate_f', DOUBLE),
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


@try_n_times(times=3, sleep_time=1)
def invoke_daily_basic(ts_code, trade_date):
    df = pro.daily_basic(ts_code=ts_code, trade_date=trade_date)
    return df


@app.task
def import_tushare_daily_basic(chain_param=None):
    """
    插入股票日线数据到最近一个工作日-1。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    :return:
    """
    table_name = 'tushare_stock_daily_basic'
    primary_keys = ["ts_code", "trade_date"]
    logging.info("更新 %s 开始", table_name)
    check_sqlite_db_primary_keys(table_name, primary_keys)
    has_table = engine_md.has_table(table_name)
    # 下面一定要注意引用表的来源，否则可能是串，提取混乱！！！
    # 比如本表是 tushare_daily_basic，所以引用的也是这个，如果引用错误，就全部乱了
    if has_table:
        sql_str = """
               select cal_date            
               FROM
                (
                 select * from tushare_trade_date trddate 
                 where( cal_date>(SELECT max(trade_date) FROM {table_name} ))
               )tt
               where (is_open=1 
                      and cal_date <= if(hour(now())<16, subdate(curdate(),1), curdate()) 
                      and exchange='SSE') """.format(table_name=table_name)
    else:
        sql_str = """
               SELECT cal_date FROM tushare_trade_date trddate WHERE (trddate.is_open=1 
            AND cal_date <= if(hour(now())<16, subdate(curdate(),1), curdate()) 
            AND exchange='SSE') ORDER BY cal_date"""
        logger.warning('%s 不存在，仅使用 tushare_stock_info 表进行计算日期范围', table_name)

    with with_db_session(engine_md) as session:
        # 获取交易日数据
        table = session.execute(sql_str)
        trade_date_list = list(row[0] for row in table.fetchall())

    try:
        for_count = len(trade_date_list)
        for num, trade_date in enumerate(trade_date_list, start=1):
            trade_date = datetime_2_str(trade_date, STR_FORMAT_DATE_TS)
            data_df = invoke_daily_basic(ts_code='', trade_date=trade_date)
            if data_df is not None and data_df.shape[0] > 0:
                data_count = bunch_insert(
                    data_df, table_name=table_name, dtype=DTYPE_TUSHARE_STOCK_DAILY_BASIC,
                    primary_keys=primary_keys)

                logging.info("%d/%d) %s 更新 %s 结束 %d 条信息被更新", num, for_count, trade_date, table_name, data_count)
            else:
                logging.info("%d/%d) %s 无数据信息可被更新",  num, for_count, trade_date)
    except:
        logger.exception("更新 %s 表异常", table_name)


if __name__ == "__main__":
    # DEBUG = True
    import_tushare_daily_basic()
