"""
Created on 2018/10/31
@author: yby
@desc    : 2018-10-31
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

INDICATOR_PARAM_LIST_TUSHARE_REPURCHASE = [
    ('ts_code', String(20)),
    ('ann_date', Date),
    ('end_date', Date),
    ('proc', String(200)),
    ('exp_date', Date),
    ('vol', DOUBLE),
    ('amount', DOUBLE),
    ('high_limit', DOUBLE),
    ('low_limit', DOUBLE),

]
# 设置 dtype
DTYPE_TUSHARE_REPURCHASE = {key: val for key, val in INDICATOR_PARAM_LIST_TUSHARE_REPURCHASE}


@try_n_times(times=5, sleep_time=3, logger=logger, exception_sleep_time=0.5)
def invoke_repurchase(ann_date):
    invoke_repurchase = pro.repurchase(ann_date=ann_date)
    return invoke_repurchase


@app.task
def import_repurchase(chain_param=None):
    """
    插入股票日线数据到最近一个工作日-1。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    :return:
    """
    table_name = 'tushare_stock_repurchase'
    logging.info("更新 %s 开始", table_name)

    has_table = engine_md.has_table(table_name)
    # 下面一定要注意引用表的来源，否则可能是串，提取混乱！！！比如本表是tushare_daily_basic，所以引用的也是这个，如果引用错误，就全部乱了l
    if has_table:
        sql_str = """
               select * from 
                (select * from tushare_trade_date trddate 
                 where (cal_date>(SELECT max(ann_date) FROM {table_name} ))
               )tt
               where (is_open=1 
                      and cal_date <= if(hour(now())<16, subdate(curdate(),1), curdate()) 
                      and exchange_id='SSE') """.format(table_name=table_name)
    else:
        sql_str = """
               SELECT cal_date FROM tushare_trade_date trddate WHERE (trddate.is_open=1 
            AND cal_date <= if(hour(now())<16, subdate(curdate(),1), curdate()) 
            AND exchange_id='SSE') 
            AND cal_date>'20120605'
            ORDER BY cal_date"""
        logger.warning('%s 不存在，仅使用 tushare_stock_info 表进行计算日期范围', table_name)

    with with_db_session(engine_md) as session:
        # 获取交易日数据
        table = session.execute(sql_str)
        ann_date_list = list(row[0] for row in table.fetchall())
    logging.info("%d 个交易日的回购信息将被更新", len(ann_date_list))
    data_df_list, data_count, all_data_count, data_len = [], 0, 0, len(ann_date_list)
    try:
        for i in range(len(ann_date_list)):
            ann_date = datetime_2_str(ann_date_list[i], STR_FORMAT_DATE_TS)
            data_df = invoke_repurchase(ann_date=ann_date)
            if data_df is not None and data_df.shape[0]>0:
                logging.info("提取%s日%d条回购信息", ann_date,data_df.shape[0])
            else:
                logging.info("%s日无股票回购公告", ann_date)
            # 把数据攒起来
            if data_df is not None and data_df.shape[0] > 0:
                data_count += data_df.shape[0]
                data_df_list.append(data_df)

            # 大于阀值有开始插入
            if data_count >= 1000:
                data_df_all = pd.concat(data_df_list)
                bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md, DTYPE_TUSHARE_REPURCHASE)
                logger.info('%d 条股票回购信息被插入 tushare_stock_repurchase 表', data_count)
                all_data_count += data_count
                data_df_list, data_count = [], 0

    finally:
        if len(data_df_list) > 0:
            data_df_all = pd.concat(data_df_list)
            data_count = bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md, DTYPE_TUSHARE_REPURCHASE)
            all_data_count = all_data_count + data_count
            logging.info("更新 %s 结束 %d 条回购信息被更新", table_name, all_data_count)


if __name__ == "__main__":
    # DEBUG = True
    import_repurchase()


# sql_str="""select * from old_tushare_stock_repurchase"""
# df=pd.read_sql(sql_str,engine_md)
# data_count=bunch_insert_on_duplicate_update(df,table_name,engine_md,DTYPE_TUSHARE_REPURCHASE)
# logging.info("更新 %s 结束 %d 条信息被更新", table_name, data_count)
