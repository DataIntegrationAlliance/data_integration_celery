"""
Created on 2018/9/29
@author: yby
@desc    : 2018-09-29
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

INDICATOR_PARAM_LIST_TUSHARE_STOCK_TOP_LIST = [
    ('trade_date', Date),
    ('ts_code', String(20)),
    ('name', String(20)),
    ('close', DOUBLE),
    ('pct_change', DOUBLE),
    ('turnover_rate', DOUBLE),
    ('amount', DOUBLE),
    ('l_sell', DOUBLE),
    ('l_buy', DOUBLE),
    ('l_amount', DOUBLE),
    ('net_amount', DOUBLE),
    ('net_rate', DOUBLE),
    ('amount_rate', DOUBLE),
    ('float_values', DOUBLE),
    ('reason', String(1000)),
]
# 设置 dtype
DTYPE_TUSHARE_STOCK_TOP_LIST = {key: val for key, val in INDICATOR_PARAM_LIST_TUSHARE_STOCK_TOP_LIST}


@try_n_times(times=3, sleep_time=1, logger=logger, exception=Exception, exception_sleep_time=5)
def invoke_top_list(trade_date):
    invoke_top_list = pro.top_list(trade_date=trade_date)
    return invoke_top_list


@app.task
def import_tushare_top_list(chain_param=None):
    """
    插入股票日线数据到最近一个工作日-1。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    :return:
    """
    table_name = 'tushare_stock_top_list'
    logging.info("更新 %s 开始", table_name)

    has_table = engine_md.has_table(table_name)
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
            AND exchange='SSE'
            and cal_date>'2005-05-31') ORDER BY cal_date"""
        logger.warning('%s 不存在，仅使用 tushare_trade_date 表进行计算日期范围', table_name)

    with with_db_session(engine_md) as session:
        # 获取交易日数据
        table = session.execute(sql_str)
        trddate = list(row[0] for row in table.fetchall())

    #定义相应的中间变量
    data_df_list, data_count, all_data_count, data_len = [], 0, 0, len(trddate)
    try:
        for i in range(len(trddate)):
            trade_date = datetime_2_str(trddate[i], STR_FORMAT_DATE_TS)
            data_df = invoke_top_list(trade_date=trade_date)
            # 把数据攒起来
            if data_df is not None and data_df.shape[0] > 0:
                data_count += data_df.shape[0]
                data_df_list.append(data_df)
            # 大于阀值有开始插入
            if data_count >= 2000:
                data_df_all = pd.concat(data_df_list)
                bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md, DTYPE_TUSHARE_STOCK_TOP_LIST)
                logging.info("更新 %s 结束 ,截至%s日 %d 条信息被更新", table_name, trade_date, all_data_count)
                all_data_count += data_count
                data_df_list, data_count = [], 0
    finally:
        if len(data_df_list) > 0:
            data_df_all = pd.concat(data_df_list)
            data_count = bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md, DTYPE_TUSHARE_STOCK_TOP_LIST)
            all_data_count=all_data_count+data_count
            logging.info("更新 %s 结束 %d 条信息被更新", table_name, all_data_count)
        if not has_table and engine_md.has_table(table_name):
            alter_table_2_myisam(engine_md, [table_name])
            # build_primary_key([table_name])
            create_pk_str = """ALTER TABLE {table_name}
                CHANGE COLUMN `ts_code` `ts_code` VARCHAR(20) NOT NULL FIRST,
                CHANGE COLUMN `trade_date` `trade_date` DATE NOT NULL AFTER `ts_code`,
                ADD PRIMARY KEY (`ts_code`, `trade_date`)""".format(table_name=table_name)
            with with_db_session(engine_md) as session:
                session.execute(create_pk_str)
            logger.info('%s 表 `ts_code`, `trade_date` 主键设置完成', table_name)


if __name__ == "__main__":
    # DEBUG = True
    import_tushare_top_list()


# 下面代码是生成fields和par的
# sub=pd.read_excel('tasks/tushare/tushare_fina_reports/fina_indicator.xlsx',header=0)[['code','types']]
# for a, b in [tuple(x) for x in sub.values]:
#     print("('%s', %s)," % (a, b))
#     # print("'%s'," % (a))
