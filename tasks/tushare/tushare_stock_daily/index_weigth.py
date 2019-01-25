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
from tasks.utils.fh_utils import try_2_date, STR_FORMAT_DATE, datetime_2_str, split_chunk, try_n_times, date_2_str
from tasks import app
from sqlalchemy.types import String, Date, Integer
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

# df = pro.index_weight(index_code='399300.SZ',trade_date='20180911')
INDICATOR_PARAM_LIST_TUSHARE_STOCK_INDEX_WEIGHT = [
    ('index_code', String(20)),
    ('con_code', String(20)),
    ('trade_date', Date),
    ('weight', DOUBLE),
]
# 设置 dtype
DTYPE_TUSHARE_STOCK_INDEX_WEIGHT = {key: val for key, val in INDICATOR_PARAM_LIST_TUSHARE_STOCK_INDEX_WEIGHT}


# DTYPE_TUSHARE_STOCK_INDEX_WEIGHT['ts_code'] = String(20)
# DTYPE_TUSHARE_STOCK_INDEX_DAILY_MD['trade_date'] = Date


@try_n_times(times=5, sleep_time=1, logger=logger, exception_sleep_time=5)
def invoke_index_weight(index_code, trade_date):
    trade_date = date_2_str(trade_date, STR_FORMAT_DATE_TS)
    invoke_index_weight = pro.index_weight(index_code=index_code, trade_date=trade_date)
    return invoke_index_weight


@app.task
def import_tushare_stock_index_weight(chain_param=None, ts_code_set=None):
    """
    插入股票日线数据到最近一个工作日-1。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    :return:
    """
    table_name = 'tushare_stock_index_weight'
    logging.info("更新 %s 开始", table_name)

    has_table = engine_md.has_table(table_name)
    # 进行表格判断，确定是否含有tushare_stock_daily

    sql_str = """SELECT ts_code index_code,trade_date trade_date_list FROM tushare_stock_index_daily_md """
    with with_db_session(engine_md) as session:
        # 获取每只股票需要获取日线数据的日期区间
        table = session.execute(sql_str)
        code_date_range_dic = {}
        # for ts_code, trade_date in table.fetchall():
        #     code_date_range_dic.setdefault(ts_code, []).append(trade_date)
        for index_code, trade_date_list in table.fetchall():
            code_date_range_dic.setdefault(index_code, []).append(trade_date_list)

    data_df_list, data_count, all_data_count, data_len = [], 0, 0, len(code_date_range_dic)
    logger.info('%d index weight will been import into tushare_stock_index_weight table', data_len)
    # 将data_df数据，添加到data_df_list
    Cycles = 1
    try:
        for num, (index_code, trade_date_list) in enumerate(code_date_range_dic.items(), start=1):
            trade_date_list_len = len(trade_date_list)
            for i, trade_date in enumerate(trade_date_list):
                # trade_date=trade_date_list[i]
                logger.debug('%d/%d) %d/%d) %s [%s]', num, data_len, i, trade_date_list_len, index_code, trade_date)
                data_df = invoke_index_weight(index_code=index_code, trade_date=trade_date)
                # 把数据攒起来
                if data_df is not None and data_df.shape[0] > 0:
                    data_count += data_df.shape[0]
                    data_df_list.append(data_df)
                # 大于阀值有开始插入
                if data_count >= 10000:
                    data_df_all = pd.concat(data_df_list)
                    bunch_insert_on_duplicate_update(
                        data_df_all, table_name, engine_md, DTYPE_TUSHARE_STOCK_INDEX_WEIGHT,
                        myisam_if_create_table=True, primary_keys=['index_code', 'con_code', 'trade_date'],
                        schema=config.DB_SCHEMA_MD)
                    logging.info("正在更新 %s 表， %d 条指数成分信息被更新", table_name, data_count)
                    all_data_count += data_count
                    data_df_list, data_count = [], 0
                # if len(data_df) > 0:
                #     data_count = bunch_insert_on_duplicate_update(data_df, table_name, engine_md,
                #                                                   DTYPE_TUSHARE_STOCK_INDEX_WEIGHT)
                #     logging.info("%s 更新 %s  %d 条信息被更新", trade_date, table_name, data_count)
                # else:
                #     break
            Cycles = Cycles + 1
            if DEBUG and Cycles > 10:
                break
    finally:
        # 导入数据库
        if len(data_df_list) > 0:
            data_df_all = pd.concat(data_df_list)
            data_count = bunch_insert_on_duplicate_update(
                data_df_all, table_name, engine_md, DTYPE_TUSHARE_STOCK_INDEX_WEIGHT,
                myisam_if_create_table=True, primary_keys=['index_code', 'con_code', 'trade_date'],
                schema=config.DB_SCHEMA_MD)
            logging.info("更新 %s 结束 %d 条指数成分信息被更新", table_name, data_count)


if __name__ == "__main__":
    # DEBUG = True
    # import_tushare_stock_info(refresh=False)
    # 更新每日股票数据
    # SQL = """SELECT ts_code FROM tushare_stock_info where ts_code>'603033.SH'"""
    # with with_db_session(engine_md) as session:
    #     # 获取每只股票需要获取日线数据的日期区间
    #     table = session.execute(SQL)
    #     ts_code_set = list([row[0] for row in table.fetchall()])
    ts_code_set = list(['399300.SZ', '399905.SZ', '399906.SZ', '000852.SH', '000016.SH'])
    import_tushare_stock_index_weight(ts_code_set=None)

    # import_stock_daily_wch()
    # wind_code_set = None
    # add_new_col_data('ebitdaps', '',wind_code_set=wind_code_set)

    # for stock_num, (ts_code, trade_date) in enumerate(code_date_range_dic.items()):
    #     print(stock_num, (ts_code, trade_date))
