"""
Created on 2018/10/30
@author: yby
@desc    : 2018-10-30
contact author:ybychem@gmail.com
"""

import pandas as pd
import logging
from tasks.backend.orm import build_primary_key
from datetime import date, datetime, timedelta
from tasks.utils.fh_utils import try_2_date, STR_FORMAT_DATE, datetime_2_str, split_chunk, try_n_times
from tasks import app
from sqlalchemy.types import String, Date, Integer, Text
from sqlalchemy.dialects.mysql import TINYINT, DOUBLE
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

INDICATOR_PARAM_LIST_TUSHARE_STOCK_EXPRESS = [
    ('ts_code', String(20)),
    ('ann_date', Date),
    ('end_date', Date),
    ('revenue', DOUBLE),
    ('operate_profit', DOUBLE),
    ('total_profit', DOUBLE),
    ('n_income', DOUBLE),
    ('total_assets', DOUBLE),
    ('total_hldr_eqy_exc_min_int', DOUBLE),
    ('diluted_eps', DOUBLE),
    ('diluted_roe', DOUBLE),
    ('yoy_net_profit', DOUBLE),
    ('bps', DOUBLE),
    ('yoy_sales', DOUBLE),
    ('yoy_op', DOUBLE),
    ('yoy_tp', DOUBLE),
    ('yoy_dedu_np', DOUBLE),
    ('yoy_eps', DOUBLE),
    ('yoy_roe', DOUBLE),
    ('growth_assets', DOUBLE),
    ('yoy_equity', DOUBLE),
    ('growth_bps', DOUBLE),
    ('or_last_year', DOUBLE),
    ('op_last_year', DOUBLE),
    ('tp_last_year', DOUBLE),
    ('np_last_year', DOUBLE),
    ('eps_last_year', DOUBLE),
    ('open_net_assets', DOUBLE),
    ('open_bps', DOUBLE),
    ('perf_summary', Text),
    ('is_audit', TINYINT),
    ('remark', Text),
]
# 设置 dtype
DTYPE_TUSHARE_STOCK_EXPRESS = {key: val for key, val in INDICATOR_PARAM_LIST_TUSHARE_STOCK_EXPRESS}


@try_n_times(times=5, sleep_time=2, logger=logger, exception=Exception, exception_sleep_time=5)
def invoke_express(ts_code, start_date, end_date):
    invoke_express = pro.express(ts_code=ts_code, start_date=start_date, end_date=end_date)
    return invoke_express


@app.task
def import_tushare_stock_express(chain_param=None, ts_code_set=None):
    """
    插入股票日线数据到最近一个工作日-1。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    :return:
    """
    table_name = 'tushare_stock_express'
    logging.info("更新 %s 开始", table_name)

    has_table = engine_md.has_table(table_name)
    # 进行表格判断，确定是否含有tushare_stock_daily
    if has_table:
        sql_str = """
            SELECT ts_code, date_frm, if(delist_date<end_date, delist_date, end_date) date_to
            FROM
            (
                SELECT info.ts_code, ifnull(ann_date, list_date) date_frm, delist_date,
                if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
                FROM 
                  tushare_stock_info info 
                LEFT OUTER JOIN
                    (SELECT ts_code, adddate(max(ann_date),1) ann_date 
                    FROM {table_name} GROUP BY ts_code) express
                ON info.ts_code = express.ts_code
            ) tt
            WHERE date_frm <= if(delist_date<end_date, delist_date, end_date) 
            ORDER BY ts_code""".format(table_name=table_name)
    else:
        sql_str = """
            SELECT ts_code, date_frm, if(delist_date<end_date, delist_date, end_date) date_to
            FROM
              (
                SELECT info.ts_code, list_date date_frm, delist_date,
                if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
                FROM tushare_stock_info info 
              ) tt
            WHERE date_frm <= if(delist_date<end_date, delist_date, end_date) 
            ORDER BY ts_code DESC """
        logger.warning('%s 不存在，仅使用 tushare_stock_info 表进行计算日期范围', table_name)
    # ts_code_set = None
    with with_db_session(engine_md) as session:
        # 获取每只股票需要获取日线数据的日期区间
        table = session.execute(sql_str)
        # 计算每只股票需要获取日线数据的日期区间
        begin_time = None
        # 获取date_from,date_to，将date_from,date_to做为value值
        code_date_range_dic = {
            ts_code: (date_from if begin_time is None else min([date_from, begin_time]), date_to)
            for ts_code, date_from, date_to in table.fetchall() if
            ts_code_set is None or ts_code in ts_code_set}

    data_df_list, data_count, all_data_count, data_len = [], 0, 0, len(code_date_range_dic)
    logger.info('%d stock balancesheets will been import into tushare_stock_express', data_len)
    # 将data_df数据，添加到data_df_list

    Cycles = 1
    try:
        for num, (ts_code, (date_from, date_to)) in enumerate(code_date_range_dic.items(), start=1):
            logger.debug('%d/%d) %s [%s - %s]', num, data_len, ts_code, date_from, date_to)
            data_df = invoke_express(ts_code=ts_code, start_date=datetime_2_str(date_from, STR_FORMAT_DATE_TS),
                                     end_date=datetime_2_str(date_to, STR_FORMAT_DATE_TS))

            if data_df is None:
                logger.warning('%d/%d) %s has no data during %s %s', num, data_len, ts_code, date_from, date_to)
                continue
            elif data_df is not None:
                logger.info('整体进度：%d/%d)， %d 条 %s 业绩快报数据被提取，起止时间为 %s 和 %s',
                            num, data_len, data_df.shape[0], ts_code, date_from, date_to)

            # 把数据攒起来
            if data_df is not None and data_df.shape[0] > 0:
                data_count += data_df.shape[0]
                data_df_list.append(data_df)

            # 大于阀值有开始插入
            if data_count >= 1000 and len(data_df_list) > 0:
                data_df_all = pd.concat(data_df_list)
                bunch_insert_on_duplicate_update(
                    data_df_all, table_name, engine_md, DTYPE_TUSHARE_STOCK_EXPRESS,
                    myisam_if_create_table=True, primary_keys=['ts_code', 'ann_date'],
                    schema=config.DB_SCHEMA_MD)
                logger.info('%d 条业绩快报数据被插入 %s 表', data_count, table_name)
                all_data_count += data_count
                data_df_list, data_count = [], 0
                # # 数据插入数据库
                # data_count = bunch_insert_on_duplicate_update(data_df, table_name, engine_md,
                #                                               DTYPE_TUSHARE_STOCK_BALABCESHEET)
                # logging.info("更新 %s 结束 %d 条信息被更新", table_name, data_count)
                # data_df = []
            # 仅调试使用
            Cycles = Cycles + 1
            if DEBUG and Cycles > 10:
                break
    finally:
        # 导入数据库
        if len(data_df_list) > 0:
            data_df_all = pd.concat(data_df_list)
            data_count = bunch_insert_on_duplicate_update(
                data_df_all, table_name, engine_md, DTYPE_TUSHARE_STOCK_EXPRESS,
                myisam_if_create_table=True, primary_keys=['ts_code', 'ann_date'],
                schema=config.DB_SCHEMA_MD)
            all_data_count = all_data_count + data_count
            logging.info("更新 %s 结束 %d 条业绩快报信息被更新", table_name, all_data_count)
            # if not has_table and engine_md.has_table(table_name):
            #     alter_table_2_myisam(engine_md, [table_name])
            #     build_primary_key([table_name])


if __name__ == "__main__":
    # DEBUG = True
    # import_tushare_stock_info(refresh=False)
    # 更新每日股票数据
    import_tushare_stock_express()

    # 去除重复数据用的
    # sql_str = """SELECT * FROM old_tushare_stock_balancesheet """
    # df=pd.read_sql(sql_str,engine_md)
    # #将数据插入新表
    # data_count = bunch_insert_on_duplicate_update(df, table_name, engine_md, dtype)
    # logging.info("更新 %s 结束 %d 条信息被更新", table_name, data_count)
