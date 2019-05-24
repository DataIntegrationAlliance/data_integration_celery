"""
Created on 2018/9/12
@author: yby
@desc    : 2018-09-13
contact author:ybychem@gmail.com
"""

import pandas as pd
import logging
from tasks.backend.orm import build_primary_key
from datetime import date, datetime, timedelta
from ibats_utils.mess import try_2_date, STR_FORMAT_DATE, datetime_2_str, split_chunk, try_n_times
from tasks import app
from sqlalchemy.types import String, Date, Integer, Text
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

INDICATOR_PARAM_LIST_TUSHARE_STOCK_TOP10_HOLDERS = [
    ('ts_code', String(20)),
    ('ann_date', Date),
    ('end_date', Date),
    ('holder_name', String(220)),
    ('hold_amount', DOUBLE),
    ('hold_ratio', DOUBLE),

]
# 设置 dtype
DTYPE_TUSHARE_STOCK_TOP10_HOLDERS = {key: val for key, val in INDICATOR_PARAM_LIST_TUSHARE_STOCK_TOP10_HOLDERS}


# dtype['ts_code'] = String(20)
# dtype['trade_date'] = Date

@try_n_times(times=5, sleep_time=2, logger=logger, exception=Exception, exception_sleep_time=5)
def invoke_top10_holders(ts_code, start_date, end_date):
    invoke_top10_holders = pro.top10_holders(ts_code=ts_code, start_date=start_date, end_date=end_date)
    return invoke_top10_holders


@app.task
def import_tushare_stock_top10_holders(ts_code_set=None, chain_param=None):
    """
    插入股票日线数据到最近一个工作日-1。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    :return:
    """
    table_name = 'tushare_stock_top10_holders'
    logging.info("更新 %s 开始", table_name)

    has_table = engine_md.has_table(table_name)
    # 进行表格判断，确定是否含有tushare_stock_daily
    if has_table:
        sql_str = """
               SELECT ts_code, date_frm, if(delist_date<end_date2, delist_date, end_date2) date_to
               FROM
               (
                   SELECT info.ts_code, ifnull(end_date, subdate(list_date,365*10)) date_frm, delist_date,
                   if(hour(now())<16, subdate(curdate(),1), curdate()) end_date2
                   FROM 
                     tushare_stock_info info 
                   LEFT OUTER JOIN
                       (SELECT ts_code, adddate(max(ann_date),1) end_date 
                       FROM {table_name} GROUP BY ts_code) top10_holders
                   ON info.ts_code = top10_holders.ts_code
               ) tt
               WHERE date_frm <= if(delist_date<end_date2, delist_date, end_date2) 
               ORDER BY ts_code""".format(table_name=table_name)
    else:
        sql_str = """
               SELECT ts_code, date_frm, if(delist_date<end_date2, delist_date, end_date2) date_to
               FROM
                 (
                   SELECT info.ts_code, subdate(list_date,365*10) date_frm, delist_date,
                   if(hour(now())<16, subdate(curdate(),1), curdate()) end_date2
                   FROM tushare_stock_info info 
                 ) tt
               WHERE date_frm <= if(delist_date<end_date2, delist_date, end_date2) 
               ORDER BY ts_code """
        logger.warning('%s 不存在，仅使用 tushare_stock_info 表进行计算日期范围', table_name)

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
    logger.info('%d data will been import into %s', data_len, table_name)
    # 将data_df数据，添加到data_df_list

    Cycles = 1
    try:
        for num, (ts_code, (date_from, date_to)) in enumerate(code_date_range_dic.items(), start=1):
            logger.debug('%d/%d) %s [%s - %s]', num, data_len, ts_code, date_from, date_to)
            data_df = invoke_top10_holders(
                ts_code=ts_code, start_date=datetime_2_str(date_from, STR_FORMAT_DATE_TS),
                end_date=datetime_2_str(date_to, STR_FORMAT_DATE_TS))
            if data_df is not None and len(data_df) > 0 and data_df['ann_date'].iloc[-1] is not None:
                last_date_in_df_last = try_2_date(data_df['ann_date'].iloc[-1])
                while last_date_in_df_last > date_from:
                    df2 = invoke_top10_holders(
                        ts_code=ts_code,
                        start_date=datetime_2_str(date_from, STR_FORMAT_DATE_TS),
                        end_date=datetime_2_str(last_date_in_df_last - timedelta(days=1),
                                                STR_FORMAT_DATE_TS))
                    if len(df2) > 0 and df2['ann_date'].iloc[-1] is not None:
                        last_date_in_df_cur = try_2_date(df2['ann_date'].iloc[-1])
                        if last_date_in_df_cur != last_date_in_df_last:
                            data_df = pd.concat([data_df, df2])
                            last_date_in_df_last = try_2_date(data_df['ann_date'].iloc[-1])
                        elif last_date_in_df_cur == last_date_in_df_last:
                            break

                    elif len(df2) > 0 and df2['ann_date'].iloc[-1] is None:
                        last_date_in_df_cur = try_2_date(df2['end_date'].iloc[-1])
                        if last_date_in_df_cur != last_date_in_df_last:
                            data_df = pd.concat([data_df, df2])
                            last_date_in_df_last = try_2_date(data_df['end_date'].iloc[-1])
                        elif last_date_in_df_cur == last_date_in_df_last:
                            break
                    else:
                        break
            if data_df is None:
                logger.warning('%d/%d) %s has no data during %s %s', num, data_len, ts_code, date_from, date_to)
            elif data_df is not None:
                logger.info('%d/%d) %d 条 %s 前10股东被提取，起止时间为 %s 和 %s', num, data_len, data_df.shape[0], ts_code,
                            date_from, date_to)
            # 把数据攒起来
            if data_df is not None and data_df.shape[0] > 0:
                data_count += data_df.shape[0]
                data_df_list.append(data_df)
            # 大于阀值有开始插入
            if data_count >= 500 and len(data_df_list) > 0:
                data_df_all = pd.concat(data_df_list)
                bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md, DTYPE_TUSHARE_STOCK_TOP10_HOLDERS)
                all_data_count += data_count
                data_df_list, data_count = [], 0
            # 仅调试使用
            Cycles = Cycles + 1
            if DEBUG and Cycles > 25:
                break
    finally:
        if len(data_df_list) > 0:
            data_df_all = pd.concat(data_df_list)
            data_count = bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md,
                                                          DTYPE_TUSHARE_STOCK_TOP10_HOLDERS)
            all_data_count = all_data_count + data_count
            logging.info("更新 %s 结束 %d 条信息被更新", table_name, all_data_count)


if __name__ == "__main__":
    # DEBUG = True
    # import_tushare_stock_info(refresh=False)
    # 更新每日股票数据
    # sql_str = """               SELECT ts_code, date_frm, if(delist_date<end_date2, delist_date, end_date2) date_to
    #                FROM
    #                  (
    #                    SELECT info.ts_code, subdate(list_date,365*10) date_frm, delist_date,
    #                    if(hour(now())<16, subdate(curdate(),1), curdate()) end_date2
    #                    FROM tushare_stock_info info
    #                  ) tt
    #                WHERE date_frm <= if(delist_date<end_date2, delist_date, end_date2) and ts_code>'600378.SH'
    #                ORDER BY ts_code"""
    # with with_db_session(engine_md) as session:
    #     # 获取交易日数据
    #     table = session.execute(sql_str)
    #     ts_code_set = list(row[0] for row in table.fetchall())

    import_tushare_stock_top10_holders()
