"""
Created on 2018/9/3
@author: yby
@desc    : 2018-09-3
contact author:ybychem@gmail.com
"""
import pandas as pd
import logging
from tasks.backend.orm import build_primary_key
from datetime import date, datetime, timedelta
from tasks.utils.fh_utils import try_2_date,STR_FORMAT_DATE,datetime_2_str,split_chunk,try_n_times
from tasks import app
from sqlalchemy.types import String, Date, Integer,Text
from sqlalchemy.dialects.mysql import DOUBLE
from tasks.backend import engine_md
from tasks.merge.code_mapping import update_from_info_table
from tasks.utils.db_utils import with_db_session, add_col_2_table, alter_table_2_myisam, \
    bunch_insert_on_duplicate_update
from tasks.tushare import pro

DEBUG = False
logger = logging.getLogger()
DATE_BASE = datetime.strptime('2005-01-01', STR_FORMAT_DATE).date()
ONE_DAY = timedelta(days=1)
# 标示每天几点以后下载当日行情数据
BASE_LINE_HOUR = 16
STR_FORMAT_DATE_TS = '%Y%m%d'

@try_n_times(times=5, sleep_time=0,exception_sleep_time=60)
def invoke_fina_audit(ts_code,start_date,end_date):
    invoke_fina_audit=pro.fina_audit(ts_code=ts_code,start_date=start_date,end_date=end_date)
    return invoke_fina_audit


@app.task
def import_tushare_stock_fina_audit(chain_param=None,ts_code_set=None):
    """
    插入股票日线数据到最近一个工作日-1。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    :return:
    """
    table_name = 'tushare_stock_fin_audit'
    logging.info("更新 %s 开始", table_name)
    param_list = [
        ('ts_code', String(20)),
        ('ann_date', Date),
        ('end_date', Date),
        ('audit_result', Text),
        ('audit_fees', DOUBLE),
        ('audit_agency',  String(100)),
        ('audit_sign', String(100)),
    ]

    has_table = engine_md.has_table(table_name)
    # 进行表格判断，确定是否含有tushare_stock_daily
    if has_table:
        sql_str = """
            SELECT ts_code, date_frm, if(delist_date<end_date, delist_date, end_date) date_to
            FROM
            (
                SELECT info.ts_code, ifnull(ann_date, subdate(list_date,365*8)) date_frm, delist_date,
                if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
                FROM 
                  tushare_stock_info info 
                LEFT OUTER JOIN
                    (SELECT ts_code, adddate(max(ann_date),1) ann_date 
                    FROM {table_name} GROUP BY ts_code) fina_audit
                ON info.ts_code = fina_audit.ts_code
            ) tt
            WHERE date_frm <= if(delist_date<end_date, delist_date, end_date) 
            ORDER BY ts_code""".format(table_name=table_name)
    else:
        sql_str = """
            SELECT ts_code, date_frm, if(delist_date<end_date, delist_date, end_date) date_to
            FROM
              (
                SELECT info.ts_code, subdate(list_date,365*10) date_frm, delist_date,
                if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
                FROM tushare_stock_info info 
              ) tt
            WHERE date_frm <= if(delist_date<end_date, delist_date, end_date) 
            ORDER BY ts_code DESC """
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
    # 设置 dtype
    dtype = {key: val for key, val in param_list}

    data_len = len(code_date_range_dic)
    logger.info('%d stocks will been import into wind_stock_daily', data_len)

    Cycles=1
    try:
        for num, (ts_code, (date_from, date_to)) in enumerate(code_date_range_dic.items(), start=1):
            logger.debug('%d/%d) %s [%s - %s]', num, data_len,ts_code, date_from, date_to)
            df = invoke_fina_audit(ts_code=ts_code, start_date=datetime_2_str(date_from,STR_FORMAT_DATE_TS),end_date=datetime_2_str(date_to,STR_FORMAT_DATE_TS))
            data_df=df
            if len(data_df)>0:
                while try_2_date(df['ann_date'].iloc[-1]) > date_from:
                    last_date_in_df_last, last_date_in_df_cur = try_2_date(df['ann_date'].iloc[-1]), None
                    df2 = invoke_fina_audit(ts_code=ts_code,start_date=datetime_2_str(date_from,STR_FORMAT_DATE_TS),
                                    end_date=datetime_2_str(try_2_date(df['ann_date'].iloc[-1])-timedelta(days=1),STR_FORMAT_DATE_TS))
                    if len(df2) > 0:
                        last_date_in_df_cur = try_2_date(df2['ann_date'].iloc[-1])
                        if last_date_in_df_cur<last_date_in_df_last:
                            data_df = pd.concat([data_df, df2])
                            df = df2
                        elif last_date_in_df_cur==last_date_in_df_last:
                            break
                        if data_df is None:
                            logger.warning('%d/%d) %s has no data during %s %s', num, data_len, ts_code, date_from, date_to)
                            continue
                        logger.info('%d/%d) %d data of %s between %s and %s', num, data_len, data_df.shape[0], ts_code, date_from,date_to)
                    elif len(df2) <= 0:
                        break
                #数据插入数据库
                data_df_all = data_df
                data_count = bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md, dtype)
                logging.info("成功更新 %s 结束 %d 条信息被更新", table_name, data_count)

            #仅调试使用
            Cycles=Cycles+1
            if DEBUG and Cycles > 10:
                break
    finally:
        # 导入数据库
        if len(data_df) > 0:
            data_df_all = data_df
            data_count = bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md, dtype)
            logging.info("成功更新 %s 结束 %d 条信息被更新", table_name, data_count)




if __name__ == "__main__":
    #DEBUG = True
    #import_tushare_stock_info(refresh=False)
    # # 更新每日股票数据
    # SQL = """SELECT ts_code FROM md_integration.tushare_stock_info where ts_code>'600267.SH'"""
    # with with_db_session(engine_md) as session:
    #     # 获取每只股票需要获取日线数据的日期区间
    #     table = session.execute(SQL)
    #     ts_code_set = list([row[0] for row in table.fetchall()])
    import_tushare_stock_fina_audit(ts_code_set=None)

