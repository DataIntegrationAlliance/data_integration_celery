"""
Created on 2018/8/14
@author: yby
@desc    : 2018-08-21 已经正式运行测试完成，可以正常使用
"""
from tasks.tushare.ts_pro_api import pro
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
from tasks.config import config
from tasks.utils.db_utils import with_db_session, add_col_2_table, alter_table_2_myisam, \
    bunch_insert_on_duplicate_update

DEBUG = False
logger = logging.getLogger()
DATE_BASE = datetime.strptime('2005-01-01', STR_FORMAT_DATE).date()
ONE_DAY = timedelta(days=1)
# 标示每天几点以后下载当日行情数据
BASE_LINE_HOUR = 16
STR_FORMAT_DATE_TS = '%Y%m%d'

INDICATOR_PARAM_LIST_TUSHARE_STOCK_DAILY_MD = [
    ('ts_code', String(20)),
    ('trade_date', Date),
    ('open', DOUBLE),
    ('high', DOUBLE),
    ('low', DOUBLE),
    ('close', DOUBLE),
    ('pre_close', DOUBLE),
    ('change', DOUBLE),
    ('pct_change', DOUBLE),
    ('vol', DOUBLE),
    ('amount', DOUBLE),
]
# 设置 dtype
DTYPE_TUSHARE_STOCK_DAILY_MD = {key: val for key, val in INDICATOR_PARAM_LIST_TUSHARE_STOCK_DAILY_MD}
DTYPE_TUSHARE_STOCK_DAILY_MD['ts_code'] = String(20)
DTYPE_TUSHARE_STOCK_DAILY_MD['trade_date'] = Date


@try_n_times(times=5, sleep_time=0, logger=logger, exception_sleep_time=60)
def invoke_daily(ts_code, start_date, end_date):
    invoke_daily = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
    return invoke_daily


def get_stock_code_set():
    """
     # 通过接口获取股票代码
    :param date_fetch:
    :return:
    """
    # date_fetch_str = date_fetch.strftime(STR_FORMAT_DATE)
    stock_df = pro.stock_basic(exchange='', is_hs='S', fields='ts_code,name')
    if stock_df is None:
        # logging.warning('%s 获取股票代码失败', date_fetch_str)
        return None
    stock_count = stock_df.shape[0]
    # logging.info('get %d stocks on %s', stock_count, date_fetch_str)
    return set(stock_df['ts_code'])


@app.task
def import_tushare_stock_info(chain_param=None, refresh=False):
    """ 获取全市场股票代码及名称
    """
    table_name = 'tushare_stock_info'
    logging.info("更新 %s 开始", table_name)
    has_table = engine_md.has_table(table_name)
    tushare_indicator_param_list = [
        ('ts_code', String(20)),
        ('symbol', String(20)),
        ('name', String(40)),
        ('area', String(100)),
        ('industry', String(200)),
        ('fullname', String(100)),
        ('enname', String(200)),
        ('market', String(100)),
        ('exchange', String(20)),
        ('curr_type', String(20)),
        ('list_status', String(20)),
        ('list_date', Date),
        ('delist_date', Date),
        ('is_hs', String(20)),
    ]
    #     # 获取列属性名，以逗号进行分割 "ipo_date,trade_code,mkt,exch_city,exch_eng"
    param = ",".join([key for key, _ in tushare_indicator_param_list])
    # 设置 dtype
    dtype = {key: val for key, val in tushare_indicator_param_list}
    dtype['ts_code'] = String(20)

    # 数据提取

    stock_info_all_df = pro.stock_basic(exchange='',
                                        fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs,is_hs,is_hs')

    logging.info('%s stock data will be import', stock_info_all_df.shape[0])
    data_count = bunch_insert_on_duplicate_update(
        stock_info_all_df, table_name, engine_md, dtype=dtype,
        myisam_if_create_table=True, primary_keys=['ts_code'], schema=config.DB_SCHEMA_MD)
    logging.info("更新 %s 完成 存量数据 %d 条", table_name, data_count)

    # 更新 code_mapping 表
    # update_from_info_table(table_name)


@app.task
def import_tushare_stock_daily(chain_param=None, ts_code_set=None):
    """
    插入股票日线数据到最近一个工作日-1。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    :return:
    """
    table_name = 'tushare_stock_daily_md'
    logging.info("更新 %s 开始", table_name)

    has_table = engine_md.has_table(table_name)
    # 进行表格判断，确定是否含有tushare_stock_daily
    if has_table:
        sql_str = """
            SELECT ts_code, date_frm, if(delist_date<end_date, delist_date, end_date) date_to
            FROM
            (
            SELECT info.ts_code, ifnull(trade_date, list_date) date_frm, delist_date,
            if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
            FROM 
                tushare_stock_info info 
            LEFT OUTER JOIN
                (SELECT ts_code, adddate(max(trade_date),1) trade_date FROM {table_name} GROUP BY ts_code) daily
            ON info.ts_code = daily.ts_code
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
            ORDER BY ts_code"""
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

    # data_len = len(code_date_range_dic)
    data_df_list, data_count, all_data_count, data_len = [], 0, 0, len(code_date_range_dic)
    logger.info('%d stocks will been import into tushare_stock_daily_md', data_len)
    # 将data_df数据，添加到data_df_list

    try:
        for num, (ts_code, (date_from, date_to)) in enumerate(code_date_range_dic.items(), start=1):
            logger.debug('%d/%d) %s [%s - %s]', num, data_len, ts_code, date_from, date_to)
            data_df = invoke_daily(ts_code=ts_code, start_date=datetime_2_str(date_from, STR_FORMAT_DATE_TS),
                                   end_date=datetime_2_str(date_to, STR_FORMAT_DATE_TS))
            # data_df = df
            if len(data_df) > 0:
                while try_2_date(data_df['trade_date'].iloc[-1]) > date_from:
                    last_date_in_df_last, last_date_in_df_cur = try_2_date(data_df['trade_date'].iloc[-1]), None
                    df2 = invoke_daily(ts_code=ts_code, start_date=datetime_2_str(date_from, STR_FORMAT_DATE_TS),
                                       end_date=datetime_2_str(
                                           try_2_date(data_df['trade_date'].iloc[-1]) - timedelta(days=1),
                                           STR_FORMAT_DATE_TS))
                    if len(df2 > 0):
                        last_date_in_df_cur = try_2_date(df2['trade_date'].iloc[-1])
                        if last_date_in_df_cur < last_date_in_df_last:
                            data_df = pd.concat([data_df, df2])
                            # df = df2
                        elif last_date_in_df_cur == last_date_in_df_last:
                            break
                        if data_df is None:
                            logger.warning('%d/%d) %s has no data during %s %s', num, data_len, ts_code, date_from,
                                           date_to)
                            continue
                        logger.info('%d/%d) %d data of %s between %s and %s', num, data_len, data_df.shape[0], ts_code,
                                    date_from, date_to)
                    else:
                        break
            # 把数据攒起来
            if data_df is not None and data_df.shape[0] > 0:
                data_count += data_df.shape[0]
                data_df_list.append(data_df)

            # 大于阀值有开始插入
            if data_count >= 500:
                data_df_all = pd.concat(data_df_list)
                bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md, DTYPE_TUSHARE_STOCK_DAILY_MD)
                all_data_count += data_count
                data_df_list, data_count = [], 0


    finally:
        # 导入数据库
        if len(data_df_list) > 0:
            data_df_all = pd.concat(data_df_list)
            data_count = bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md,
                                                          DTYPE_TUSHARE_STOCK_DAILY_MD)
            all_data_count = all_data_count + data_count
            logging.info("更新 %s 结束 %d 条信息被更新", table_name, all_data_count)
            if not has_table and engine_md.has_table(table_name):
                alter_table_2_myisam(engine_md, [table_name])
                build_primary_key([table_name])


if __name__ == "__main__":
    import_tushare_stock_info(refresh=False)
    import_tushare_stock_daily(ts_code_set=None)
