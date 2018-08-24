import pandas as pd
from tasks import app
from tasks.wind import invoker
from tasks.backend import engine_md
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects.mysql import DOUBLE
from datetime import date, datetime, timedelta
from direstinvoker import APIError, UN_AVAILABLE_DATE
from tasks.backend.orm import build_primary_key
from tasks.utils.fh_utils import STR_FORMAT_DATE, date_2_str, str_2_date, get_last
from tasks.utils.db_utils import with_db_session, add_col_2_table, alter_table_2_myisam, \
    bunch_insert_on_duplicate_update
import logging
from sqlalchemy.types import String, Date, Integer, DateTime

logger = logging.getLogger()
DATE_BASE = datetime.strptime('2005-01-01', STR_FORMAT_DATE).date()
ONE_DAY = timedelta(days=1)
DEBUG = False


@app.task
def import_stock_tick(wind_code_set: None):
    """
    插入股票日线数据到最近一个工作日-1
    :return: 
    """
    import_count = 0
    table_name = 'wind_stock_tick'
    has_table = engine_md.has_table(table_name)
    param_list = [
        ('open', DOUBLE),
        ('high', DOUBLE),
        ('low', DOUBLE),
        ('pre_close', DOUBLE),
        ('last', DOUBLE),
        ('volume', DOUBLE),
        ('bsize1', DOUBLE),
        ('asize1', DOUBLE),
        ('ask1', DOUBLE),
        ('bid1', DOUBLE),
        ('amt', DOUBLE),
    ]
    wind_indictor_str = ",".join([key for key, _ in param_list])
    if has_table:
        sql_str = """
                    SELECT wind_code, date_frm, if(delist_date<end_date, delist_date, end_date) date_to
                    FROM
                    (
                    SELECT info.wind_code, ifnull(trade_date, ipo_date) date_frm, delist_date,
                    if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
                    FROM 
                        wind_stock_info info 
                    LEFT OUTER JOIN
                        (SELECT wind_code, adddate(max(trade_date),1) trade_date FROM {table_name} GROUP BY wind_code) tick
                    ON info.wind_code = tick.wind_code
                    ) tt
                    WHERE date_frm <= if(delist_date<end_date, delist_date, end_date) 
                    ORDER BY wind_code""".format(table_name=table_name)
    else:
        sql_str = """
                    SELECT wind_code, date_frm, if(delist_date<end_date, delist_date, end_date) date_to
                    FROM
                      (
                        SELECT info.wind_code, ipo_date date_frm, delist_date,
                        if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
                        FROM wind_stock_info info 
                      ) tt
                    WHERE date_frm <= if(delist_date<end_date, delist_date, end_date) 
                    ORDER BY wind_code"""
        logger.warning('%s 不存在，仅使用 wind_stock_info 表进行计算日期范围', table_name)
    with with_db_session(engine_md) as session:
        # 获取每只股票需要获取日线数据的日期区间
        table = session.execute(sql_str)
        # 计算每只股票需要获取日线数据的日期区间
        begin_time = None
        # 获取date_from,date_to，将date_from,date_to做为value值
        stock_date_dic = {
            wind_code: (date_from if begin_time is None else min([date_from, begin_time]), date_to)
            for wind_code, date_from, date_to in table.fetchall() if
            wind_code_set is None or wind_code in wind_code_set}

    data_df_list = []
    logger.info('%d stocks tick will been import', len(stock_date_dic))
    try:
        # base_date = min(trade_date_sorted_list)
        data_count = 0
        for stock_num, (wind_code, (date_from, date_to)) in enumerate(stock_date_dic.items()):
            # 获取股票量价等行情数据
            # wind_indictor_str = "ask1,bid1,asize1,bsize1,volume,amt,pre_close,open,high,low,last"
            try:
                data_df = invoker.wst(wind_code, wind_indictor_str, date_from, date_to)
            except APIError as exp:
                data_df = None
                if exp.ret_dic['error_code'] == -40520007:
                    logger.warning('%s[%s - %s] %s', wind_code, date_from, date_to, exp.ret_dic['error_msg'])
                    continue
            if data_df is None:
                logger.warning('%d) %s has no data during %s %s', stock_num, wind_code, date_from, date_to)
                continue
            logger.info('%d) %d data of %s between %s and %s', stock_num, data_df.shape[0], wind_code, date_from,
                        date_to)
            data_df['wind_code'] = wind_code
            data_df_list.append(data_df)
            data_count += data_df.shape[0]
            if data_count >= 10000:
                try:
                    import_count += insert_into_db(data_df_list, engine_md)
                finally:
                    data_df_list = []
                    data_count = 0
                    if DEBUG and stock_num >= 1:
                        break
    finally:
        # 导入数据库
        import_count += insert_into_db(data_df_list, engine_md)
    return import_count


def insert_into_db(data_df_list, engine_md):
    data_count = len(data_df_list)
    table_name = 'wind_stock_tick'
    has_table = engine_md.has_table(table_name)
    param_list = [
        ('datetime', DateTime),
        ('open', DOUBLE),
        ('high', DOUBLE),
        ('low', DOUBLE),
        ('close', DOUBLE),
        ('ask1', DOUBLE),
        ('bid1', DOUBLE),
        ('asize1', DOUBLE),
        ('bsize1', DOUBLE),
        ('volume', DOUBLE),
        ('amount', DOUBLE),
        ('preclose', DOUBLE),
    ]
    dtype = {key: val for key, val in param_list}
    dtype['wind_code'] = String(20)
    if data_count > 0:
        data_df_all = pd.concat(data_df_list)
        data_df_all.index.rename('datetime', inplace=True)
        data_df_all.reset_index(inplace=True)
        data_df_all.set_index(['wind_code', 'datetime'], inplace=True)
        data_df_all.reset_index(inplace=True)
        bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md, dtype=dtype)
        logger.info('%d data imported', data_df_all.shape[0])
        if not has_table and engine_md.has_table(table_name):
            alter_table_2_myisam(engine_md, [table_name])
            build_primary_key([table_name])

    return data_count


if __name__ == '__main__':
    wind_code_set = None
    DEBUG = True
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s [%(name)s:%(funcName)s] %(message)s')
    import_count = 1
    while import_count > 0:
        try:
            # 更新每日股票数据
            import_count = import_stock_tick(wind_code_set)
        except IntegrityError:
            logger.exception("import_stock_tick exception")
