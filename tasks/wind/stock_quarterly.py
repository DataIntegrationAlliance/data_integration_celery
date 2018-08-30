import logging
import pandas as pd
from tasks import app
from tasks.wind import invoker
from tasks.backend import engine_md
from tasks.backend.orm import build_primary_key
from datetime import date, datetime, timedelta
from sqlalchemy.types import String, Date, Integer
from tasks.utils.db_utils import bunch_insert_on_duplicate_update
from tasks.utils.db_utils import with_db_session, alter_table_2_myisam
from tasks.utils.fh_utils import STR_FORMAT_DATE, date_2_str, str_2_date, get_last, get_first

logger = logging.getLogger()
DATE_BASE = datetime.strptime('2005-01-01', STR_FORMAT_DATE).date()
ONE_DAY = timedelta(days=1)
DEBUG = False


@app.task
def import_stock_quertarly(wind_code_set=None):
    """
    插入股票日线数据到最近一个工作日-1
    :return: 
    """
    logging.info("更新 wind_stock_quertarly 开始")
    table_name = 'wind_stock_quertarly'
    has_table = engine_md.has_table(table_name)
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
                   (SELECT wind_code, adddate(max(trade_date),1) trade_date FROM {table_name} GROUP BY wind_code) quertarly
               ON info.wind_code = quertarly.wind_code
           ) tt
           WHERE date_frm <= if(delist_date<end_date, delist_date, end_date) 
           ORDER BY wind_code;""".format(table_name=table_name)
    else:
        logger.warning('wind_stock_quertarly 不存在，仅使用 wind_stock_info 表进行计算日期范围')
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
    # 获取股票量价等行情数据
    param_list = [
        ('roic_ttm', String(20)),
        ('yoyprofit', String(20)),
        ('ebit', String(20)),
        ('ebit2', String(20)),
        ('ebit2_ttm', String(20)),
        ('surpluscapitalps', String(20)),
        ('undistributedps', String(20)),
        ('stm_issuingdate', String(20)),
    ]
    # 获取参数列表
    wind_indictor_str = ",".join(key for key, _ in param_list)
    dtype = {key: val for key, val in param_list}
    dtype['wind_code'] = String(20)
    dtype['trade_date'] = Date
    # # 标示每天几点以后下载当日行情数据
    # BASE_LINE_HOUR = 16
    # with with_db_session(engine_md) as session:
    #     # 获取每只股票最新交易日数据
    #     sql_str = 'select wind_code, max(Trade_date) from wind_stock_quertarly group by wind_code'
    #     table = session.execute(sql_str)
    #     stock_trade_date_latest_dic = dict(table.fetchall())
    #     # 获取市场有效交易日数据
    #     sql_str = "select trade_date from wind_trade_date where trade_date > '2005-1-1'"
    #     table = session.execute(sql_str)
    #     trade_date_sorted_list = [t[0] for t in table.fetchall()]
    #     trade_date_sorted_list.sort()
    #     # 获取每只股票上市日期、退市日期
    #     table = session.execute('SELECT wind_code, ipo_date, delist_date FROM wind_stock_info')
    #     stock_date_dic = {wind_code: (ipo_date, delist_date if delist_date is None or delist_date > UN_AVAILABLE_DATE else None) for
    #                       wind_code, ipo_date, delist_date in table.fetchall()}
    # date_ending = date.today() - ONE_DAY if datetime.now().hour < BASE_LINE_HOUR else date.today()
    # data_df_list = []
    # logger.info('%d stocks will been import into wind_stock_quertarly', len(stock_date_dic))
    data_df_list = []
    logger.info('%d stocks will been import into wind_stock_quertarly', len(stock_date_dic))

    try:
        for stock_num, (wind_code, (date_from, date_to)) in enumerate(stock_date_dic.items()):
            # 获取股票量价等行情数据
            # w.wsd("002122.SZ", "roic_ttm,yoyprofit,ebit,ebit2,ebit2_ttm,surpluscapitalps,undistributedps,stm_issuingdate", "2012-12-31", "2017-12-06", "unit=1;rptType=1;Period=Q")
            data_df = invoker.wsd(wind_code, wind_indictor_str, date_from, date_to, "unit=1;rptType=1;Period=Q")
            if data_df is None:
                logger.warning('%d) %s has no data during %s %s', stock_num, wind_code, date_from, date_to)
                continue
            data_df.rename(columns={c: str(c).lower() for c in data_df.columns}, inplace=True)
            # 清理掉期间全空的行
            for trade_date in list(data_df.index[:10]):
                is_all_none = data_df.loc[trade_date].apply(lambda x: x is None).all()
                if is_all_none:
                    logger.warning("%s %s 数据全部为空", wind_code, trade_date)
                    data_df.drop(trade_date, inplace=True)
            logger.info('%d) %d data of %s between %s and %s', stock_num, data_df.shape[0], wind_code, date_from,
                        date_to)
            data_df['wind_code'] = wind_code
            data_df.index.rename('trade_date', inplace=True)
            data_df.reset_index(inplace=True)
            data_df_list.append(data_df)
            if DEBUG and len(data_df_list) > 10:
                break
    finally:
        # 导入数据库
        if len(data_df_list) > 0:
            data_df_all = pd.concat(data_df_list)
            bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md, dtype=dtype)
            logging.info("更新 wind_stock_quertarly 结束 %d 条信息被更新", data_df_all.shape[0])
            if not has_table and engine_md.has_table(table_name):
                alter_table_2_myisam(engine_md, [table_name])
                build_primary_key([table_name])


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s [%(name)s:%(funcName)s] %(message)s')
    # 更新股票季报数据
    #DEBUG = True
    wind_code_set = None
    import_stock_quertarly(wind_code_set)

    # 添加某列信息
    # fill_history()
    # fill_col()
