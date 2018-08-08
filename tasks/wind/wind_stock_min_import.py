from datetime import date, datetime, timedelta
import pandas as pd
import numpy as np
from sqlalchemy.exc import IntegrityError

from config_fh import get_db_engine, get_db_session, STR_FORMAT_DATE, UN_AVAILABLE_DATE, WIND_REST_URL
from fh_tools.windy_utils_rest import WindRest, APIError
from fh_tools.fh_utils import get_last, get_first
import logging
from sqlalchemy.types import String, Date, Float, Integer, DateTime
logger = logging.getLogger()
DATE_BASE = datetime.strptime('2005-01-01', STR_FORMAT_DATE).date()
ONE_DAY = timedelta(days=1)


def import_stock_tick():
    """
    插入股票日线数据到最近一个工作日-1
    :return: 
    """
    import_count = 0
    w = WindRest(WIND_REST_URL)
    engine = get_db_engine(db_name='dev_db')
    with get_db_session(engine) as session:
        # 获取每只股票最新交易日数据
        sql_str = 'select wind_code, max(datetime) from wind_stock_tick group by wind_code'
        table = session.execute(sql_str)
        stock_trade_date_latest_dic = dict(table.fetchall())
        # 获取市场有效交易日数据，只需要近7个工作日的数据即可
        sql_str = "select trade_date from wind_trade_date where trade_date <= :trade_date order by trade_date desc limit 6"
        table = session.execute(sql_str, params={'trade_date': date.today() - ONE_DAY})
        trade_date_sorted_list = [t[0] for t in table.fetchall()]
        trade_date_sorted_list.sort()
        # 获取每只股票上市日期、退市日期
        table = session.execute('SELECT wind_code, ipo_date, delist_date FROM wind_stock_info')
        stock_date_dic = {wind_code: (ipo_date, delist_date if delist_date is None or delist_date > UN_AVAILABLE_DATE else None) for
                          wind_code, ipo_date, delist_date in table.fetchall()}
    today_t_1 = date.today() - ONE_DAY
    data_df_list = []
    logger.info('%d stocks tick will been import', len(stock_date_dic))
    try:
        base_date = min(trade_date_sorted_list)
        data_count = 0
        for stock_num, (wind_code, (date_ipo, date_delist)) in enumerate(stock_date_dic.items()):
            # date_ipo, date_delist = date_pair
            # 获取 date_from
            if wind_code in stock_trade_date_latest_dic:
                date_latest_t1 = stock_trade_date_latest_dic[wind_code].date() + ONE_DAY
                date_from = max([date_latest_t1, base_date, date_ipo])
            else:
                date_from = max([base_date, date_ipo])
            date_from = get_first(trade_date_sorted_list, lambda x: x >= date_from)
            if date_from is None:
                continue
            datetime_from = datetime(date_from.year, date_from.month, date_from.day, 9)
            # 获取 date_to
            if date_delist is None:
                date_to = today_t_1
            else:
                date_to = min([date_delist, today_t_1])
            date_to = get_last(trade_date_sorted_list, lambda x: x <= date_to)
            if date_to is None:
                continue
            datetime_to = datetime(date_to.year, date_to.month, date_to.day, 15, 2)
            if datetime_from > datetime_to:
                continue
            # 获取股票量价等行情数据
            wind_indictor_str = "ask1,bid1,asize1,bsize1,volume,amt,pre_close,open,high,low,last"
            try:
                data_df = w.wst(wind_code, wind_indictor_str, datetime_from, datetime_to)
            except APIError as exp:
                data_df = None
                if exp.ret_dic['error_code'] == -40520007:
                    logger.warning('%s[%s - %s] %s', wind_code, datetime_from, datetime_to, exp.ret_dic['error_msg'])
                    continue
            if data_df is None:
                logger.warning('%d) %s has no data during %s %s', stock_num, wind_code, date_from, date_to)
                continue
            logger.info('%d) %d data of %s between %s and %s', stock_num, data_df.shape[0], wind_code, date_from, date_to)
            data_df['wind_code'] = wind_code
            data_df_list.append(data_df)
            data_count += data_df.shape[0]
            if data_count >= 20000:
                try:
                    import_count += insert_into_db(data_df_list, engine)
                finally:
                    data_df_list = []
                    data_count = 0
    finally:
        # 导入数据库
        import_count += insert_into_db(data_df_list, engine)
    return import_count

def insert_into_db(data_df_list, engine):
    data_count = len(data_df_list)
    if data_count > 0:
        data_df_all = pd.concat(data_df_list)
        data_df_all.index.rename('datetime', inplace=True)
        data_df_all.reset_index(inplace=True)
        data_df_all.set_index(['wind_code', 'datetime'], inplace=True)
        data_df_all.to_sql('wind_stock_tick', engine, if_exists='append',
                           dtype={
                               'wind_code': String(20),
                               'datetime': DateTime,
                               'open': Float,
                               'high': Float,
                               'low': Float,
                               'close': Float,
                               'ask1': Float,
                               'bid1': Float,
                               'asize1': Integer,
                               'bsize1': Integer,
                               'volume': Integer,
                               'amount': Float,
                               'preclose': Float,
                           }
                           )
        logger.info('%d data imported', data_df_all.shape[0])
    return data_count

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s [%(name)s:%(funcName)s] %(message)s')
    import_count = 1
    while import_count > 0:
        try:
            # 更新每日股票数据
            import_count = import_stock_tick()
        except IntegrityError:
            logger.exception("import_stock_tick exception")
