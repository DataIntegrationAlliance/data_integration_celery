from datetime import date, datetime, timedelta

import math
import pandas as pd
import numpy as np
from config_fh import get_db_engine, get_db_session, STR_FORMAT_DATE, UN_AVAILABLE_DATE, WIND_REST_URL
from fh_tools.windy_utils_rest import WindRest
from fh_tools.fh_utils import get_last, get_first
import logging
from sqlalchemy.types import String, Date, Float, Integer
logger = logging.getLogger()
DATE_BASE = datetime.strptime('2005-01-01', STR_FORMAT_DATE).date()
ONE_DAY = timedelta(days=1)


def import_stock_quertarly():
    """
    插入股票日线数据到最近一个工作日-1
    :return: 
    """
    logging.info("更新 wind_stock_quertarly 开始")
    # 标示每天几点以后下载当日行情数据
    BASE_LINE_HOUR = 16

    w = WindRest(WIND_REST_URL)
    engine = get_db_engine()
    with get_db_session(engine) as session:
        # 获取每只股票最新交易日数据
        sql_str = 'select wind_code, max(Trade_date) from wind_stock_quertarly group by wind_code'
        table = session.execute(sql_str)
        stock_trade_date_latest_dic = dict(table.fetchall())
        # 获取市场有效交易日数据
        sql_str = "select trade_date from wind_trade_date where trade_date > '2005-1-1'"
        table = session.execute(sql_str)
        trade_date_sorted_list = [t[0] for t in table.fetchall()]
        trade_date_sorted_list.sort()
        # 获取每只股票上市日期、退市日期
        table = session.execute('SELECT wind_code, ipo_date, delist_date FROM wind_stock_info')
        stock_date_dic = {wind_code: (ipo_date, delist_date if delist_date is None or delist_date > UN_AVAILABLE_DATE else None) for
                          wind_code, ipo_date, delist_date in table.fetchall()}
    date_ending = date.today() - ONE_DAY if datetime.now().hour < BASE_LINE_HOUR else date.today()
    data_df_list = []
    logger.info('%d stocks will been import into wind_stock_quertarly', len(stock_date_dic))
    # 获取股票量价等行情数据
    field_col_name_dic = {
        'roic_ttm': 'roic_ttm',
        'yoyprofit': 'yoyprofit',
        'ebit': 'ebit',
        'ebit2': 'ebit2',
        'ebit2_ttm': 'ebit2_ttm',
        'surpluscapitalps': 'surpluscapitalps',
        'undistributedps': 'undistributedps',
        'stm_issuingdate': 'stm_issuingdate',
    }
    wind_indictor_str = ",".join(field_col_name_dic.keys())
    upper_col_2_name_dic = {name.upper(): val for name, val in field_col_name_dic.items()}
    try:
        for stock_num, (wind_code, date_pair) in enumerate(stock_date_dic.items()):
            date_ipo, date_delist = date_pair
            # 获取 date_from
            if wind_code in stock_trade_date_latest_dic:
                date_latest_t1 = stock_trade_date_latest_dic[wind_code] + ONE_DAY
                date_from = max([date_latest_t1, DATE_BASE, date_ipo])
            else:
                date_from = max([DATE_BASE, date_ipo])
            date_from = get_first(trade_date_sorted_list, lambda x: x >= date_from)
            # 获取 date_to
            if date_delist is None:
                date_to = date_ending
            else:
                date_to = min([date_delist, date_ending])
            date_to = get_last(trade_date_sorted_list, lambda x: x <= date_to)
            if date_from is None or date_to is None or date_from > date_to:
                continue
            # 获取股票量价等行情数据
            # w.wsd("002122.SZ", "roic_ttm,yoyprofit,ebit,ebit2,ebit2_ttm,surpluscapitalps,undistributedps,stm_issuingdate", "2012-12-31", "2017-12-06", "unit=1;rptType=1;Period=Q")
            data_df = w.wsd(wind_code, wind_indictor_str, date_from, date_to, "unit=1;rptType=1;Period=Q")
            if data_df is None:
                logger.warning('%d) %s has no data during %s %s', stock_num, wind_code, date_from, date_to)
                continue
            data_df.rename(columns=upper_col_2_name_dic, inplace=True)
            # 清理掉期间全空的行
            for trade_date in list(data_df.index):
                is_all_none = data_df.loc[trade_date].apply(lambda x: x is None).all()
                if is_all_none:
                    logger.warning("%s %s 数据全部为空", wind_code, trade_date)
                    data_df.drop(trade_date, inplace=True)
            logger.info('%d) %d data of %s between %s and %s', stock_num, data_df.shape[0], wind_code, date_from, date_to)
            data_df['wind_code'] = wind_code
            data_df_list.append(data_df)
            # if len(data_df_list) > 10:
            #     break
    finally:
        # 导入数据库
        if len(data_df_list) > 0:
            data_df_all = pd.concat(data_df_list)
            data_df_all.index.rename('trade_date', inplace=True)
            data_df_all.reset_index(inplace=True)
            data_df_all.set_index(['wind_code', 'trade_date'], inplace=True)
            data_df_all.to_sql('wind_stock_quertarly', engine, if_exists='append')
            logging.info("更新 wind_stock_quertarly 结束 %d 条信息被更新", data_df_all.shape[0])


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s [%(name)s:%(funcName)s] %(message)s')
    # 更新股票季报数据
    import_stock_quertarly()
    # 添加某列信息
    # fill_history()
    # fill_col()
