# -*- coding: utf-8 -*-
"""
Created on 2017/4/14
@author: MG
"""

# w.wsd("300343.SZ", "div_exdate", "2005-01-01", "2017-04-01", "Days=Alldays")
# w.wss("002482.SZ,300343.SZ", "div_recorddate,div_exdate","rptDate=20151231")
# w.wss("002482.SZ,300343.SZ", "div_cashbeforetax2,div_cashaftertax2,div_stock2,div_capitalization2,div_capitalization,div_stock,div_cashaftertax,div_cashbeforetax,div_cashandstock,div_recorddate,div_exdate,div_paydate,div_trddateshareb,div_preDisclosureDate,div_prelandate,div_smtgdate,div_impdate","rptDate=20151231;currencyType=BB")
import pandas as pd
from config_fh import get_db_session, STR_FORMAT_DATE, WIND_REST_URL, get_db_engine
from datetime import datetime, date
from fh_tools.windy_utils_rest import WindRest
from sqlalchemy.types import String, Date, FLOAT

UN_AVAILABLE_DATE = datetime.strptime('1900-01-01', STR_FORMAT_DATE).date()
TODAY = date.today()
BASE_DATE = datetime.strptime('2010-12-31', STR_FORMAT_DATE).date()


def str_date(x):
    if x is None:
        ret_x = None
    else:
        ret_x = datetime.strptime(x, STR_FORMAT_DATE).date()
        if ret_x < UN_AVAILABLE_DATE:
            ret_x = None
    return ret_x


def import_wind_bonus():
    w = WindRest(WIND_REST_URL)
    engine = get_db_engine()
    with get_db_session(engine) as session:
        table = session.execute('SELECT wind_code, ipo_date, delist_date FROM wind_stock_info')
        stock_date_dic = {wind_code: (ipo_date, delist_date if delist_date > UN_AVAILABLE_DATE else None) for wind_code, ipo_date, delist_date in table.fetchall()}
    print(len(stock_date_dic))
    DATE_LIST = [datetime.strptime('2010-12-31', STR_FORMAT_DATE).date(),
                 datetime.strptime('2011-12-31', STR_FORMAT_DATE).date(),
                 datetime.strptime('2012-12-31', STR_FORMAT_DATE).date(),
                 datetime.strptime('2013-12-31', STR_FORMAT_DATE).date(),
                 datetime.strptime('2014-12-31', STR_FORMAT_DATE).date(),
                 datetime.strptime('2015-12-31', STR_FORMAT_DATE).date(),
                 ]
    dic_exdate_df_list = []
    for rep_date in DATE_LIST:
        rep_date_str = rep_date.strftime('%Y%m%d')
        stock_list = [s for s, date_pair in stock_date_dic.items() if date_pair[0] < rep_date and (rep_date < date_pair[1] if date_pair[1] is not None else True)]
        dic_exdate_df = w.wss(stock_list, "div_cashbeforetax2,div_cashaftertax2,div_stock2,div_capitalization2,div_capitalization,div_stock,div_cashaftertax,div_cashbeforetax,div_cashandstock,div_recorddate,div_exdate,div_paydate,div_trddateshareb,div_preDisclosureDate,div_prelandate,div_smtgdate,div_impdate","rptDate=%s;currencyType=BB" % rep_date_str)
        dic_exdate_df_list.append(dic_exdate_df)

    dic_exdate_df_all = pd.concat(dic_exdate_df_list)
    dic_exdate_df_all.index.rename('wind_code', inplace=True)
    dic_exdate_df_all.drop_duplicates(inplace=True)
    dic_exdate_df_all['DIV_EXDATE'] = dic_exdate_df_all['DIV_EXDATE'].apply(str_date)
    dic_exdate_df_all['DIV_PAYDATE'] = dic_exdate_df_all['DIV_PAYDATE'].apply(str_date)
    dic_exdate_df_all['DIV_IMPDATE'] = dic_exdate_df_all['DIV_IMPDATE'].apply(str_date)
    dic_exdate_df_all['DIV_RECORDDATE'] = dic_exdate_df_all['DIV_RECORDDATE'].apply(str_date)
    dic_exdate_df_all['DIV_PREDISCLOSUREDATE'] = dic_exdate_df_all['DIV_PREDISCLOSUREDATE'].apply(str_date)
    dic_exdate_df_all['DIV_PRELANDATE'] = dic_exdate_df_all['DIV_PRELANDATE'].apply(str_date)
    dic_exdate_df_all['DIV_SMTGDATE'] = dic_exdate_df_all['DIV_SMTGDATE'].apply(str_date)
    dic_exdate_df_all['DIV_TRDDATESHAREB'] = dic_exdate_df_all['DIV_TRDDATESHAREB'].apply(str_date)

    condition = ~(dic_exdate_df_all['DIV_EXDATE'].apply(lambda x: x is None) &
                  dic_exdate_df_all['DIV_PAYDATE'].apply(lambda x: x is None) &
                  dic_exdate_df_all['DIV_IMPDATE'].apply(lambda x: x is None) &
                  dic_exdate_df_all['DIV_RECORDDATE'].apply(lambda x: x is None)
                  )
    dic_exdate_df_available = dic_exdate_df_all[condition]
    dic_exdate_df_available.to_sql('wind_stock_bonus', engine, if_exists='append',
                                   dtype={
                                       'wind_code': String(20),
                                       'div_cashbeforetax2': FLOAT,
                                       'div_cashaftertax2': FLOAT,
                                       'div_stock2': FLOAT,
                                       'div_capitalization2': FLOAT,
                                       'div_capitalization': FLOAT,
                                       'div_stock': FLOAT,
                                       'div_cashaftertax': FLOAT,
                                       'div_cashbeforetax': FLOAT,
                                       'div_cashandstock': FLOAT,
                                       'div_recorddate': Date,
                                       'div_exdate': Date,
                                       'div_paydate': Date,
                                       'div_trddateshareb': Date,
                                       'div_preDisclosureDate': Date,
                                       'div_prelandate': Date,
                                       'div_smtgdate': Date,
                                       'div_impdate': Date
                                   }
                                   )

if __name__ == "__main__":
    import_wind_bonus()
