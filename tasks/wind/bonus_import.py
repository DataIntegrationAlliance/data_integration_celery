# -*- coding: utf-8 -*-
"""
Created on 2017/4/14
@author: MG
"""

# w.wsd("300343.SZ", "div_exdate", "2005-01-01", "2017-04-01", "Days=Alldays")
# w.wss("002482.SZ,300343.SZ", "div_recorddate,div_exdate","rptDate=20151231")
# w.wss("002482.SZ,300343.SZ", "div_cashbeforetax2,div_cashaftertax2,div_stock2,div_capitalization2,div_capitalization,div_stock,div_cashaftertax,div_cashbeforetax,div_cashandstock,div_recorddate,div_exdate,div_paydate,div_trddateshareb,div_preDisclosureDate,div_prelandate,div_smtgdate,div_impdate","rptDate=20151231;currencyType=BB")
import pandas as pd
from tasks import app
from tasks.wind import invoker
from datetime import datetime, date
from tasks.backend import engine_md
from tasks.backend.orm import build_primary_key
from tasks.utils.db_utils import with_db_session, bunch_insert_on_duplicate_update
from tasks.utils.fh_utils import STR_FORMAT_DATE
from tasks.utils.db_utils import alter_table_2_myisam
from sqlalchemy.types import String, Date, FLOAT
from sqlalchemy.dialects.mysql import DOUBLE
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


@app.task
def import_wind_bonus(chain):
    """
    :return:
    """
    table_name = 'wind_stock_bonus'
    has_table = engine_md.has_table(table_name)
    param_list = [
        ('div_cashbeforetax2', DOUBLE),
        ('div_cashaftertax2', DOUBLE),
        ('div_stock2', DOUBLE),
        ('div_capitalization2', DOUBLE),
        ('div_capitalization', DOUBLE),
        ('div_stock', DOUBLE),
        ('div_cashaftertax', DOUBLE),
        ('div_cashbeforetax', DOUBLE),
        ('div_cashandstock', DOUBLE),
        ('div_recorddate', Date),
        ('div_exdate', Date),
        ('div_paydate', Date),
        ('div_trddateshareb', Date),
        ('div_preDisclosureDate', Date),
        ('div_prelandate', Date),
        ('div_smtgdate', Date),
        ('div_impdate', Date),
    ]
    param = ",".join([key for key, _ in param_list])
    with with_db_session(engine_md) as session:
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
    dtype = {key: val for key, val in param_list}
    dtype['wind_code'] = String(20)
    dic_exdate_df_list = []
    for rep_date in DATE_LIST:
        rep_date_str = rep_date.strftime('%Y%m%d')
        stock_list = [s for s, date_pair in stock_date_dic.items() if date_pair[0] < rep_date and (rep_date < date_pair[1] if date_pair[1] is not None else True)]
        dic_exdate_df = invoker.wss(stock_list, param)
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
    bunch_insert_on_duplicate_update(dic_exdate_df_available, table_name, engine_md, dtype=dtype)
    if not has_table and engine_md.has_table(table_name):
        alter_table_2_myisam(engine_md, [table_name])
        build_primary_key([table_name])

if __name__ == "__main__":
    import_wind_bonus()
