#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""

"""
import pandas as pd
import logging
from tasks import app
import numpy as np
from direstinvoker.utils.fh_utils import date_2_str
from tasks import bunch_insert_on_duplicate_update, alter_table_2_myisam, build_primary_key
from tasks.backend import engine_md
from tasks.utils.db_utils import with_db_session
from tasks.merge import mean_value, prefer_left, prefer_right, merge_data, get_value
from sqlalchemy.types import String, Date, Integer, Text
from sqlalchemy.dialects.mysql import DOUBLE
from tasks.utils.fh_utils import log_param_when_exception

logger = logging.getLogger()


@app.task
def merge_future_info():
    """
    合并 wind，ifind 数据到对应名称的表中
    :return:
    """
    table_name = 'future_info'
    logging.info("更新 %s 开始", table_name)
    has_table = engine_md.has_table(table_name)
    ifind_table_name = 'ifind_{table_name}'.format(table_name=table_name)
    wind_table_name = 'wind_{table_name}'.format(table_name=table_name)
    # ifind_model = TABLE_MODEL_DIC[ifind_table_name]
    # wind_model = TABLE_MODEL_DIC[wind_table_name]
    # with with_db_session(engine_md) as session:
    #     session.query(ifind_model, wind_model).filter(ifind_model.c.ths_code == wind_model.c.wind_code)
    ifind_sql_str = "select * from {table_name}".format(table_name=ifind_table_name)
    wind_sql_str = "select * from {table_name}".format(table_name=wind_table_name)
    ifind_df = pd.read_sql(ifind_sql_str, engine_md)  # , index_col='ths_code'
    wind_df = pd.read_sql(wind_sql_str, engine_md)  # , index_col='wind_code'
    joined_df = pd.merge(ifind_df, wind_df, how='outer',
                         left_on='ths_code', right_on='wind_code', indicator='indicator_column')
    col_merge_dic = {
        'unique_code': (String(20), prefer_left, {'left_key': 'ths_code', 'right_key': 'wind_code'}),
        'sec_name': (String(50), prefer_left, {'left_key': 'ths_future_short_name_future', 'right_key': 'sec_name'}),
        'dl_month': (String(20), prefer_left, {'left_key': 'ths_delivery_month_future', 'right_key': 'dlmonth'}),
        'delivery_date': (
            Date, prefer_left, {'left_key': 'ths_last_delivery_date_future', 'right_key': 'lastdelivery_date'}),
        'prince': (DOUBLE, prefer_left, {'left_key': 'ths_mini_chg_price_future', 'right_key': 'mfprice'}),
        'ex_eng': (String(20), get_value, {'key': 'ths_exchange_short_name_future'}),
        'sc_code': (String(20), prefer_left, {'left_key': 'ths_td_variety_future', 'right_key': 'sccode'}),
        'p_unit': (String(20), prefer_left, {'left_key': 'ths_pricing_unit_future', 'right_key': 'punit'}),
        'l_prince': (DOUBLE, prefer_left, {'left_key': 'ths_listing_benchmark_price_future', 'right_key': 'lprice'}),
        'margin': (DOUBLE, prefer_left, {'left_key': 'ths_initial_td_deposit_future', 'right_key': 'margin'}),
        'ratio_lmit': (DOUBLE, get_value, {'key': 'ths_chg_ratio_lmit_future'}),
        'enshort_name': (String(100), get_value, {'key': 'ths_contract_en_short_name_future'}),
        'en_name': (String(100), get_value, {'key': 'ths_contract_en_name_future'}),
        'future_code': (String(20), prefer_left, {'left_key': 'ths_future_code_future', 'right_key': 'trade_code'}),
        'last_date': (Date, prefer_left, {'left_key': 'ths_last_td_date_future', 'right_key': 'lasttrade_date'}),
        'con_month': (String(60), get_value, {'key': 'ths_contract_month_explain_future'}),
        'de_date': (String(60), get_value, {'key': 'ths_delivery_date_explain_future'}),
        'td_date': (String(60), get_value, {'key': 'ths_last_td_date_explian_future'}),
        'benchmark_': (DOUBLE, get_value, {'key': 'ths_listing_benchmark_price_future'}),
        'sec_type': (String(20), get_value, {'key': 'ths_sec_type_future'}),
        'td_time': (String(60), get_value, {'key': 'ths_td_time_explain_future'}),
        'td_unit': (String(20), get_value, {'key': 'ths_td_unit_future'}),
        'td_variety': (String(20), get_value, {'key': 'ths_td_variety_future'}),
        'sec_englishname': (String(50), get_value, {'key': 'sec_englishname'}),
        'exch_eng': (String(50), get_value, {'key': 'exch_eng'}),
        'changelt': (DOUBLE, get_value, {'key': 'changelt'}),
        'contractmultiplier': (String(100), get_value, {'key': 'contractmultiplier'}),
        'ftmargins': (String(100), get_value, {'key': 'ftmargins'}),
        'thours': (String(200), get_value, {'key': 'thours'}),
        'ipo_date': (Date, prefer_left, {'left_key': 'ths_start_trade_date_future', 'right_key': 'ipo_date'}),
    }
    col_merge_rule_dic = {
        key: (val[1], val[2]) for key, val in col_merge_dic.items()
    }
    dtype = {
        key: val[0] for key, val in col_merge_dic.items()
    }
    data_df = merge_data(joined_df, col_merge_rule_dic)
    data_count = bunch_insert_on_duplicate_update(data_df, table_name, engine_md, dtype)
    logger.info('%s 新增或更新记录 %d 条', table_name, data_count)
    if not has_table and engine_md.has_table(table_name):
        alter_table_2_myisam(engine_md, [table_name])
        # build_primary_key([table_name])
        create_pk_str = """ALTER TABLE {table_name}
                        CHANGE COLUMN `unique_code` `unique_code` VARCHAR(20) NOT NULL ,
                        ADD PRIMARY KEY (`unique_code`)""".format(table_name=table_name)
        with with_db_session(engine_md) as session:
            session.execute(create_pk_str)

    return data_df


@app.task
def merge_future_daily(date_from=None):
    """
    合并 wind，ifind 数据到对应名称的表中
    :param date_from:
    :return:
    """
    table_name = 'future_daily'
    logging.info("更新 %s 开始", table_name)
    has_table = engine_md.has_table(table_name)
    if date_from is None and has_table:
        sql_str = "select adddate(max(trade_date),1) from {table_name}".format(table_name=table_name)
        with with_db_session(engine_md) as session:
            date_from = date_2_str(session.execute(sql_str).scalar())
    ifind_table_name = 'ifind_{table_name}'.format(table_name=table_name)
    wind_table_name = 'wind_{table_name}'.format(table_name=table_name)
    if date_from is None:
        ifind_sql_str = "select * from {table_name}".format(table_name=ifind_table_name)
        wind_sql_str = "select * from {table_name}".format(table_name=wind_table_name)
        ifind_df = pd.read_sql(ifind_sql_str, engine_md)  # , index_col='ths_code'
        wind_df = pd.read_sql(wind_sql_str, engine_md)  # , index_col='wind_code'
    else:
        ifind_sql_str = "select * from {table_name} where time >= %s".format(table_name=ifind_table_name)
        wind_sql_str = "select * from {table_name} where trade_date >= %s".format(table_name=wind_table_name)
        ifind_df = pd.read_sql(ifind_sql_str, engine_md, params=[date_from])  # , index_col='ths_code'
        wind_df = pd.read_sql(wind_sql_str, engine_md, params=[date_from])  # , index_col='wind_code'
    # change (ifind_df, wind_df)  into joined_df
    joined_df = pd.merge(ifind_df, wind_df, how='outer',
                         left_on=['ths_code', 'time'], right_on=['wind_code', 'trade_date'],
                         indicator='indicator_column')
    # data, columns processing
    col_merge_dic = {
        'unique_code': (String(20), prefer_left, {'left_key': 'ths_code', 'right_key': 'wind_code'}),
        'trade_date': (Date, prefer_left, {'left_key': 'time', 'right_key': 'trade_date'}),
        'open': (DOUBLE, mean_value, {
            'left_key': 'open_x', 'right_key': 'open_y',
            'warning_accuracy': 0.01, 'primary_keys': ('ths_code', 'time')}),
        'high': (DOUBLE, mean_value, {
            'left_key': 'high_x', 'right_key': 'high_y',
            'warning_accuracy': 0.01, 'primary_keys': ('ths_code', 'time')}),
        'low': (DOUBLE, mean_value, {
            'left_key': 'low_x', 'right_key': 'low_y',
            'warning_accuracy': 0.01, 'primary_keys': ('ths_code', 'time')}),
        # TODO: 原因不详，wind接口取到的部分 close 数据不准确
        'close': (DOUBLE, prefer_left, {
            'left_key': 'close_x', 'right_key': 'close_y',
            'warning_accuracy': 0.01, 'primary_keys': ('ths_code', 'time')}),
        'volume': (DOUBLE, mean_value, {
            'left_key': 'volume_x', 'right_key': 'volume_y',
            'warning_accuracy': 10, 'primary_keys': ('ths_code', 'time')}),
        'amount': (DOUBLE, mean2_value, {
            'left_key': 'amount', 'right_key': 'amt',
            'warning_accuracy': 1, 'primary_keys': ('ths_code', 'time')}),

        'settle': (DOUBLE, mean_value, {
            'left_key': 'settlement', 'right_key': 'settle',
            'warning_accuracy': 0.01, 'primary_keys': ('ths_code', 'time')}),
        'position': (DOUBLE, mean_value, {
            'left_key': 'openInterest', 'right_key': 'position',
            'warning_accuracy': 1, 'primary_keys': ('ths_code', 'time')}),
        'maxupordown': (Integer, get_value, {'key': 'maxupordown'}),
        'st_stock': (DOUBLE, get_value, {'key': 'st_stock'}),
        'instrument_id': (String(20), get_value, {'key': 'instrument_id'}),
        'positionchange': (DOUBLE, get_value, {'key': 'positionChange'}),
        'preclose': (String(20), get_value, {'key': 'preClose'}),
        'presettlement': (DOUBLE, get_value, {'key': 'preSettlement'}),
        'amplitude': (DOUBLE, get_value, {'key': 'amplitude'}),
        'avgprice': (DOUBLE, get_value, {'key': 'avgPrice'}),
        'change': (DOUBLE, get_value, {'key': 'change'}),
        'change_settlement': (DOUBLE, get_value, {'key': 'change_settlement'}),
        'chg_settlement': (DOUBLE, get_value, {'key': 'chg_settlement'}),
    }

    col_merge_rule_dic = {
        key: (val[1], val[2]) for key, val in col_merge_dic.items()
    }
    dtype = {
        key: val[0] for key, val in col_merge_dic.items()
    }
    data_df = merge_data(joined_df, col_merge_rule_dic)
    data_count = bunch_insert_on_duplicate_update(data_df, table_name, engine_md, dtype)
    logger.info('%s 新增或更新记录 %d 条', table_name, data_count)
    if not has_table and engine_md.has_table(table_name):
        alter_table_2_myisam(engine_md, [table_name])
        build_primary_key([table_name])

    return data_df


def max_up_or_down(data_s: pd.Series, ths_key, wind_key, primary_keys=None, **kwargs):
    ths_val = data_s[ths_key]
    if ths_val == '跌停':
        ret_code_ths = -1
    elif ths_val == '涨停':
        ret_code_ths = 1
    elif ths_val == ('非涨跌停', '停牌'):
        ret_code_ths = 0
    else:
        ret_code_ths = None

    wind_val = data_s[wind_key]
    if wind_val in (1, -1, 0):
        ret_code_wind = wind_val
    else:
        ret_code_wind = None

    if ret_code_ths is None and ret_code_wind is None:
        pk_str = ','.join([str(data_s[key]) for key in primary_keys]) if primary_keys is not None else None
        if pk_str is None:
            msg = '%s = %s; %s = %s; 状态不明' % (ths_key, str(ths_val), wind_key, str(wind_val))
        else:
            msg = '[%s] %s = %s; %s = %s; 状态不明' % (pk_str, ths_key, str(ths_val), wind_key, str(wind_val))
        # logger.debug(msg)
        ret_code = -2
    elif ret_code_ths is None:
        ret_code = ret_code_wind
    elif ret_code_wind is None:
        ret_code = ret_code_ths
    elif ret_code_ths == ret_code_wind:
        ret_code = ret_code_wind
    else:
        pk_str = ','.join([str(data_s[key]) for key in primary_keys]) if primary_keys is not None else None
        if pk_str is None:
            msg = '%s = %s; %s = %s; 状态冲突' % (ths_key, str(ths_val), wind_key, str(wind_val))
        else:
            msg = '[%s] %s = %s; %s = %s; 状态冲突' % (pk_str, ths_key, str(ths_val), wind_key, str(wind_val))
        logger.warning(msg)
        ret_code = -3

    return ret_code


def is_not_nan_or_none(x):
    """
    判断是否不是 NAN 或 None
    :param x:
    :return:
    """
    return False if x is None else not (isinstance(x, float) and np.isnan(x))


@log_param_when_exception
def mean2_value(data_s: pd.Series, left_key, right_key, primary_keys=None, multiple=None, standard_value=None,
                warning_accuracy=None, **kwargs):
    """
    取均值，默认如果不同则 warning
    :param data_s:
    :param left_key:
    :param right_key:
    :param primary_keys:
    :param warning_accuracy:
    :param kwargs:
    :return:
    """
    value_list = []
    multiple = 10000
    standard_value = 10000000
    data_s[left_key] = data_s[left_key] * multiple
    if is_not_nan_or_none(data_s[left_key]):
        value_list.append(data_s[left_key])
    if is_not_nan_or_none(data_s[right_key]):
        value_list.append(data_s[right_key])
    data_count = len(value_list)
    if data_count == 2 and (
            (warning_accuracy is None and value_list[0] != value_list[1])
            or
            (warning_accuracy is not None and abs(value_list[0] - value_list[1]) >= standard_value)
    ):
        pk_str = ','.join([str(data_s[key]) for key in primary_keys]) if primary_keys is not None else None
        if pk_str is None:
            msg = '%s=%f 与 %s=%f 数值不同' % (left_key, value_list[0], right_key, value_list[1])
        else:
            msg = '[%s] %s=%f 与 %s=%f 数值差异较大' % (pk_str, left_key, value_list[0], right_key, value_list[1])
        # warnings.warn(msg, UserWarning)
        logger.warning(msg)
    if data_count == 2:
        ret_val = (sum(value_list) / data_count)
    elif data_count == 1:
        ret_val = value_list[0]
    else:
        ret_val = None
    return ret_val


if __name__ == "__main__":
    # data_df = merge_future_info()
    # print(data_df[:5])
    multiple = None
    data_df = merge_future_daily(date_from=None)
