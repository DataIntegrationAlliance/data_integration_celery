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
def merge_index_info():
    """
    合并 wind，ifind 数据到对应名称的表中
    :return:
    """
    table_name = 'index_info'
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
        'sec_name': (String(20), prefer_left, {'left_key': 'ths_index_short_name_index', 'right_key': 'sec_name'}),
        'crm_issuer': (String(20), prefer_left, {'left_key': 'ths_publish_org_index', 'right_key': 'crm_issuer'}),
        'base_date': (
            Date, prefer_left, {'left_key': 'ths_index_base_period_index', 'right_key': 'basedate'}),
        'basevalue': (DOUBLE, prefer_left, {'left_key': 'ths_index_base_point_index', 'right_key': 'basevalue'}),
        'country': (String(20), get_value, {'key': 'country'}),
        'launchdate': (Date, get_value, {'key': 'launchdate'}),
        'index_code': (String(20), get_value, {'key': 'ths_index_code_index'}),
        'index_category': (String(10), get_value, {'key': 'ths_index_category_index'}),

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
def merge_index_daily(date_from=None):
    """
    合并 wind，ifind 数据到对应名称的表中
    :param date_from:
    :return:
    """
    table_name = 'index_daily'
    logging.info("更新 %s 开始", table_name)
    has_table = engine_md.has_table(table_name)
    if date_from is None and has_table:
        sql_str = "select adddate(max(trade_date),1) from {table_name}".format(table_name=table_name)
        with with_db_session(engine_md) as session:
            date_from = date_2_str(session.execute(sql_str).scalar())
    ifind_table_name_his = 'ifind_{table_name}_his'.format(table_name=table_name)
    ifind_table_name_ds = 'ifind_{table_name}_ds'.format(table_name=table_name)
    wind_table_name = 'wind_{table_name}'.format(table_name=table_name)
    if date_from is None:
        ifind_his_sql_str = "select * from {table_name}".format(table_name=ifind_table_name_his)
        ifind_ds_sql_str = "select * from {table_name}".format(table_name=ifind_table_name_ds)
        wind_sql_str = "select * from {table_name}".format(table_name=wind_table_name)
        ifind_his_df = pd.read_sql(ifind_his_sql_str, engine_md)  # , index_col='ths_code'
        ifind_ds_df = pd.read_sql(ifind_ds_sql_str, engine_md)  # , index_col='ths_code'
        wind_df = pd.read_sql(wind_sql_str, engine_md)  # , index_col='wind_code'
    else:
        ifind_his_sql_str = "select * from {table_name} where time >= %s".format(table_name=ifind_table_name_his)
        ifind_ds_sql_str = "select * from {table_name} where time >= %s".format(table_name=ifind_table_name_ds)
        wind_sql_str = "select * from {table_name} where trade_date >= %s".format(table_name=wind_table_name)
        ifind_his_df = pd.read_sql(ifind_his_sql_str, engine_md, params=[date_from])  # , index_col='ths_code'
        ifind_ds_df = pd.read_sql(ifind_ds_sql_str, engine_md, params=[date_from])  # , index_col='ths_code'
        wind_df = pd.read_sql(wind_sql_str, engine_md, params=[date_from])  # , index_col='wind_code'

    # change (ifind_df, wind_df)  into joined_df

    ifind_df = pd.merge(ifind_his_df, ifind_ds_df, how='outer',
                        on=['ths_code', 'time'])
    joined_df = pd.merge(ifind_df, wind_df, how='outer',
                         left_on=['ths_code', 'time'], right_on=['wind_code', 'trade_date'],
                         indicator='indicator_column')

    # data, columns processing
    col_merge_dic = {
        'unique_code': (String(20), prefer_left, {'left_key': 'ths_code', 'right_key': 'wind_code'}),
        'trade_date': (Date, prefer_left, {'left_key': 'time', 'right_key': 'trade_date'}),
        'open': (DOUBLE, mean_value, {
            'left_key': 'open_x', 'right_key': 'open_x',
            'warning_accuracy': 0.01, 'primary_keys': ('ths_code', 'time')}),
        'high': (DOUBLE, mean_value, {
            'left_key': 'high_x', 'right_key': 'high_x',
            'warning_accuracy': 0.01, 'primary_keys': ('ths_code', 'time')}),
        'low': (DOUBLE, mean_value, {
            'left_key': 'low_x', 'right_key': 'low_y',
            'warning_accuracy': 0.01, 'primary_keys': ('ths_code', 'time')}),
        'close': (DOUBLE, prefer_left, {
            'left_key': 'close_x', 'right_key': 'close_y',
            'warning_accuracy': 0.01, 'primary_keys': ('ths_code', 'time')}),
        'volume': (DOUBLE, mean3_value, {
            'left_key': 'volume_x', 'right_key': 'volume_y',
            'warning_accuracy': 10, 'primary_keys': ('ths_code', 'time')}),
        'amount': (DOUBLE, mean_value, {
            'left_key': 'amount', 'right_key': 'amt',
            'warning_accuracy': 100, 'primary_keys': ('ths_code', 'time')}),
        'free_turn': (DOUBLE, mean2_value, {
            'left_key': 'turnoverRatio', 'right_key': 'free_turn',
            'warning_accuracy': 0.01, 'primary_keys': ('ths_code', 'time')}),
        'avgPrice': (DOUBLE, get_value, {'key': 'avgPrice'}),
        'changeRatio': (DOUBLE, get_value, {'key': 'changeRatio'}),
        'floatCapitalOfAShares': (DOUBLE, get_value, {'key': 'floatCapitalOfAShares'}),
        'floatCapitalOfBShares': (DOUBLE, get_value, {'key': 'floatCapitalOfBShares'}),
        'floatSharesOfAShares': (DOUBLE, get_value, {'key': 'floatSharesOfAShares'}),
        'floatSharesOfBShares': (DOUBLE, get_value, {'key': 'floatSharesOfBShares'}),
        'pb': (DOUBLE, get_value, {'key': 'pb'}),
        'pcf': (DOUBLE, get_value, {'key': 'pcf'}),
        'pe': (DOUBLE, get_value, {'key': 'pe'}),
        'pe_ttm': (DOUBLE, get_value, {'key': 'preClose'}),
        'preClose': (DOUBLE, get_value, {'key': 'avgPrice'}),
        'ps': (DOUBLE, get_value, {'key': 'ps'}),
        'totalCapital': (DOUBLE, get_value, {'key': 'totalCapital'}),
        'totalShares': (DOUBLE, get_value, {'key': 'totalShares'}),
        'transactionAmount': (DOUBLE, get_value, {'key': 'transactionAmount'}),
        'current_index': (DOUBLE, get_value, {'key': 'ths_current_mv_index'}),
        'dividend_rate_index': (DOUBLE, get_value, {'key': 'ths_dividend_rate_index'}),
        'buy_amt_index': (DOUBLE, get_value, {'key': 'ths_financing_buy_amt_int_index'}),
        'payment_amt_index': (DOUBLE, get_value, {'key': 'ths_financing_payment_amt_int_index'}),
        'ths_flaot_mv_ratio_index': (DOUBLE, get_value, {'key': 'ths_flaot_mv_ratio_index'}),
        'ths_float_ashare_index': (DOUBLE, get_value, {'key': 'ths_float_ashare_index'}),
        'ths_float_ashare_mv_index': (DOUBLE, get_value, {'key': 'ths_float_ashare_mv_index'}),
        'ths_float_ashare_to_total_shares_index': (
            DOUBLE, get_value, {'key': 'ths_float_ashare_to_total_shares_index'}),
        'ths_float_bshare_index': (DOUBLE, get_value, {'key': 'ths_float_bshare_index'}),
        'ths_float_bshare_to_total_shares_index': (
            DOUBLE, get_value, {'key': 'ths_float_bshare_to_total_shares_index'}),
        'ths_float_hshare_to_total_shares_index': (
            DOUBLE, get_value, {'key': 'ths_float_hshare_to_total_shares_index'}),
        'ths_limited_ashare_index': (DOUBLE, get_value, {'key': 'ths_limited_ashare_index'}),
        'ths_margin_trading_amtb_index': (DOUBLE, get_value, {'key': 'ths_margin_trading_amtb_index'}),
        'ths_margin_trading_balance_index': (DOUBLE, get_value, {'key': 'ths_margin_trading_balance_index'}),
        'ths_margin_trading_repay_amt_index': (DOUBLE, get_value, {'key': 'ths_margin_trading_repay_amt_index'}),
        'ths_margin_trading_sell_amt_index': (DOUBLE, get_value, {'key': 'ths_margin_trading_sell_amt_index'}),
        'ths_float_hshare_index': (DOUBLE, get_value, {'key': 'ths_float_hshare_index'}),
        'ths_limited_bshare_index': (DOUBLE, get_value, {'key': 'ths_limited_bshare_index'}),
        'ths_market_value_index': (DOUBLE, get_value, {'key': 'ths_market_value_index'}),
        'ths_mt_payment_vol_int_index': (DOUBLE, get_value, {'key': 'ths_mt_payment_vol_int_index'}),
        'ths_mt_sell_vol_int_index': (DOUBLE, get_value, {'key': 'ths_mt_sell_vol_int_index'}),
        'ths_mv_csrc_alg_index': (DOUBLE, get_value, {'key': 'ths_mv_csrc_alg_index'}),
        'ths_pb_index': (DOUBLE, get_value, {'key': 'ths_pb_index'}),
        'ths_pcf_index': (DOUBLE, get_value, {'key': 'ths_pcf_index'}),
        'ths_pe_index': (DOUBLE, get_value, {'key': 'ths_pe_index'}),
        'ths_ps_index': (DOUBLE, get_value, {'key': 'ths_ps_index'}),
        'ths_short_selling_amtb_index': (DOUBLE, get_value, {'key': 'ths_short_selling_amtb_index'}),
        'ths_short_selling_payment_vol_index': (DOUBLE, get_value, {'key': 'ths_short_selling_payment_vol_index'}),
        'ths_short_selling_sell_vol_index': (DOUBLE, get_value, {'key': 'ths_short_selling_sell_vol_index'}),
        'ths_short_selling_vol_balance_index': (DOUBLE, get_value, {'key': 'ths_short_selling_vol_balance_index'}),
        'ths_state_owned_lp_shares_index': (DOUBLE, get_value, {'key': 'ths_state_owned_lp_shares_index'}),
        'ths_state_owned_shares_index': (DOUBLE, get_value, {'key': 'ths_state_owned_shares_index'}),
        'ths_total_domestic_lp_shares_index': (DOUBLE, get_value, {'key': 'ths_total_domestic_lp_shares_index'}),
        'ths_total_float_shares_index': (DOUBLE, get_value, {'key': 'ths_total_float_shares_index'}),
        'ths_total_float_shares_ratio_index': (DOUBLE, get_value, {'key': 'ths_total_float_shares_ratio_index'}),
        'ths_total_limited_ashare_ratio_index': (DOUBLE, get_value, {'key': 'ths_total_limited_ashare_ratio_index'}),
        'ths_total_shares_index': (DOUBLE, get_value, {'key': 'ths_total_shares_index'}),
        'ths_unfloat_shares_index': (DOUBLE, get_value, {'key': 'ths_unfloat_shares_index'}),
        'ths_annual_volatility_index': (DOUBLE, get_value, {'key': 'ths_annual_volatility_index'}),

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


def is_not_nan_or_none(x):
    """
    判断是否不是 NAN 或 None
    :param x:
    :return:
    """
    return False if x is None else not (isinstance(x, float) and np.isnan(x))


@log_param_when_exception
def mean2_value(data_s: pd.Series, left_key, right_key, primary_keys=None, standard_value=None,
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
    standard_value = 100
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


def mean3_value(data_s: pd.Series, left_key, right_key, primary_keys=None, multiple=None,
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
    multiple = 100
    data_s[left_key] = data_s[left_key] * multiple
    if is_not_nan_or_none(data_s[left_key]):
        value_list.append(data_s[left_key])
    if is_not_nan_or_none(data_s[right_key]):
        value_list.append(data_s[right_key])
    data_count = len(value_list)
    if data_count == 2 and (
            (warning_accuracy is None and value_list[0] != value_list[1])
            or
            (warning_accuracy is not None and abs(value_list[0] - value_list[1]) >= warning_accuracy)
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
    # data_df = merge_index_info()
    # print(data_df[:5])
    multiple = None
    data_df = merge_index_daily(date_from=None)
