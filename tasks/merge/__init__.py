#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/8/5 10:50
@File    : __init__.py.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
import pandas as pd
from functools import partial
# import warnings
import logging
from tasks.utils.fh_utils import is_not_nan_or_none, log_param_when_exception, iter_2_range
from tasks.backend import engine_md

logger = logging.getLogger()


@log_param_when_exception
def prefer_left(data_s: pd.Series, left_key, right_key, **kwargs):
    if left_key is not None and is_not_nan_or_none(data_s[left_key]):
        return data_s[left_key]
    elif right_key is not None and is_not_nan_or_none(data_s[right_key]):
        return data_s[right_key]
    else:
        return None


@log_param_when_exception
def prefer_right(data_s: pd.Series, left_key, right_key, **kwargs):
    if right_key is not None and is_not_nan_or_none(data_s[right_key]):
        return data_s[right_key]
    elif left_key is not None and is_not_nan_or_none(data_s[left_key]):
        return data_s[left_key]
    else:
        return None


@log_param_when_exception
def mean_value(data_s: pd.Series, left_key, right_key, primary_keys=None, warning_accuracy=None, **kwargs):
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


@log_param_when_exception
def get_value(data_s: pd.Series, key, default=None, **kwargs):
    if is_not_nan_or_none(data_s[key]):
        return data_s[key]
    else:
        return default


def merge_data(data_df: pd.DataFrame, col_merge_rule_dic: dict) -> pd.DataFrame:
    data_list = []
    col_handler_list = [
        (key, partial(v[0], **v[1]))
        for key, v in col_merge_rule_dic.items()]
    for _, data_s in data_df.T.items():
        data_list.append({key: handler(data_s) for key, handler in col_handler_list})

    return pd.DataFrame(data_list)


def get_ifind_daily_df(table_name, date_from) -> pd.DataFrame:
    if date_from is None:
        sql_str = "select * from {table_name}".format(table_name=table_name)
        data_df = pd.read_sql(sql_str, engine_md)  # , index_col='ths_code'
    else:
        sql_str = "select * from {table_name} where time >= %s".format(table_name=table_name)
        data_df = pd.read_sql(sql_str, engine_md, params=[date_from])  # , index_col='ths_code'
    return data_df


def get_wind_daily_df(table_name, date_from) -> pd.DataFrame:
    if date_from is None:
        sql_str = "select * from {table_name}".format(table_name=table_name)
        data_df = pd.read_sql(sql_str, engine_md)  # , index_col='ths_code'
    else:
        sql_str = "select * from {table_name} where time >= %s".format(table_name=table_name)
        data_df = pd.read_sql(sql_str, engine_md, params=[date_from])  # , index_col='ths_code'
    return data_df


if __name__ == "__main__":
    for x in iter_2_range([1, 2, 3]):
        print(x)
