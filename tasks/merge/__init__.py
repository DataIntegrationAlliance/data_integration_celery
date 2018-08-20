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
import warnings
import logging
from tasks.utils.fh_utils import is_not_nan_or_none, log_param_when_exception

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
def mean_value(data_s: pd.Series, left_key, right_key, primary_keys=None, **kwargs):
    """
    取均值，默认如果不同则 warning
    :param data_s:
    :param left_key:
    :param right_key:
    :param primary_keys:
    :param kwargs:
    :return:
    """
    value_list = []
    if is_not_nan_or_none(data_s[left_key]):
        value_list.append(data_s[left_key])
    if is_not_nan_or_none(data_s[right_key]):
        value_list.append(data_s[right_key])
    data_count = len(value_list)
    if data_count == 2 and value_list[0] != value_list[1]:
        msg = '[%s] %s=%f 与 %s=%f 数值不同' % (
            ','.join([str(data_s[key]) for key in primary_keys]),
            left_key, value_list[0], right_key, value_list[1])
        warnings.warn(msg, UserWarning)
        logger.warning(msg)
    return sum(value_list) / data_count


def merge_data(data_df: pd.DataFrame, col_merge_rule_dic: dict) -> pd.DataFrame:
    data_list = []
    col_handler_list = [
        (key, partial(v[0], **v[1]))
        for key, v in col_merge_rule_dic.items()]
    for _, data_s in data_df.T.items():
        data_list.append({key: handler(data_s) for key, handler in col_handler_list})

    return pd.DataFrame(data_list)
