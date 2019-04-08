#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 19-4-8 上午11:23
@File    : stock_daily.py
@contact : mmmaaaggg@163.com
@desc    : 检查财务数据是否有效
主要方法，将历史的有效表进行备份，并与当日更新后的数据进行比较，查看是否存在历史数据不一致的问题
"""
from tasks.jqdata.available_check import check_diff, backup_table


def check(table_name='jq_stock_daily_md_pre'):
    """
    检查 jq_stock_daily_md_pre 表，如果一致则进行备份
    :param table_name:
    :return:
    """
    df = backup_table(table_name)
    if df.shape[0] == 0:
        check_diff(table_name)


if __name__ == '__main__':
    check()
