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
from tasks import app
from tasks.jqdata.available_check import check_diff, backup_table
import logging


logger = logging.getLogger(__name__)


@app.task
def check_all(chain_param=None):
    from tasks.jqdata.stock_daily import TABLE_NAME as TABLE_NAME_STOCK_MD
    from tasks.jqdata.finance_report.balance import TABLE_NAME as TABLE_NAME_BALANCE
    from tasks.jqdata.finance_report.cashflow import TABLE_NAME as TABLE_NAME_CASHFLOW
    from tasks.jqdata.finance_report.income import TABLE_NAME as TABLE_NAME_INCOME
    table_name_list = [
        TABLE_NAME_STOCK_MD,
        TABLE_NAME_BALANCE,
        TABLE_NAME_CASHFLOW,
        TABLE_NAME_INCOME,
    ]
    data_count = len(table_name_list)
    is_ok_list = []
    for num, table_name in enumerate(table_name_list, start=1):
        logger.debug('%d/%d) 检查 %s', num, data_count, table_name)
        is_ok = check(table_name)
        is_ok_list.append((table_name, is_ok))
    logger.info('检查结果：')
    for num, (table_name, is_ok) in enumerate(is_ok_list, start=1):
        logger.info('%d/%d) %s 检查 %s', num, data_count, table_name, '通过' if is_ok else '不通过')


def check(table_name='jq_stock_daily_md_pre'):
    """
    检查 jq_stock_daily_md_pre 表，如果一致则进行备份
    :param table_name:
    :return:
    """
    df = check_diff(table_name)
    is_ok = df is None or df.shape[0] == 0
    if is_ok:
        backup_table(table_name)
    return is_ok


if __name__ == '__main__':
    check_all()
