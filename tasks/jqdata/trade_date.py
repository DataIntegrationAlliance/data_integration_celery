#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 19-3-26 上午7:57
@File    : trade_date.py
@contact : mmmaaaggg@163.com
@desc    : 导入聚宽 交易日数据，使用 get_all_trade_days get_trade_days 两个接口
"""
from datetime import timedelta, date
import pandas as pd
from tasks import app, config
from tasks.jqdata import get_all_trade_days, get_trade_days
from tasks.backend import engine_md
import logging
from sqlalchemy.types import Date
from tasks.utils.db_utils import execute_scalar, bunch_insert_on_duplicate_update

logger = logging.getLogger(__name__)
TABLE_NAME = 'jq_trade_date'


@app.task
def import_jq_trade_date(chain_param=None):
    """
    插入股票日线数据到最近一个工作日-1。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    :return:
    """
    logger.info("更新 %s 开始", TABLE_NAME)
    # 判断表是否已经存在
    has_table = engine_md.has_table(TABLE_NAME)
    if has_table:
        trade_day_max = execute_scalar(engine_md, f'SELECT max(trade_date) FROM {TABLE_NAME}')
        trade_date_list = get_trade_days(start_date=(trade_day_max + timedelta(days=1)),
                                         end_date=(date.today() + timedelta(days=366)))
    else:
        trade_date_list = get_all_trade_days()

    date_count = len(trade_date_list)
    if date_count == 0:
        logger.info("没有更多的交易日数据可被导入")
        return

    logger.info("%d 条交易日数据将被导入", date_count)
    trade_date_df = pd.DataFrame({'trade_date': trade_date_list})
    bunch_insert_on_duplicate_update(trade_date_df, TABLE_NAME, engine_md,
                                     dtype={'trade_date': Date},
                                     myisam_if_create_table=True,
                                     primary_keys=['trade_date'], schema=config.DB_SCHEMA_MD)
    logger.info('%d 条交易日数据导入 %s 完成', date_count, TABLE_NAME)


if __name__ == "__main__":
    import_jq_trade_date()
