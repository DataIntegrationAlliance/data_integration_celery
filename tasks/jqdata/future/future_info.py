#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 19-4-19 下午2:56
@File    : future_info.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from sqlalchemy import String, Date

from tasks.backend import bunch_insert_p
from tasks.jqdata import import_info_table
from tasks import app
import logging
import re

logger = logging.getLogger(__name__)
TYPE_NAME = 'futures'
TABLE_NAME = f'jq_{TYPE_NAME}_info'


@app.task
def import_jq_future_info(chain_param=None, refresh=False):
    """ 获取全市场股票代码及名称
    """
    pattern = re.compile(r"[A-Za-z]+(?=\d{3,4}\.[A-Za-z]{4}$)")
    df = import_info_table(TYPE_NAME, insert_db=False)
    df['underlying_symbol'] = df['jq_code'].apply(lambda x: pattern.search(x).group())
    logger.info("更新 %s 开始", TABLE_NAME)
    # has_table = engine_md.has_table(table_name)
    param_list = [
        ('jq_code', String(20)),
        ('underlying_symbol', String(6)),
        ('display_name', String(20)),
        ('name', String(20)),
        ('start_date', Date),
        ('end_date', Date),
    ]
    # 设置 dtype
    dtype = {key: val for key, val in param_list}
    data_count = bunch_insert_p(df, table_name=TABLE_NAME, dtype=dtype, primary_keys=['jq_code'])
    logging.info("更新 %s 完成 存量数据 %d 条", TABLE_NAME, data_count)


if __name__ == "__main__":
    import_jq_future_info()
