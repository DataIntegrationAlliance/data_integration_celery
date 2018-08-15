#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/8/15 9:06
@File    : edb.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from datetime import date
import pandas as pd
from sqlalchemy.types import String, Date
from sqlalchemy.dialects.mysql import DOUBLE
from tasks import app
from direstinvoker.utils.fh_utils import str_2_date, date_2_str
from tasks import engine_md
from tasks.ifind import invoker
from tasks.utils.db_utils import with_db_session, alter_table_2_myisam
from tasks.utils.db_utils import bunch_insert_on_duplicate_update
import logging

logger = logging.getLogger()
DEBUG = False


@app.task
def import_edb():
    """
    通过ifind接口获取并导入EDB数据
    :return:
    """
    table_name = 'ifind_edb'
    has_table = engine_md.has_table(table_name)
    indicators_dic = {
        "M002043802": ("制造业PMI", "2005-01-01"),
        "M002811185": ("财新PMI:综合产出指数", "2005-01-01"),
        "M002811186": ("财新PMI:制造业", "2005-01-01"),
        "M002811190": ("财新PMI:服务业经营活动指数", "2005-01-01"),
        "M002822183": ("GDP:累计同比", "1990-01-01"),
        "M002826938": ("GDP:同比", "1990-01-01"),
        "M001620247": ("GDP:全国", "1990-01-01"),
        "M001620253": ("GDP:人均", "1990-01-01"),
    }
    if has_table:
        sql_str = """select id, adddate(max(time),1) from {table_name} group by id""".format(table_name=table_name)
        with with_db_session(engine_md) as session:
            indicators_date_dic = dict(session.execute(sql_str).fetchall())
    else:
        indicators_date_dic = {}

    # 设置 dtype
    dtype = {
        'id': String(20),
        'name': String(100),
        'time': Date,
        'value': DOUBLE,
    }
    data_df_list = []
    try:
        for num, (indicators, (item_name, start_date_str)) in enumerate(indicators_dic.items()):
            begin_time = indicators_date_dic[indicators] if indicators in indicators_date_dic else start_date_str
            end_time = date.today()
            if str_2_date(begin_time) > end_time:
                continue
            begin_time, end_time = date_2_str(begin_time), date_2_str(end_time)
            logger.info("获取 %s %s [%s - %s] 数据", indicators, item_name, begin_time, end_time)
            data_df = invoker.THS_EDBQuery(indicators=indicators,
                                           begintime=begin_time,
                                           endtime=end_time)
            data_df['name'] = item_name
            data_df_list.append(data_df)
            if DEBUG and len(data_df_list) > 1:
                break
    finally:
        data_df_all = pd.concat(data_df_list)
        data_count = bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md, dtype=dtype)
        logger.info("更新 %s 结束 %d 条信息被更新", table_name, data_count)
        if not has_table and engine_md.has_table(table_name):
            alter_table_2_myisam(engine_md, [table_name])
            create_pk_str = """ALTER TABLE {table_name}
                CHANGE COLUMN `id` `id` VARCHAR(20) NOT NULL FIRST,
                CHANGE COLUMN `time` `time` DATE NOT NULL AFTER `id`,
                ADD PRIMARY KEY (`id`, `time`)""".format(table_name=table_name)
            with with_db_session(engine_md) as session:
                session.execute(create_pk_str)
            logger.info('%s 表 `id`, `time` 主键设置完成', table_name)


def get_edb_item_date_range():
    table_name = 'ifind_edb'
    sql_str = "SELECT id, name, min(time) date_from, max(time) date_to FROM {table_name} group by id". format(
        table_name=table_name)
    df = pd.read_sql(sql_str, engine_md)
    return df


if __name__ == "__main__":
    # DEBUG = True
    import_edb()
    df = get_edb_item_date_range()
    print(df)
