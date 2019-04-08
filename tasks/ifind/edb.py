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
from tasks.backend import engine_md
from tasks.ifind import invoker
from ibats_utils.db import with_db_session, alter_table_2_myisam
from ibats_utils.db import bunch_insert_on_duplicate_update
import logging

logger = logging.getLogger()
DEBUG = False


@app.task
def import_edb(chain_param=None):
    """
    通过ifind接口获取并导入EDB数据
    :param chain_param: 该参数仅用于 task.chain 串行操作时，上下传递参数使用
    :return:
    """
    table_name = 'ifind_edb'
    has_table = engine_md.has_table(table_name)
    indicators_dic = {
        "M002043802": ("制造业PMI", "manufacturing_PMI", "2005-01-01", None),
        "M002811185": ("财新PMI:综合产出指数", "Caixin_PMI_composite_output_index", "2005-01-01", None),
        "M002811186": ("财新PMI:制造业", "Caixin_PMI_manufacturing", "2005-01-01",None),
        "M002811190": ("财新PMI:服务业经营活动指数", "Caixin_PMI_service_business_activity_index", "2005-01-01", None),
        "M002822183": ("GDP:累计同比", "GDP_cumulative_yoy", "1990-01-01", None),
        "M002826938": ("GDP:同比", "GDP_yoy", "1990-01-01", None),
        "M001620247": ("GDP:全国", "GDP_nationwide", "1990-01-01", None),
        "M001620253": ("GDP:人均", "GDP_per_capita", "1990-01-01", None),
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
        'cn_name': String(100),
        'en_name': String(100),
        'time': Date,
        'note':String(500),
        'value': DOUBLE,
    }
    data_df_list = []
    try:
        for num, (indicators, (item_cn_name, item_en_name,start_date_str,item_note)) in enumerate(indicators_dic.items()):
            begin_time = indicators_date_dic[indicators] if indicators in indicators_date_dic else start_date_str
            end_time = date.today()
            if str_2_date(begin_time) > end_time:
                continue
            begin_time, end_time = date_2_str(begin_time), date_2_str(end_time)
            logger.info("获取 %s %s [%s - %s] 数据", indicators, item_cn_name, begin_time, end_time)
            data_df = invoker.THS_EDBQuery(indicators=indicators,
                                           begintime=begin_time,
                                           endtime=end_time)
            data_df['cn_name'] = item_cn_name
            data_df['en_name'] = item_en_name
            data_df['note'] = item_note
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
    sql_str = "SELECT id, name, min(time) date_from, max(time) date_to FROM {table_name} group by id".format(
        table_name=table_name)
    df = pd.read_sql(sql_str, engine_md)
    return df


if __name__ == "__main__":
    # DEBUG = True
    import_edb()

