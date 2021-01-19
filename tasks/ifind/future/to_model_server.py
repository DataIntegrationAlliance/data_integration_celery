"""
@author  : MG
@Time    : 2021/1/19 14:43
@File    : to_model_server.py
@contact : mmmaaaggg@163.com
@desc    : 用于
"""
import logging

import pandas as pd
from ibats_utils.db import with_db_session

from tasks import app
from tasks.backend import engine_md
from tasks.ifind.future.to_vnpy import get_code_list_by_types

logger = logging.getLogger()


@app.task
def daily_2_model_server(chain_param=None, instrument_types=None):
    from tasks.config import config
    from tasks.backend import engine_dic
    table_name = 'ifind_future_daily'
    engine_model_server = engine_dic[config.DB_SCHEMA_MODEL]
    has_table = engine_model_server.has_table(table_name)
    if not has_table:
        logger.error('当前数据库 %s 没有 %s 表，建议使用先建立相应的数据库表后再进行导入操作',
                     engine_model_server, table_name)
        return

    code_list = get_code_list_by_types(instrument_types)
    code_count = len(code_list)
    for n, ths_code in enumerate(code_list, start=1):
        # symbol, exchange = ths_code.split('.')
        sql_str = f"select max(`time`) from {table_name} where ths_code = :ths_code"
        with with_db_session(engine_model_server) as session:
            trade_date_max = session.scalar(sql_str, params={'ths_code': ths_code})

        # 读取日线数据
        if trade_date_max is None:
            sql_str = f"select * from {table_name} where ths_code = %s and `close` <> 0"
            df = pd.read_sql(sql_str, engine_md, params=[ths_code]).dropna()
        else:
            sql_str = f"select * from {table_name} where ths_code = %s and trade_date > %s and `close` <> 0"
            df = pd.read_sql(sql_str, engine_md, params=[ths_code, trade_date_max]).dropna()

        df_len = df.shape[0]
        if df_len == 0:
            continue

        df.to_sql(table_name, engine_model_server, if_exists='append', index=False)
        logger.info("%d/%d) %s %d data -> %s",
                    n, code_count, ths_code, df.shape[0], table_name)


def run_task():
    daily_2_model_server()


if __name__ == "__main__":
    run_task()
