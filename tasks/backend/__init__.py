#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/6/12 13:47
@File    : __init__.py.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from sqlalchemy import create_engine
from tasks.config import config
from ibats_utils.db import bunch_insert_on_duplicate_update, with_db_session, execute_scalar, execute_sql
from functools import partial
import pandas as pd

engine_dic = {key: create_engine(url) for key, url in config.DB_URL_DIC.items()}
engine_md = engine_dic[config.DB_SCHEMA_MD]
bunch_insert_p = partial(bunch_insert_on_duplicate_update,
                         engine=engine_md, myisam_if_create_table=True, schema=config.DB_SCHEMA_MD)
with_db_session_p = partial(with_db_session, engine=engine_md)
execute_scalar_p = partial(execute_scalar, engine=engine_md)
execute_sql_commit = partial(execute_sql, engine=engine_md, commit=True)


def bunch_insert(df, table_name, dtype, primary_keys):
    from tasks.utils.to_sqlite import bunch_insert_sqlite
    if isinstance(df, list):
        df = pd.concat(df)

    data_count = bunch_insert_p(df, table_name=table_name, dtype=dtype, primary_keys=primary_keys)

    if config.ENABLE_EXPORT_2_SQLITE:
        bunch_insert_sqlite(df, mysql_table_name=table_name, primary_keys=primary_keys)

    return data_count
