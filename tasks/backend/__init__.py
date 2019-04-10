#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/6/12 13:47
@File    : __init__.py.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from sqlalchemy import create_engine, MetaData
from tasks.config import config
from ibats_utils.db import bunch_insert_on_duplicate_update, with_db_session, execute_scalar, execute_sql
from functools import partial

engine_dic = {key: create_engine(url) for key, url in config.DB_URL_DIC.items()}
engine_md = engine_dic[config.DB_SCHEMA_MD]
bunch_insert = partial(bunch_insert_on_duplicate_update,
                       engine=engine_md, myisam_if_create_table=True, schema=config.DB_SCHEMA_MD)
with_db_session_p = partial(with_db_session, engine=engine_md)
execute_scalar_p = partial(execute_scalar, engine=engine_md)
execute_sql_commit = partial(execute_sql, engine=engine_md, commit=True)
