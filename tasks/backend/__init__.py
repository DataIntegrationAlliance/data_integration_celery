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

engine_dic = {key: create_engine(url) for key, url in config.DB_URL_DIC.items()}
metadata_dic = {key: MetaData(engine) for key, engine in engine_dic.items()}
engine_md = engine_dic[config.DB_NAME_MD]
metadata_md = metadata_dic[config.DB_NAME_MD]
