#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2019/2/26 17:38
@File    : __init__.py.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from tasks.config import config
from jqdatasdk import *

auth(config.JQ_USERNAME, config.JQ_PASSWORD)

