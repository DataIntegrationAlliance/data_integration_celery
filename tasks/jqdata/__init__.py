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
import jqdatasdk
import logging


logger = logging.getLogger(__name__)
HAS_AUTHORIZED = False
AUTHORIZED_SUCC = False


def check_before_run(check_func):

    import functools

    def func_wrapper(target_func):

        @functools.wraps(target_func)
        def call_func(*args, **kwargs):
            is_ok = check_func()
            if is_ok:
                return target_func(*args, **kwargs)
            else:
                raise ImportError('jqdatasdk 授权失败')

        return call_func

    return func_wrapper


def auth_once():
    global HAS_AUTHORIZED, AUTHORIZED_SUCC
    if HAS_AUTHORIZED:
        try:
            jqdatasdk.auth(config.JQ_USERNAME, config.JQ_PASSWORD)
            AUTHORIZED_SUCC = True
            logger.info('jqdatasdk.auth 授权成功')
        except:
            logger.exception("jqdatasdk 授权异常")

        HAS_AUTHORIZED = True

    return AUTHORIZED_SUCC


finance = jqdatasdk.finance


@check_before_run(auth_once)
def get_trade_days(*args, **kwargs):
    return jqdatasdk.get_trade_days(*args, **kwargs)


@check_before_run(auth_once)
def get_all_securities(*args, **kwargs):
    return jqdatasdk.get_all_securities(*args, **kwargs)


@check_before_run(auth_once)
def get_price(*args, **kwargs):
    return jqdatasdk.get_price(*args, **kwargs)


@check_before_run(auth_once)
def get_all_trade_days(*args, **kwargs):
    return jqdatasdk.get_all_trade_days(*args, **kwargs)


@check_before_run(auth_once)
def query(*args, **kwargs):
    return jqdatasdk.query(*args, **kwargs)
