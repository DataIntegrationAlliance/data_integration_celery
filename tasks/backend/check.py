#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/6/6 9:57
@File    : check.py
@contact : mmmaaaggg@163.com
@desc    : 用于对系统配置的环境进行检测，检查是否环境可用，包括mysql、redis等
"""
from tasks.config import config
import logging
logger = logging.getLogger()
_signal = {}


def check():
    ok_list = []
    is_ok = check_jq()
    ok_list.append(is_ok)
    if is_ok:
        logger.info("redis 检测成功")

    return all(ok_list)


def check_jq():
    try:
        import jqdatasdk
        jqdatasdk.auth(config.JQ_USERNAME, config.JQ_PASSWORD)
        return True
    except:
        logger.exception("聚宽链接失败")
        return False


if __name__ == "__main__":
    is_ok = check_jq()
    # is_ok = check()
    logger.info("全部检测完成，%s", is_ok)
