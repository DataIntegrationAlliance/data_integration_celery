#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/6/6 9:57
@File    : check.py
@contact : mmmaaaggg@163.com
@desc    : 用于对系统配置的环境进行检测，检查是否环境可用，包括mysql、redis等
"""
from config import Config
import threading
import json
import time
import logging
from abat.common import PeriodType
from huobifeeder.utils.fh_utils import bytes_2_str
from huobifeeder.utils.redis import get_channel, get_redis
logger = logging.getLogger()
_signal = {}


def _timer(channel):
    global _signal
    count = 0
    r = get_redis()
    while not _signal['redis']:
        md_str = json.dumps({"message": "Hello World!!", "count": count})
        r.publish(channel, md_str)
        logger.debug("发布成功 %s", md_str)
        count += 1
        if count >= 15:
            break
        time.sleep(1)


def check_redis():
    """
    检测redis是否可以正常工作
    """
    global _signal
    instrument_id = 'rb1805'
    channel = get_channel(market=None, period=PeriodType.Year1, symbol=instrument_id)
    _signal['redis'] = False

    timer_t = threading.Thread(target=_timer, args=(channel,))
    timer_t.start()

    def _receiver(channel):
        # 接收订阅的行情，成功接收后退出
        global _signal
        redis_client = get_redis()
        pub_sub = redis_client.pubsub()
        pub_sub.psubscribe(channel)
        for item in pub_sub.listen():
            logger.debug("接收成功 %s", item)
            if item['type'] == 'pmessage':
                md_dic_str = bytes_2_str(item['data'])
                md_dic = json.loads(md_dic_str)
                if "message" in md_dic and "count" in md_dic:
                    _signal['redis'] = True
                    logger.debug("接收到消息")
                    break

    receiver_t = threading.Thread(target=_receiver, args=(channel,))
    receiver_t.start()

    for n in range(20):
        if _signal['redis']:
            logging.debug("检测redis %d %s", n, _signal['redis'])
            timer_t.join(1)
            break
        time.sleep(1)
    else:
        logger.error("redis 检测未通过")

    return _signal['redis']


def check():
    ok_list = []
    is_ok = check_redis()
    ok_list.append(is_ok)
    if is_ok:
        logger.info("redis 检测成功")

    return all(ok_list)


if __name__ == "__main__":
    is_ok = check()
    logger.info("全部检测完成，%s", is_ok)
