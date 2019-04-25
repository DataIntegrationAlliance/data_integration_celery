#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/7/23 15:01
@File    : __init__.py.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from celery import Celery, group
from tasks.config import celery_config, config
import logging
import click

from tasks.utils.to_sqlite import transfer_mysql_to_sqlite

logger = logging.getLogger()
app = Celery('tasks',
             config_source=celery_config,
             # broker='amqp://mg:123456@localhost:5672/celery_tasks',
             # backend='amqp://mg:123456@localhost:5672/backend',
             # backend='rpc://',
             )
app.config_from_object(celery_config)


@app.task
def add(x, y):
    """only for test use"""
    return x + y


@app.task
def test_task_n(chain_param=None, n=1):
    """
    only for test use
    :param chain_param: 该参数仅用于 task.chain 串行操作时，上下传递参数使用
    :param n:
    :return:
    """
    import time
    time.sleep(n)
    logger.info('test task %d', n)


@app.task
def test_task():
    """only for test use"""
    logger.info('test task')


# group 语句中的任务并行执行，chain语句串行执行
# group([(test_task_n.s(n=4)|test_task_n.s(n=2)), (test_task_n.s(n=1)|test_task_n.s(n=3))])()


try:
    from tasks.ifind import ifind_import_once, ifind_weekly_task, ifind_daily_task
except ImportError:
    logger.exception("加载 tasks.ifind 失败，该异常不影响其他功能正常使用")

try:
    from tasks.wind import wind_import_once, wind_weekly_task, wind_daily_task
except ImportError:
    logger.exception("加载 tasks.wind 失败，该异常不影响其他功能正常使用")

try:
    from tasks.cmc import cmc_daily_task, cmc_import_once, cmc_weekly_task, cmc_latest_task
except ImportError:
    logger.exception("加载 tasks.cmc 失败，该异常不影响其他功能正常使用")
    cmc_daily_task, cmc_import_once, cmc_weekly_task = None, None, None

try:
    import tushare as ts

    logger.info('设置tushare token')
    ts.set_token(config.TUSHARE_TOKEN)
except AttributeError:
    logger.exception("加载 tushare token 设置失败，该异常不影响其他功能正常使用")

try:
    from tasks.tushare.app_tasks import tushare_daily_task, tushare_weekly_task, tushare_import_once, \
        tushare_tasks_local_first_time, tushare_tasks_local
except ImportError:
    logger.exception("加载 tasks.tushare 失败，该异常不影响其他功能正常使用")
    tushare_daily_task, tushare_weekly_task, tushare_import_once = None, None, None

try:
    from tasks.jqdata.app_tasks import jq_finance_task, jq_once_task, jq_weekly_task, jq_tasks_local_first_time, \
        jq_tasks_local, jq_daily_task
except ImportError:
    logger.exception("加载 tasks.tushare 失败，该异常不影响其他功能正常使用")
    jq_finance_task, jq_once_task, jq_weekly_task = None, None, None


@app.task
def grouped_task_daily():
    """only for test use"""
    group([
        # wind_daily_task,
        # ifind_daily_task,
        tushare_daily_task,
        # cmc_daily_task,
        jq_daily_task,
        jq_finance_task,
    ]
    ).delay()


@app.task
def grouped_task_weekly():
    """only for test use"""
    group([
        # wind_weekly_task,
        # ifind_weekly_task,
        tushare_weekly_task,
        # cmc_weekly_task,
        jq_weekly_task,
    ]
    ).delay()


@app.task
def grouped_task_once():
    """only for test use"""
    group([
        # wind_import_once,
        # ifind_import_once,
        tushare_import_once,
        # cmc_import_once,
        jq_once_task,
    ]
    ).delay()


@app.task
def grouped_task_latest():
    """only for test use"""
    group([
        cmc_latest_task,
    ]).delay()


@app.task
def grouped_task_test():
    """only for test use"""
    group([
        (test_task.s() | test_task_n.s()),
        (test_task.s() | test_task_n.s(n=2))
    ]).delay()


func_list = [
    (tushare_tasks_local_first_time, 'Tushare 首次执行+日常任务'),
    (tushare_tasks_local, 'Tushare 日常任务'),
    (transfer_mysql_to_sqlite, 'Tushare MySQL -> SQLite'),
    (jq_tasks_local_first_time, 'JQData 首次执行+日常任务'),
    (jq_tasks_local, 'JQData 日常任务'),
]


def main():
    promt_str = '0) 退出  '
    promt_str += '  '.join(['%d) %s' % (num, _[1]) for num, _ in enumerate(func_list, start=1)])

    @click.command()
    @click.option('--num', type=click.IntRange(0, len(func_list)), prompt=promt_str)
    def task_choose(num, **kwargs):
        if num is None:
            logger.error('输入 num 参数无效')
            return False
        elif num == 0:
            return True
        else:
            func_list[num - 1][0]()
            return False

    finished = False
    while not finished:
        finished = task_choose(standalone_mode=False)


if __name__ == "__main__":
    main()
