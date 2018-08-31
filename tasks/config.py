#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/3/30 17:53
@File    : config.py
@contact : mmmaaaggg@163.com
@desc    :
"""
import os
import logging
from logging.config import dictConfig
import platform
basedir = os.path.abspath(os.path.dirname(__file__))
IS_LINUX_OS = platform.os.name != 'nt'
if IS_LINUX_OS:
    from celery.schedules import crontab


class CeleryConfig:
    # Celery settings
    broker_url = 'amqp://mg:***@localhost:3306/celery_tasks',
    result_backend = 'amqp://mg:***@localhost:3306/backend'
    accept_content = ['json']  # , 'pickle'
    timezone = 'Asia/Shanghai'
    imports = ('tasks', )
    # beat_schedule = {
    #     'ptask': {
    #         'task': 'celery_task.task.chain_task',
    #         # 'schedule': timedelta(seconds=5),
    #         'schedule': crontab(hour='18') if IS_LINUX_OS else None,
    #         # 'args': (16, 73),  # 当前任务没有参数
    #     },
    # }


# Use a Class-based config to avoid needing a 2nd file
# os.getenv() enables configuration through OS environment variables
class ConfigClass(object):
    # Sql Alchemy settings
    DB_SCHEMA_MD = 'md_integration'
    DB_URL_DIC = {
        DB_SCHEMA_MD: "mysql://mg:***@localhost/{DB_SCHEMA_MD}?charset=utf8".format(
            DB_SCHEMA_MD=DB_SCHEMA_MD)
    }

    # ifind settings
    IFIND_REST_URL = "http://localhost:5000/iFind/"
    # wind settings
    WIND_REST_URL = "http://10.0.5.63:5000/wind/"
    # Tushare settings
    TUSHARE_TOKEN = "***"
    # CMC settings
    CMC_PRO_API_KEY = "***"

    # log settings
    logging_config = dict(
        version=1,
        formatters={
            'simple': {
                'format': '%(asctime)s %(name)s|%(module)s.%(funcName)s:%(lineno)d %(levelname)s %(message)s'}
        },
        handlers={
            'file_handler':
                {
                    'class': 'logging.handlers.RotatingFileHandler',
                    'filename': 'logger.log',
                    'maxBytes': 1024 * 1024 * 10,
                    'backupCount': 5,
                    'level': 'DEBUG',
                    'formatter': 'simple',
                    'encoding': 'utf8'
                },
            'console_handler':
                {
                    'class': 'logging.StreamHandler',
                    'level': 'DEBUG',
                    'formatter': 'simple'
                }
        },

        root={
            'handlers': ['console_handler'],  # , 'file_handler'
            'level': logging.DEBUG,
        }
    )
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARN)
    logging.getLogger('urllib3.connectionpool').setLevel(logging.INFO)
    dictConfig(logging_config)


config = ConfigClass()
celery_config = CeleryConfig()
