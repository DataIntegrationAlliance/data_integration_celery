#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/3/30 17:53
@File    : config.py
@contact : mmmaaaggg@163.com
@desc    :
"""
import logging
from logging.config import dictConfig
from celery.schedules import crontab


class CeleryConfig:
    # Celery settings
    broker_url = 'amqp://mg:***@localhost:5672/celery_tasks',
    result_backend = 'amqp://mg:***@localhost:5672/backend'
    accept_content = ['json']  # , 'pickle'
    timezone = 'Asia/Shanghai'
    imports = ('tasks', )
    beat_schedule = {
        'daily_task': {
            'task': 'tasks.grouped_task_daily',
            'schedule': crontab(hour='17', minute=15, day_of_week='1-5'),
        },
        'weekly_task': {
            'task': 'tasks.grouped_task_weekly',
            'schedule': crontab(hour='9', minute=50,day_of_week='6'),
        },
    }
    broker_heartbeat = 0


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
    WIND_REST_URL = "http://localhost:5000/wind/"

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
            'handlers': ['console_handler', 'file_handler'],
            'level': logging.DEBUG,
        }
    )
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARN)
    logging.getLogger('urllib3.connectionpool').setLevel(logging.INFO)
    dictConfig(logging_config)


config = ConfigClass()
celery_config = CeleryConfig()
