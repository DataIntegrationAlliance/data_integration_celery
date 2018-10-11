#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : yby
@Time    : 2018/9/15 17:53
@File    : config.py
@contact :
@desc    :yeung's config
"""
import os
import logging
from logging.config import dictConfig
import platform
basedir = os.path.abspath(os.path.dirname(__file__))
IS_LINUX_OS = platform.os.name != 'nt'
from celery.schedules import crontab
import tasks

class CeleryConfig:
    # Celery settings
    broker_url = 'amqp://yeung:yeung870110@localhost:5672/celery_tasks',
    result_backend = 'amqp://yeung:yeung870110@localhost:5672/backend'
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
        DB_SCHEMA_MD: "mysql://root:041001131@localhost/{DB_SCHEMA_MD}?charset=utf8".format(
            DB_SCHEMA_MD=DB_SCHEMA_MD)
    }

    # ifind settings
    IFIND_REST_URL = "http://localhost:5000/iFind/"
    # wind settings
    WIND_REST_URL = "http://localhost:5000/wind/"
    # WIND_REST_URL = "http://10.0.3.66:5000/wind/"
    # WIND_REST_URL = "http://10.0.5.61:5000/wind/"
    # WIND_REST_URL = "http://10.0.5.63:5000/wind/"

    # Tushare settings
    TUSHARE_TOKEN = "92d1cbe389dc2c724f98748edcc2a01abbe7df4cac721fb1ff03b363"
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
