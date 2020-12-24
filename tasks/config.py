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
    broker_url = 'amqp://m*:***@localhost:5672/celery_tasks',
    result_backend = 'amqp://m*:***@localhost:5672/backend'
    accept_content = ['json']  # , 'pickle'
    timezone = 'Asia/Shanghai'
    imports = ('tasks',)
    beat_schedule = {
        # 可根据实际情况进行设置
        'daily_task_tushare': {
            'task': 'tasks.tushare.app_tasks.tushare_tasks_local',
            'schedule': crontab(hour='17', minute=3, day_of_week='1-5'),
        },
        'daily_task_jq': {
            'task': 'tasks.jqdata.app_tasks.jq_tasks_local',
            'schedule': crontab(hour='16', minute=46),  # , day_of_week='1-5'
        },
    }
    broker_heartbeat = 0


# Use a Class-based config to avoid needing a 2nd file
# os.getenv() enables configuration through OS environment variables
class ConfigClass(object):
    # Sql Alchemy settings
    DB_SCHEMA_MD = 'md_integration'
    DB_SCHEMA_VNPY = 'vnpy'
    DB_SCHEMA_ZNJC = 'znjc'
    DB_URL_DIC = {
        DB_SCHEMA_MD: "mysql://m*:****@localhost/{DB_SCHEMA}?charset=utf8".format(
            DB_SCHEMA=DB_SCHEMA_MD),
        DB_SCHEMA_VNPY: "mysql://m*:****@localhost/{DB_SCHEMA}?charset=utf8".format(
            DB_SCHEMA=DB_SCHEMA_VNPY),
        DB_SCHEMA_ZNJC: "mysql://m*:****@localhost/{DB_SCHEMA}?charset=utf8".format(
            DB_SCHEMA=DB_SCHEMA_ZNJC)
    }

    # ifind settings
    IFIND_REST_URL = "http://localhost:5000/iFind/"
    # wind settings
    WIND_REST_URL = "http://localhost:5000/wind/"

    # Tushare settings
    TUSHARE_TOKEN = "***"
    TUSHARE_THREADPOOL_WORKERS = None  # 代表无限制
    # CMC settings
    CMC_PRO_API_KEY = "***"

    JQ_USERNAME = "***"
    JQ_PASSWORD = "***"

    ENABLE_EXPORT_2_SQLITE = False
    SQLITE_FOLDER_PATH = r"/home/mg/github/data_integration_celery/sqlite_db"

    RQDATAC_USER_NAME = '***'
    RQDATAC_QUANT_PASSWORD = '***'

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
    from ibats_utils.mess import logger as logger_mass
    logger_mass.setLevel(logging.DEBUG)
    dictConfig(logging_config)

    MYSTEEL_USERNAME = "***"
    MYSTEEL_PASSWORD = "***"
    DRIVER_PATH = "***"


config = ConfigClass()
celery_config = CeleryConfig()
