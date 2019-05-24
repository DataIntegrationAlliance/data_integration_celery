"""
Created on 2018/11/20
@author: yby
@desc    : 2018-11-20
contact author:ybychem@gmail.com
"""
import logging
from datetime import datetime, timedelta

from ibats_utils.mess import STR_FORMAT_DATE
from sqlalchemy.dialects.mysql import TINYINT
from sqlalchemy.types import String, Date

from tasks import app
from tasks.backend import engine_md, bunch_insert
from tasks.tushare.ts_pro_api import pro

DEBUG = False
logger = logging.getLogger()
DATE_BASE = datetime.strptime('2005-01-01', STR_FORMAT_DATE).date()
ONE_DAY = timedelta(days=1)
# 标示每天几点以后下载当日行情数据
BASE_LINE_HOUR = 16
STR_FORMAT_DATE_TS = '%Y%m%d'

INDICATOR_PARAM_LIST_TUSHARE_FUTURE_TRADE_CAL = [
    ('exchange', String(20)),
    ('cal_date', Date),
    ('is_open', TINYINT),
    ('pretrade_date', Date),
]
# 设置 dtype
DTYPE_TUSHARE_FUTURE_TRADE_CAL = {key: val for key, val in INDICATOR_PARAM_LIST_TUSHARE_FUTURE_TRADE_CAL}


@app.task
def import_future_trade_cal(chain_param=None):
    """
    插入股票日线数据到最近一个工作日-1。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    :return:
    """
    table_name = 'tushare_future_trade_cal'
    logging.info("更新 %s 开始", table_name)

    has_table = engine_md.has_table(table_name)

    try:
        data_df = pro.trade_cal(exchange='DCE')
        if len(data_df) > 0:
            data_count = bunch_insert(data_df, table_name=table_name, dtype=DTYPE_TUSHARE_FUTURE_TRADE_CAL,
                                      primary_keys=['exchange', 'cal_date'])
            logging.info("更新期货交易日历数据结束， %d 条信息被更新", data_count)
        else:
            logging.info("无数据信息可被更新")
    finally:

        logger.info('%s 表 数据更新完成', table_name)


if __name__ == "__main__":
    # DEBUG = True
    import_future_trade_cal()
