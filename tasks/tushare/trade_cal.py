"""
Created on 2018/9/7
@author: yby
@desc    : 2018-09-7可正常运行
contact author:ybychem@gmail.com
"""
import pandas as pd
import logging
from tasks.backend.orm import build_primary_key
from datetime import date, datetime, timedelta
from tasks.utils.fh_utils import try_2_date, STR_FORMAT_DATE, datetime_2_str, split_chunk
from tasks import app
from sqlalchemy.types import String, Date, Integer
from sqlalchemy.dialects.mysql import DOUBLE
from tasks.backend import engine_md
from tasks.merge.code_mapping import update_from_info_table
from tasks.utils.db_utils import with_db_session, bunch_insert_on_duplicate_update
from tasks.tushare.ts_pro_api import pro

DEBUG = False
logger = logging.getLogger()
DATE_BASE = datetime.strptime('2005-01-01', STR_FORMAT_DATE).date()
ONE_DAY = timedelta(days=1)
# 标示每天几点以后下载当日行情数据
BASE_LINE_HOUR = 16
STR_FORMAT_DATE_TS = '%Y%m%d'


@app.task
def import_trade_date(chain_param=None):
    """
    增量导入交易日数据导数据库表 wind_trade_date，默认导入未来300天的交易日数据
    2018-01-17 增加港股交易日数据，眼前考虑对减少对已有代码的冲击，新建一张 wind_trade_date_hk表
    日后将会考虑将两张表进行合并
    :return:
    """
    table_name = "tushare_trade_date"
    exch_code_trade_date_dic = {}
    with with_db_session(engine_md) as session:
        try:
            table = session.execute('SELECT exchange_id,max(cal_date) FROM {table_name} GROUP BY exchange_id'.format(
                table_name=table_name
            ))
            exch_code_trade_date_dic = {exch_code: trade_date for exch_code, trade_date in table.fetchall()}
        except Exception as exp:
            logger.exception("交易日获取异常")

    exchange_code_dict = {
        "HKEX": "香港联合交易所",
        "SZSE": "深圳证券交易所",
        "SSE": "上海证券交易所",
    }
    exchange_code_list = list(exchange_code_dict.keys())
    for exchange_code in exchange_code_list:
        if exchange_code in exch_code_trade_date_dic:
            trade_date_max = exch_code_trade_date_dic[exchange_code]
            start_date_str = (trade_date_max + timedelta(days=1)).strftime(STR_FORMAT_DATE_TS)
        else:
            start_date_str = '19900101'

        end_date_str = (date.today() + timedelta(days=310)).strftime(STR_FORMAT_DATE_TS)
        trade_date_df = pro.trade_cal(exchange_id='', start_date=start_date_str, end_date=end_date_str)
        if trade_date_df is None or trade_date_df.shape[0] == 0:
            logger.warning('%s[%s] [%s - %s] 没有查询到交易日期',
                           exchange_code_dict[exchange_code], exchange_code, start_date_str, end_date_str)
            continue
        date_count = trade_date_df.shape[0]
        logger.info("%s[%s] %d 条交易日数据将被导入 %s",
                    exchange_code_dict[exchange_code], exchange_code, date_count, table_name)
        date_count = bunch_insert_on_duplicate_update(trade_date_df, table_name, engine_md, dtype={
            'exchange_id': String(10),
            'cal_date': Date,
            'is_open': DOUBLE,
        }, myisam_if_create_table=True)
        logger.info('%s[%s] %d 条交易日数据导入 %s 完成',
                    exchange_code_dict[exchange_code], exchange_code, date_count, table_name)


if __name__ == "__main__":
    # 导入日期数据
    import_trade_date()
