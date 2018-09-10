"""
Created on 2018/8/25
@author: yby
@ybychem@gmail.com
@desc    : 2018-09-02 已经正式运行测试完成，可以正常使用
"""
import tushare as ts
import pandas as pd
import logging
from tasks.backend.orm import build_primary_key
from datetime import date, datetime, timedelta
from tasks.utils.fh_utils import try_2_date, STR_FORMAT_DATE, datetime_2_str, split_chunk
from tasks import app
from sqlalchemy.types import String, Date, Text,Integer
from sqlalchemy.dialects.mysql import DOUBLE
from tasks.backend import engine_md
from tasks.merge.code_mapping import update_from_info_table
from tasks.utils.db_utils import with_db_session, add_col_2_table, alter_table_2_myisam, \
    bunch_insert_on_duplicate_update

DEBUG = False
logger = logging.getLogger()
pro = ts.pro_api()
DATE_BASE = datetime.strptime('2005-01-01', STR_FORMAT_DATE).date()
ONE_DAY = timedelta(days=1)
# 标示每天几点以后下载当日行情数据
BASE_LINE_HOUR = 16
STR_FORMAT_DATE_TS = '%Y%m%d'

@app.task
def import_tushare_suspend(chain_param=None):
    """
    插入股票日线数据到最近一个工作日-1。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    :return:
    """
    table_name = 'tushare_suspend'
    logging.info("更新 %s 开始", table_name)
    param_list = [
        ('ts_code', String(20)),
        ('suspend_date', Date),
        ('resume_date', Date),
        ('ann_date', Date),
        ('suspend_reason', Text),
        ('reason_type', Text),

    ]

    has_table = engine_md.has_table(table_name)
    # 进行表格判断，确定是否含有tushare_suspend

    # 下面一定要注意引用表的来源，否则可能是串，提取混乱！！！比如本表是tushare_daily_basic，所以引用的也是这个，如果引用错误，就全部乱了l
    if has_table:
        sql_str = """
                  select cal_date            
                  FROM
                   (
                    select * from tushare_trade_date trddate 
                    where( cal_date>(SELECT max(suspend_date) FROM {table_name} ))
                  )tt
                  where (is_open=1 
                         and cal_date <= if(hour(now())<16, subdate(curdate(),1), curdate()) 
                         and exchange_id='SSE') """.format(table_name='tushare_suspend')
    else:
        sql_str = """
                  select cal_date from tushare_trade_date trddate where (trddate.is_open=1 
               and cal_date <= if(hour(now())<16, subdate(curdate(),1), curdate()) 
               and exchange_id='SSE') order by cal_date"""
        logger.warning('%s 不存在，仅使用 tushare_stock_info 表进行计算日期范围', table_name)

    with with_db_session(engine_md) as session:
        # 获取交易日数据
        table = session.execute(sql_str)
        trddate = list(row[0] for row in table.fetchall())
    # 设置 dtype
    dtype = {key: val for key, val in param_list}


    try:
        for i in range(len(trddate)):
            suspend_date = datetime_2_str(trddate[i], STR_FORMAT_DATE_TS)
            data_df = pro.suspend(ts_code='', suspend_date=suspend_date, resume_date='',fields='')
            if len(data_df) > 0:
                data_count = bunch_insert_on_duplicate_update(data_df, table_name, engine_md, dtype)
                logging.info("%s 更新停复牌信息表 %s 结束 %d 条信息被更新", suspend_date,table_name, data_count)
            else:
                logging.info("%s 当日无停牌股票",suspend_date)

    finally:
        if not has_table and engine_md.has_table(table_name):
            alter_table_2_myisam(engine_md, [table_name])
            # build_primary_key([table_name])
            create_pk_str = """ALTER TABLE {table_name}
                            CHANGE COLUMN `ts_code` `ts_code` VARCHAR(20) NOT NULL FIRST,
                            CHANGE COLUMN `suspend_date` `suspend_date` DATE NOT NULL AFTER `ts_code`,
                            ADD PRIMARY KEY (`ts_code`, `suspend_date`)""".format(table_name=table_name)
            with with_db_session(engine_md) as session:
                session.execute(create_pk_str)
            logger.info('%s 表 `ts_code`, `trade_date` 主键设置完成', table_name)


if __name__ == "__main__":
    # DEBUG = True
    import_tushare_suspend()



