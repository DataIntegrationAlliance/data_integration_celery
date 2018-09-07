# -*- coding: utf-8 -*-
"""
Created on 2017/4/20
@author: MG
@desc    : 2018-08-29 已经正式运行测试完成，可以正常使用
"""
from datetime import date, datetime, timedelta
from sqlalchemy.types import String, Date, Integer
from tasks import app
from tasks.ifind import invoker
from tasks.utils.fh_utils import STR_FORMAT_DATE
from tasks.utils.db_utils import with_db_session, bunch_insert_on_duplicate_update, alter_table_2_myisam
from tasks.backend import engine_md
import logging
logger = logging.getLogger()


@app.task
def import_trade_date(chain_param=None):
    """
    增量导入交易日数据导数据库表 wind_trade_date，默认导入未来300天的交易日数据
    2018-01-17 增加港股交易日数据，眼前考虑对减少对已有代码的冲击，新建一张 wind_trade_date_hk表
    日后将会考虑将两张表进行合并
    :param chain_param: 该参数仅用于 task.chain 串行操作时，上下传递参数使用
    :return: 
    """
    table_name = 'ifind_trade_date'
    has_table = engine_md.has_table(table_name)
    exch_code_trade_date_dic = {}
    with with_db_session(engine_md) as session:
        try:
            table = session.execute('SELECT exch_code,max(trade_date) FROM {table_name} GROUP BY exch_code'.format(
                table_name=table_name
            ))
            exch_code_trade_date_dic = {exch_code: trade_date for exch_code, trade_date in table.fetchall()}
        except Exception as exp:
            logger.exception("交易日获取异常")

    exchange_code_dict = {
        "HKEX": "香港",
        "NYMEX": "纽约证券交易所",
        "SZSE": "深圳",
        "CBOT": "芝加哥商品交易所",
        "NASDAQ": "纳斯达克",
        "AMEX": "美国证券交易所",
        "ICE": "洲际交易所",
        "BMD": "马来西亚衍生品交易所"
    }
    exchange_code_list = list(exchange_code_dict.keys())
    for exchange_code in exchange_code_list:
        if exchange_code in exch_code_trade_date_dic:
            trade_date_max = exch_code_trade_date_dic[exchange_code]
            start_date_str = (trade_date_max + timedelta(days=1)).strftime(STR_FORMAT_DATE)
        else:
            start_date_str = '1980-01-01'

        end_date_str = (date.today() + timedelta(days=310)).strftime(STR_FORMAT_DATE)
        trade_date_df = invoker.THS_DateQuery(
            exchange_code, 'dateType:0,period:D,dateFormat:0', start_date_str, end_date_str)
        if trade_date_df is None or trade_date_df.shape[0] == 0:
            logger.warning('%s[%s] [%s - %s] 没有查询到交易日期',
                           exchange_code_dict[exchange_code], exchange_code, start_date_str, end_date_str)
            continue

        data_count = trade_date_df.shape[0]
        logger.info("%s[%s] %d 条交易日数据将被导入 %s",
                    exchange_code_dict[exchange_code], exchange_code, data_count, table_name)
        # with with_db_session(engine_md) as session:
        #     session.execute("INSERT INTO ifind_trade_date (trade_date,exch_code) VALUE (:trade_date,:exch_code)",
        #                     params=[{'trade_date': trade_date, 'exch_code': exchange_code} for trade_date in
        #                             trade_date_df['time']])
        trade_date_df['exch_code'] = exchange_code
        # trade_date_df.rename(columns={'time': 'trade_date'}, inplace=True)
        # trade_date_df.to_sql('ifind_trade_date', engine_md, if_exists='append', index=False, dtype={
        #     'exch_code': String(10),
        #     'trade_date': Date,
        # })
        data_count = bunch_insert_on_duplicate_update(trade_date_df, table_name, engine_md, dtype={
            'exch_code': String(10),
            'time': Date,
        })
        logger.info('%s[%s] %d 条交易日数据导入 %s 完成',
                    exchange_code_dict[exchange_code], exchange_code, data_count, table_name)
        if not has_table and engine_md.has_table(table_name):
            alter_table_2_myisam(engine_md, [table_name])
            # build_primary_key([table_name])
            create_pk_str = """ALTER TABLE {table_name}
                CHANGE COLUMN `exch_code` `exch_code` VARCHAR(10) NOT NULL FIRST,
                CHANGE COLUMN `time` `time` DATE NOT NULL AFTER `exch_code`,
                ADD PRIMARY KEY (`exch_code`, `time`)""".format(table_name=table_name)
            with with_db_session(engine_md) as session:
                session.execute(create_pk_str)
            logger.info('%s 表 `exch_code`, `time` 主键设置完成', table_name)


if __name__ == "__main__":
    # 导入日期数据
    import_trade_date()
