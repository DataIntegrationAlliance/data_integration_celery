# -*- coding: utf-8 -*-
"""
Created on 2017/4/14
@author: MG
"""
import pandas as pd
import logging
from datetime import date, datetime, timedelta
from direstinvoker.utils.fh_utils import str_2_date, date_2_str
from tasks.wind import invoker, APIError
from tasks.utils.fh_utils import STR_FORMAT_DATE, split_chunk
from tasks import app
from sqlalchemy.types import String, Date, Integer, Text
from sqlalchemy.dialects.mysql import DOUBLE
from tasks.backend import engine_md
from tasks.utils.db_utils import with_db_session, alter_table_2_myisam, \
    bunch_insert_on_duplicate_update

DEBUG = False
logger = logging.getLogger()
DATE_BASE = datetime.strptime('2005-01-01', STR_FORMAT_DATE).date()
ONE_DAY = timedelta(days=1)
# 标示每天几点以后下载当日行情数据
BASE_LINE_HOUR = 16


@app.task
def import_macroeconomy_info(chain_param=None):
    """
    :param chain_param:  在celery 中將前面結果做爲參數傳給後面的任務
    :return:
    """
    table_name = 'wind_macroeconomy_info'
    has_table = engine_md.has_table(table_name)
    indicators_dic = [
        # 企业信心指标
        ["M5786898", "BCI", "中国企业经营状况指数", "2011-09-30", None, '长江商学院'],

        # 中国GDP 宏观数据
        # CPI
        ["M0000612", "CPI", "CPI", "1990-01-31", None, 'CPI同比'],
        ["M0000616", "CPI_food", "CPI食品", "1994-01-31", None, '食品CPI同比'],
        ["M0000613", "CPI_nofood", "CPI非食品", "2001-01-31", None, '非食品CPI同比'],
        ["M0085932", "CPI_core", "核心CPI", "2013-01-31", None, '不包括食品和能源CPI同比'],
        # PPI
        ["M0001227", "PPI_industrial_yoy", "工业品PPI当月同比", "1996-10-31", None, '全部工业品当月同比'],
        ["M0049160", "PPI_industrial_mom", "工业品PPI当月环比", "2002-01-31", None, '全部工业品当月环比'],
        ["M0001244", "PPI_industrial_cumulative_yoy", "工业品PPI累计同比", "1996-10-31", None, '全部工业品累计同比'],
        ["M0001228", "PPI_means_production_yoy", "生产资料PPI当月同比", "1996-10-31", None, '生产资料PPI当月同比'],
        ["M0001232", "PPI_means_subsistence_yoy", "生活资料PPI当月同比", "1996-10-31", None, "生活资料PPI当月同比"],
        #股票市场资金流向
        ["M0062050", "active_buy_A", "资金主动买入A股", "2010-01-05", None, "单位:亿元"],
        ["M0062051", "active_buy_main_board", "资金主动买入主板", "2010-01-05", None, "单位:亿元"],
        ["M0062052", "active_buy_SME_board", "资金主动买入中小板", "2010-01-05", None, "单位:亿元"],
        ["M0062053", "active_buy_GEM_board", "资金主动买入创业板", "2010-01-07", None, "单位:亿元"],
        ["M5543291", "active_buy_ZZ500", "资金主动买入中证500", "2007-01-15", None, "单位:亿元"],
        ["M5543267", "active_buy_SH50", "资金主动买入上证50", "2006-12-05", None, "单位:亿元"],
        ["M0062054", "active_buy_HS300", "资金主动买入沪深300", "2009-01-07", None, "单位:亿元"],


    ]
    dtype = {
        'wind_code': String(20),
        'en_name': String(120),
        'cn_name': String(120),
        'begin_date': Date,
        'end_date': Date,
        'remark': Text,
    }
    name_list = ['wind_code', 'en_name', 'cn_name', 'begin_date', 'end_date', 'remark']
    info_df = pd.DataFrame(data=indicators_dic, columns=name_list)
    data_count = bunch_insert_on_duplicate_update(info_df, table_name, engine_md, dtype)
    logger.info('%d 条记录被更新', data_count)
    if not has_table and engine_md.has_table(table_name):
        alter_table_2_myisam(engine_md, [table_name])
        create_pk_str = """ALTER TABLE {table_name}
            CHANGE COLUMN `wind_code` `wind_code` VARCHAR(20) NOT NULL FIRST,
            ADD PRIMARY KEY (`wind_code`)""".format(table_name=table_name)
        with with_db_session(engine_md) as session:
            session.execute(create_pk_str)
        logger.info('%s 表 wind_code 主键设置完成', table_name)


@app.task
def import_macroeconom_edb(chain_param=None, wind_code_set=None):
    """
    通过wind接口获取并导入EDB数据
    :param chain_param:  在celery 中將前面結果做爲參數傳給後面的任務
    :return:
    """
    table_name = 'wind_macroeconomy_edb'
    has_table = engine_md.has_table(table_name)
    logging.info("更新 %s 开始", table_name)
    param_list = [
        ('close', DOUBLE),
        ('wind_code',String(20)),
        ('trade_date', Date),
    ]
    rename_col_dic = {key.upper(): key.lower() for key, _ in param_list}

    # 进行表格判断，确定是否含有 wind_macroeconomy_edb
    if has_table:
        sql_str = """
                SELECT wind_code, date_frm, if(end_date<end_date2, end_date, end_date2) date_to
                FROM
                (
                SELECT info.wind_code, ifnull(trade_date, begin_date) date_frm, end_date,
                if(hour(now())<16, subdate(curdate(),1), curdate()) end_date2
                FROM 
                    wind_macroeconomy_info info 
                LEFT OUTER JOIN
                    (SELECT wind_code, adddate(max(trade_date),1) trade_date FROM {table_name} GROUP BY wind_code) daily
                ON info.wind_code = daily.wind_code
                ) tt
                WHERE date_frm <= if(end_date<end_date2, end_date, end_date2) 
                ORDER BY wind_code""".format(table_name=table_name)
    else:
        logger.warning('%s 不存在，仅使用 wind_macroeconomy_info 表进行计算日期范围', table_name)
        sql_str = """
                SELECT wind_code, date_frm, if(end_date<end_date2, end_date, end_date2) date_to
                FROM
                  (
                    SELECT info.wind_code, begin_date date_frm, end_date,
                    if(hour(now())<16, subdate(curdate(),1), curdate()) end_date2
                    FROM wind_macroeconomy_info info 
                  ) tt
                WHERE date_frm <= if(end_date<end_date2, end_date, end_date2) 
                ORDER BY wind_code"""

    with with_db_session(engine_md) as session:
        # 获取每只股票需要获取日线数据的日期区间
        table = session.execute(sql_str)
        # 计算每只股票需要获取日线数据的日期区间
        begin_time,wind_code_set = None,None
        # 获取date_from,date_to，将date_from,date_to做为value值
        code_date_range_dic = {
            wind_code: (date_from if begin_time is None else min([date_from, begin_time]), date_to)
            for wind_code, date_from, date_to in table.fetchall() if
            wind_code_set is None or wind_code in wind_code_set}
    # 设置 dtype
    dtype = {key: val for key, val in param_list}

    data_df_list = []
    data_len = len(code_date_range_dic)
    logger.info('%d macroeconomic information will been import into wind_macroeconomy_edb', data_len)
    # 将data_df数据，添加到data_df_list
    try:
        for num, (key_code, (date_from, date_to)) in enumerate(code_date_range_dic.items(), start=1):
            logger.debug('%d/%d) %s [%s - %s]', num, data_len, key_code, date_from, date_to)
            try:
                data_df = invoker.edb(key_code, date_from, date_to, options='')
            except APIError as exp:
                logger.exception("%d/%d) %s 执行异常", num, data_len, key_code)
                if exp.ret_dic.setdefault('error_code', 0) in (
                        -40520007,  # 没有可用数据
                        -40521009,  # 数据解码失败。检查输入参数是否正确，如：日期参数注意大小月月末及短二月
                ):
                    continue
                else:
                    break
            if data_df is None:
                logger.warning('%d/%d) %s has no data during %s %s', num, data_len, key_code, date_from, date_to)
                continue
            logger.info('%d/%d) %d data of %s between %s and %s', num, data_len, data_df.shape[0], key_code, date_from,
                        date_to)
            data_df['wind_code'] = key_code
            data_df.rename(columns={key_code.upper(): 'value'}, inplace=True)
            data_df_list.append(data_df)
            # 仅调试使用
            if DEBUG and len(data_df_list) > 2:
                break

    finally:
        # 导入数据库
        if len(data_df_list) > 0:
            data_df_all = pd.concat(data_df_list)
            data_df_all.index.rename('trade_date', inplace=True)
            data_df_all.reset_index(inplace=True)
            # data_df_all.rename(columns=rename_col_dic, inplace=True)
            data_count = bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md, dtype)
            logging.info("更新 %s 结束 %d 条信息被更新", table_name, data_count)
            if not has_table and engine_md.has_table(table_name):
                alter_table_2_myisam(engine_md, [table_name])
                # build_primary_key([table_name])
                create_pk_str = """ALTER TABLE {table_name}
                    CHANGE COLUMN `wind_code` `wind_code` VARCHAR(20) NOT NULL FIRST,
                    CHANGE COLUMN `trade_date` `trade_date` DATE NOT NULL AFTER `wind_code`,
                    ADD PRIMARY KEY (`wind_code`, `trade_date`)""".format(table_name=table_name)
                with with_db_session(engine_md) as session:
                    session.execute(create_pk_str)
                logger.info('%s 表 wind_code `trade_date` 主键设置完成', table_name)


if __name__ == "__main__":
    # DEBUG = True
    import_macroeconomy_info(chain_param=None)
    # 更新每日股票数据
    import_macroeconom_edb(chain_param=None)
#
# sql_str = """SELECT edb.wind_code,edb.trade_date,edb.value FROM wind_commodity_edb edb left join wind_commodity_info info on info.wind_code=edb.wind_code where info.en_name like 'PPI%%';"""
# df=pd.read_sql(sql_str,engine_md)
# # #将数据插入新表
# data_count = bunch_insert_on_duplicate_update(df, table_name, engine_md, dtype)
# logging.info("更新 %s 结束 %d 条信息被更新", table_name, data_count)