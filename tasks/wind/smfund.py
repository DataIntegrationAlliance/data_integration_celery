# -*- coding: utf-8 -*-
"""
Created on Fri Mar 31 10:52:30 2017

@author: Yupeng Guo - alanguoyupeng@163.com

"""
import pandas as pd
from direstinvoker import APIError
from tasks import app
from tasks.backend.orm import build_primary_key
from datetime import datetime, date, timedelta
from sqlalchemy.types import String, Date, Float
from pandas.tslib import Timestamp
from sqlalchemy.dialects.mysql import DOUBLE, TEXT
from tasks.utils.db_utils import with_db_session, bunch_insert_on_duplicate_update, alter_table_2_myisam
from tasks.wind import invoker
from tasks.backend import engine_md
from tasks.merge.code_mapping import update_from_info_table
from tasks.utils.fh_utils import STR_FORMAT_DATE, date_2_str, str_2_date
import logging

DEBUG = False
logger = logging.getLogger()
ONE_DAY = timedelta(days=1)
# 标示每天几点以后下载当日行情数据
BASE_LINE_HOUR = 16


@app.task
def import_smfund_info(chain_param=None):
    """
    :param chain_param:  在celery 中將前面結果做爲參數傳給後面的任務
    :return:
    """
    table_name = "wind_smfund_info"
    has_table = engine_md.has_table(table_name)
    # w.start()
    types = {u'主动股票型分级母基金': 1000007766000000,
             u'被动股票型分级母基金': 1000007767000000,
             u'纯债券型分级母基金': 1000007768000000,
             u'混合债券型分级母基金': 1000007769000000,
             u'混合型分级母基金': 1000026143000000,
             u'QDII分级母基金': 1000019779000000
             }
    col_name_param_list = [
        ('wind_code', String(20)),
        ('fund_type', String(20)),
        ('sec_name', String(50)),
        ('class_a_code', String(20)),
        ('class_a_name', String(50)),
        ('class_b_code', String(20)),
        ('class_b_name', String(50)),
        ('track_indexcode', String(20)),
        ('track_indexname', String(50)),
        ('a_pct', DOUBLE),
        ('b_pct', DOUBLE),
        ('upcv_nav', DOUBLE),
        ('downcv_nav', DOUBLE),
        ('max_purchasefee', DOUBLE),
        ('max_redemptionfee', DOUBLE),
    ]
    col_name = ",".join([col_name for col_name, _ in col_name_param_list])
    # 获取各个历史时段的分级基金列表，并汇总全部基金代码
    dates = ['2011-01-01', '2013-01-01', '2015-01-01', '2017-01-01', '2018-01-01']  # 分三个时间点获取市场上所有分级基金产品
    df = pd.DataFrame()
    # 获取接数据
    for date_p in dates:
        temp_df = invoker.wset("sectorconstituent", "date=%s;sectorid=1000006545000000" % date_p)
        df = df.append(temp_df)
    wind_code_all = df['wind_code'].unique()
    # 设置dtype
    dtype = {key: val for key, val in col_name_param_list}
    dtype['wind_code'] = String(20)
    dtype["tradable"] = String(20)
    dtype["fund_setupdate"] = Date
    dtype["fund_maturitydate"] = Date
    if has_table:
        with with_db_session(engine_md) as session:
            table = session.execute("SELECT wind_code FROM wind_smfund_info")
            wind_code_existed = set([content[0] for content in table.fetchall()])
        wind_code_new = list(set(wind_code_all) - wind_code_existed)
    else:
        wind_code_new = list(set(wind_code_all))
    # if len(wind_code_new) == 0:
    #     print('no sm fund imported')
    # 查询数据库，剔除已存在的基金代码
    wind_code_new = [code for code in wind_code_new if code.find('!') < 0]
    info_df = invoker.wss(wind_code_new, 'fund_setupdate, fund_maturitydate')
    if info_df is None:
        raise Exception('no data')
    info_df['FUND_SETUPDATE'] = info_df['FUND_SETUPDATE'].apply(lambda x: str_2_date(x))
    info_df['FUND_MATURITYDATE'] = info_df['FUND_MATURITYDATE'].apply(lambda x: str_2_date(x))
    info_df.rename(columns={'FUND_SETUPDATE': 'fund_setupdate', 'FUND_MATURITYDATE': 'fund_maturitydate'}, inplace=True)
    field = col_name
    # field = "fund_type,wind_code,sec_name,class_a_code,class_a_name,class_b_code,class_b_name,a_pct,b_pct,upcv_nav,downcv_nav,track_indexcode,track_indexname,max_purchasefee,max_redemptionfee"

    for code in info_df.index:
        beginDate = info_df.loc[code, 'fund_setupdate'].strftime('%Y-%m-%d')
        temp_df = invoker.wset("leveragedfundinfo",
                               "date=%s;windcode=%s;field=%s" % (beginDate, code, field))  # ;field=%s  , field
        df = df.append(temp_df)
        if DEBUG and len(df) > 10:
            break
    df.set_index('wind_code', inplace=True)
    df['tradable'] = df.index.map(lambda x: x if 'S' in x else None)
    # df.index = df.index.map(lambda x: x[:-2] + 'OF')
    info_df = info_df.join(df, how='outer')
    # TODO: 需要检查一下代码
    info_df.rename(columns={'a_nav': 'nav_a', 'b_nav': 'nav_b', 'a_fs_inc': 'fs_inc_a', 'b_fs_inc': 'fs_inc_b'})
    info_df.index.rename('wind_code', inplace=True)
    info_df.reset_index(inplace=True)
    bunch_insert_on_duplicate_update(info_df, table_name, engine_md, dtype=dtype)
    logging.info("更新 %s 完成 存量数据 %d 条", table_name, len(info_df))
    if not has_table and engine_md.has_table(table_name):
        alter_table_2_myisam(engine_md, [table_name])
        build_primary_key([table_name])

    # 更新 code_mapping 表
    update_from_info_table(table_name)


@app.task
def import_smfund_daily(chain_param=None):
    """
    :param chain_param:  在celery 中將前面結果做爲參數傳給後面的任務
    :return:
    """
    table_name = "wind_smfund_daily"
    has_table = engine_md.has_table(table_name)
    col_name_param_list = [
        ('next_pcvdate', Date),
        ('a_nav', DOUBLE),
        ('b_nav', DOUBLE),
        ('a_fs_inc', DOUBLE),
        ('b_fs_inc', DOUBLE),
        ('cur_interest', DOUBLE),
        ('next_interest', DOUBLE),
        ('ptm_year', DOUBLE),
        ('anal_pricelever', DOUBLE),
        ('anal_navlevel', DOUBLE),
        ('t1_premium', DOUBLE),
        ('t2_premium', DOUBLE),
        ('dq_status', String(50)),
        ('tm_type', TEXT),
        ('code_p', String(20)),
        ('trade_date', Date),
        ('open', DOUBLE),
        ('high', DOUBLE),
        ('low', DOUBLE),
        ('close', DOUBLE),
        ('volume', DOUBLE),
        ('amt', DOUBLE),
        ('pct_chg', DOUBLE),
        ('open_a', DOUBLE),
        ('high_a', DOUBLE),
        ('low_a', DOUBLE),
        ('close_a', DOUBLE),
        ('volume_a', DOUBLE),
        ('amt_a', DOUBLE),
        ('pct_chg_a', DOUBLE),
        ('open_b', DOUBLE),
        ('high_b', DOUBLE),
        ('low_b', DOUBLE),
        ('close_b', DOUBLE),
        ('volume_b', DOUBLE),
        ('amt_b', DOUBLE),
        ('pct_chg_b', DOUBLE),

    ]
    # wset的调用参数
    wind_indictor_str = ",".join([key for key, _ in col_name_param_list[:14]])
    # 设置dtype类型
    dtype = {key: val for key, val in col_name_param_list}

    date_ending = date.today() - ONE_DAY if datetime.now().hour < BASE_LINE_HOUR else date.today()
    date_ending_str = date_ending.strftime('%Y-%m-%d')
    # 对于 表格是否存在进行判断，取值
    if has_table:
        sql_str = """
            SELECT wind_code, ifnull(date, fund_setupdate) date_start, class_a_code, class_b_code
            FROM wind_smfund_info fi LEFT OUTER JOIN
            (SELECT code_p, adddate(max(trade_date), 1) trade_date_max FROM wind_smfund_daily GROUP BY code_p) smd
            ON fi.wind_code = smd.code_p
            WHERE fund_setupdate IS NOT NULL
            AND class_a_code IS NOT NULL
            AND class_b_code IS NOT NULL"""
    else:
        sql_str = """
            SELECT wind_code, ifnull(date, fund_setupdate) date_start, class_a_code, class_b_code
            FROM wind_smfund_info
            WHERE fund_setupdate IS NOT NULL
            AND class_a_code IS NOT NULL
            AND class_b_code IS NOT NULL"""
    df = pd.read_sql(sql_str, engine_md)
    df.set_index('wind_code', inplace=True)

    data_len = df.shape[0]
    logger.info('分级基金数量: %d', data_len)
    index_start = 1
    # 获取data_from
    for data_num, wind_code in enumerate(df.index, start=1):  # 可调整 # [100:min([df_count, 200])]
        if data_num < index_start:
            continue
        logger.info('%d/%d) %s start to import', data_num, data_len, wind_code)
        date_from = df.loc[wind_code, 'date_start']
        date_from = str_2_date(date_from)
        if type(date_from) not in (date, datetime, Timestamp):
            logger.info('%d/%d) %s has no fund_setupdate will be ignored', data_num, data_len, wind_code)
            # print(df.iloc[i, :])
            continue
        date_from_str = date_from.strftime('%Y-%m-%d')
        if date_from > date_ending:
            logger.info('%d/%d) %s %s %s 跳过', data_num, data_len, wind_code, date_from_str, date_ending_str)
            continue
        # 设置wsd接口参数
        field = "open,high,low,close,volume,amt,pct_chg"
        # wsd_cache(w, code, field, beginTime, today, "")
        try:
            df_p = invoker.wsd(wind_code, field, date_from_str, date_ending_str, "")
        except APIError as exp:
            logger.exception("%d/%d) %s 执行异常", data_num, data_len, wind_code)
            if exp.ret_dic.setdefault('error_code', 0) in (
                    -40520007,  # 没有可用数据
                    -40521009,  # 数据解码失败。检查输入参数是否正确，如：日期参数注意大小月月末及短二月
            ):
                continue
            else:
                break
        if df_p is None:
            continue
        df_p.rename(columns=lambda x: x.swapcase(), inplace=True)
        df_p['code_p'] = wind_code
        code_a = df.loc[wind_code, 'class_a_code']
        if code_a is None:
            print('%d %s has no code_a will be ignored' % (data_num, wind_code))
            # print(df.iloc[i, :])
            continue
        # 获得数据存储到df_a里面
        # df_a = wsd_cache(w, code_a, field, beginTime, today, "")
        df_a = invoker.wsd(code_a, field, date_from_str, date_ending_str, "")
        df_a.rename(columns=lambda x: x.swapcase() + '_a', inplace=True)
        code_b = df.loc[wind_code, 'class_b_code']
        # df_b = wsd_cache(w, code_b, field, beginTime, today, "")
        # 获取接口数据 获得df_b
        df_b = invoker.wsd(code_b, field, date_from_str, date_ending_str, "")
        df_b.columns = df_b.columns.map(lambda x: x.swapcase() + '_b')
        new_df = pd.DataFrame()
        for date_str in df_p.index:
            # time = date_str.date().strftime('%Y-%m-%d')
            field = "date=%s;windcode=%s;field=%s" % (
                date_str, wind_code, wind_indictor_str)
            # wset_cache(w, "leveragedfundinfo", field)
            temp = invoker.wset("leveragedfundinfo", field)
            temp['date'] = date_str
            new_df = new_df.append(temp)
            if DEBUG and len(new_df) > 8:
                break
        # 将获取信息进行表格联立 合并
        new_df['next_pcvdate'] = new_df['next_pcvdate'].map(lambda x: str_2_date(x) if x is not None else x)
        new_df.set_index('date', inplace=True)
        one_df = pd.concat([df_p, df_a, df_b, new_df], axis=1)
        one_df.index.rename('trade_date', inplace=True)
        one_df.reset_index(inplace=True)
        #    one_df['date'] = one_df['date'].map(lambda x: x.date())
        one_df.rename(columns={'date': 'trade_date'}, inplace=True)
        # one_df.rename(columns={"index":'trade_date'},inplace=True)
        # one_df.set_index(['code_p', 'trade_date'], inplace=True)
        bunch_insert_on_duplicate_update(one_df, table_name, engine_md, dtype=dtype)
        logger.info('%d/%d) %s import success', data_num, data_len, wind_code)
        if not has_table and engine_md.has_table(table_name):
            alter_table_2_myisam(engine_md, [table_name])
            # build_primary_key([table_name])
            # 手动创建主键， 主键不是wind_code
            create_pk_str = """ALTER TABLE {table_name}
                CHANGE COLUMN `code_p` `code_p` VARCHAR(20) NOT NULL ,
                CHANGE COLUMN `trade_date` `trade_date` DATE NOT NULL ,
                ADD PRIMARY KEY (`code_p`, `trade_date`)""".format(table_name=table_name)
            with with_db_session(engine_md) as session:
                session.execute(create_pk_str)

    # info_df = info_df.append(one_df)
    # dump_cache()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s [%(name)s:%(funcName)s] %(message)s')
    DEBUG = True
    # import_smfund_info(chain_param=None)
    import_smfund_daily(chain_param=None)
