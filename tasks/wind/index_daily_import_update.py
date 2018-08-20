# -*- coding: utf-8 -*-
"""
Created on Thu Apr  6 11:11:26 2017

@author: Yupeng Guo
"""
from direstinvoker.iwind import APIError
import pandas as pd
from tasks import app
from tasks.utils.fh_utils import get_last, get_first, str_2_date, date_2_str, get_cache_file_path
from datetime import date, timedelta, datetime
from sqlalchemy.types import String, Date
from sqlalchemy.dialects.mysql import DOUBLE
from tasks.wind import invoker
from tasks.backend import engine_md
from tasks.backend.orm import build_primary_key
from tasks.merge.code_mapping import update_from_info_table
from tasks.utils.db_utils import with_db_session, bunch_insert_on_duplicate_update, alter_table_2_myisam
import logging

DEBUG = False
logger = logging.getLogger()
ONE_DAY = timedelta(days=1)
# 标示每天几点以后下载当日行情数据
BASE_LINE_HOUR = 16


# 初始化服务器接口，用于下载万得数据


def fill_wind_index_daily_col():
    with with_db_session(engine_md) as session:
        sql_str = "select wind_code, min(trade_date), max(trade_date) from wind_index_daily group by wind_code"
        table = session.execute(sql_str)
        wind_date_dic = {content[0]: (content[1], content[2]) for content in table.fetchall()}
        for wind_code, date_pair in wind_date_dic.items():
            logger.debug('invoke wsd for %s between %s and %s', wind_code, date_pair[0], date_pair[1])
            data_df = invoker.wsd(wind_code, "turn,free_turn", date_pair[0], date_pair[1], "")
            data_df.dropna(inplace=True)
            if data_df.shape[0] == 0:
                continue
            logger.debug('%d data importing for %s', data_df.shape[0], wind_code)
            data_df['WIND_CODE'] = wind_code
            data_df.index.rename('TRADE_DATE', inplace=True)
            data_df.reset_index(inplace=True)
            data_list = list(data_df.T.to_dict().values())
            sql_str = """
                update wind_index_daily
                set turn=:TURN, free_turn=:FREE_TURN
                where wind_code= :WIND_CODE
                and trade_date = :TRADE_DATE"""
            session.execute(sql_str, params=data_list)


def import_wind_index_daily_first(wind_codes):
    """
    首次导入某指数使用
    :param wind_codes: 可以是字符串，也可以是字符串的list 
    :return: 
    """
    table_name = 'wind_index_daily'
    has_table = engine_md.has_table(table_name)
    col_name_param_list = [
        ('open', String(20)),
        ('high', String(20)),
        ('low', String(20)),
        ('close', String(20)),
        ('volume', String(20)),
        ('amt', String(20)),
        ('turn', String(20)),
        ('free_turn', String(20)),
    ]
    wind_indictor_str = ",".join([key for key, _ in col_name_param_list])
    # 设置dtype类型
    dtype = {key: val for key, val in col_name_param_list}
    dtype['wind_code'] = String(20)
    # dtype['trade_date'] = Date,
    # yestday = date.today() - timedelta(days=1)
    date_ending = date.today() - ONE_DAY if datetime.now().hour < BASE_LINE_HOUR else date.today()
    info = invoker.wss(wind_codes, "basedate,sec_name")
    for code in info.index:
        begin_date = str_2_date(info.loc[code, 'BASEDATE'])
        # index_name = info.loc[code, 'SEC_NAME']
        index_df = invoker.wsd(code, wind_indictor_str, begin_date, date_ending)
        index_df.reset_index(inplace=True)
        index_df.rename(columns={'index': 'trade_date'}, inplace=True)
        index_df.trade_date = pd.to_datetime(index_df.trade_date)
        index_df.trade_date = index_df.trade_date.map(lambda x: x.date())
        index_df['wind_code'] = code
        # index_df['index_name'] = index_name
        index_df.set_index(['wind_code', 'trade_date'], inplace=True)
        # 仅仅调试时使用
        if DEBUG and len(index_df) > 4000:
            break
        # index_df.to_sql(table_name, engine_md, if_exists='append', index_label=['wind_code', 'trade_date'],
        #                 dtype={
        #                 'wind_code': String(20),
        #                 'trade_date': Date,
        #             })
        bunch_insert_on_duplicate_update(index_df, table_name, engine_md, dtype=dtype)
        logger.info('Success import %s with %d data' % (code, index_df.shape[0]))
        if not has_table and engine_md.has_table(table_name):
            alter_table_2_myisam(engine_md, [table_name])
            build_primary_key([table_name])


@app.tasks
def import_wind_index_daily():
    """导入指数数据"""
    table_name = "wind_index_daily"
    has_table = engine_md.has_table(table_name)
    col_name_param_list = [
        ('open', DOUBLE),
        ('high', DOUBLE),
        ('low', DOUBLE),
        ('close', DOUBLE),
        ('volume', DOUBLE),
        ('amt', DOUBLE),
        ('turn', DOUBLE),
        ('free_turn', DOUBLE),
    ]
    wind_indictor_str = ",".join([key for key, _ in col_name_param_list])
    rename_col_dic = {key.upper(): key.lower() for key, _ in col_name_param_list}
    dtype = {key: val for key, val in col_name_param_list}
    dtype['wind_code'] = String(20)
    # TODO: 'trade_date' 声明为 Date 类型后，插入数据库会报错，目前原因不详，日后再解决
    # dtype['trade_date'] = Date,

    # yesterday = date.today() - timedelta(days=1)
    # date_ending = date.today() - ONE_DAY if datetime.now().hour < BASE_LINE_HOUR else date.today()
    # sql_str = """select wii.wind_code, wii.sec_name, ifnull(adddate(latest_date, INTERVAL 1 DAY), wii.basedate) date_from
    #     from wind_index_info wii left join
    #     (
    #         select wind_code,index_name, max(trade_date) as latest_date
    #         from wind_index_daily group by wind_code
    #     ) daily
    #     on wii.wind_code=daily.wind_code"""
    # with with_db_session(engine_md) as session:
    #     table = session.execute(sql_str)
    #     wind_code_date_from_dic = {wind_code: (sec_name, date_from) for wind_code, sec_name, date_from in table.fetchall()}
    # with with_db_session(engine_md) as session:
    #     # 获取市场有效交易日数据
    #     sql_str = "select trade_date from wind_trade_date where trade_date > '2005-1-1'"
    #     table = session.execute(sql_str)
    #     trade_date_sorted_list = [t[0] for t in table.fetchall()]
    #     trade_date_sorted_list.sort()
    # date_to = get_last(trade_date_sorted_list, lambda x: x <= date_ending)
    # data_len = len(wind_code_date_from_dic)

    if has_table:
        sql_str = """
              SELECT wind_code, date_frm, if(launchdate<end_date, launchdate, end_date) date_to
              FROM
              (
                  SELECT info.wind_code, ifnull(trade_date, basedate) date_frm, launchdate,
                  if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
                  FROM 
                      wind_index_info info 
                  LEFT OUTER JOIN
                      (SELECT wind_code, adddate(max(trade_date),1) trade_date FROM {table_name} GROUP BY wind_code) daily
                  ON info.wind_code = daily.wind_code
              ) tt
              WHERE date_frm <= if(launchdate<end_date, launchdate, end_date) 
              ORDER BY wind_code""".format(table_name=table_name)
    else:
        logger.warning('%s 不存在，仅使用 wind_index_info 表进行计算日期范围', table_name)
        sql_str = """
              SELECT wind_code, date_frm, if(launchdate<end_date, launchdate, end_date) date_to
              FROM
              (
                  SELECT info.wind_code, basedate date_frm, launchdate,
                  if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
                  FROM wind_index_info info 
              ) tt
              WHERE date_frm <= if(launchdate<end_date, launchdate, end_date) 
              ORDER BY wind_code;"""

    with with_db_session(engine_md) as session:
        # 获取每只股票需要获取日线数据的日期区间
        table = session.execute(sql_str)
        # 获取每只股票需要获取日线数据的日期区间
        begin_time = None
        wind_code_date_from_dic = {
            wind_code: (date_from if begin_time is None else min([date_from, begin_time]), date_to)
            for wind_code, date_from, date_to in table.fetchall() if
            wind_code_set is None or wind_code in wind_code_set}

    data_len = len(wind_code_date_from_dic)

    logger.info('%d indexes will been import', data_len)
    for data_num, (wind_code, (date_from, date_to)) in enumerate(wind_code_date_from_dic.items()):
        if str_2_date(date_from) > date_to:
            logger.warning("%d/%d) %s %s - %s 跳过", data_num, data_len, wind_code, date_from, date_to)
            continue
        try:
            temp = invoker.wsd(wind_code, wind_indictor_str, date_from, date_to)
        except APIError as exp:
            logger.exception("%d/%d) %s 执行异常", data_num, data_len, wind_code)
            if exp.ret_dic.setdefault('error_code', 0) in (
                    -40520007,  # 没有可用数据
                    -40521009,  # 数据解码失败。检查输入参数是否正确，如：日期参数注意大小月月末及短二月
            ):
                continue
            else:
                break
        temp.reset_index(inplace=True)
        temp.rename(columns={'index': 'trade_date'}, inplace=True)
        temp.rename(columns=rename_col_dic, inplace=True)
        temp.trade_date = temp.trade_date.apply(str_2_date)
        temp['wind_code'] = wind_code
        bunch_insert_on_duplicate_update(temp, table_name, engine_md, dtype=dtype)
        logger.info('更新指数 %s 至 %s 成功', wind_code, date_2_str(date_to))
        if not has_table and engine_md.has_table(table_name):
            alter_table_2_myisam(engine_md, [table_name])
            build_primary_key([table_name])


def import_wind_index_daily_by_xls(file_path, wind_code, index_name):
    """
    将历史数据净值文件导入数据库
    1990年前的数据，wind端口无法导出，但可以通过wind终端导出文件后再导入数据库
    :param file_path: 
    :param wind_code: 
    :param index_name: 
    :return: 
    """
    table_name = "wind_index_daily"
    has_table = engine_md.has_table(table_name)
    data_df = pd.read_excel(file_path)
    data_df.rename(columns={
        "日期": "trade_date",
        "开盘价(元)": "open",
        "最高价(元)": "high",
        "最低价(元)": "low",
        "收盘价(元)": "close",
        "成交额(百万)": "amt",
        "成交量(股)": "volume",
    }, inplace=True)
    data_df["wind_code"] = wind_code
    data_df['index_name'] = index_name
    data_df.set_index(['wind_code', 'trade_date'], inplace=True)
    # 删除历史数据
    with with_db_session(engine_md) as session:
        # 获取市场有效交易日数据
        sql_str = "delete from wind_index_daily where wind_code = :wind_code"
        table = session.execute(sql_str, params={"wind_code": wind_code})
    # 导入历史数据
    # data_df.to_sql('wind_index_daily', engine_md, if_exists='append', index_label=['wind_code', 'trade_date'],
    #             dtype={
    #                 'wind_code': String(20),
    #                 'trade_date': Date,
    #             })
    bunch_insert_on_duplicate_update(data_df, table_name, engine_md, dtype={
        'wind_code': String(20),
        'trade_date': Date,
    })
    if not has_table and engine_md.has_table(table_name):
        alter_table_2_myisam(engine_md, [table_name])
        build_primary_key([table_name])
    logger.info("%s %s %d 条数据被导入", wind_code, index_name, data_df.shape[0])


@app.tasks
def import_wind_index_info(wind_codes):
    """
    导入指数信息
    :param wind_codes: 
    :return: 
    """
    table_name = 'wind_index_info'
    has_table = engine_md.has_table(table_name)
    col_name_param_list = [
        ('LAUNCHDATE', Date),
        ('BASEDATE', Date),
        ('BASEVALUE', DOUBLE),
        ('COUNTRY', String(20)),
        ('CRM_ISSUER', String(20)),
        ('SEC_NAME', String(20)),
    ]
    col_name_param = ",".join([key.lower() for key, _ in col_name_param_list])
    col_name_param_dic = {col_name.upper(): col_name.lower() for col_name, _ in col_name_param_list}
    # 设置dtype类型
    dtype = {key.lower(): val for key, val in col_name_param_list}
    dtype['wind_code'] = String(20)

    info_df = invoker.wss(wind_codes, col_name_param)
    if info_df is None or info_df.shape[0] == 0:
        logger.warning("没有数据可导入")
        return
    info_df.rename(columns=col_name_param_dic, inplace=True)
    info_df.index.rename("wind_code", inplace=True)
    info_df.reset_index(inplace=True)
    bunch_insert_on_duplicate_update(info_df, table_name, engine_md, dtype=dtype)
    # info_df.to_sql(table_name, engine_md, if_exists='append', index=True,
    #                 dtype={
    #                 'wind_code': String(20),
    #                 'launchdate': Date,
    #                 'basedate': Date,
    #                 'basevalue': DOUBLE,
    #                 'country': String(20),
    #                 'crm_issuer': String(20),
    #                 'sec_name': String(20),
    #                 })
    logger.info('%d 条指数信息导入成功\n%s', info_df.shape[0], info_df)
    if not has_table and engine_md.has_table(table_name):
        alter_table_2_myisam(engine_md, [table_name])
        build_primary_key([table_name])

    # 更新 code_mapping 表
    update_from_info_table(table_name)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s [%(name)s] %(message)s')
    DEBUG = True
    # 数据库 wind_index_daily 表中新增加指数
    wind_codes = ['HSCEI.HI', 'HSI.HI', 'HSML25.HI', 'HSPI.HI', '000001.SH', '000016.SH',
                  '000300.SH', '000905.SH', '037.CS', '399001.SZ', '399005.SZ', '399006.SZ', '399101.SZ',
                  '399102.SZ',
                  ]
    # wind_codes = ['CES120.CSI']
    # import_wind_index_info(wind_codes)
    import_wind_index_daily_first(wind_codes)
    wind_code_set = None
    # 每日更新指数信息
    # import_wind_index_daily()
    # fill_wind_index_daily_col()

    # 每日生成指数导出文件给王淳
    wind_code_list = ['HSI.HI', 'HSCEI.HI']
    # export_index_daily(wind_code_list)

    # file_path = r'd:\Downloads\CES120.xlsx'
    wind_code = "CES120.CSI"
    # import_wind_index_daily_by_xls(file_path, wind_code, index_name)
