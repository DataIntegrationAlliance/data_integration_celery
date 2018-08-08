# -*- coding: utf-8 -*-
"""
Created on Thu Apr  6 11:11:26 2017

@author: Yupeng Guo
"""

from fh_tools.windy_utils_rest import WindRest, APIError
from fh_tools.fh_utils import get_first, get_last, str_2_date, date_2_str, get_cache_file_path
import pandas as pd
from datetime import date, timedelta, datetime
from sqlalchemy.types import String, Date
from sqlalchemy.dialects.mysql import DOUBLE
from config_fh import get_db_engine, WIND_REST_URL, get_db_session, ANALYSIS_CACHE_FILE_NAME
import logging
logger = logging.getLogger()
ONE_DAY = timedelta(days=1)
# 标示每天几点以后下载当日行情数据
BASE_LINE_HOUR = 16
rest = WindRest(WIND_REST_URL)  # 初始化服务器接口，用于下载万得数据


def fill_wind_index_daily_col():
    with get_db_session() as session:
        sql_str = "select wind_code, min(trade_date), max(trade_date) from wind_index_daily group by wind_code"
        table = session.execute(sql_str)
        wind_date_dic = {content[0]: (content[1], content[2]) for content in table.fetchall()}
        for wind_code, date_pair in wind_date_dic.items():
            logger.debug('invoke wsd for %s between %s and %s', wind_code, date_pair[0], date_pair[1])
            data_df = rest.wsd(wind_code, "turn,free_turn", date_pair[0], date_pair[1], "")
            data_df.dropna(inplace=True)
            if data_df.shape[0] == 0:
                continue
            logger.debug('%d data importing for %s', data_df.shape[0], wind_code)
            data_df['WIND_CODE'] = wind_code
            data_df.index.rename('TRADE_DATE', inplace=True)
            data_df.reset_index(inplace=True)
            data_list = list(data_df.T.to_dict().values())
            sql_str = """update wind_index_daily
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
    engine = get_db_engine()
    # yestday = date.today() - timedelta(days=1)
    date_ending = date.today() - ONE_DAY if datetime.now().hour < BASE_LINE_HOUR else date.today()
    info = rest.wss(wind_codes, "basedate,sec_name")
    for code in info.index:
        begin_date = str_2_date(info.loc[code, 'BASEDATE'])
        index_name = info.loc[code, 'SEC_NAME']
        index_df = rest.wsd(code, "open,high,low,close,volume,amt,turn,free_turn", begin_date, date_ending)
        index_df.reset_index(inplace=True)
        index_df.rename(columns={'index': 'trade_date'}, inplace=True)
        index_df.trade_date = pd.to_datetime(index_df.trade_date)
        index_df.trade_date = index_df.trade_date.map(lambda x: x.date())
        index_df['wind_code'] = code
        index_df['index_name'] = index_name
        index_df.set_index(['wind_code', 'trade_date'], inplace=True)
        table_name = 'wind_index_daily'
        index_df.to_sql(table_name, engine, if_exists='append', index_label=['wind_code', 'trade_date'],
                        dtype={
                        'wind_code': String(20),
                        'trade_date': Date,
                    })
        logger.info('Success import %s - %s with %d data' % (code, index_name, index_df.shape[0]))


def import_wind_index_daily():
    """导入指数数据"""
    engine = get_db_engine()
    # yesterday = date.today() - timedelta(days=1)
    date_ending = date.today() - ONE_DAY if datetime.now().hour < BASE_LINE_HOUR else date.today()
    sql_str = """select wii.wind_code, wii.sec_name, ifnull(adddate(latest_date, INTERVAL 1 DAY), wii.basedate) date_from
        from wind_index_info wii left join 
        (
            select wind_code,index_name, max(trade_date) as latest_date 
            from wind_index_daily group by wind_code
        ) daily
        on wii.wind_code=daily.wind_code"""
    with get_db_session(engine) as session:
        table = session.execute(sql_str)
        wind_code_date_from_dic = {wind_code: (sec_name, date_from) for wind_code, sec_name, date_from in table.fetchall()}
    with get_db_session(engine) as session:
        # 获取市场有效交易日数据
        sql_str = "select trade_date from wind_trade_date where trade_date > '2005-1-1'"
        table = session.execute(sql_str)
        trade_date_sorted_list = [t[0] for t in table.fetchall()]
        trade_date_sorted_list.sort()
    date_to = get_last(trade_date_sorted_list, lambda x: x <= date_ending)
    data_len = len(wind_code_date_from_dic)
    logger.info('%d indexes will been import', data_len)
    for data_num, (wind_code, (index_name, date_from)) in enumerate(wind_code_date_from_dic.items()):
        if str_2_date(date_from) > date_to:
            logger.warning("%d/%d) %s %s - %s 跳过", data_num, data_len, wind_code, date_from, date_to)
            continue
        try:
            temp = rest.wsd(wind_code, "open,high,low,close,volume,amt,turn,free_turn", date_from, date_to)
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
        # temp.trade_date = temp.trade_date.apply(str_2_date)
        temp['wind_code'] = wind_code
        temp['index_name'] = index_name
        temp.set_index(['wind_code', 'trade_date'], inplace=True)
        temp.to_sql('wind_index_daily', engine, if_exists='append', index_label=['wind_code', 'trade_date'],
                    dtype={
                        'wind_code': String(20),
                        'trade_date': Date,
                    })
        logger.info('更新指数 %s %s 至 %s 成功', wind_code, index_name, date_2_str(date_to))


def import_wind_index_daily_by_xls(file_path, wind_code, index_name):
    """
    将历史数据净值文件导入数据库
    1990年前的数据，wind端口无法导出，但可以通过wind终端导出文件后再导入数据库
    :param file_path: 
    :param wind_code: 
    :param index_name: 
    :return: 
    """
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
    engine = get_db_engine()
    with get_db_session(engine) as session:
        # 获取市场有效交易日数据
        sql_str = "delete from wind_index_daily where wind_code = :wind_code"
        table = session.execute(sql_str, params={"wind_code": wind_code})
    # 导入历史数据
    data_df.to_sql('wind_index_daily', engine, if_exists='append', index_label=['wind_code', 'trade_date'],
                dtype={
                    'wind_code': String(20),
                    'trade_date': Date,
                })
    logger.info("%s %s %d 条数据被导入", wind_code, index_name, data_df.shape[0])


def import_wind_index_info(wind_codes):
    """
    导入指数信息
    :param wind_codes: 
    :return: 
    """
    engine = get_db_engine()
    info_df = rest.wss(wind_codes, "sec_name,launchdate,basedate,basevalue,crm_issuer,country")
    if info_df is None or info_df.shape[0] == 0:
        logger.warning("没有数据可导入")
        return
    info_df.rename(columns={
        'LAUNCHDATE': 'launchdate',
        'BASEDATE': 'basedate',
        'BASEVALUE': 'basevalue',
        'COUNTRY': 'country',
        'CRM_ISSUER': 'crm_issuer',
        'SEC_NAME': 'sec_name',
    }, inplace=True)
    info_df.index.rename("wind_code", inplace=True)
    table_name = 'wind_index_info'
    info_df.to_sql(table_name, engine, if_exists='append', index=True,
                    dtype={
                    'wind_code': String(20),
                    'launchdate': Date,
                    'basedate': Date,
                    'basevalue': DOUBLE,
                    'country': String(20),
                    'crm_issuer': String(20),
                    'sec_name': String(20),
                    })
    logger.info('%d 条指数信息导入成功\n%s', info_df.shape[0], info_df)


def export_index_daily(wind_code_list):
    """
    供王淳使用，每天收盘后导出最新的指数数据
    :param wind_code_list: 
    :return: 
    """
    engine = get_db_engine()
    for wind_code in wind_code_list:
        sql_str = """select trade_date 'DATETIME', OPEN, HIGH, LOW, CLOSE, VOLUME, 0 OI
    from wind_index_daily where wind_code = %s order by trade_date"""
        data_df = pd.read_sql(sql_str, engine, params=[wind_code])
        file_path = get_cache_file_path(ANALYSIS_CACHE_FILE_NAME, "%s.csv" % wind_code)
        data_df.to_csv(file_path, index=False)
        logger.info("生成指数行情文件： %s 完成\n%s", wind_code, file_path)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s [%(name)s] %(message)s')

    # 数据库 wind_index_daily 表中新增加指数
    # wind_codes = ['HSCEI.HI', 'HSI.HI', 'HSML25.HI', 'HSPI.HI', '000001.SH', '000016.SH',
    #               '000300.SH', '000905.SH', '037.CS', '399001.SZ', '399005.SZ', '399006.SZ', '399101.SZ',
    #               '399102.SZ',
    #               ]
    # wind_codes = ['CES120.CSI']
    # import_wind_index_info(wind_codes)
    # import_wind_index_daily_first(wind_codes)

    # 每日更新指数信息
    import_wind_index_daily()
    # fill_wind_index_daily_col()

    # 每日生成指数导出文件给王淳
    wind_code_list = ['HSI.HI', 'HSCEI.HI']
    export_index_daily(wind_code_list)

    # file_path = r'd:\Downloads\CES120.xlsx'
    # wind_code, index_name = "CES120.CSI", "中华120"
    # import_wind_index_daily_by_xls(file_path, wind_code, index_name)