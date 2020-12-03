# -*- coding: utf-8 -*-
"""
Created on 2018/2/12
@author  : MG
@Time    : 2019/1/14 15:59
@File    : reorg_md_2_db.py
@contact : mmmaaaggg@163.com
@desc    : 从 QABAT 项目迁移来
将期货行情数据进行整理，相同批准不同合约的行情合并成为连续行情，并进行复权处理，然后保存到数据库
"""

import math, os
import pandas as pd
import logging
import re
from sqlalchemy.dialects.mysql import DOUBLE
from sqlalchemy.types import String, Date, Time, DateTime, Integer
from tasks.backend import engine_md
from ibats_utils.db import with_db_session, bunch_insert_on_duplicate_update
from ibats_utils.mess import str_2_float
from tasks.config import config

logger = logging.getLogger()
# 对于郑商所部分合约需要进行特殊的名称匹配规则
re_pattern_instrument_type_3num_by_wind_code = re.compile(r"(?<=(SR|CF))\d{3}(?=.CZC)", re.IGNORECASE)
re_pattern_instrument_header_by_wind_code = re.compile(r"\d{3,4}(?=.\w)", re.IGNORECASE)
re_pattern_instrument_type_3num_by_instrument_id = re.compile(r"(?<=(SR|CF))\d{3}$", re.IGNORECASE)
re_pattern_instrument_header_by_instrument_id = re.compile(r"\d{3,4}$", re.IGNORECASE)


def get_all_instrument_type():

    sql_str = "select wind_code from wind_future_daily group by wind_code"
    with with_db_session(engine_md) as session:
        instrument_list = [row[0] for row in session.execute(sql_str).fetchall()]
        re_pattern_instrument_type = re.compile(r'\D+(?=\d{3})', re.IGNORECASE)
        instrument_type_set = {re_pattern_instrument_type.search(name).group() for name in instrument_list}
    return list(instrument_type_set)


def get_instrument_num(instrument_str, by_wind_code=True):
    """
    获取合约的年月数字
    郑商所部分合约命名规则需要区别开来
    例如：白糖SR、棉花CF 
    SR0605.CZC 200605交割
    SR1605.CZC 201605交割
    SR607.CZC 201607交割
    :param instrument_str: 
    :param by_wind_code: 
    :return: 
    """
    if by_wind_code:
        m = re_pattern_instrument_type_3num_by_wind_code.search(instrument_str)
    else:
        m = re_pattern_instrument_type_3num_by_instrument_id.search(instrument_str)
    if m is not None:
        # 郑商所部分合约命名规则需要区别开来，3位数字的需要+1000
        # 例如：白糖SR、棉花CF
        # SR0605.CZC 200605交割
        # SR1605.CZC 201605交割
        # SR607.CZC 201607交割  + 1000
        inst_num = int(m.group())
        inst_num = inst_num + 1000
    else:
        if by_wind_code:
            m = re_pattern_instrument_header_by_wind_code.search(instrument_str)
        else:
            m = re_pattern_instrument_header_by_instrument_id.search(instrument_str)
        if m is None:
            raise ValueError('%s 不是有效的合约' % instrument_str)
        else:
            # RU9507.SHF 199507 交割
            # RU0001.SHF 200001 交割
            # RU1811.SHF 201811 交割
            inst_num = int(m.group())
            inst_num = inst_num if inst_num < 9000 else inst_num - 10000

    return inst_num


def is_earlier_instruments(inst_a, inst_b, by_wind_code=True):
    """
    比较两个合约交割日期 True
    :param inst_a: 
    :param inst_b: 
    :param by_wind_code: 
    :return: 
    """
    inst_num_a = get_instrument_num(inst_a, by_wind_code)
    inst_num_b = get_instrument_num(inst_b, by_wind_code)
    return inst_num_a < inst_num_b


def is_later_instruments(inst_a, inst_b, by_wind_code=True):
    """
    比较两个合约交割日期 True
    :param inst_a:
    :param inst_b:
    :param by_wind_code:
    :return:
    """
    inst_num_a = get_instrument_num(inst_a, by_wind_code)
    inst_num_b = get_instrument_num(inst_b, by_wind_code)
    return inst_num_a > inst_num_b


def update_data_reorg_df_2_db(instrument_type, table_name, data_df, engine=None):
    """将 DataFrame 数据保存到 数据库对应的表中"""
    dtype = {
        'trade_date': Date,
        'Contract': String(20),
        'ContractNext': String(20),
        'Close': DOUBLE,
        'CloseNext': DOUBLE,
        'TermStructure': DOUBLE,
        'Volume': DOUBLE,
        'VolumeNext': DOUBLE,
        'OI': DOUBLE,
        'OINext': DOUBLE,
        'Open': DOUBLE,
        'OpenNext': DOUBLE,
        'High': DOUBLE,
        'HighNext': DOUBLE,
        'Low': DOUBLE,
        'LowNext': DOUBLE,
        'WarehouseWarrant': DOUBLE,
        'WarehouseWarrantNext': DOUBLE,
        'adj_factor_main': DOUBLE,
        'adj_factor_secondary': DOUBLE,
        'instrument_type': String(20),
    }
    if engine is None:
        engine = engine_md

    # 为了解决 AttributeError: 'numpy.float64' object has no attribute 'translate' 错误，需要将数据类型转换成 float
    data_df["Close"] = data_df["Close"].apply(str_2_float)
    data_df["CloseNext"] = data_df["CloseNext"].apply(str_2_float)
    data_df["TermStructure"] = data_df["TermStructure"].apply(str_2_float)
    data_df["Volume"] = data_df["Volume"].apply(str_2_float)
    data_df["VolumeNext"] = data_df["VolumeNext"].apply(str_2_float)
    data_df["OI"] = data_df["OI"].apply(str_2_float)
    data_df["OINext"] = data_df["OINext"].apply(str_2_float)
    data_df["Open"] = data_df["Open"].apply(str_2_float)
    data_df["OpenNext"] = data_df["OpenNext"].apply(str_2_float)
    data_df["High"] = data_df["High"].apply(str_2_float)
    data_df["HighNext"] = data_df["HighNext"].apply(str_2_float)
    data_df["Low"] = data_df["Low"].apply(str_2_float)
    data_df["LowNext"] = data_df["LowNext"].apply(str_2_float)
    data_df["WarehouseWarrant"] = data_df["WarehouseWarrant"].apply(str_2_float)
    data_df["WarehouseWarrantNext"] = data_df["WarehouseWarrantNext"].apply(str_2_float)
    data_df["adj_factor_main"] = data_df["adj_factor_main"].apply(str_2_float)
    data_df["adj_factor_secondary"] = data_df["adj_factor_secondary"].apply(str_2_float)
    # 清理历史记录
    with with_db_session(engine) as session:
        sql_str = """SELECT table_name FROM information_schema.TABLES 
            WHERE table_name = :table_name and TABLE_SCHEMA=(select database())"""
        # 复权数据表
        is_existed = session.execute(sql_str, params={"table_name": table_name}).fetchone()
        if is_existed is not None:
            session.execute("delete from %s where instrument_type = :instrument_type" % table_name,
                            params={"instrument_type": instrument_type})
            logger.debug("删除 %s 中的 %s 历史数据", table_name, instrument_type)

    # 插入数据库
    # pd.DataFrame.to_sql(data_df, table_name, engine_md, if_exists='append', index=False, dtype=dtype)
    bunch_insert_on_duplicate_update(data_df, table_name, engine,
                                     dtype=dtype, myisam_if_create_table=True,
                                     primary_keys=['trade_date', 'Contract'], schema=config.DB_SCHEMA_MD)


def data_reorg_daily(instrument_type, update_table=True) -> (pd.DataFrame, pd.DataFrame):
    """
    将每一个交易日主次合约合约行情信息进行展示
    :param instrument_type:
    :param update_table: 是否更新数据库
    :return: 
    """
    sql_str = r"""select wind_code, trade_date, open, high, low, close, volume, position, st_stock 
      from wind_future_daily where wind_code regexp %s"""
    data_df = pd.read_sql(sql_str, engine_md, params=[r'^%s[0-9]+\.[A-Z]+' % (instrument_type, )])
    date_instrument_vol_df = data_df.pivot(index="trade_date", columns="wind_code", values="volume").sort_index()
    date_instrument_open_df = data_df.pivot(index="trade_date", columns="wind_code", values="open")
    date_instrument_low_df = data_df.pivot(index="trade_date", columns="wind_code", values="low")
    date_instrument_high_df = data_df.pivot(index="trade_date", columns="wind_code", values="high")
    date_instrument_close_df = data_df.pivot(index="trade_date", columns="wind_code", values="close")
    date_instrument_position_df = data_df.pivot(index="trade_date", columns="wind_code", values="position")
    date_instrument_st_stock_df = data_df.pivot(index="trade_date", columns="wind_code", values="st_stock")
    logger.info("查询 %s 包含 %d 条记录 %d 个合约", instrument_type, *date_instrument_vol_df.shape)

    instrument_id_main, instrument_id_secondary = None, None
    date_instrument_id_dic = {}
    # 按合约号排序
    instrument_id_list_sorted = list(date_instrument_vol_df.columns)
    instrument_id_list_sorted.sort(key=get_instrument_num)
    date_instrument_vol_df = date_instrument_vol_df[instrument_id_list_sorted]
    date_reorg_data_dic = {}
    trade_date_error = None
    # 逐日检查主力合约，次主力合约列表
    # 主要逻辑：
    # 每天检查次一日的主力合约，次主力合约
    # 主力合约为当日成交量最大合约，合约号只能前进不能后退
    # 次主力合约的交割日期要大于主力合约的交割日期，
    trade_date_list = list(date_instrument_vol_df.index)
    trade_date_available_list = []
    for nday, trade_date in enumerate(trade_date_list):
        if instrument_id_main is not None:
            date_instrument_id_dic[trade_date] = (instrument_id_main, instrument_id_secondary)
        vol_s = date_instrument_vol_df.loc[trade_date].dropna()

        instrument_id_main_cur, instrument_id_secondary_cur = instrument_id_main, instrument_id_secondary
        if vol_s.shape[0] == 0:
            logger.warning("%s 没有交易量数据", trade_date)
            continue
        # 当日没有交易量数据，或者主力合约已经过期
        if instrument_id_main not in vol_s.index:
            instrument_id_main = None
        for try_time in [1, 2]:
            for instrument_id in vol_s.index:
                if instrument_id_main is not None and is_earlier_instruments(instrument_id, instrument_id_main):
                    continue
                if instrument_id_main is None and instrument_id_main_cur is not None and is_earlier_instruments(
                        instrument_id, instrument_id_main_cur):
                    continue
                # if math.isnan(vol_s[instrument_id]):
                #     continue
                # 比较出主力合约 和次主力合约
                if instrument_id_main is None:
                    instrument_id_main = instrument_id
                elif instrument_id_main in vol_s and vol_s[instrument_id_main] < vol_s[instrument_id]:
                    instrument_id_main, instrument_id_secondary = instrument_id, None
                elif instrument_id_secondary is None:
                    if instrument_id_main != instrument_id:
                        instrument_id_secondary = instrument_id
                elif is_earlier_instruments(instrument_id_secondary, instrument_id) \
                        and instrument_id_secondary in vol_s and vol_s[instrument_id_secondary] < vol_s[instrument_id]:
                    instrument_id_secondary = instrument_id
            if instrument_id_main is None:
                logger.warning("%s 主力合约%s缺少成交量数据，其他合约没有可替代数据，因此继续使用当前行情数据",
                               trade_date, instrument_id_main_cur)
                instrument_id_main = instrument_id_main_cur
            else:
                break

        # 异常处理
        if instrument_id_secondary is None:
            if trade_date_error is None:
                trade_date_error = trade_date
                logger.warning("%s 当日主力合约 %s, 没有次主力合约", trade_date, instrument_id_main)
        elif trade_date_error is not None:
            logger.warning("%s 当日主力合约 %s, 次主力合约 %s", trade_date, instrument_id_main, instrument_id_secondary)
            trade_date_error = None

        if nday > 0:
            # 如果主力合约切换，则计算调整因子
            if instrument_id_main_cur is not None and instrument_id_main_cur != instrument_id_main:
                trade_date_last = trade_date_available_list[-1]
                close_last_main_cur = date_instrument_close_df[instrument_id_main_cur][trade_date_last]
                close_last_main = date_instrument_close_df[instrument_id_main][trade_date_last]
                adj_chg_main = close_last_main / close_last_main_cur
                if trade_date_last in date_reorg_data_dic:
                    date_reorg_data_dic[trade_date_last]['adj_chg_main'] = adj_chg_main
            # 如果次主力合约切换，则计算调整因子
            if instrument_id_secondary_cur is not None and instrument_id_secondary_cur != instrument_id_secondary:
                if instrument_id_secondary is not None:
                    trade_date_last = trade_date_available_list[-1]
                    close_last_secondary_cur = date_instrument_close_df[instrument_id_secondary_cur][trade_date_last]
                    close_last_secondary = date_instrument_close_df[instrument_id_secondary][trade_date_last]
                    adj_chg_secondary = close_last_secondary / close_last_secondary_cur
                    if trade_date_last in date_reorg_data_dic:
                        date_reorg_data_dic[trade_date_last]['adj_chg_secondary'] = adj_chg_secondary

            # 数据重组
            close_main = date_instrument_close_df[instrument_id_main][trade_date]
            close_last_day_main = date_instrument_close_df[instrument_id_main][trade_date_list[nday - 1]]
            close_secondary = date_instrument_close_df[instrument_id_secondary][
                trade_date] if instrument_id_secondary is not None else math.nan
            if math.isnan(close_secondary) and math.isnan(close_main):
                logger.warning("%s 主力合约 %s 次主力合约 %s 均无收盘价数据",
                               trade_date, instrument_id_main, instrument_id_secondary)
                continue
            date_reorg_data_dic[trade_date] = {
                "trade_date": trade_date,
                "Contract": instrument_id_main,
                "ContractNext": instrument_id_secondary if instrument_id_secondary is not None else None,
                "Close": close_main,
                "CloseNext": close_secondary,
                "TermStructure": math.nan if math.isnan(close_last_day_main) else (
                    (close_main - close_last_day_main) / close_last_day_main),
                "Volume": date_instrument_vol_df[instrument_id_main][trade_date],
                "VolumeNext": date_instrument_vol_df[instrument_id_secondary][
                    trade_date] if instrument_id_secondary is not None else math.nan,
                "OI": date_instrument_position_df[instrument_id_main][trade_date],
                "OINext": date_instrument_position_df[instrument_id_secondary][
                    trade_date] if instrument_id_secondary is not None else math.nan,
                "Open": date_instrument_open_df[instrument_id_main][trade_date],
                "OpenNext": date_instrument_open_df[instrument_id_secondary][
                    trade_date] if instrument_id_secondary is not None else math.nan,
                "High": date_instrument_high_df[instrument_id_main][trade_date],
                "HighNext": date_instrument_high_df[instrument_id_secondary][
                    trade_date] if instrument_id_secondary is not None else math.nan,
                "Low": date_instrument_low_df[instrument_id_main][trade_date],
                "LowNext": date_instrument_low_df[instrument_id_secondary][
                    trade_date] if instrument_id_secondary is not None else math.nan,
                "WarehouseWarrant": date_instrument_st_stock_df[instrument_id_main][trade_date],
                "WarehouseWarrantNext": date_instrument_st_stock_df[instrument_id_secondary][
                    trade_date] if instrument_id_secondary is not None else math.nan,
                "adj_chg_main": 1,
                "adj_chg_secondary": 1,
            }
        trade_date_available_list.append(trade_date)
    # 建立DataFrame
    if len(date_reorg_data_dic) == 0:
        data_no_adj_df, data_adj_df = None, None
    else:
        date_reorg_data_df = pd.DataFrame(date_reorg_data_dic).T.sort_index(ascending=False)
        date_reorg_data_df['instrument_type'] = instrument_type
        date_reorg_data_df['adj_factor_main'] = date_reorg_data_df['adj_chg_main'].cumprod()
        date_reorg_data_df['adj_factor_secondary'] = date_reorg_data_df['adj_chg_secondary'].cumprod()
        col_list = ["trade_date", "Contract", "ContractNext", "Close", "CloseNext", "TermStructure",
                    "Volume", "VolumeNext", "OI", "OINext", "Open", "OpenNext", "High", "HighNext",
                    "Low", "LowNext", "WarehouseWarrant", "WarehouseWarrantNext",
                    "adj_factor_main", "adj_factor_secondary", "instrument_type"]
        # 无复权价格
        data_no_adj_df = date_reorg_data_df[col_list].copy().sort_index()

        # 前复权价格
        date_reorg_data_df['Open'] = date_reorg_data_df['Open'] * date_reorg_data_df['adj_factor_main']
        date_reorg_data_df['High'] = date_reorg_data_df['High'] * date_reorg_data_df['adj_factor_main']
        date_reorg_data_df['Low'] = date_reorg_data_df['Low'] * date_reorg_data_df['adj_factor_main']
        date_reorg_data_df['Close'] = date_reorg_data_df['Close'] * date_reorg_data_df['adj_factor_main']
        date_reorg_data_df['OpenNext'] = date_reorg_data_df['OpenNext'] * date_reorg_data_df['adj_factor_secondary']
        date_reorg_data_df['HighNext'] = date_reorg_data_df['HighNext'] * date_reorg_data_df['adj_factor_secondary']
        date_reorg_data_df['LowNext'] = date_reorg_data_df['LowNext'] * date_reorg_data_df['adj_factor_secondary']
        date_reorg_data_df['CloseNext'] = date_reorg_data_df['CloseNext'] * date_reorg_data_df['adj_factor_secondary']

        data_adj_df = date_reorg_data_df[col_list].copy().sort_index()

        # date_adj_df.reset_index(inplace=True)
        # data_adj_df.rename(columns={
        #     "Date": "trade_date"
        # }, inplace=True)

        if update_table:
            table_name = 'wind_future_continuous_adj'
            update_data_reorg_df_2_db(instrument_type, table_name, data_adj_df)
            table_name = 'wind_future_continuous_no_adj'
            update_data_reorg_df_2_db(instrument_type, table_name, data_no_adj_df)

    return data_no_adj_df, data_adj_df


def handle_data(instrument_type_list, period="daily", update_table=True, export_2_csv=False):
    """将各个品种的，指定周期行情数据复权输出"""
    folder_path = os.path.join(os.path.abspath('.'), 'output', 'commodity_%s' % period)
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    for instrument_type in instrument_type_list:
        logger.info("开始导出 %s 相关数据", instrument_type)
        if period == "daily":
            date_noadj_df, date_adj_df = data_reorg_daily(instrument_type, update_table=update_table)
        # elif period == "half_day":
        #     date_noadj_df, date_adj_df = data_reorg_half_daily(instrument_type)
        # elif period in (PeriodType.Min1, PeriodType.Min5, PeriodType.Min15):
        #     date_noadj_df, date_adj_df = data_reorg_min_n(instrument_type, period)
        else:
            raise ValueError("%s 不支持" % period)
        if export_2_csv and date_noadj_df is not None:
            file_name = "%s ConInfoFull.csv" % instrument_type
            file_path = os.path.join(folder_path, file_name)
            logger.info(file_path)
            date_noadj_df.to_csv(file_path, index=False)

            file_name = "%s ConInfoFull_Adj.csv" % instrument_type
            file_path = os.path.join(folder_path, file_name)
            date_adj_df.to_csv(file_path, index=False)
            logger.info(file_path)


def wind_future_continuous_md():
    """将期货合约数据合并成为连续合约数据，并保存数据库"""
    instrument_type_list = get_all_instrument_type()
    handle_data(instrument_type_list, period="daily", update_table=True, export_2_csv=False)


if __name__ == "__main__":
    # 将期货合约数据合并成为连续合约数据，并保存数据库
    # wind_future_continuous_md()

    # instrument_type_list = ["RU", "AG", "AU", "RB", "HC", "J", "JM", "I", "CU",
    #                         "AL", "ZN", "PB", "NI", "SN",
    #                         "SR", "CF"]
    # instrument_type_list = ["RB"]
    instrument_type_list = get_all_instrument_type()
    handle_data(instrument_type_list, period="daily", export_2_csv=False)
    # handle_data(instrument_type_list, period="half_day")
    # handle_data(instrument_type_list, period=PeriodType.Min15)
    # handle_data(instrument_type_list, period=PeriodType.Min5)

    # 单独生成某合约历史数据
    # instrument_type = "RU"
    # period = PeriodType.Min15
    # data_reorg_daily(instrument_type)
    # data_reorg_half_daily(instrument_type)
    # data_reorg_min_n(instrument_type, period)
