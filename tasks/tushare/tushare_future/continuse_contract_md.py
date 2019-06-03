# -*- coding: utf-8 -*-
"""
Created on 2018/2/12
@author  : MG
@Time    : 2019/5/27 15:59
@File    : continuse_contract_md.py
@contact : mmmaaaggg@163.com
@desc    : 从 tasks/wind/future_reorg/reorg_md_2_db.py 迁移来
将期货行情数据进行整理，相同批准不同合约的行情合并成为连续行情，并进行复权处理，然后保存到数据库
"""

import logging
import math
import os
import re
from collections import defaultdict

import pandas as pd
from ibats_utils.db import with_db_session
from ibats_utils.mess import str_2_float
from sqlalchemy.dialects.mysql import DOUBLE
from sqlalchemy.types import String, Date

from tasks.backend import engine_md, bunch_insert

logger = logging.getLogger()
# 对于郑商所部分合约需要进行特殊的名称匹配规则
re_pattern_instrument_type_3num_by_wind_code = re.compile(r"(?<=(SR|CF))\d{3}(?=.CZC)", re.IGNORECASE)
re_pattern_instrument_header_by_wind_code = re.compile(r"\d{3,4}(?=.\w)", re.IGNORECASE)
re_pattern_instrument_type_3num_by_instrument_id = re.compile(r"(?<=(SR|CF))\d{3}$", re.IGNORECASE)
re_pattern_instrument_header_by_instrument_id = re.compile(r"\d{3,4}$", re.IGNORECASE)


def get_all_instrument_type():
    """获取合约类型列表"""
    sql_str = "select fut_code from tushare_future_basic group by fut_code"
    with with_db_session(engine_md) as session:
        instrument_type_list = [row[0] for row in session.execute(sql_str).fetchall()]
    return instrument_type_list


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


def update_df_2_db(instrument_type, table_name, data_df):
    """将 DataFrame 数据保存到 数据库对应的表中"""
    dtype = {
        'trade_date': Date,
        'Contract': String(20),
        'ContractNext': String(20),
        'Close': DOUBLE,
        'CloseNext': DOUBLE,
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
        'Amount': DOUBLE,
        'AmountNext': DOUBLE,
        'adj_factor_main': DOUBLE,
        'adj_factor_secondary': DOUBLE,
        'instrument_type': String(20),
    }
    # 为了解决 AttributeError: 'numpy.float64' object has no attribute 'translate' 错误，需要将数据类型转换成 float
    data_df["Close"] = data_df["Close"].apply(str_2_float)
    data_df["CloseNext"] = data_df["CloseNext"].apply(str_2_float)
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
    data_df["Amount"] = data_df["Amount"].apply(str_2_float)
    data_df["AmountNext"] = data_df["AmountNext"].apply(str_2_float)
    data_df["adj_factor_main"] = data_df["adj_factor_main"].apply(str_2_float)
    data_df["adj_factor_secondary"] = data_df["adj_factor_secondary"].apply(str_2_float)
    # 清理历史记录
    with with_db_session(engine_md) as session:
        sql_str = """SELECT table_name FROM information_schema.TABLES 
            WHERE table_name = :table_name and TABLE_SCHEMA=(select database())"""
        # 复权数据表
        is_existed = session.execute(sql_str, params={"table_name": table_name}).fetchone()
        if is_existed is not None:
            session.execute("delete from %s where instrument_type = :instrument_type" % table_name,
                            params={"instrument_type": instrument_type})
            logger.debug("删除 %s 中的 %s 历史数据", table_name, instrument_type)

    # 插入数据库
    bunch_insert(data_df, table_name=table_name, dtype=dtype, primary_keys=['trade_date', 'Contract'])


def get_main_sec_contract_iter(data_df, instrument_type, max_mean_n_for_secondary_contract=3):
    sql_str = r"""select ts_code, delist_date from tushare_future_basic 
        where fut_code=:fut_code and delist_date is not null"""
    with with_db_session(engine_md) as session:
        table = session.execute(sql_str, params={'fut_code': instrument_type})
        ts_code_delist_date_dic = dict(table.fetchall())

    instrument_id_list_sorted = list(data_df.columns)
    instrument_id_list_sorted.sort(key=lambda x: ts_code_delist_date_dic[x])
    contract_col_idx_dic = {_: num for num, _ in enumerate(instrument_id_list_sorted)}
    sorted_df = data_df.sort_index()[instrument_id_list_sorted]
    row_num, trade_date_list, trade_date_count = 0, list(data_df.index), len(data_df.index)
    col_idx_main_contract, col_idx_sec_contract = None, None
    col_idx_main_contract_last, col_idx_sec_contract_last = None, None
    # 找到最近的一个有成交量数据的交易日
    for row_num, trade_date in enumerate(trade_date_list[row_num:], row_num):
        if not pd.isna(col_idx_main_contract):
            col_idx_main_contract_last = col_idx_main_contract
            data_s = sorted_df.iloc[row_num, col_idx_main_contract_last:].dropna()
        else:
            data_s = sorted_df.iloc[row_num, :].dropna()

        if not pd.isna(col_idx_sec_contract):
            col_idx_sec_contract_last = col_idx_sec_contract
        if data_s.shape[0] == 0:
            trade_date = trade_date_list[row_num]
            logger.warning("%s 没有有效数据", trade_date)
            continue
        # logger.debug('%s', data_s)

        # 获取主力合约名称
        main_contract = data_s.idxmax()
        if pd.isna(main_contract):
            logger.warning("%s 没有有效数据", trade_date)
            continue
        col_idx_main_contract = contract_col_idx_dic[main_contract]
        # 获取主力合约成交量
        main_val = data_s[main_contract]
        if pd.isna(main_val):
            logger.warning("%s 没有有效数据", trade_date)
            continue
        # 获取次主力合约名称
        # 次主力合约筛选标志，主力合约之后且近5日均值最大者
        row_num_n = row_num - max_mean_n_for_secondary_contract if row_num >= max_mean_n_for_secondary_contract else 0
        if pd.isna(col_idx_sec_contract_last):
            later_contract_s = sorted_df.iloc[row_num_n: row_num + 1, col_idx_main_contract + 1:].dropna(axis=1).mean()
        elif col_idx_main_contract + 1 > col_idx_sec_contract_last:
            later_contract_s = sorted_df.iloc[row_num_n: row_num + 1, col_idx_main_contract + 1:].dropna(axis=1).mean()
        else:
            later_contract_s = sorted_df.iloc[row_num_n: row_num + 1, col_idx_sec_contract_last:].dropna(axis=1).mean()
        if later_contract_s.shape[0] == 0:
            trade_date = trade_date_list[row_num]
            logger.warning("%s 主力合约 %s, 没有次主力合约", trade_date, main_contract)
            yield trade_date, main_contract, None
            continue

        sec_contract = later_contract_s.idxmax()
        if pd.isna(sec_contract):
            logger.warning("%s 主力合约 %s、没有次主力合约", trade_date, main_contract)
            continue
        col_idx_sec_contract = contract_col_idx_dic[sec_contract]
        # 获取次主力合约成交量
        second_val = data_s[sec_contract]
        if pd.isna(second_val):
            logger.warning("%s 主力合约 %s, 没有次主力合约", trade_date, main_contract)
            yield trade_date, main_contract, None
            continue

        yield trade_date, main_contract, sec_contract


def reorg_2_continuous_md(instrument_type, update_table=True, export_2_csv=False) -> (pd.DataFrame, pd.DataFrame):
    """
    将每一个交易日主次合约行情信息进行展示
    adj_chg_main, adj_chg_secondary 为前复权调整因子
    :param instrument_type:
    :param update_table: 是否更新数据库
    :param export_2_csv: 是否导出csv
    :return:
    """
    sql_str = r"""select ts_code, trade_date, open, high, low, close, vol, oi, ifnull(amount, close * vol) amount
        from tushare_future_daily_md 
        where ts_code in (
            select ts_code from tushare_future_basic 
            where fut_code=%s and delist_date is not null)"""
    data_df = pd.read_sql(sql_str, engine_md, params=[instrument_type])
    date_instrument_vol_df = data_df.pivot(index="trade_date", columns="ts_code", values="vol").sort_index()
    date_instrument_open_df = data_df.pivot(index="trade_date", columns="ts_code", values="open").sort_index()
    date_instrument_low_df = data_df.pivot(index="trade_date", columns="ts_code", values="low").sort_index()
    date_instrument_high_df = data_df.pivot(index="trade_date", columns="ts_code", values="high").sort_index()
    date_instrument_close_df = data_df.pivot(index="trade_date", columns="ts_code", values="close").sort_index()
    date_instrument_position_df = data_df.pivot(index="trade_date", columns="ts_code", values="oi").sort_index()
    date_instrument_amount_df = data_df.pivot(index="trade_date", columns="ts_code", values="amount").sort_index()
    logger.info("查询 %s 包含 %d 条记录 %d 个合约", instrument_type, *date_instrument_vol_df.shape)

    # date_instrument_id_dic = {}
    # 按合约号排序
    date_reorg_data_dic = defaultdict(dict)
    # 逐日检查主力合约，次主力合约列表
    # 主要逻辑：
    # 每天检查次一日的主力合约，次主力合约
    # 主力合约为当日成交量最大合约，合约号只能前进不能后退
    # 次主力合约的交割日期要大于主力合约的交割日期，
    main_contract_last, sec_contract_last, trade_date_last = None, None, None
    for nday, (trade_date, main_contract, sec_contract) in enumerate(
            get_main_sec_contract_iter(date_instrument_vol_df, instrument_type)):
        # if main_contract is not None:
        #     date_instrument_id_dic[trade_date] = (main_contract, sec_contract)

        # 调整连续主力合约调整因子
        if main_contract_last is not None and main_contract_last != main_contract:
            # 主力合约切换，则计算调整因子
            close_cur_contract = date_instrument_close_df.loc[trade_date, main_contract]
            close_last_contract = date_instrument_close_df.loc[trade_date, main_contract_last]
            adj_chg_main = close_cur_contract / close_last_contract
        else:
            adj_chg_main = 1

        # 调整连续次主力合约调整因子
        if main_contract_last is not None and main_contract_last != main_contract:
            # 次主力合约切换，则计算调整因子
            close_cur_contract = date_instrument_close_df.loc[trade_date, main_contract]
            close_last_contract = date_instrument_close_df.loc[trade_date, main_contract_last]
            adj_chg_secondary = close_cur_contract / close_last_contract
        else:
            adj_chg_secondary = 1

        # 数据重组
        close_main = date_instrument_close_df.loc[trade_date, main_contract]
        close_secondary = (date_instrument_close_df.loc[trade_date, sec_contract]) \
            if sec_contract is not None else math.nan
        if math.isnan(close_secondary) and math.isnan(close_main):
            logger.warning("%s 主力合约 %s 次主力合约 %s 均无收盘价数据", trade_date, main_contract, sec_contract)
            continue
        if sec_contract is None or pd.isna(sec_contract):
            date_reorg_data_dic[trade_date] = {
                "trade_date": trade_date,
                "Contract": main_contract,
                "ContractNext": sec_contract if sec_contract is not None else None,
                "Close": close_main,
                "CloseNext": close_secondary,
                "Volume": date_instrument_vol_df.loc[trade_date, main_contract],
                "VolumeNext": math.nan,
                "OI": date_instrument_position_df.loc[trade_date, main_contract],
                "OINext": math.nan,
                "Open": date_instrument_open_df.loc[trade_date, main_contract],
                "OpenNext": math.nan,
                "High": date_instrument_high_df.loc[trade_date, main_contract],
                "HighNext": math.nan,
                "Low": date_instrument_low_df.loc[trade_date, main_contract],
                "LowNext": math.nan,
                "Amount": date_instrument_amount_df.loc[trade_date, main_contract],
                "AmountNext": math.nan,
                "adj_chg_main": adj_chg_main,
                "adj_chg_secondary": adj_chg_secondary,
            }
        else:
            date_reorg_data_dic[trade_date] = {
                "trade_date": trade_date,
                "Contract": main_contract,
                "ContractNext": sec_contract if sec_contract is not None else None,
                "Close": close_main,
                "CloseNext": close_secondary,
                "Volume": date_instrument_vol_df.loc[trade_date, main_contract],
                "VolumeNext": date_instrument_vol_df.loc[trade_date, sec_contract],
                "OI": date_instrument_position_df.loc[trade_date, main_contract],
                "OINext": date_instrument_position_df.loc[trade_date, sec_contract],
                "Open": date_instrument_open_df.loc[trade_date, main_contract],
                "OpenNext": date_instrument_open_df.loc[trade_date, sec_contract],
                "High": date_instrument_high_df.loc[trade_date, main_contract],
                "HighNext": date_instrument_high_df.loc[trade_date, sec_contract],
                "Low": date_instrument_low_df.loc[trade_date, main_contract],
                "LowNext": date_instrument_low_df.loc[trade_date, sec_contract],
                "Amount": date_instrument_amount_df.loc[trade_date, main_contract],
                "AmountNext": date_instrument_amount_df.loc[trade_date, sec_contract],
                "adj_chg_main": adj_chg_main,
                "adj_chg_secondary": adj_chg_secondary,
            }
        main_contract_last, sec_contract_last, trade_date_last = main_contract, sec_contract, trade_date

    # 建立DataFrame
    if len(date_reorg_data_dic) == 0:
        data_no_adj_df, data_adj_df = None, None
    else:
        new_df = pd.DataFrame(date_reorg_data_dic).T.sort_index(ascending=False)
        new_df['instrument_type'] = instrument_type
        new_df['adj_factor_main'] = new_df['adj_chg_main'].cumprod().shift(1).fillna(1)
        new_df['adj_factor_secondary'] = new_df['adj_chg_secondary'].cumprod().shift(1).fillna(1)
        new_df = new_df.sort_index()
        col_list = ["trade_date", "Contract", "ContractNext", "Open", "OpenNext", "High", "HighNext",
                    "Low", "LowNext", "Close", "CloseNext", "Volume", "VolumeNext", "OI", "OINext",
                    "Amount", "AmountNext",
                    "adj_factor_main", "adj_factor_secondary", "instrument_type"]
        # 无复权价格
        data_no_adj_df = new_df[col_list].copy()

        # 前复权价格
        new_df['Open'] = new_df['Open'] * new_df['adj_factor_main']
        new_df['High'] = new_df['High'] * new_df['adj_factor_main']
        new_df['Low'] = new_df['Low'] * new_df['adj_factor_main']
        new_df['Close'] = new_df['Close'] * new_df['adj_factor_main']
        new_df['OpenNext'] = new_df['OpenNext'] * new_df['adj_factor_secondary']
        new_df['HighNext'] = new_df['HighNext'] * new_df['adj_factor_secondary']
        new_df['LowNext'] = new_df['LowNext'] * new_df['adj_factor_secondary']
        new_df['CloseNext'] = new_df['CloseNext'] * new_df['adj_factor_secondary']

        data_adj_df = new_df[col_list].copy()

    if update_table:
        table_name = 'tushare_future_continuous_adj'
        update_df_2_db(instrument_type, table_name, data_adj_df)
        table_name = 'tushare_future_continuous_no_adj'
        update_df_2_db(instrument_type, table_name, data_no_adj_df)

    if export_2_csv and data_adj_df is not None:
        folder_path = os.path.join(os.path.abspath('.'), 'output', 'commodity')
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        file_name = f"{instrument_type} original.csv"
        file_path = os.path.join(folder_path, file_name)
        logger.info(file_path)
        # 2019-06-03
        # 当前处于策略研发需要，仅输出主力合约中的量价相关数据
        data_no_adj_df.to_csv(file_path, index=False)

        file_name = f"{instrument_type}.csv"
        file_path = os.path.join(folder_path, file_name)
        output_df = data_adj_df[[
            "trade_date", "instrument_type", "Open", "High", "Low", "Close", "Volume", "OI", "Amount"
        ]].dropna()
        output_df.rename(columns={_: _.lower() for _ in output_df.columns}).to_csv(file_path, index=False)
        logger.info(file_path)

    return data_no_adj_df, data_adj_df


def tushare_future_continuous_md():
    """将期货合约数据合并成为连续合约数据，并保存数据库"""
    instrument_type_list = get_all_instrument_type()
    # instrument_type_list = ["RU", "AG", "AU", "RB", "HC", "J", "JM", "I", "CU",
    #                         "AL", "ZN", "PB", "NI", "SN",
    #                         "SR", "CF"]
    instrument_type_list = ["RU"]  # , "RB"
    for instrument_type in instrument_type_list:
        logger.info("开始导出 %s 相关数据", instrument_type)
        data_no_adj_df, data_adj_df = reorg_2_continuous_md(instrument_type, update_table=True, export_2_csv=True)
        import matplotlib.pyplot as plt
        data_no_adj_df.Close.plot(legend=True)
        # data_no_adj_df.CloseNext.rename('Close Sec').plot(legend=True)
        data_adj_df.Close.rename('Close Adj').plot(legend=True)
        # data_adj_df.CloseNext.rename('Close Sec Adj').plot(legend=True)
        plt.suptitle(instrument_type)
        plt.grid(True)
        plt.show()


def _test_get_main_sec_contract_iter():
    instrument_type = 'RB'
    sql_str = r"""select ts_code, trade_date, vol
        from tushare_future_daily_md 
        where ts_code in (
            select ts_code from tushare_future_basic 
            where fut_code=%s and delist_date is not null)"""
    data_df = pd.read_sql(sql_str, engine_md, params=[instrument_type])
    logger.info('data_df.shape %s', data_df.shape)
    date_instrument_vol_df = data_df.pivot(index="trade_date", columns="ts_code", values="vol").sort_index()
    logger.info('date_instrument_vol_df.shape %s', date_instrument_vol_df.shape)
    for nday, (trade_date, main_contract, sec_contract) in enumerate(
            get_main_sec_contract_iter(date_instrument_vol_df, instrument_type), start=1):
        logger.info('%4d) %s %s %s', nday, trade_date, main_contract, sec_contract)


if __name__ == "__main__":
    # 将期货合约数据合并成为连续合约数据，并保存数据库
    tushare_future_continuous_md()
    # _test_get_main_sec_contract_iter()
