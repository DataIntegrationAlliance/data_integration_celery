"""
@author  : MG
@Time    : 2021/1/27 11:15
@File    : reversion_rights_md.py
@contact : mmmaaaggg@163.com
@desc    : 用于产生复权行情数据
"""

import logging
import typing
from datetime import timedelta
from enum import Enum

import pandas as pd
from ibats_utils.db import with_db_session, bunch_insert_on_duplicate_update
from ibats_utils.mess import str_2_date
from sqlalchemy.dialects.mysql import DOUBLE
from sqlalchemy.types import String, Date

from tasks import config
from tasks.backend import engine_md

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


class Method(Enum):
    """
    方法枚举，value为对应的默认 adj_factor 因子值
    """
    division = 1
    diff = 0


def generate_md_with_adj_factor(instrument_type: str,
                                method: typing.Optional[Method] = Method.division):
    """
    将指定期货品种生产复权后价格。
    主力合约名称 f"{instrument_type.upper()}9999"
    次主力合约名称 f"{instrument_type.upper()}8888"
    仅针对 OHLC 四个价格进行复权处理
    """
    table_name = 'ifind_future_adj_factor'
    sql_str = f"""SELECT trade_date, instrument_id_main, adj_factor_main, 
        instrument_id_secondary, adj_factor_secondary
        FROM {table_name}
        where instrument_type=%s and method=%s"""
    adj_factor_df = pd.read_sql(sql_str, engine_md, params=[instrument_type, method.name])
    adj_factor_df['trade_date_larger_than'] = adj_factor_df['trade_date'].shift(1)
    adj_factor_df.set_index(['trade_date_larger_than', 'trade_date'], inplace=True)

    instrument_id_set = set(adj_factor_df['instrument_id_main']) | set(adj_factor_df['instrument_id_secondary'])
    in_clause_str = "'" + "', '".join(instrument_id_set) + "'"
    # daily_table_name = 'ifind_future_daily'
    sql_str = f"""SELECT ths_code Contract, `time` trade_date, `Open`, `High`, `Low`, `Close`, Volume, openInterest OI
    FROM ifind_future_daily 
    where ths_code in ({in_clause_str})
    """
    daily_df = pd.read_sql(sql_str, engine_md)
    n_final = adj_factor_df.shape[0]
    data_count = 0
    for n, ((trade_date_larger_than, trade_date), adj_factor_s) in enumerate(adj_factor_df.iterrows(), start=1):
        # 截止当日下午3点收盘，保守计算，延迟1个小时，到16点
        if pd.isna(trade_date_larger_than):
            start = None
        else:
            start = trade_date_larger_than + timedelta(days=1)

        if n == n_final:
            if start is None:
                start = str_2_date('1990-01-01')
                end = str_2_date('2090-12-31')
            else:
                end = start + timedelta(days=365)
        else:
            end = trade_date
            if start is None:
                start = end - timedelta(days=365)

        # 对 主力、次主力合约复权
        is_match = (
                           daily_df['code'] >= pd.to_datetime(start)
                   ) & (
                           daily_df['code'] < pd.to_datetime(end)
                   )
        instrument_id_main = adj_factor_s['instrument_id_main']
        adj_factor_main = adj_factor_s['adj_factor_main']
        main_df = daily_df[(daily_df['code'] == instrument_id_main) & is_match].copy()
        instrument_id_secondary = adj_factor_s['instrument_id_secondary']
        adj_factor_secondary = adj_factor_s['adj_factor_secondary']
        sec_df = daily_df[(daily_df['code'] == instrument_id_secondary) & is_match].copy()
        rename_dic = {_: f"{_}Next" for _ in sec_df.columns if _ != 'trade_date'}
        sec_df.rename(columns=rename_dic, inplace=True)
        main_sec_df = pd.merge(main_df, sec_df, on='trade_date')
        main_sec_df['instrument_type'] = instrument_type
        main_sec_df['adj_factor_main'] = adj_factor_main
        main_sec_df['adj_factor_secondary'] = adj_factor_secondary
        dtype = {
            'trade_date': Date,
            'Contract': String(20),
            'ContractNext': String(20),
            'instrument_type': String(20),
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
            'adj_factor_main': DOUBLE,
            'adj_factor_secondary': DOUBLE
        }
        table_name = 'wind_future_continuous_no_adj'
        bunch_insert_on_duplicate_update(
            main_sec_df, table_name, engine_md, dtype=dtype,
            primary_keys=['instrument_type', 'trade_date'], schema=config.DB_SCHEMA_MD
        )

        main_sec_df['Open'] *= adj_factor_main
        main_sec_df['High'] *= adj_factor_main
        main_sec_df['Low'] *= adj_factor_main
        main_sec_df['Close'] *= adj_factor_main
        main_sec_df['Volume'] *= adj_factor_main
        main_sec_df['OI'] *= adj_factor_main
        main_sec_df['OpenNext'] *= adj_factor_secondary
        main_sec_df['HighNext'] *= adj_factor_secondary
        main_sec_df['LowNext'] *= adj_factor_secondary
        main_sec_df['CloseNext'] *= adj_factor_secondary
        main_sec_df['VolumeNext'] *= adj_factor_secondary
        main_sec_df['OINext'] *= adj_factor_secondary
        table_name = 'wind_future_continuous_adj'
        bunch_insert_on_duplicate_update(
            main_sec_df, table_name, engine_md, dtype=dtype,
            primary_keys=['instrument_type', 'trade_date'], schema=config.DB_SCHEMA_MD
        )
        data_count += main_sec_df.shape[0]
        logger.info("%s [%s ~ %s] 包含 %d 条数据，复权保存完成",
                    instrument_type, start, end, main_sec_df.shape[0])

    logger.info(f'{instrument_type.upper()} {data_count} 条记录 复权保存完成')


def _test_generate_md_with_adj_factor():
    instrument_type = 'rb'
    generate_md_with_adj_factor(instrument_type, method=Method.division)


def generate_adj_md(instrument_types: typing.Optional[list], method: Method, until_instrument_type=None):
    """
    对指定类型进行复权数据生成
    :param instrument_types 类型列表，如果为None则为全部生成
    :param method 方法筛选
    :param until_instrument_type 指定当前品种才开始运行（仅同调试使用），有时候运行中途出错，再次运行时，从报错的品种开始重新运行，而不是全部重新运行
    """
    if instrument_types is None:
        sql_str = """SELECT distinct instrument_type 
        FROM ifind_future_adj_factor where method = :method"""
        with with_db_session(engine_md) as session:
            instrument_types = [
                row[0] for row in session.execute(sql_str, params={'method': method.name}).fetchall()]

    for instrument_type in instrument_types:
        generate_md_with_adj_factor(instrument_type, method=method)


def _test_generate_adj_md():
    instrument_types = None
    # instrument_types = [
    #     'rb', 'hc', 'i', 'j', 'jm', 'ru', 'bu', 'ma', 'cu', 'fg', 'jd',
    #     'ap', 'cj', 'a', 'b', 'm', 'oi', 'y', 'rm', 'p', 'rs', 'sr', 'cf'
    # ]
    generate_adj_md(
        instrument_types,
        method=Method.division,
        # until_instrument_type='V',
    )


if __name__ == "__main__":
    # _test_generate_md_with_adj_factor()
    _test_generate_adj_md()
