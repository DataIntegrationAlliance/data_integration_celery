"""
@author  : MG
@Time    : 2020/9/27 8:10
@File    : reversion_rights_factor.py
@contact : mmmaaaggg@163.com
@desc    : 用于计算给定期货品种的前复权因子、对应的合约及日期，导入数据库
"""
import logging
import os
from collections import defaultdict
from enum import Enum

import numpy as np
import pandas as pd
from ibats_utils.db import bunch_insert_on_duplicate_update, with_db_session
from ibats_utils.mess import str_2_float, date_2_str
from sqlalchemy.dialects.mysql import DOUBLE
from sqlalchemy.types import String, Date

from tasks import app
from tasks.backend import engine_md
from tasks.config import config
from tasks.wind.future_reorg.reorg_md_2_db import is_earlier_instruments, is_later_instruments, \
    get_all_instrument_type, get_instrument_last_trade_date_dic

logger = logging.getLogger()


class Method(Enum):
    """
    方法枚举，value为对应的默认 adj_factor 因子值
    """
    division = 1
    diff = 0


def calc_adj_factor(close_df, trade_date, instrument_id_curr, instrument_id_last, method: Method = Method.division):
    """
    合约切换时计算两合约直接的价格调整因子
    该调整因子为前复权因子，使用方法
    method='division'
    复权后价格 = instrument_id_last 合约的价格 * adj_factor
    method='diff'
    复权后价格 = instrument_id_last 合约的价格 + adj_factor
    :param close_df:
    :param trade_date:
    :param instrument_id_curr:
    :param instrument_id_last:
    :param method: division 除法  diff  差值发
    :return:
    """
    close_last = close_df[instrument_id_last][trade_date]
    close_curr = close_df[instrument_id_curr][trade_date]
    if method == Method.division:
        adj_factor = close_curr / close_last
    elif method == Method.diff:
        adj_factor = close_curr - close_last
    else:
        raise ValueError(f"method {method} 不被支持")
    return adj_factor


def generate_reversion_rights_factors(instrument_type, switch_by_key='position', method: Method = Method.division):
    """
    给定期货品种，历史合约的生成前复权因子
    :param instrument_type: 合约品种，RB、I、HC 等
    :param switch_by_key: position 持仓量, volume 成交量, st_stock 注册仓单量
    :param method: division 除法  diff  差值发
    :return:
    """
    instrument_last_trade_date_dic = get_instrument_last_trade_date_dic()
    instrument_type = instrument_type.upper()
    # 获取当前期货品种全部历史合约的日级别行情数据
    sql_str = r"""select wind_code, trade_date, open, close, """ + switch_by_key + """ 
      from wind_future_daily where wind_code regexp %s"""
    data_df = pd.read_sql(sql_str, engine_md, params=[r'^%s[0-9]+\.[A-Z]+' % (instrument_type,)])
    close_df = data_df.pivot(index="trade_date", columns="wind_code", values="close")
    switch_by_df = data_df.pivot(index="trade_date", columns="wind_code", values=switch_by_key).sort_index()
    logger.info("查询 %s 包含 %d 条记录 %d 个合约", instrument_type, *switch_by_df.shape)

    # 主力合约号，次主力合约号
    instrument_id_main, instrument_id_secondary = None, None
    # date_instrument_id_dic = {}
    # 按合约号排序，历史合约号从远至近排序
    instrument_id_list_sorted = list(switch_by_df.columns)
    instrument_id_list_sorted.sort(key=lambda x: instrument_last_trade_date_dic[x])
    switch_by_df = switch_by_df[instrument_id_list_sorted]
    date_adj_factor_dic = defaultdict(dict)
    # 逐日检查主力合约，次主力合约列表
    # 主要逻辑：
    # 每天检查次一日的主力合约，次主力合约
    # 主力合约为当日“持仓量”最大合约，合约号只能前进不能后退
    # 次主力合约的交割日期要大于主力合约的交割日期，
    trade_date_list = list(switch_by_df.index)
    trade_date_latest = np.max(trade_date_list)
    trade_date_available_list = []
    trade_date, trade_date_last = None, None  # 当前交易日， 上一个交易日日期
    for n, trade_date in enumerate(trade_date_list):
        # if instrument_id_main is not None \
        #         and date_instrument_id_dic[trade_date_last] != date_instrument_id_dic[trade_date]:
        #     # 前一日根据 switch_by_s 变化判定主力合约，次主力合约是否切换
        #     # 次日记录主力合约切换
        #     # 仅记录合约变化的情况
        #     date_instrument_id_dic[trade_date] = (instrument_id_main, instrument_id_secondary)

        # 筛选有效数据
        switch_by_s = switch_by_df.loc[trade_date].dropna()
        if switch_by_s.shape[0] == 0:
            logger.warning("%d) %s 没有 %s 数据", n, trade_date, switch_by_key)
            continue
        instrument_id_main_last, instrument_id_secondary_last = instrument_id_main, instrument_id_secondary
        # 当日主力合约没有数据，或者主力合约已经过期
        no_data_for_main_instrument = instrument_id_main not in switch_by_s.index
        # 循环查找各个合约，寻找更合适的主力合约
        for instrument_id in switch_by_s.index:
            if instrument_id_main is not None \
                    and is_earlier_instruments(instrument_id, instrument_id_main):
                # 如果 合约日期比 当前主力合约日期早，则直接跳过
                continue
            # 判断主力合约
            if instrument_id_main is None:
                instrument_id_main = instrument_id
            elif instrument_id_main in switch_by_s and switch_by_s[instrument_id_main] < switch_by_s[instrument_id]:
                instrument_id_main = instrument_id
                if instrument_id_secondary is not None \
                        and (instrument_id_main == instrument_id_secondary
                             or is_later_instruments(instrument_id_main, instrument_id_secondary)):
                    # 如果次主力合约不是晚于主力合约则至 None，重新寻找合适的次主力合约
                    instrument_id_secondary = None

            # 判断次主力合约
            if instrument_id_secondary is None:
                if instrument_id_main != instrument_id:
                    instrument_id_secondary = instrument_id
            elif is_earlier_instruments(instrument_id_secondary, instrument_id) \
                    and instrument_id_secondary in switch_by_s \
                    and switch_by_s[instrument_id_secondary] < switch_by_s[instrument_id]:
                instrument_id_secondary = instrument_id

        # 检查主力合约是否确定
        if instrument_id_main is None:
            logger.warning("%d) %s 主力合约缺少主力合约", n, trade_date)

        # 检查次主力合约是否确定
        if instrument_id_secondary is None:  # and instrument_id_main_last is not None
            logger.warning("%d) %s 当日主力合约 %s, 没有次主力合约",
                           n, trade_date, instrument_id_main)

        # 如果主力合约切换，则计算调整因子
        if instrument_id_main_last is not None \
                and instrument_id_main_last != instrument_id_main:
            trade_date_last = trade_date_available_list[-1]
            adj_chg = calc_adj_factor(
                close_df, trade_date_last,
                instrument_id_curr=instrument_id_main,
                instrument_id_last=instrument_id_main_last,
                method=method
            )
            date_adj_factor_dic[trade_date_last]['adj_factor_main'] = adj_chg
            date_adj_factor_dic[trade_date_last]['instrument_id_main'] = instrument_id_main_last

        # 如果次主力合约切换，则计算调整因子
        if instrument_id_secondary_last is not None \
                and instrument_id_secondary is not None \
                and instrument_id_secondary_last != instrument_id_secondary:
            trade_date_last = trade_date_available_list[-1]
            adj_chg = calc_adj_factor(
                close_df, trade_date_last,
                instrument_id_curr=instrument_id_secondary,
                instrument_id_last=instrument_id_secondary_last,
                method=method
            )
            date_adj_factor_dic[trade_date_last]['adj_factor_secondary'] = adj_chg
            date_adj_factor_dic[trade_date_last]['instrument_id_secondary'] = instrument_id_secondary_last

        # 记录有效日期
        trade_date_available_list.append(trade_date)
        trade_date_last = trade_date

    if trade_date is None or instrument_id_main is None or instrument_id_secondary is None:
        logger.warning("当前品种 %s 最后一个交易日期 %s 主力合约 %s 次主力合约 %s，历史数据错误",
                       instrument_type, trade_date, instrument_id_main, instrument_id_secondary)
    else:
        # 记录最新一个交易日的主力合约次主力合约调整因子 为1
        date_adj_factor_dic[trade_date] = {
            'adj_factor_main': method.value,
            'instrument_id_main': instrument_id_main,
            'adj_factor_secondary': method.value,
            'instrument_id_secondary': instrument_id_secondary,
        }

    # 构造复权因子数据
    adj_factor_df = pd.DataFrame(date_adj_factor_dic).T.sort_index(ascending=False)
    available_titles = [
        "instrument_id_main",
        "adj_factor_main",
        "instrument_id_secondary",
        "adj_factor_secondary",
    ]
    missing_columns = set(available_titles) - set(adj_factor_df.columns)
    if len(missing_columns) > 0:
        logger.error("%s 缺少 %s 数据信息，无法生成复权因子", instrument_type, missing_columns)
        return None, None

    adj_factor_df = adj_factor_df[available_titles]
    if method == Method.division:
        adj_factor_df['adj_factor_main'] = adj_factor_df['adj_factor_main'].fillna(1).cumprod()
        adj_factor_df['adj_factor_secondary'] = adj_factor_df['adj_factor_secondary'].fillna(1).cumprod()
    elif method == Method.diff:
        adj_factor_df['adj_factor_main'] = adj_factor_df['adj_factor_main'].fillna(1).cumsum()
        adj_factor_df['adj_factor_secondary'] = adj_factor_df['adj_factor_secondary'].fillna(1).cumsum()
    else:
        raise ValueError(f"method={method} 不被支持")

    adj_factor_df = adj_factor_df.ffill().sort_index().reset_index().rename(
        columns={'index': 'trade_date'})
    adj_factor_df["instrument_type"] = instrument_type
    return adj_factor_df, trade_date_latest


def update_df_2_db(instrument_type, table_name, data_df, dtype=None):
    """将 DataFrame 数据保存到 数据库对应的表中"""
    # 为了解决 AttributeError: 'numpy.float64' object has no attribute 'translate' 错误，需要将数据类型转换成 float
    data_df["adj_factor_main"] = data_df["adj_factor_main"].apply(str_2_float)
    data_df["adj_factor_secondary"] = data_df["adj_factor_secondary"].apply(str_2_float)
    # 清理历史记录
    with with_db_session(engine_md) as session:
        sql_str = "SELECT table_name FROM information_schema.TABLES " \
                  "WHERE table_name = :table_name and TABLE_SCHEMA=(select database())"
        # 复权数据表
        is_existed = session.execute(sql_str, params={"table_name": table_name}).fetchone()
        if is_existed is not None:
            session.execute("delete from %s where instrument_type = :instrument_type" % table_name,
                            params={"instrument_type": instrument_type})
            logger.debug("删除 %s 中的 %s 历史数据，重新载入新的复权数据", table_name, instrument_type)

    # 插入数据库
    # pd.DataFrame.to_sql(data_df, table_name, engine_md, if_exists='append', index=False, dtype=dtype)
    bunch_insert_on_duplicate_update(
        data_df, table_name, engine_md,
        dtype=dtype, myisam_if_create_table=True,
        primary_keys=['trade_date', 'instrument_type'], schema=config.DB_SCHEMA_MD)


def save_adj_factor(instrument_types: list, to_db=True, to_csv=True):
    """

    :param instrument_types: 合约类型
    :param to_db: 是否保存到数据库
    :param to_csv: 是否保存到csv文件
    :return:
    """
    dir_path = 'output'
    if to_csv:
        # 建立 output folder
        os.makedirs(dir_path, exist_ok=True)

    for method in Method:
        for n, instrument_type in enumerate(instrument_types):
            logger.info("生成 %s 复权因子", instrument_type)
            adj_factor_df, trade_date_latest = generate_reversion_rights_factors(instrument_type, method=method)
            if adj_factor_df is None:
                continue

            if to_csv:
                csv_file_name = f'adj_factor_{instrument_type}_{method.name}.csv'
                folder_path = os.path.join(dir_path, date_2_str(trade_date_latest))
                csv_file_path = os.path.join(folder_path, csv_file_name)
                os.makedirs(folder_path, exist_ok=True)
                adj_factor_df.to_csv(csv_file_path, index=False)

            if to_db:
                table_name = 'wind_future_adj_factor'
                dtype = {
                    'trade_date': Date,
                    'instrument_id_main': String(20),
                    'adj_factor_main': DOUBLE,
                    'instrument_id_secondary': String(20),
                    'adj_factor_secondary': DOUBLE,
                    'instrument_type': String(20),
                    'method': String(20),
                }
                adj_factor_df['method'] = method.name
                update_df_2_db(instrument_type, table_name, adj_factor_df, dtype)

            logger.info("生成 %s 复权因子 %s 条记录[%s]",  # \n%s
                        instrument_type, adj_factor_df.shape[0], method.name
                        # , adj_factor_df
                        )


def _test_generate_reversion_rights_factors():
    adj_factor_df, trade_date_latest = generate_reversion_rights_factors(instrument_type='hc')
    print(adj_factor_df)


@app.task
def task_save_adj_factor(chain_param=None):
    # instrument_types = ['rb', 'i', 'hc']
    instrument_types = get_all_instrument_type()
    # instrument_types = ['ap']
    save_adj_factor(instrument_types=instrument_types)


if __name__ == "__main__":
    # _test_generate_reversion_rights_factors()
    task_save_adj_factor()
