"""
@author  : MG
@Time    : 2021/1/19 9:07
@File    : reversion_rights_factor.py
@contact : mmmaaaggg@163.com
@desc    : 用于
"""
import logging
import re
from functools import lru_cache

import pandas as pd
from ibats_utils.db import with_db_session

from tasks import app
from tasks.backend import engine_md
from tasks.wind.future_reorg.reversion_rights_factor import ReversionRightsMethod, \
    generate_reversion_rights_factors_by_df, save_adj_factor_all

logger = logging.getLogger()


def get_all_instrument_type():
    sql_str = "select ths_code from ifind_future_daily group by ths_code"
    with with_db_session(engine_md) as session:
        instrument_list = [row[0] for row in session.execute(sql_str).fetchall()]
    re_pattern_instrument_type = re.compile(r'\D+(?=\d{3})', re.IGNORECASE)
    instrument_type_set = {re_pattern_instrument_type.search(name).group() for name in instrument_list}
    return list(instrument_type_set)


@lru_cache()
def get_instrument_last_trade_date_dic() -> dict:
    sql_str = """SELECT ths_code, ths_last_td_date_future FROM ifind_future_info 
        where ths_start_trade_date_future is not null
        and ths_last_td_date_future is not null
        and ths_last_delivery_date_future is not null"""
    with with_db_session(engine_md) as session:
        table = session.execute(sql_str)
        instrument_last_trade_date_dic = dict(table.fetchall())
    return instrument_last_trade_date_dic


def generate_reversion_rights_factors(instrument_type, switch_by_key='openInterest',
                                      method: ReversionRightsMethod = ReversionRightsMethod.division):
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
    sql_str = f"""select daily.ths_code, `time` trade_date, `close`, {switch_by_key}
        from ifind_future_daily daily
        inner join (
            SELECT ths_code FROM ifind_future_info 
            where ths_start_trade_date_future is not null 
            and ths_last_td_date_future is not null 
            and ths_last_delivery_date_future is not null
            and ths_code regexp %s 
        ) info
        on daily.ths_code = info.ths_code"""
    data_df = pd.read_sql(sql_str, engine_md, params=[r'^%s[0-9]+\.[A-Z]{3,4}' % (instrument_type,)])
    close_df = data_df.pivot(index="trade_date", columns="ths_code", values="close")
    switch_by_df = data_df.pivot(index="trade_date", columns="ths_code", values=switch_by_key).sort_index()
    if switch_by_df.shape[0] == 0:
        logger.warning("查询 %s 包含 %d 条记录 %d 个合约，跳过", instrument_type, *switch_by_df.shape)
    else:
        logger.info("查询 %s 包含 %d 条记录 %d 个合约", instrument_type, *switch_by_df.shape)

    return generate_reversion_rights_factors_by_df(
        instrument_type, switch_by_key, close_df, switch_by_df,
        instrument_last_trade_date_dic,
        method)


def _test_generate_reversion_rights_factors():
    adj_factor_df, trade_date_latest = generate_reversion_rights_factors(instrument_type='hc')
    print(adj_factor_df)


@app.task
def task_save_adj_factor(chain_param=None):
    # instrument_types = ['rb', 'i', 'hc']
    instrument_types = get_all_instrument_type()
    # instrument_types = ['rb']
    save_adj_factor_all(
        instrument_types=instrument_types, db_table_name="ifind_future_adj_factor", multi_process=0,
        generate_reversion_rights_factors_func=generate_reversion_rights_factors,
    )


if __name__ == "__main__":
    # _test_generate_reversion_rights_factors()
    task_save_adj_factor()
