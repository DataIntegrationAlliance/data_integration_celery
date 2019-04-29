#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 19-4-29 下午2:29
@File    : index_weights.py
@contact : mmmaaaggg@163.com
@desc    : https://www.joinquant.com/help/api/help?name=JQData#get_index_weights-%E8%8E%B7%E5%8F%96%E6%8C%87%E6%95%B0%E6%88%90%E4%BB%BD%E8%82%A1%E6%9D%83%E9%87%8D%EF%BC%88%E6%9C%88%E5%BA%A6%EF%BC%89
get_index_weights -获取指数成份股权重（月度）
get_index_weights(index_id, date=None)
获取指数成份股给定日期的权重数据，每月更新一次，请点击指数列表查看指数信息

参数

index_id: 代表指数的标准形式代码， 形式：指数代码.交易所代码，例如"000001.XSHG"。
date: 查询权重信息的日期，形式："%Y-%m-%d"，例如"2018-05-03"；
返回

查询到对应日期，且有权重数据，返回 pandas.DataFrame， code(股票代码)，display_name(股票名称), date(日期), weight(权重)；
查询到对应日期，且无权重数据， 返回距离查询日期最近日期的权重信息；
找不到对应日期的权重信息， 返回距离查询日期最近日期的权重信息；
示例

#获取2018年5月9日这天的上证指数的成份股权重
df = get_index_weights(index_id="000001.XSHG", date="2018-05-09")
print(df)

＃输出
code          display_name      date     weight
603648.XSHG         畅联股份  2018-05-09   0.023
603139.XSHG         康惠制药  2018-05-09   0.007
603138.XSHG         海量数据  2018-05-09   0.015
603136.XSHG          天目湖  2018-05-09   0.009
603131.XSHG         上海沪工  2018-05-09   0.011
                 ...         ...     ...
603005.XSHG         晶方科技  2018-05-09   0.023
603007.XSHG         花王股份  2018-05-09   0.013
603006.XSHG         联明股份  2018-05-09   0.008
603009.XSHG         北特科技  2018-05-09   0.014
603008.XSHG          喜临门  2018-05-09   0.022
"""
import pandas as pd
from sqlalchemy.dialects.mysql import DOUBLE
from tasks import app
from sqlalchemy.types import String, Date, Integer
from tasks.jqdata import get_index_weights

from tasks.jqdata.index import import_data

INDICATOR_PARAM_LIST = [
    ('index_symbol', String(20)),
    ('trade_date', Date),
    ('jq_code', String(20)),
    ('display_name', String(20)),
    ('weight', DOUBLE),
]
# 设置 dtype
DTYPE = {key: val for key, val in INDICATOR_PARAM_LIST}


def invoke_api(index_symbol, trade_date) -> (pd.DataFrame, None):
    df = get_index_weights(index_symbol, trade_date)
    if df is None or df.shape[0] == 0:
        return None
    df.index.rename('jq_code', inplace=True)
    df.reset_index(inplace=True)
    df['index_symbol'] = index_symbol
    df.rename(columns={'code': 'jq_code', 'date': 'trade_date'}, inplace=True)
    return df


@app.task
def import_jq_index_weights(chain_param=None, ts_code_set=None):
    import_data(table_name='jq_index_weights', dtype=DTYPE, invoke_api=invoke_api, ts_code_set=ts_code_set,
                is_monthly=True)


if __name__ == "__main__":
    import_jq_index_weights()
