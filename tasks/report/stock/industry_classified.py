#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/10/16 14:53
@File    : industry_classified.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
import matplotlib.pyplot as plt
from pylab import mpl
from tasks.backend import engine_md
import pandas as pd
from tasks.utils.db_utils import with_db_session
from tasks.utils.fh_utils import iter_2_range
from collections import defaultdict
import logging

logger = logging.getLogger()
mpl.rcParams['font.sans-serif'] = ['SimHei']  # 字体可以根据需要改动
mpl.rcParams['axes.unicode_minus'] = False  # 解决中文减号不显示的问题


def plot_industry_classified_mid(col_name='ev2_to_ebitda'):
    # sql_str = """select sector_code, sector_name,base.trade_date, sum(ev2_to_ebitda) tot_val
    #     from (
    #         SELECT * FROM fof_ams_dev.wind_sectorconstituent where sector_name like 'cs%%'
    #     ) base
    #     LEFT JOIN
    #     (
    #     select trade_date, wind_code, ev2_to_ebitda from wind_stock_daily where ev2_to_ebitda is not null
    #     ) val
    #     on base.trade_date = val.trade_date
    #     and base.wind_code = val.wind_code
    #     group by sector_code, base.trade_date
    #     having tot_val is not null"""
    # TODO: 待行业数据下载齐全后可生成相应的分布图
    sector_sql_str = """SELECT sector_name, trade_date, wind_code FROM fof_ams_dev.wind_sectorconstituent 
        where sector_name like 'cs%'"""
    with with_db_session(engine_md) as session:
        table = session.execute(sector_sql_str)
        sector_trade_date_wind_code_list_dic = defaultdict(dict)
        num = 0
        for num, (sector_name, trade_date, wind_code) in enumerate(table.fetchall(), start=1):
            if sector_name not in sector_trade_date_wind_code_list_dic:
                sector_trade_date_wind_code_list_dic[sector_name] = {
                    'trade_date_set': set(),
                    'trade_date_wind_code_list_dic': defaultdict(list)
                }
            sector_trade_date_wind_code_list_dic[sector_name]['trade_date_set'].add(trade_date)
            sector_trade_date_wind_code_list_dic[sector_name][
                'trade_date_wind_code_list_dic'][trade_date].append(wind_code)
    sector_count = len(sector_trade_date_wind_code_list_dic)
    logger.debug('获取行业数据 %d 条 %d 个行业', num, sector_count)

    stock_sql_str = f"""select wind_code, trade_date, `{col_name}` from wind_stock_daily 
        where `{col_name}` is not null"""
    data_df = pd.read_sql(stock_sql_str, engine_md)
    logger.debug('获取行情数据 %d 条', data_df.shape[0])
    pivot_df = data_df.pivot(index='trade_date', columns='wind_code', values=col_name).sort_index()
    logger.debug('转换数据 %s', pivot_df.shape)

    sector_trade_date_val_list_dic, sector_trade_date_val_dic = {}, {}
    logger.debug('计算 %d 个行业中位数', sector_count)
    for num, (sector_name, data_dic) in enumerate(sector_trade_date_wind_code_list_dic.items(), start=1):
        trade_date_list = list(data_dic['trade_date_set'])
        trade_date_list.sort()
        trade_date_list_len = len(trade_date_list)
        logger.debug('%d/%d) %s %d 个交易日', num, sector_count, sector_name, trade_date_list_len)
        trade_date_wind_code_list_dic = data_dic['trade_date_wind_code_list_dic']
        # for trade_date, wind_code_list in trade_date_wind_code_list_dic.items():
        for num2, (trade_date_from, trade_date_to) in enumerate(
                iter_2_range(trade_date_list, has_left_outer=False), start=1):
            wind_code_list = trade_date_wind_code_list_dic[trade_date_from]
            # logger.debug('%d/%d) [%d/%d] %s [%s %s)', num, sector_count, num2, trade_date_list_len,
            #              sector_name, trade_date_from, trade_date_to, )
            # 计算中位数
            try:
                tmp_df = pivot_df.loc[trade_date_from:trade_date_to, wind_code_list]
                if tmp_df.shape[0] == 0:
                    continue
            except KeyError:
                continue
            val_s = tmp_df.median(axis=1)
            if trade_date_to is not None:
                # 去除最后一天
                val_s = val_s.iloc[:-1]
            # 保存到dict
            if sector_name not in sector_trade_date_val_list_dic:
                sector_trade_date_val_list_dic[sector_name] = [val_s]
            else:
                sector_trade_date_val_list_dic[sector_name].append(val_s)

        # 合并计算结果成为 一个 Series
        if sector_name in sector_trade_date_val_list_dic and len(sector_trade_date_val_list_dic[sector_name]) > 0:
            logger.debug('%s %d 个交易日合并数据', sector_name, len(trade_date_list))
            sector_trade_date_val_dic[sector_name] = pd.concat(sector_trade_date_val_list_dic[sector_name])

    # 数据合并
    # 将所有 sector 的 数据合并成为 DataFrame
    logger.debug('合并 %d 个行业数据', sector_count)
    data_df = pd.DataFrame(sector_trade_date_val_dic)
    data_df.to_excel('median.xls', legend=False)
    data_df.plot()
    plt.show()


if __name__ == "__main__":
    plot_industry_classified_mid()
