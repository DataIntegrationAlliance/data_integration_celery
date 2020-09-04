"""
@author  : MG
@Time    : 2020/9/3 17:39
@File    : merge_2_n_bar.py
@contact : mmmaaaggg@163.com
@desc    : 用于读取csv或excel文件，将5分钟数据合并成为N分钟数据（10,15,30,1H，2H）
"""
import math
import os
import re
import logging
import pandas as pd
from datetime import datetime, timedelta
import tasks.config  # NOQA

dt_base = datetime.strptime('1899-12-30', '%Y-%m-%d')
logger = logging.getLogger()


def generate_bar_dt(dt, minutes):
    date_only = datetime(dt.year, dt.month, dt.day)
    delta = dt.to_pydatetime() - date_only
    key = math.ceil(delta.seconds / 60 / minutes)
    by = date_only + timedelta(minutes=key * minutes)
    return by


def merge_2_n_bar(file_path, minutes_list=[10, 15, 20, 30, 60, 120], output_path=None, dt_2_float=True):
    _, ext = os.path.splitext(file_path)
    if ext == ".csv":
        df = pd.read_csv(file_path, header=None)
    else:
        df = pd.read_excel(file_path, header=None)

    logger.info("加载数据 %d 条", df.shape[0])
    dt_s = df[0].apply(lambda x: dt_base + timedelta(days=x))
    result_dic = {}
    list_len = len(minutes_list)
    for n, minutes in enumerate(minutes_list, start=1):
        bar_dt_s = dt_s.apply(lambda x: generate_bar_dt(x, minutes))
        data_list = []
        for bar_dt, sub_df in df.groupby(by=bar_dt_s):
            data_list.append({
                'datetime': ((bar_dt - dt_base).total_seconds() / 60 / 60 / 24) if dt_2_float else bar_dt,
                'open': sub_df.iloc[0, 1],
                'high': sub_df.iloc[:, 2].max(),
                'low': sub_df.iloc[0, 3].min(),
                'close': sub_df.iloc[-1, 4],
                # 'volume': sub_df.iloc[:, 5].sum(),  # 目前没有 volume
            })

        new_df = pd.DataFrame(data_list)[['datetime', 'open', 'high', 'low', 'close']].ffill()
        logger.info("%d/%d) 整理完成转化 %d 分钟线后 %d 条", n, list_len, minutes, new_df.shape[0])

        dir_path, file_name = os.path.split(file_path)
        if dir_path == '':
            dir_path = os.path.curdir

        new_file_name = file_name.replace(re.search(r'=\d', file_name).group(), f'={minutes}')
        output_path = os.path.join(dir_path, new_file_name)

        if ext == '.csv':
            new_df.to_csv(output_path, index=False, header=None)
        else:
            new_df.to_excel(output_path, index=False, header=None)

        logger.info("%d/%d) 输出文件 %s", n, list_len, output_path)
        result_dic[minutes] = new_df

    return result_dic


if __name__ == "__main__":
    merge_2_n_bar(
        file_path=r'C:\Users\26559\Downloads\章\历年RB01BarSize=5高开低收.xls',
        # minutes_list=[20, 30, 60, 120]
    )
