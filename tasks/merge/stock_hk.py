#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/8/31 9:57
@File    : stock_hk.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
import pandas as pd
import logging
from tasks import app
from direstinvoker.utils.fh_utils import date_2_str
from tasks import bunch_insert_on_duplicate_update, alter_table_2_myisam, build_primary_key
from tasks.backend import engine_md
from tasks.utils.db_utils import with_db_session
# from tasks.merge import mean_value, prefer_left, prefer_right, merge_data, get_value
from sqlalchemy.types import String, Date, Integer, Text
# from sqlalchemy.dialects.mysql import DOUBLE
from tasks.merge import get_ifind_daily_df, get_wind_daily_df, generate_range
from tasks.ifind.stock_hk import DTYPE_STOCK_HK_DAILY_DS, DTYPE_STOCK_HK_DAILY_HIS, DTYPE_STOCK_HK_FIN, \
    DTYPE_STOCK_HK_REPORT_DATE
from tasks.utils.fh_utils import is_nan_or_none

logger = logging.getLogger()
DEBUG = False


@app.task
def merge_ifind_stock_hk_daily(date_from=None):
    """
    合并 wind，ifind 数据到对应名称的表中
    :param date_from:
    :return:
    """
    table_name = 'ifind_stock_hk_daily'
    logging.info("合成 %s 开始", table_name)
    has_table = engine_md.has_table(table_name)
    if date_from is None and has_table:
        sql_str = "select adddate(max(`time`),1) from {table_name}".format(table_name=table_name)
        with with_db_session(engine_md) as session:
            date_from = date_2_str(session.execute(sql_str).scalar())
    ifind_his_df = get_ifind_daily_df('ifind_stock_hk_daily_his', date_from)
    ifind_ds_df = get_ifind_daily_df('ifind_stock_hk_daily_ds', date_from)
    ifind_report_date_df = get_ifind_daily_df('ifind_stock_hk_report_date', None)
    ifind_fin_df = get_ifind_daily_df('ifind_stock_hk_fin', None)
    ifind_fin_df_g = ifind_fin_df.groupby('ths_code')
    # 合并 ds his 数据
    ifind_his_ds_df = pd.merge(ifind_his_df, ifind_ds_df, how='outer',
                               on=['ths_code', 'time'])
    ifind_his_ds_df_g = ifind_his_ds_df.groupby('ths_code')
    logging.debug("提取数据完成")
    # 计算 财报披露时间
    report_date_dic_dic = {}
    for report_date_g in [ifind_report_date_df.groupby(['ths_code', 'ths_perf_brief_actual_dd_hks']),
                          ifind_report_date_df.groupby(['ths_code', 'ths_perf_report_actual_dd_hks'])]:
        for num, ((ths_code, report_date), data_df) in enumerate(report_date_g, start=1):
            if is_nan_or_none(report_date):
                continue
            report_date_dic = report_date_dic_dic.setdefault(ths_code, {})
            ifind_fin_df_tmp = ifind_fin_df_g.get_group(ths_code)
            if report_date not in report_date_dic:
                ifind_fin_df_tmp = ifind_fin_df_tmp[ifind_fin_df_tmp['time'] >= report_date]
                if ifind_fin_df_tmp.shape[0] > 0:
                    report_date_dic[report_date] = ifind_fin_df_tmp.sort_values('time').iloc[0]

    # 设置 dtype
    dtype = {'report_date': Date}
    for dic in [DTYPE_STOCK_HK_DAILY_DS, DTYPE_STOCK_HK_REPORT_DATE, DTYPE_STOCK_HK_FIN, DTYPE_STOCK_HK_DAILY_HIS]:
        for key, val in dic.items():
            dtype[key] = val

    logging.debug("计算财报日期完成")
    # 整理 data_df 数据
    tot_data_count, data_count, data_df_list, for_count = 0, 0, [], len(report_date_dic_dic)
    try:
        for num, (ths_code, report_date_dic) in enumerate(report_date_dic_dic.items(), start=1):
            ifind_his_ds_df_cur_ths_code = ifind_his_ds_df_g.get_group(ths_code)
            logger.debug('%d/%d) 处理 %s %d 条数据', num, for_count, ths_code, ifind_his_ds_df_cur_ths_code.shape[0])
            report_date_list = list(report_date_dic.keys())
            report_date_list.sort()
            for report_date_from, report_date_to in generate_range(report_date_list):
                logger.debug('%d/%d) 处理 %s [%s - %s]',
                             num, for_count, ths_code, date_2_str(report_date_from), date_2_str(report_date_to))
                # 计算有效的日期范围
                if report_date_from is None:
                    is_fit = ifind_his_ds_df_cur_ths_code['time'] < report_date_to
                elif report_date_to is None:
                    is_fit = ifind_his_ds_df_cur_ths_code['time'] >= report_date_from
                else:
                    is_fit = (ifind_his_ds_df_cur_ths_code['time'] < report_date_to) & (
                            ifind_his_ds_df_cur_ths_code['time'] >= report_date_from)
                # 获取日期范围内的数据
                ifind_his_ds_df_segment = ifind_his_ds_df_cur_ths_code[is_fit].copy()
                segment_count = ifind_his_ds_df_segment.shape[0]
                if segment_count == 0:
                    continue
                fin_s = report_date_dic[report_date_from] if report_date_from is not None else None
                for key in DTYPE_STOCK_HK_FIN.keys():
                    if key in ('ths_code', 'time'):
                        continue
                    ifind_his_ds_df_segment[key] = fin_s[key] if fin_s is not None and key in fin_s else None
                ifind_his_ds_df_segment['report_date'] = report_date_from
                # 添加数据到列表
                data_df_list.append(ifind_his_ds_df_segment)
                data_count += segment_count

            if DEBUG and len(data_df_list) > 1:
                break

            # 保存数据库
            if data_count > 10000:
                # 保存到数据库
                data_df = pd.concat(data_df_list)
                data_count = bunch_insert_on_duplicate_update(data_df, table_name, engine_md, dtype)
                tot_data_count += data_count
                data_count, data_df_list = 0, []

    finally:
        # 保存到数据库
        if len(data_df_list) > 0:
            data_df = pd.concat(data_df_list)
            data_count = bunch_insert_on_duplicate_update(data_df, table_name, engine_md, dtype)
            tot_data_count += data_count

        logger.info('%s 新增或更新记录 %d 条', table_name, tot_data_count)
        if not has_table and engine_md.has_table(table_name):
            alter_table_2_myisam(engine_md, [table_name])
            build_primary_key([table_name])


if __name__ == "__main__":
    # DEBUG = True
    merge_ifind_stock_hk_daily()
