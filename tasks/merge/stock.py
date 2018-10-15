#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/8/16 10:13
@File    : stock.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
import pandas as pd
import logging
from tasks import app
from direstinvoker.utils.fh_utils import date_2_str
from tasks.backend.orm import build_primary_key
from tasks.utils.db_utils import bunch_insert_on_duplicate_update, alter_table_2_myisam
from tasks.backend import engine_md
from tasks.utils.db_utils import with_db_session
from tasks.merge import mean_value, prefer_left, prefer_right, merge_data, get_value
from sqlalchemy.types import String, Date, Integer, Text
from sqlalchemy.dialects.mysql import DOUBLE
from tasks.utils.fh_utils import is_nan_or_none
from tasks.ifind.stock import DTYPE_STOCK_DAILY_DS, DTYPE_STOCK_DAILY_HIS, DTYPE_STOCK_DAILY_FIN, \
    DTYPE_STOCK_REPORT_DATE
from tasks.merge import get_ifind_daily_df, get_wind_daily_df, generate_range

logger = logging.getLogger()
DEBUG = False


@app.task
def merge_stock_info():
    """
    合并 wind，ifind 数据到对应名称的表中
    :return:
    """
    table_name = 'stock_info'
    logging.info("更新 %s 开始", table_name)
    has_table = engine_md.has_table(table_name)
    ifind_table_name = 'ifind_{table_name}'.format(table_name=table_name)
    wind_table_name = 'wind_{table_name}'.format(table_name=table_name)
    # ifind_model = TABLE_MODEL_DIC[ifind_table_name]
    # wind_model = TABLE_MODEL_DIC[wind_table_name]
    # with with_db_session(engine_md) as session:
    #     session.query(ifind_model, wind_model).filter(ifind_model.c.ths_code == wind_model.c.wind_code)
    ifind_sql_str = "select * from {table_name}".format(table_name=ifind_table_name)
    wind_sql_str = "select * from {table_name}".format(table_name=wind_table_name)
    ifind_df = pd.read_sql(ifind_sql_str, engine_md)  # , index_col='ths_code'
    wind_df = pd.read_sql(wind_sql_str, engine_md)  # , index_col='wind_code'
    joined_df = pd.merge(ifind_df, wind_df, how='outer',
                         left_on='ths_code', right_on='wind_code', indicator='indicator_column')
    col_merge_dic = {
        'unique_code': (String(20), prefer_left, {'left_key': 'ths_code', 'right_key': 'wind_code'}),
        'sec_name': (String(20), prefer_left, {'left_key': 'ths_stock_short_name_stock', 'right_key': 'sec_name'}),
        'cn_name': (String(100), get_value, {'key': 'ths_corp_cn_name_stock'}),
        'en_name': (String(100), get_value, {'key': 'ths_corp_name_en_stock'}),
        'delist_date': (Date, prefer_left, {'left_key': 'ths_delist_date_stock', 'right_key': 'delist_date'}),
        'ipo_date': (Date, prefer_left, {'left_key': 'ths_ipo_date_stock', 'right_key': 'ipo_date'}),
        'pre_name': (Text, prefer_left, {'left_key': 'ths_corp_name_en_stock', 'right_key': 'prename'}),
        'established_date': (Date, get_value, {'key': 'ths_established_date_stock'}),
        'exch_city': (String(20), get_value, {'key': 'exch_city'}),
        'exch_cn': (String(20), get_value, {'key': 'ths_listing_exchange_stock'}),
        'exch_eng': (String(20), get_value, {'key': 'exch_eng'}),
        'stock_code': (String(20), prefer_left, {'left_key': 'ths_stock_code_stock', 'right_key': 'trade_code'}),
        'mkt': (String(20), get_value, {'key': 'mkt'}),
    }

    col_merge_rule_dic = {
        key: (val[1], val[2]) for key, val in col_merge_dic.items()
    }
    dtype = {
        key: val[0] for key, val in col_merge_dic.items()
    }
    data_df = merge_data(joined_df, col_merge_rule_dic)
    data_count = bunch_insert_on_duplicate_update(data_df, table_name, engine_md, dtype)
    logger.info('%s 新增或更新记录 %d 条', table_name, data_count)
    if not has_table and engine_md.has_table(table_name):
        alter_table_2_myisam(engine_md, [table_name])
        build_primary_key([table_name])

    return data_df


@app.task
def merge_stock_daily(date_from=None):
    """
    合并 wind，ifind 数据到对应名称的表中
    :param date_from:
    :return:
    """
    table_name = 'stock_daily'
    logging.info("更新 %s 开始", table_name)
    has_table = engine_md.has_table(table_name)
    if date_from is None and has_table:
        sql_str = "select adddate(max(trade_date),1) from {table_name}".format(table_name=table_name)
        with with_db_session(engine_md) as session:
            date_from = date_2_str(session.execute(sql_str).scalar())
    ifind_table_ds_name = 'ifind_{table_name}_ds'.format(table_name=table_name)
    ifind_table_his_name = 'ifind_{table_name}_his'.format(table_name=table_name)
    wind_table_name = 'wind_{table_name}'.format(table_name=table_name)
    if date_from is None:
        ifind_his_sql_str = "select * from {table_name}".format(table_name=ifind_table_ds_name)
        ifind_ds_sql_str = "select * from {table_name}".format(table_name=ifind_table_his_name)
        wind_sql_str = "select * from {table_name}".format(table_name=wind_table_name)
        ifind_his_df = pd.read_sql(ifind_his_sql_str, engine_md)  # , index_col='ths_code'
        ifind_ds_df = pd.read_sql(ifind_ds_sql_str, engine_md)  # , index_col='ths_code'
        wind_df = pd.read_sql(wind_sql_str, engine_md)  # , index_col='wind_code'
    else:
        ifind_his_sql_str = "select * from {table_name} where time >= %s".format(table_name=ifind_table_ds_name)
        ifind_ds_sql_str = "select * from {table_name} where time >= %s".format(table_name=ifind_table_his_name)
        wind_sql_str = "select * from {table_name} where trade_date >= %s".format(table_name=wind_table_name)
        ifind_his_df = pd.read_sql(ifind_his_sql_str, engine_md, params=[date_from])  # , index_col='ths_code'
        ifind_ds_df = pd.read_sql(ifind_ds_sql_str, engine_md, params=[date_from])  # , index_col='ths_code'
        wind_df = pd.read_sql(wind_sql_str, engine_md, params=[date_from])  # , index_col='wind_code'

    ifind_df = pd.merge(ifind_his_df, ifind_ds_df, how='outer',
                        on=['ths_code', 'time'])
    joined_df = pd.merge(ifind_df, wind_df, how='outer',
                         left_on=['ths_code', 'time'], right_on=['wind_code', 'trade_date'],
                         indicator='indicator_column')
    col_merge_dic = {
        'unique_code': (String(20), prefer_left, {'left_key': 'ths_code', 'right_key': 'wind_code'}),
        'trade_date': (Date, prefer_left, {'left_key': 'time', 'right_key': 'trade_date'}),
        'open': (DOUBLE, mean_value, {
            'left_key': 'open_x', 'right_key': 'open_y',
            'warning_accuracy': 0.01, 'primary_keys': ('ths_code', 'time')}),
        'high': (DOUBLE, mean_value, {
            'left_key': 'high_x', 'right_key': 'high_y',
            'warning_accuracy': 0.01, 'primary_keys': ('ths_code', 'time')}),
        'low': (DOUBLE, mean_value, {
            'left_key': 'low_x', 'right_key': 'low_y',
            'warning_accuracy': 0.01, 'primary_keys': ('ths_code', 'time')}),
        # TODO: 原因不详，wind接口取到的部分 close 数据不准确
        'close': (DOUBLE, prefer_left, {
            'left_key': 'close_x', 'right_key': 'close_y',
            'warning_accuracy': 0.01, 'primary_keys': ('ths_code', 'time')}),
        'volume': (DOUBLE, mean_value, {
            'left_key': 'volume_x', 'right_key': 'volume_y',
            'warning_accuracy': 1, 'primary_keys': ('ths_code', 'time')}),
        'amount': (DOUBLE, mean_value, {
            'left_key': 'amount', 'right_key': 'amt',
            'warning_accuracy': 1, 'primary_keys': ('ths_code', 'time')}),
        # 总股本字段：同花顺的 totalShares 字段以变动日期为准，wind total_shares 以公告日为准
        # 因此出现冲突时应该以 wind 为准
        'total_shares': (DOUBLE, prefer_right, {
            'left_key': 'totalShares', 'right_key': 'total_shares'}),
        # 'susp_days': (Integer, '***', {
        #     'left_key': 'ths_up_and_down_status_stock', 'right_key': 'susp_days', 'other_key': 'trade_status',
        #  'primary_keys': ('ths_code', 'time')}),
        'max_up_or_down': (Integer, max_up_or_down, {
            'ths_key': 'ths_up_and_down_status_stock', 'wind_key': 'maxupordown',
            'primary_keys': ('ths_code', 'time')}),
        'total_capital': (DOUBLE, get_value, {'key': 'totalCapital'}),
        'float_capital': (DOUBLE, get_value, {'key': 'floatCapitalOfAShares'}),
        'pct_chg': (DOUBLE, mean_value, {
            'left_key': 'changeRatio', 'right_key': 'pct_chg',
            'warning_accuracy': 0.01, 'primary_keys': ('ths_code', 'time')}),
        'float_a_shares': (DOUBLE, get_value, {'key': 'floatSharesOfAShares'}),  # 对应wind float_a_shares
        'free_float_shares': (DOUBLE, get_value, {'key': 'free_float_shares'}),  # 对应 ths ths_free_float_shares_stock
        # PE_TTM 对应 ths ths_pe_ttm_stock 以财务报告期为基准日，对应 wind pe_ttm 以报告期为准
        # 因此应该如有不同应该以 wind 为准
        'pe_ttm': (DOUBLE, prefer_right, {
            'left_key': 'ths_pe_ttm_stock', 'right_key': 'pe_ttm',
            'warning_accuracy': 0.01, 'primary_keys': ('ths_code', 'time')}),
        'pe': (DOUBLE, get_value, {'key': 'pe'}),
        'pb': (DOUBLE, get_value, {'key': 'pb'}),
        'ps': (DOUBLE, get_value, {'key': 'ps'}),
        'pcf': (DOUBLE, get_value, {'key': 'pcf'}),
    }

    col_merge_rule_dic = {
        key: (val[1], val[2]) for key, val in col_merge_dic.items()
    }
    dtype = {
        key: val[0] for key, val in col_merge_dic.items()
    }
    data_df = merge_data(joined_df, col_merge_rule_dic)
    data_count = bunch_insert_on_duplicate_update(data_df, table_name, engine_md, dtype)
    logger.info('%s 新增或更新记录 %d 条', table_name, data_count)
    if not has_table and engine_md.has_table(table_name):
        alter_table_2_myisam(engine_md, [table_name])
        build_primary_key([table_name])

    return data_df


def max_up_or_down(data_s: pd.Series, ths_key, wind_key, primary_keys=None, **kwargs):
    ths_val = data_s[ths_key]
    if ths_val == '跌停':
        ret_code_ths = -1
    elif ths_val == '涨停':
        ret_code_ths = 1
    elif ths_val == ('非涨跌停', '停牌'):
        ret_code_ths = 0
    else:
        ret_code_ths = None

    wind_val = data_s[wind_key]
    if wind_val in (1, -1, 0):
        ret_code_wind = wind_val
    else:
        ret_code_wind = None

    if ret_code_ths is None and ret_code_wind is None:
        pk_str = ','.join([str(data_s[key]) for key in primary_keys]) if primary_keys is not None else None
        if pk_str is None:
            msg = '%s = %s; %s = %s; 状态不明' % (ths_key, str(ths_val), wind_key, str(wind_val))
        else:
            msg = '[%s] %s = %s; %s = %s; 状态不明' % (pk_str, ths_key, str(ths_val), wind_key, str(wind_val))
        # logger.debug(msg)
        ret_code = -2
    elif ret_code_ths is None:
        ret_code = ret_code_wind
    elif ret_code_wind is None:
        ret_code = ret_code_ths
    elif ret_code_ths == ret_code_wind:
        ret_code = ret_code_wind
    else:
        pk_str = ','.join([str(data_s[key]) for key in primary_keys]) if primary_keys is not None else None
        if pk_str is None:
            msg = '%s = %s; %s = %s; 状态冲突' % (ths_key, str(ths_val), wind_key, str(wind_val))
        else:
            msg = '[%s] %s = %s; %s = %s; 状态冲突' % (pk_str, ths_key, str(ths_val), wind_key, str(wind_val))
        logger.warning(msg)
        ret_code = -3

    return ret_code


def get_ifind_daily_df(table_name, date_from) -> pd.DataFrame:
    if date_from is None:
        sql_str = "select * from {table_name}".format(table_name=table_name)
        data_df = pd.read_sql(sql_str, engine_md)  # , index_col='ths_code'
    else:
        sql_str = "select * from {table_name} where time >= %s".format(table_name=table_name)
        data_df = pd.read_sql(sql_str, engine_md, params=[date_from])  # , index_col='ths_code'
    return data_df


def get_ifind_report_date_df(table_name, date_from) -> pd.DataFrame:
    table_name = 'ifind_stock_report_date'
    if date_from is None:
        sql_str = """select distinct ths_code,ths_regular_report_actual_dd_stock 
            from {table_name} where ths_regular_report_actual_dd_stock is not null""".format(
            table_name=table_name)
        data_df = pd.read_sql(sql_str, engine_md)  # , index_col='ths_code'
    else:
        sql_str = """select distinct ths_code, ths_regular_report_actual_dd_stock
         from {table_name} where ths_regular_report_actual_dd_stock is not null""".format(
            table_name=table_name)
        data_df = pd.read_sql(sql_str, engine_md, params=[date_from])  # , index_col='ths_code'
    return data_df


def merge_ifind_stock_daily(ths_code_set: set = None, date_from=None):
    """将ds his 以及财务数据合并为 daily 数据"""
    table_name = 'ifind_stock_daily'
    logging.info("合成 %s 开始", table_name)
    has_table = engine_md.has_table(table_name)
    if date_from is None and has_table:
        sql_str = "select adddate(max(`time`),1) from {table_name}".format(table_name=table_name)
        with with_db_session(engine_md) as session:
            date_from = date_2_str(session.execute(sql_str).scalar())
    # 獲取各個表格數據
    ifind_his_df = get_ifind_daily_df('ifind_stock_daily_his', date_from)
    ifind_ds_df = get_ifind_daily_df('ifind_stock_daily_ds', date_from)
    ifind_report_date_df = get_ifind_report_date_df('ifind_stock_report_date', None)
    ifind_fin_df = get_ifind_daily_df('ifind_stock_fin', None)
    ifind_fin_df_g = ifind_fin_df.groupby('ths_code')
    ths_code_set_4_daily = set(ifind_fin_df_g.size().index)
    # 合并 ds his 数据
    ifind_his_ds_df = pd.merge(ifind_his_df, ifind_ds_df, how='outer',
                               on=['ths_code', 'time'])  # 拼接後續有nan,無數據
    ifind_his_ds_df_g = ifind_his_ds_df.groupby('ths_code')
    logger.debug("提取数据完成")
    # 计算 财报披露时间
    report_date_dic_dic = {}
    for report_date_g in [ifind_report_date_df.groupby(['ths_code', 'ths_regular_report_actual_dd_stock'])]:
        for num, ((ths_code, report_date), data_df) in enumerate(report_date_g, start=1):
            if ths_code_set is not None and ths_code not in ths_code_set:
                continue
            if is_nan_or_none(report_date):
                continue
            report_date_dic = report_date_dic_dic.setdefault(ths_code, {})
            if ths_code not in ths_code_set_4_daily:
                logger.error('fin 表中不存在 %s 的財務數據', ths_code)
                continue
            ifind_fin_df_temp = ifind_fin_df_g.get_group(ths_code)
            if report_date not in report_date_dic_dic:
                ifind_fin_df_temp = ifind_fin_df_temp[ifind_fin_df_temp['time'] <= report_date]
                if ifind_fin_df_temp.shape[0] > 0:
                    report_date_dic[report_date] = ifind_fin_df_temp.sort_values('time').iloc[0]

    # # 设置 dtype
    dtype = {'report_date': Date}
    for dic in [DTYPE_STOCK_DAILY_DS, DTYPE_STOCK_REPORT_DATE, DTYPE_STOCK_DAILY_FIN, DTYPE_STOCK_DAILY_HIS]:
        for key, val in dic.items():
            dtype[key] = val

    logger.debug("计算财报日期完成")
    # 整理 data_df 数据
    tot_data_count, data_count, data_df_list, for_count = 0, 0, [], len(report_date_dic_dic)
    try:
        for num, (ths_code, report_date_dic) in enumerate(report_date_dic_dic.items(), start=1):  # key:ths_code
            # TODO: 檢查判斷 ths_code 是否存在在ifind_fin_df_g 裏面,,size暫時使用  以後在驚醒改進
            if ths_code not in ifind_his_ds_df_g.size():
                logger.error('fin 表中不存在 %s 的財務數據', ths_code)
                continue
            # open low  等 is NAN 2438
            ifind_his_ds_df_cur_ths_code = ifind_his_ds_df_g.get_group(ths_code)  # shape[1] 30
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
                for key in DTYPE_STOCK_DAILY_FIN.keys():
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
    # data_df = merge_stock_info()
    # print(data_df)
    ths_code_set = {'600618.SH'}
    merge_ifind_stock_daily(ths_code_set)
