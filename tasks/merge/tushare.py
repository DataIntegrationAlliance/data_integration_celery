#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/9/13 9:37
@File    : tushare_suspend.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
import pandas as pd
import itertools
from collections import defaultdict
from sqlalchemy.types import String, Date, Integer, Text
from tasks import app
from tasks.backend import engine_md
from tasks.backend.orm import build_primary_key
from tasks.merge import iter_2_range
from tasks.utils.db_utils import with_db_session, bunch_insert_on_duplicate_update
from tasks.utils.fh_utils import is_any, is_nan_or_none, date_2_str
from tasks.tushare.tushare_stock_daily.adj_factor import DTYPE_TUSHARE_STOCK_DAILY_ADJ_FACTOR
from tasks.tushare.tushare_stock_daily.daily_basic import DTYPE_TUSHARE_STOCK_DAILY_BASIC
from tasks.tushare.tushare_stock_daily.stock import DTYPE_TUSHARE_STOCK_DAILY_MD
from tasks.tushare.tushare_fina_reports.balancesheet import DTYPE_TUSHARE_STOCK_BALABCESHEET
from tasks.tushare.tushare_fina_reports.income import DTYPE_TUSHARE_STOCK_INCOME
from tasks.tushare.tushare_fina_reports.cashflow import DTYPE_TUSHARE_CASHFLOW
from tasks.tushare.tushare_fina_reports.fina_indicator import DTYPE_STOCK_FINA_INDICATOR
import logging

logger = logging.getLogger()
DEBUG = False


def get_tushre_merge_stock_fin_df() -> (pd.DataFrame, dict):
    dtype = {key: val for key, val in itertools.chain(
        DTYPE_TUSHARE_STOCK_BALABCESHEET.items(),
        DTYPE_TUSHARE_STOCK_INCOME.items(),
        DTYPE_TUSHARE_CASHFLOW.items(),
        DTYPE_STOCK_FINA_INDICATOR.items(),
    )}
    # ('ts_code', 'f_ann_date', 'ann_date', 'end_date', 'report_type', 'comp_type', 'ebit', 'ebitda')
    duplicate_col_name_set = \
        (set(DTYPE_TUSHARE_STOCK_BALABCESHEET.keys()) & (set(DTYPE_TUSHARE_STOCK_INCOME.keys()))) | \
        (set(DTYPE_TUSHARE_CASHFLOW.keys()) & (set(DTYPE_TUSHARE_STOCK_INCOME.keys()))) | \
        (set(DTYPE_TUSHARE_STOCK_BALABCESHEET.keys()) & (set(DTYPE_TUSHARE_CASHFLOW.keys()))) | \
        (set(DTYPE_STOCK_FINA_INDICATOR.keys()) & (set(DTYPE_TUSHARE_STOCK_BALABCESHEET.keys()))) | \
        (set(DTYPE_STOCK_FINA_INDICATOR.keys()) & (set(DTYPE_TUSHARE_CASHFLOW.keys()))) | \
        (set(DTYPE_STOCK_FINA_INDICATOR.keys()) & (set(DTYPE_TUSHARE_STOCK_INCOME.keys())))
    gap_col_name_set = duplicate_col_name_set - {
        'ts_code', 'f_ann_date', 'ann_date', 'end_date', 'report_type', 'comp_type', 'ebit', 'ebitda'}
    if len(gap_col_name_set) > 0:
        logger.error("存在重复列 %s 将在查询列表中剔除", gap_col_name_set)

    col_names = [col_name for col_name in dtype if col_name not in duplicate_col_name_set]
    if len(col_names) == 0:
        col_names_str = ""
    else:
        col_names_str = ",\n  `" + "`, `".join(col_names) + "`"

    sql_str = """select 
            ifnull(income.ts_code, ifnull(balancesheet.ts_code, cashflow.ts_code)) ts_code, 
            ifnull(income.f_ann_date, ifnull(balancesheet.f_ann_date, cashflow.f_ann_date)) f_ann_date,
            ifnull(income.ann_date, ifnull(balancesheet.ann_date, cashflow.ann_date)) ann_date,
            ifnull(income.end_date, ifnull(balancesheet.end_date, cashflow.end_date)) end_date,
            income.report_type report_type_income, 
            cashflow.report_type report_type_cashflow, 
            balancesheet.report_type report_type_balancesheet, 
            ifnull(income.comp_type, ifnull(balancesheet.comp_type, cashflow.comp_type)) comp_type,
            ifnull(income.ebitda, indicator.ebitda) ebitda,
            ifnull(income.ebit, indicator.ebit) ebit
            {col_names}
            from tushare_stock_income income
            left outer join tushare_stock_balancesheet balancesheet
            on income.ts_code = balancesheet.ts_code
            and income.f_ann_date = balancesheet.f_ann_date
            left outer join tushare_stock_cashflow cashflow
            on income.ts_code = cashflow.ts_code
            and income.f_ann_date = cashflow.f_ann_date
            left outer join tushare_stock_fin_indicator indicator 
            on income.ts_code = indicator.ts_code
            and income.f_ann_date = indicator.ann_date
            union
            select  
            ifnull(income.ts_code, ifnull(balancesheet.ts_code, cashflow.ts_code)) ts_code, 
            ifnull(income.f_ann_date, ifnull(balancesheet.f_ann_date, cashflow.f_ann_date)) f_ann_date,
            ifnull(income.ann_date, ifnull(balancesheet.ann_date, cashflow.ann_date)) ann_date,
            ifnull(income.end_date, ifnull(balancesheet.end_date, cashflow.end_date)) end_date,
            income.report_type report_type_income, 
            cashflow.report_type report_type_cashflow, 
            balancesheet.report_type report_type_balancesheet, 
            ifnull(income.comp_type, ifnull(balancesheet.comp_type, cashflow.comp_type)) comp_type,
            ifnull(income.ebitda, indicator.ebitda) ebitda,
            ifnull(income.ebit, indicator.ebit) ebit
            {col_names}
            from tushare_stock_balancesheet balancesheet
            left outer join tushare_stock_income income
            on income.ts_code = balancesheet.ts_code
            and income.f_ann_date = balancesheet.f_ann_date
            left outer join tushare_stock_cashflow cashflow
            on balancesheet.ts_code = cashflow.ts_code
            and balancesheet.f_ann_date = cashflow.f_ann_date
            left outer join tushare_stock_fin_indicator indicator 
            on balancesheet.ts_code = indicator.ts_code
            and balancesheet.f_ann_date = indicator.ann_date
            union
            select  
            ifnull(income.ts_code, ifnull(balancesheet.ts_code, cashflow.ts_code)) ts_code, 
            ifnull(income.f_ann_date, ifnull(balancesheet.f_ann_date, cashflow.f_ann_date)) f_ann_date,
            ifnull(income.ann_date, ifnull(balancesheet.ann_date, cashflow.ann_date)) ann_date,
            ifnull(income.end_date, ifnull(balancesheet.end_date, cashflow.end_date)) end_date,
            income.report_type report_type_income, 
            cashflow.report_type report_type_cashflow, 
            balancesheet.report_type report_type_balancesheet, 
            ifnull(income.comp_type, ifnull(balancesheet.comp_type, cashflow.comp_type)) comp_type,
            ifnull(income.ebitda, indicator.ebitda) ebitda,
            ifnull(income.ebit, indicator.ebit) ebit
            {col_names}
            from tushare_stock_cashflow cashflow
            left outer join tushare_stock_balancesheet balancesheet
            on cashflow.ts_code = balancesheet.ts_code
            and cashflow.f_ann_date = balancesheet.f_ann_date
            left outer join tushare_stock_income income
            on cashflow.ts_code = income.ts_code
            and cashflow.f_ann_date = income.f_ann_date
            left outer join tushare_stock_fin_indicator indicator 
            on cashflow.ts_code = indicator.ts_code
            and cashflow.f_ann_date = indicator.ann_date""".format(col_names=col_names_str)
    data_df = pd.read_sql(sql_str, engine_md)  # , index_col='ts_code'

    return data_df, dtype


def get_tushare_daily_merged_df(ths_code_set: set = None, date_from=None) -> (pd.DataFrame, dict):
    """获取tushre合并后的日级别数据"""
    dtype = {key: val for key, val in itertools.chain(
        DTYPE_TUSHARE_STOCK_DAILY_BASIC.items(),
        DTYPE_TUSHARE_STOCK_DAILY_MD.items(),
        DTYPE_TUSHARE_STOCK_DAILY_ADJ_FACTOR.items())}
    col_names = [col_name for col_name in dtype if col_name not in ('ts_code', 'trade_date', 'close')]
    if len(col_names) == 0:
        col_names_str = ""
    else:
        col_names_str = ",\n  `" + "`, `".join(col_names) + "`"

    if date_from is None:
        sql_str = """SELECT md.ts_code, md.trade_date, ifnull(md.close, basic.close) close {col_names}
            FROM
            (
                SELECT * FROM tushare_stock_daily_md
            ) md
            LEFT OUTER JOIN
            (
                SELECT * FROM tushare_stock_daily_basic 
            ) basic
            ON md.ts_code = basic.ts_code
            AND md.trade_date = basic.trade_date
            LEFT OUTER JOIN
            (
                SELECT * FROM tushare_stock_daily_adj_factor
            ) adj_factor
            ON md.ts_code = adj_factor.ts_code
            AND md.trade_date = adj_factor.trade_date""".format(col_names=col_names_str)
        data_df = pd.read_sql(sql_str, engine_md)
    else:
        sql_str = """SELECT md.ts_code, md.trade_date, ifnull(md.close, basic.close) close {col_names}
            FROM
            (
                SELECT * FROM tushare_stock_daily_md WHERE trade_date >= %s
            ) md
            LEFT OUTER JOIN
            (
                SELECT * FROM tushare_stock_daily_basic WHERE trade_date >= %s
            ) basic
            ON md.ts_code = basic.ts_code
            AND md.trade_date = basic.trade_date
            LEFT OUTER JOIN
            (
                SELECT * FROM tushare_stock_daily_adj_factor WHERE trade_date >= %s
            ) adj_factor
            ON md.ts_code = adj_factor.ts_code
            AND md.trade_date = adj_factor.trade_date""".format(col_names=col_names_str)
        data_df = pd.read_sql(sql_str, engine_md, params=[date_from, date_from, date_from])
        if ths_code_set is not None:
            data_df = data_df[data_df['ts_code'].apply(lambda x: x in ths_code_set)]

        # 增加停牌标志位
        data_df, dtype = concat_suspend(data_df, dtype)
    return data_df, dtype


def get_suspend_to_dic():
    """将 tushare_stock_daily_suspend 转换成日期范围字典，key：股票代码；value：日期范围"""
    with with_db_session(engine_md) as session:
        sql_str = """SELECT ts_code, suspend_date, resume_date FROM tushare_stock_daily_suspend"""
        table = session.execute(sql_str)
        code_date_range_dic = defaultdict(list)
        for ts_code, suspend_date, resume_date in table.fetchall():
            if suspend_date is None:
                continue
            code_date_range_dic[ts_code].append(
                (suspend_date, suspend_date if resume_date is None else resume_date))

    return code_date_range_dic


def is_suspend(code_date_range_dic, code_trade_date_s):
    """判断某 ts_code 的 trade_date 是否为停牌日"""
    ts_code = code_trade_date_s['ts_code']
    date_cur = code_trade_date_s['trade_date']
    data_range_list = code_date_range_dic[ts_code]
    # print(code_trade_date_s)
    return 1 if is_any(data_range_list, lambda date_range: date_range[0] <= date_cur <= date_range[1]) else 0


def concat_suspend(data_df, dtype_daily: dict):
    """将停牌日信息 suspend 扩展到 日级别数据 df 中"""
    code_date_range_dic = get_suspend_to_dic()
    data_df['suspend'] = data_df[['ts_code', 'trade_date']].apply(
        lambda x: is_suspend(code_date_range_dic, x), axis=1)
    dtype = dtype_daily.copy()
    dtype['suspend'] = Integer
    return data_df, dtype


@app.task
def merge_tushare_stock_daily(ths_code_set: set = None, date_from=None):
    """A股行情数据、财务信息 合并成为到 日级别数据"""
    table_name = 'tushare_stock_daily'
    logging.info("合成 %s 开始", table_name)
    has_table = engine_md.has_table(table_name)
    if date_from is None and has_table:
        sql_str = "select adddate(max(`trade_date`),1) from {table_name}".format(table_name=table_name)
        with with_db_session(engine_md) as session:
            date_from = date_2_str(session.execute(sql_str).scalar())

    # 获取日级别数据
    # TODO: 增加 ths_code_set 参数
    daily_df, dtype_daily = get_tushare_daily_merged_df(ths_code_set, date_from)

    daily_df_g = daily_df.groupby('ts_code')
    ths_code_set_4_daily = set(daily_df_g.size().index)

    # 获取合并后的财务数据
    ifind_fin_df, dtype_fin = get_tushre_merge_stock_fin_df()

    # 整理 dtype
    dtype = dtype_daily.copy()
    dtype.update(dtype_fin)
    logging.debug("提取财务数据完成")
    # 计算 财报披露时间
    report_date_dic_dic = {}
    for num, ((ths_code, report_date), data_df) in enumerate(
            ifind_fin_df.groupby(['ts_code', 'f_ann_date']), start=1):
        if ths_code_set is not None and ths_code not in ths_code_set:
            continue
        if is_nan_or_none(report_date):
            continue
        report_date_dic = report_date_dic_dic.setdefault(ths_code, {})
        if report_date not in report_date_dic_dic:
            if data_df.shape[0] > 0:
                report_date_dic[report_date] = data_df.iloc[0]

    logger.debug("计算财报日期完成")
    # 整理 data_df 数据
    tot_data_count, data_count, data_df_list, for_count = 0, 0, [], len(report_date_dic_dic)
    try:
        for num, (ths_code, report_date_dic) in enumerate(report_date_dic_dic.items(), start=1):  # key:ths_code
            # TODO: 檢查判斷 ths_code 是否存在在ifind_fin_df_g 裏面,,size暫時使用  以後在驚醒改進
            if ths_code not in ths_code_set_4_daily:
                logger.error('fin 表中不存在 %s 的財務數據', ths_code)
                continue

            daily_df_cur_ts_code = daily_df_g.get_group(ths_code)
            logger.debug('%d/%d) 处理 %s %d 条数据', num, for_count, ths_code, daily_df_cur_ts_code.shape[0])
            report_date_list = list(report_date_dic.keys())
            report_date_list.sort()
            report_date_list_len = len(report_date_list)
            for num_sub, (report_date_from, report_date_to) in enumerate(iter_2_range(report_date_list)):
                logger.debug('%d/%d) %d/%d) 处理 %s [%s - %s]',
                             num, for_count, num_sub, report_date_list_len,
                             ths_code, date_2_str(report_date_from), date_2_str(report_date_to))
                # 计算有效的日期范围
                if report_date_from is None:
                    is_fit = daily_df_cur_ts_code['trade_date'] < report_date_to
                elif report_date_to is None:
                    is_fit = daily_df_cur_ts_code['trade_date'] >= report_date_from
                else:
                    is_fit = (daily_df_cur_ts_code['trade_date'] < report_date_to) & (
                            daily_df_cur_ts_code['trade_date'] >= report_date_from)
                # 获取日期范围内的数据
                ifind_his_ds_df_segment = daily_df_cur_ts_code[is_fit].copy()
                segment_count = ifind_his_ds_df_segment.shape[0]
                if segment_count == 0:
                    continue
                fin_s = report_date_dic[report_date_from] if report_date_from is not None else None
                for key in dtype_fin.keys():
                    if key in ('ts_code', 'trade_date'):
                        continue
                    ifind_his_ds_df_segment[key] = fin_s[key] if fin_s is not None and key in fin_s else None

                ifind_his_ds_df_segment['report_date'] = report_date_from
                # 添加数据到列表
                data_df_list.append(ifind_his_ds_df_segment)
                data_count += segment_count

            if DEBUG and len(data_df_list) > 1:
                break

            # 保存数据库
            if data_count > 1000:
                # 保存到数据库
                data_df = pd.concat(data_df_list)
                data_count = bunch_insert_on_duplicate_update(
                    data_df, table_name, engine_md, dtype, myisam_if_create_table=True)
                tot_data_count += data_count
                data_count, data_df_list = 0, []

    finally:
        # 保存到数据库
        if len(data_df_list) > 0:
            data_df = pd.concat(data_df_list)
            data_count = bunch_insert_on_duplicate_update(
                data_df, table_name, engine_md, dtype, myisam_if_create_table=True)
            tot_data_count += data_count

        logger.info('%s 新增或更新记录 %d 条', table_name, tot_data_count)
        if not has_table and engine_md.has_table(table_name):
            build_primary_key([table_name])


if __name__ == "__main__":
    # DEBUG = True
    # data_df, dtype_daily = get_tushare_daily_merged_df()
    # data_df, dtype_daily = concat_suspend(data_df, dtype_daily)
    merge_tushare_stock_daily(ths_code_set=None, date_from=None)
