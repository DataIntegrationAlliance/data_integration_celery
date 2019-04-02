#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 19-3-25 下午3:45
@File    : income_2_daily.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from tasks.jqdata.finance_report import fill_season_data
from tasks.jqdata.finance_report.income import DTYPE_INCOME, TABLE_NAME as TABLE_NAME_FIN_REPORT
from tasks.jqdata.trade_date import TABLE_NAME as TABLE_NAME_TRADE_DATE
from tasks.backend import engine_md, bunch_insert
from tasks import app
import pandas as pd
import logging
import datetime
from tasks.utils.db_utils import with_db_session
from tasks.utils.fh_utils import get_first_idx, get_last_idx

logger = logging.getLogger(__name__)
TABLE_NAME = f"{TABLE_NAME_FIN_REPORT}_daily"
DTYPE_INCOME_DAILY = DTYPE_INCOME.copy()
# 通过 tasks/jqdata/finance_report/__init__.py get_accumulation_col_names_4_income 函数计算获得
ACCUMULATION_COL_NAME_LIST = [
    'total_operating_revenue', 'operating_revenue', 'total_operating_cost', 'operating_tax_surcharges',
    'administration_expense', 'asset_impairment_loss', 'investment_income', 'invest_income_associates',
    'exchange_income', 'other_items_influenced_income', 'operating_profit', 'non_operating_revenue',
    'non_operating_expense', 'total_profit', 'income_tax', 'net_profit', 'np_parent_company_owners',
    'basic_eps', 'diluted_eps']

for key in ('company_name', 'company_id', 'a_code', 'b_code', 'h_code', 'source', 'id'):
    del DTYPE_INCOME_DAILY[key]
# def get_ordered_date():
#     """
#     获取排序后的，股票财报ID、pub_date、report_date列表
#     :return:
#     """
#     table_name_daily = f"{TABLE_NAME}_daily"
#     sql_str = f"""select code, pub_date, max(report_date) from {TABLE_NAME}
#     where report_type=0 group by code, pub_date order by code, pub_date"""
#     code_date_dic = defaultdict(list)
#     with with_db_session(engine_md) as session:
#         table = session.execute(sql_str)
#         for code, pub_date, report_date in table.fetchall():
#             code_date_dic[code].append((pub_date, report_date))
#
#     return code_date_dic


@app.task
def save_2_daily():
    """
    将财务数据（季度）保存成日级别数据
    :return:
    """
    if not engine_md.has_table(TABLE_NAME_FIN_REPORT):
        logger.info('%s 不存在，无需转化成日级别数据', TABLE_NAME_FIN_REPORT)
    today = datetime.date.today()

    has_table = engine_md.has_table(TABLE_NAME)
    # 获取每只股票最新的交易日，以及截至当期日期的全部交易日数据
    with with_db_session(engine_md) as session:
        sql_str = f"""select trade_date from {TABLE_NAME_TRADE_DATE} where trade_date<=:today order by trade_date"""
        table = session.execute(sql_str, params={"today": today})
        trade_date_list = list(table.fetchall())
        if has_table:
            sql_str = f"""select code, max(trade_date) from {TABLE_NAME} group by code"""
            table = session.execute(sql_str)
            code_date_latest_dic = dict(table.fetchall())
        else:
            code_date_latest_dic = {}

    # 获取季度、半年、年报财务数据
    col_name_list = list(DTYPE_INCOME_DAILY.keys())
    col_name_list_str = ','.join([f'income.`{col_name}` {col_name}' for col_name in col_name_list])
    sql_str = f"""SELECT {col_name_list_str} FROM {TABLE_NAME_FIN_REPORT} income inner join 
        (
            select code, pub_date, max(report_date) report_date 
            from {TABLE_NAME_FIN_REPORT} where report_type=0 group by code, pub_date
        ) base_date
        where income.report_type=0
        and income.code = base_date.code
        and income.pub_date = base_date.pub_date
        and income.report_date = base_date.report_date
        order by code, pub_date"""
    dfg_by_code = pd.read_sql(sql_str, engine_md).set_index('report_date', drop=False).groupby('code')
    dfg_len = len(dfg_by_code)
    data_new_s_list, accumulation_col_name_list = [], None
    # 按股票代码分组，分别按日进行处理
    for num, (code, df_by_code) in enumerate(dfg_by_code, start=1):
        df_by_code.sort_index(inplace=True)

        df_len = df_by_code.shape[0]
        df_by_code[['pub_date_next', 'report_date_next']] = df_by_code[['pub_date', 'report_date']].shift(-1)
        # 将相关周期累加增长字段转化为季度增长字段
        for col_name in ACCUMULATION_COL_NAME_LIST:
            # df_by_code = fill_season_data(df_by_code, 'total_operating_revenue')
            df_by_code, col_name_season = fill_season_data(df_by_code, col_name)
            # 更新 DTYPE_INCOME_DAILY
            if col_name_season not in DTYPE_INCOME_DAILY:
                DTYPE_INCOME_DAILY[col_name_season] = DTYPE_INCOME_DAILY[col_name]

        # df_by_code['code'] = code
        trade_date_latest = code_date_latest_dic[code] if code in code_date_latest_dic else None
        for num_sub, data_s in enumerate(df_by_code.T.items(), start=1):
            pub_date = data_s['pub_date']
            report_date = data_s['report_date']
            pub_date_next = data_s['pub_date_next']
            # 检查 最新交易日是否以及大于下一条财报日期，如果是则跳过当前数据
            if trade_date_latest is not None and pub_date_next is not None and trade_date_latest > pub_date_next:
                continue
            if trade_date_latest is None:
                date_from_idx = get_first_idx(trade_date_list, lambda x: x >= pub_date)
            else:
                date_from_idx = get_first_idx(trade_date_list, lambda x: x > trade_date_latest)
            if pub_date_next is None:
                date_to_idx = get_last_idx(trade_date_list, lambda x: x <= today)
            else:
                date_to_idx = get_last_idx(trade_date_list, lambda x: x < pub_date_next)

            logger.debug('%d/%d) %d/%d) %s [%s, %s) 预计转化 %d 条日级别数据，报告日：%s，',
                         num, dfg_len, num, df_len, code, trade_date_list[date_from_idx], trade_date_list[date_to_idx],
                         date_to_idx - date_from_idx + 1, report_date)
            for trade_date in trade_date_list[date_from_idx:(date_to_idx + 1)]:
                data_new_s = data_s.copy()
                data_new_s['trade_date'] = trade_date
                data_new_s_list.append(data_new_s)

        if len(data_new_s_list) > 0:
            data_count = save_data(data_new_s_list)
            logger.info("%d/%d) %s %d 条记录被保存", num, dfg_len, code, data_count)
            data_new_s_list = []


def save_data(data_new_s_list: list):
    df = pd.DataFrame(data_new_s_list)
    data_count = bunch_insert(df, TABLE_NAME, dtype=DTYPE_INCOME, primary_keys=['id'])
    return data_count


if __name__ == "__main__":
    save_2_daily()
