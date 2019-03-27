#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 19-3-25 下午3:45
@File    : income_2_daily.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from tasks.jqdata.finance_report.income import DTYPE_INCOME, TABLE_NAME as TABLE_NAME_FIN_REPORT
from tasks.jqdata.trade_date import TABLE_NAME as TABLE_NAME_TRADE_DATE
from tasks.backend import engine_md, bunch_insert
from tasks import app
import pandas as pd
import numpy as np
import logging
import datetime
from tasks.utils.db_utils import with_db_session
from tasks.utils.fh_utils import get_first_idx, get_last_idx, str_2_date

logger = logging.getLogger(__name__)
TABLE_NAME = f"{TABLE_NAME_FIN_REPORT}_daily"
DTYPE_INCOME_DAILY = DTYPE_INCOME.copy()
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
    data_new_s_list = []
    # 按股票代码分组，分别按日进行处理
    for num, (code, df_by_code) in enumerate(dfg_by_code, start=1):
        df_len = df_by_code.shape[0]
        df_by_code[['pub_date_next', 'report_date_next']] = df_by_code[['pub_date', 'report_date']].shift(-1)
        df_by_code = fill_season_data(df_by_code, 'total_operating_revenue')
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
            data_count = save_2_daily(data_new_s_list)
            logger.info("%d/%d) %s %d 条记录被保存", num, dfg_len, code, data_count)
            data_new_s_list = []


def save_data(data_new_s_list: list):
    df = pd.DataFrame(data_new_s_list)
    data_count = bunch_insert(df, TABLE_NAME, dtype=DTYPE_INCOME, primary_keys=['id'])
    return data_count


def fill_season_data(df: pd.DataFrame, col_name):
    """
    按季度补充数据
    :param df:
    :param col_name:
    :return:
    """
    col_name_season = f'{col_name}_season'
    if df.shape[0] == 0:
        logger.warning('df %s 没有数据', df.shape)
        df[col_name_season] = np.nan
        return
    # 例如：null_col_index_list = list(df.index[df['total_operating_revenue'].isnull()])
    # [datetime.date(1989, 12, 31)]
    if col_name_season not in df:
        df[col_name_season] = np.nan
    else:
        logger.warning('%s df %s 已经存在 %s 列数据', df['code'].iloc[0], df.shape, col_name_season)

    data_last_s, report_date_last, df_len = None, None, df.shape[0]
    for row_num, (report_date, data_s) in enumerate(df.T.items()):
        # 当期 col_name_season 值：
        # 1） 如果前一条 col_name 值不为空， 且当前 col_name 值不为空，且上一条记录与当前记录为同一年份
        #     使用前一条记录的 col_name 值 与 当前 col_name 值 的差
        # 2） 如果（前一条 col_name 值为空 或 上一条记录与当前记录为同一年份）， 且当前 col_name 值不为空
        #    1）季报数据直接使用
        #    2）半年报数据 1/2
        #    3）三季报数据 1/3
        #    4）年报数据 1/4
        # 3） 如果前一条 col_name 值不为空， 且当前 col_name 值为空
        #     使用前一条 col_name 值
        #     同时，使用线性增长，设置当期 col_name 值

        # 前一条 col_name 值为空
        is_nan_last_col_name = data_last_s is None or np.isnan(data_last_s[col_name])
        # 当前 col_name 值为空
        is_nan_curr_col_name = np.isnan(data_s[col_name])
        # 当期记录与前一条记录为同一年份
        is_same_year = report_date_last.year == report_date.year if report_date_last is not None else False
        if not is_nan_last_col_name and not is_nan_curr_col_name and is_same_year:
            value = data_s[col_name] - data_last_s[col_name]
            data_s[col_name_season] = value / ((report_date.month - report_date_last.month) / 3)
        elif (is_nan_last_col_name or not is_same_year) and not is_nan_curr_col_name:
            value = data_s[col_name]
            month = report_date.month
            if month == 3:
                pass
            elif month in (6, 9, 12):
                value = value / (month / 3)
            else:
                raise ValueError(f"{report_date} 不是有效的日期")
            data_s[col_name_season] = value
        elif not is_nan_last_col_name and is_nan_curr_col_name:
            value = data_last_s[col_name_season]
            data_s[col_name_season] = value
            # 将当前 col_name 值设置为 前一记录 col_name 值 加上 value 意味着线性增长
            month = report_date.month
            if month == 3:
                data_s[col_name] = value
            elif month in (6, 9, 12):
                data_s[col_name] = value * (month / 3)
            else:
                raise ValueError(f"{report_date} 不是有效的日期")
        else:
            logger.warning("%d/%d) %s 缺少数据无法补充缺失字段 %s", row_num, df_len, report_date, col_name)

        # 保存当期记录到 data_last_s
        data_last_s = data_s
        report_date_last = report_date
        df.loc[report_date] = data_s

    return df


def _test_fill_season_data():
    """
    测试 filll_season_data 函数
    测试数据
                        code report_date  revenue
    report_date
    2000-12-31   000001.XSHE  2000-12-31    400.0
    2001-03-31   000001.XSHE  2001-03-31      NaN
    2001-06-30   000001.XSHE  2001-06-30    600.0
    2001-09-30   000001.XSHE  2001-09-30      NaN
    2001-12-31   000001.XSHE  2001-12-31   1400.0
    2002-12-31   000001.XSHE  2002-12-31   1600.0

    转换后数据
                        code report_date  revenue  revenue_season
    report_date
    2000-12-31   000001.XSHE  2000-12-31    400.0           100.0
    2001-03-31   000001.XSHE  2001-03-31    100.0           100.0
    2001-06-30   000001.XSHE  2001-06-30    600.0           500.0
    2001-09-30   000001.XSHE  2001-09-30   1500.0           500.0
    2001-12-31   000001.XSHE  2001-12-31   1400.0          -100.0
    2002-12-31   000001.XSHE  2002-12-31   1600.0           400.0
    :return:
    """
    label = 'revenue'
    df = pd.DataFrame({
        'report_date': [str_2_date('2000-12-31'), str_2_date('2001-3-31'), str_2_date('2001-6-30'),
                       str_2_date('2001-9-30'), str_2_date('2001-12-31'), str_2_date('2002-12-31')],
        label: [400, np.nan, 600, np.nan, 1400, 1600],
    })
    df['code'] = '000001.XSHE'
    df = df[['code', 'report_date', label]]
    df.set_index('report_date', drop=False, inplace=True)
    print(df)
    df_new = fill_season_data(df, label)
    print(df_new)
    assert df.loc[str_2_date('2001-3-31'), label] == 100, \
        f"{label} {str_2_date('2001-3-31')} 应该等于前一年的 1/4，当前 {df.loc[str_2_date('2001-3-31'), label]}"


if __name__ == "__main__":
    _test_fill_season_data()
