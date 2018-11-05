#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/11/2 16:33
@File    : worst_fund.py
@contact : mmmaaaggg@163.com
@desc    : 简单统计一下日期段内公募基金业绩表现情况
"""
import pandas as pd
from tasks.backend import engine_md
from tasks.utils.db_utils import with_db_session
from tasks.utils.fh_utils import date_2_str
import logging

logger = logging.getLogger(__name__)


def stat_fund(date_from, date_to):
    sql_str = """
        SELECT (@rowNum:=@rowNum+1) AS rowNo, t.* FROM 
        (
            SELECT date_from.ts_code, basic.name, basic.management, date_from.date_from, nav_to.end_date, 
                nav_from.accum_nav nav_from, nav_to.accum_nav nav_to, nav_to.accum_nav/nav_from.accum_nav pct_chg
            FROM
            (
                SELECT ts_code, max(end_date) date_from FROM tushare_fund_nav WHERE end_date<= :date_from 
                GROUP BY ts_code
            ) date_from
            JOIN
            (
                SELECT ts_code, max(end_date) date_to FROM tushare_fund_nav WHERE end_date<= :date_to 
                GROUP BY ts_code
            ) date_to
            ON date_from.ts_code = date_to.ts_code
            JOIN tushare_fund_nav nav_from
            ON date_from.ts_code = nav_from.ts_code
            AND date_from.date_from = nav_from.end_date
            JOIN tushare_fund_nav nav_to
            ON date_to.ts_code = nav_to.ts_code
            AND date_to.date_to = nav_to.end_date
            JOIN tushare_fund_basic basic
            ON date_from.ts_code = basic.ts_code
            WHERE basic.name NOT LIKE '%B%' and basic.name NOT LIKE '%A%' and basic.name NOT LIKE '%C%'
            HAVING nav_to.accum_nav IS NOT NULL AND nav_from.accum_nav IS NOT NULL and pct_chg != 1 and pct_chg < 2
            ORDER BY nav_to.accum_nav/nav_from.accum_nav
        ) t"""
    # data_df = pd.read_sql(sql_str, engine_md)
    with with_db_session(engine_md) as session:
        session.execute("Select (@rowNum :=0) ;")
        table = session.execute(sql_str, params={'date_from': date_2_str(date_from), 'date_to': date_2_str(date_to)})
        data = [[d for d in row] for row in table.fetchall()]
    data_df = pd.DataFrame(
        data,
        columns=['rowNo', 'ts_code', 'name', 'management', 'date_from', 'date_to', 'nav_from', 'nav_to', 'pct_chg']
    )
    return data_df.describe()['pct_chg']


if __name__ == "__main__":
    date_from, date_to = '2018-6-30', '2018-9-30'
    stat_s = stat_fund(date_from, date_to)
    stat_s.to_csv(f'fund_stat_{date_from}_{date_to}.csv')
    logger.info("统计结果[%s, %s]\n%s", date_from, date_to, stat_s)
