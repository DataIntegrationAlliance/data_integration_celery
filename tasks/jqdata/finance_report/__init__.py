#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2019/2/26 17:38
@File    : __init__.py.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
import datetime
from tasks.jqdata import finance, query
import logging
from datetime import date
from tasks.utils.fh_utils import str_2_date, date_2_str, iter_2_range, range_date, get_first_idx, get_last_idx
from tasks.backend import engine_md, bunch_insert
from tasks.utils.db_utils import bunch_insert_on_duplicate_update, execute_scalar, with_db_session
import pandas as pd
import numpy as np
from tasks.config import config


logger = logging.getLogger(__name__)


class FinanceReportSaver:

    def __init__(self, table_name, dtype, statement):
        self.logger = logging.getLogger(__name__)
        self.BASE_DATE = str_2_date('1989-12-01')
        self.loop_step = 20
        self.dtype = dtype
        self.table_name = table_name
        self.statement = statement

    def get_df_iter(self, date_start, date_end, step, df_len_limit=3000, deep=0):
        """
        获取日期范围内的数据，当数据记录大于上限条数时，将日期范围进行二分法拆分，迭代进行查询
        :param date_start:
        :param date_end:
        :param step:
        :param df_len_limit:
        :param deep:
        :return:
        """
        for num, (date_from, date_to) in enumerate(iter_2_range(range_date(
                date_start, date_end, step), has_left_outer=False, has_right_outer=False), start=1):
            q = query(self.statement).filter(
                self.statement.pub_date > date_2_str(date_from),
                self.statement.pub_date <= date_2_str(date_to))

            df = finance.run_query(q)
            df_len = df.shape[0]
            if df_len >= df_len_limit:
                if step >= 2:
                    self.logger.warning('%s%d) [%s ~ %s] 包含 %d 条数据，可能已经超越 %d 条提取上限，开始进一步分割日期',
                                        '  ' * deep, num, date_from, date_to, df_len, df_len_limit)
                    yield from self.get_df_iter(date_from, date_to, step // 2, deep=deep + 1)
                else:
                    self.logger.warning('%s%d) [%s ~ %s] 包含 %d 条数据，可能已经超越 %d 条提取上限且无法再次分割日期范围，手动需要补充提取剩余数据',
                                        '  ' * deep, num, date_from, date_to, df_len, df_len_limit)
                    yield df, date_from, date_to
            else:
                self.logger.debug('%s%d) [%s ~ %s] 包含 %d 条数据', '  ' * deep, num, date_from, date_to, df_len)
                yield df, date_from, date_to

    def save(self):
        self.logger.info("更新 %s 开始", self.table_name)
        has_table = engine_md.has_table(self.table_name)
        # 判断表是否已经存在
        if has_table:
            sql_str = f"""select max(pub_date) from {self.table_name}"""
            date_start = execute_scalar(engine_md, sql_str)
            self.logger.info('查询 %s 数据使用起始日期 %s', self.table_name, date_2_str(date_start))
        else:
            date_start = self.BASE_DATE
            self.logger.warning('%s 不存在，使用基础日期 %s', self.table_name, date_2_str(date_start))

        # 查询最新的 pub_date
        date_end = datetime.date.today()
        if date_start >= date_end:
            self.logger.info('%s 已经是最新数据，无需进一步获取', date_start)
            return
        data_count_tot = 0
        try:
            for num, (df, date_from, date_to) in enumerate(self.get_df_iter(date_start, date_end, self.loop_step)):
                # logger.debug('%d) [%s ~ %s] 包含 %d 条数据', num, date_from, date_to, df.shape[0])
                if df is not None and df.shape[0] > 0:
                    data_count = bunch_insert_on_duplicate_update(
                        df, self.table_name, engine_md,
                        dtype=self.dtype, myisam_if_create_table=True,
                        primary_keys=['id'], schema=config.DB_SCHEMA_MD)
                    data_count_tot += data_count
        finally:
            # 导入数据库
            logging.info("更新 %s 结束 %d 条信息被更新", self.table_name, data_count_tot)


def check_accumulation_cols(df: pd.DataFrame):
    """
    检查当期 DataFrame 哪些 column 是周期性累加的
    :param df:
    :return:
    """
    accumulation_col_name_list = []
    for col_name in df.columns:
        data_s = df[col_name]
        report_date_last, data_last, is_growing_inner_year, is_down_1st_season = None, None, None, None
        fit_count, available_count = 0, 0
        first_data = data_s.iloc[0]
        if isinstance(first_data, date) or isinstance(first_data, str):
            continue
        for num, (report_date, data) in enumerate(data_s.items()):
            if data is None or isinstance(data, date) or isinstance(data, str) \
                    or (isinstance(data, float) and np.isnan(data)):
                continue

            if report_date_last is not None:
                if report_date_last.year == report_date.year:
                    if report_date.month > report_date_last.month:
                        # 同一年份内，数值持续增长
                        if is_growing_inner_year is None:
                            is_growing_inner_year = data > data_last
                        else:
                            is_growing_inner_year &= data > data_last
                    else:
                        raise ValueError(f"report_date_last:{report_date_last} report_date:{report_date}")
                else:
                    # 不同年份，年报与一季报或半年报相比下降
                    if report_date_last.month == 12 and report_date.month in (3, 6):
                        is_down_1st_season = data < data_last
                        available_count += 1
                    else:
                        is_down_1st_season = False

                    if is_growing_inner_year is not None and is_down_1st_season and is_growing_inner_year:
                        fit_count += 1

                    # 重置 is_growing_inner_year 标志
                    is_growing_inner_year = None

            # 赋值 last 变量
            report_date_last, data_last = report_date, data

        if available_count >= 3 and fit_count / available_count > 0.5:
            accumulation_col_name_list.append(col_name)

    return accumulation_col_name_list


def fill_season_data(df: pd.DataFrame, col_name):
    """
    按季度补充数据
    :param df:
    :param col_name:
    :return:
    """
    col_name_season = f'{col_name}_season'
    # 没有数据则直接跳过
    if df.shape[0] == 0:
        logger.warning('df %s 没有数据', df.shape)
        df[col_name_season] = np.nan
        return
    # 例如：null_col_index_list = list(df.index[df['total_operating_revenue'].isnull()])
    # [datetime.date(1989, 12, 31)]
    code = df['code'].iloc[0]
    if col_name_season not in df:
        df.loc[:, col_name_season] = np.nan
    else:
        logger.warning('%s df %s 已经存在 %s 列数据', code, df.shape, col_name_season)

    data_last_s, report_date_last, df_len = None, None, df.shape[0]
    for row_num, (report_date, data_s) in enumerate(df.T.items(), start=1):
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
            logger.warning("%d/%d) %s %s 缺少数据无法补充缺失字段 %s", row_num, df_len, code, report_date, col_name)

        # 保存当期记录到 data_last_s
        data_last_s = data_s
        report_date_last = report_date
        df.loc[report_date] = data_s

    return df, col_name_season


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


def _test_check_accumulation_cols():
    label = 'revenue'           # 周期增长
    label2 = 'revenue_season'   # 非周期增长
    df = pd.DataFrame({
        'report_date': [
            str_2_date('2000-3-31'), str_2_date('2000-6-30'), str_2_date('2000-9-30'), str_2_date('2000-12-31'),
            str_2_date('2001-3-31'), str_2_date('2001-6-30'), str_2_date('2001-12-31'),
            str_2_date('2002-6-30'), str_2_date('2002-12-31'),
            str_2_date('2003-3-31'), str_2_date('2003-12-31')
        ],
        label: [
            200, 400, 600, 800,
            np.nan, 600, 1200,
            700, 1400,
            400, 1600],
        label2: [
            200, 200, 200, 200,
            200, 400, 600,
            700, 700,
            400, 400],
    })
    df.set_index('report_date', drop=False, inplace=True)
    df.sort_index(inplace=True)
    print(df)
    accumulation_col_name_list = check_accumulation_cols(df)
    print("accumulation_col_name_list", accumulation_col_name_list)
    assert len(accumulation_col_name_list) == 1, f'{accumulation_col_name_list} 长度错误'
    assert 'revenue' in accumulation_col_name_list


def get_accumulation_col_names(table_name, dtype: dict):
    # 获取季度、半年、年报财务数据
    col_name_list = list(dtype.keys())
    col_name_list_str = ','.join([f'income.`{col_name}` {col_name}' for col_name in col_name_list])
    sql_str = f"""SELECT {col_name_list_str} FROM {table_name} income inner join 
        (
            select code, pub_date, max(report_date) report_date 
            from {table_name} where report_type=0 group by code, pub_date
        ) base_date
        where income.report_type=0
        and income.code = base_date.code
        and income.pub_date = base_date.pub_date
        and income.report_date = base_date.report_date
        and income.code = '000001.XSHE'
        order by code, income.report_date"""
    df = pd.read_sql(sql_str, engine_md).set_index('report_date', drop=False)
    accumulation_col_name_list = check_accumulation_cols(df)
    logger.info("%s 周期性增长列名称包括：\n%s", table_name, accumulation_col_name_list)
    return accumulation_col_name_list


def get_accumulation_col_names_for(report):
    """
    筛选周期增长的字段，供后续代码将相关字段转化成季度值字段
    :return:
    """
    if report == 'income':
        from tasks.jqdata.finance_report.income import TABLE_NAME as TABLE_NAME_FIN_REPORT
        from tasks.jqdata.finance_report.income_2_daily import DTYPE_INCOME_DAILY
    elif report == 'cashflow':
        from tasks.jqdata.finance_report.cashflow import TABLE_NAME as TABLE_NAME_FIN_REPORT

    accumulation_col_name_list = get_accumulation_col_names(TABLE_NAME_FIN_REPORT, DTYPE_INCOME_DAILY)
    return accumulation_col_name_list


def transfer_report_2_daily(table_name_report: str, table_name_daily: str, table_name_trade_date: str,
                            dtype_daily: dict, accumulation_col_name_list):
    """
    将财务数据（季度）保存成日级别数据
    :param table_name_report:
    :param table_name_daily:
    :param table_name_trade_date:
    :param dtype_daily:
    :param accumulation_col_name_list:
    :return:
    """
    if not engine_md.has_table(table_name_report):
        logger.info('%s 不存在，无需转化成日级别数据', table_name_report)
    today = datetime.date.today()

    has_table = engine_md.has_table(table_name_daily)
    # 获取每只股票最新的交易日，以及截至当期日期的全部交易日数据
    with with_db_session(engine_md) as session:
        sql_str = f"""select trade_date from {table_name_trade_date} where trade_date<=:today order by trade_date"""
        table = session.execute(sql_str, params={"today": today})
        trade_date_list = [_[0] for _ in table.fetchall()]
        if has_table:
            sql_str = f"""select code, max(trade_date) from {table_name_daily} group by code"""
            table = session.execute(sql_str)
            code_date_latest_dic = dict(table.fetchall())
        else:
            code_date_latest_dic = {}

    # 获取季度、半年、年报财务数据
    col_name_list = list(dtype_daily.keys())
    col_name_list_str = ','.join([f'report.`{col_name}` {col_name}' for col_name in col_name_list])
    sql_str = f"""SELECT {col_name_list_str} FROM {table_name_report} report inner join 
        (
            select code, pub_date, max(report_date) report_date 
            from {table_name_report} where report_type=0 group by code, pub_date
        ) base_date
        where report.report_type=0
        and report.code = base_date.code
        and report.pub_date = base_date.pub_date
        and report.report_date = base_date.report_date
        order by code, pub_date"""
    dfg_by_code = pd.read_sql(sql_str, engine_md).set_index('report_date', drop=False).sort_index().groupby('code')
    dfg_len = len(dfg_by_code)
    data_new_s_list = []
    # 按股票代码分组，分别按日进行处理
    for num, (code, df_by_code) in enumerate(dfg_by_code, start=1):
        # df_by_code.sort_index(inplace=True)  # 前面代码以及有此功能
        df_by_code = df_by_code.copy()
        df_len = df_by_code.shape[0]
        # df_by_code.loc[:, ['pub_date_next', 'report_date_next']] = df_by_code[['pub_date', 'report_date']].shift(-1)
        df_by_code.loc[:, 'pub_date_next'] = df_by_code['pub_date'].shift(-1)
        df_by_code.loc[:, 'report_date_next'] = df_by_code['report_date'].shift(-1)
        # 将相关周期累加增长字段转化为季度增长字段
        for col_name in accumulation_col_name_list:
            # df_by_code = fill_season_data(df_by_code, 'total_operating_revenue')
            df_by_code, col_name_season = fill_season_data(df_by_code, col_name)
            # 更新 dtype_daily
            if col_name_season not in dtype_daily:
                dtype_daily[col_name_season] = dtype_daily[col_name]

        # df_by_code['code'] = code
        trade_date_latest = code_date_latest_dic[code] if code in code_date_latest_dic else None
        for num_sub, (report_date, data_s) in enumerate(df_by_code.T.items(), start=1):
            pub_date = data_s['pub_date']
            # report_date = data_s['report_date']
            pub_date_next = data_s['pub_date_next']
            # 检查 最新交易日是否已经大于下一条财报日期，如果是则跳过当前数据
            if not pd.isnull(trade_date_latest) and not pd.isnull(pub_date_next) and trade_date_latest > pub_date_next:
                continue
            # 获取 交易日区间
            if pd.isnull(trade_date_latest):
                date_from_idx = get_first_idx(trade_date_list, lambda x: x >= pub_date)
            else:
                date_from_idx = get_first_idx(trade_date_list, lambda x: x > trade_date_latest)
            if pd.isnull(pub_date_next):
                date_to_idx = get_last_idx(trade_date_list, lambda x: x <= today)
            else:
                date_to_idx = get_last_idx(trade_date_list, lambda x: x < pub_date_next)

            if date_from_idx is None:
                logger.warning('%d/%d) %d/%d) %s 没有找到有效的起始日期 pub_date: %s trade_date_latest: %s ',
                               num, dfg_len, num_sub, df_len, code, pub_date, trade_date_latest)
                continue
            if date_to_idx is None:
                logger.warning('%d/%d) %d/%d) %s 没有找到有效的截至日期 today: %s pub_date_next: %s ',
                               num, dfg_len, num_sub, df_len, code, today, pub_date_next)
                continue
            if date_from_idx > date_to_idx:
                logger.warning(
                    '%d/%d) %d/%d) %s %s > %s 不匹配 pub_date: %s trade_date_latest: %s today: %s pub_date_next: %s ',
                    num, dfg_len, num_sub, df_len, code, trade_date_list[date_from_idx], trade_date_list[date_to_idx],
                    pub_date, trade_date_latest, today, pub_date_next)
                continue

            logger.debug(
                '%d/%d) %d/%d) %s [%s, %s) 预计转化 %d 条日级别数据，报告日：%s，',
                num, dfg_len, num_sub, df_len, code, trade_date_list[date_from_idx], trade_date_list[date_to_idx],
                date_to_idx - date_from_idx + 1, report_date)
            # 补充交易日区间的每日数据
            for trade_date in trade_date_list[date_from_idx:(date_to_idx + 1)]:
                data_new_s = data_s.copy()
                data_new_s['trade_date'] = trade_date
                data_new_s_list.append(data_new_s)

        if len(data_new_s_list) > 0:
            data_count = save_data_2_daily_table(data_new_s_list, table_name_daily, dtype_daily)
            logger.info("%d/%d) %s %d 条记录被保存", num, dfg_len, code, data_count)
            data_new_s_list = []


def save_data_2_daily_table(data_new_s_list: list, table_name, dtype: dict):
    df = pd.DataFrame(data_new_s_list)
    data_count = bunch_insert(df, table_name, dtype=dtype, primary_keys=['id', 'trade_date'])
    return data_count


if __name__ == "__main__":
    # _test_fill_season_data()
    # _test_check_accumulation_cols()
    for report in {'income'}:
        get_accumulation_col_names_for(report)
