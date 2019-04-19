#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2019/2/26 17:56
@File    : income.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from tasks.jqdata import finance, query
import logging
import datetime
from ibats_utils.mess import str_2_date, date_2_str, iter_2_range, range_date
from tasks import app
from sqlalchemy.types import String, Date, Integer
from sqlalchemy.dialects.mysql import DOUBLE
from tasks.backend import engine_md
from ibats_utils.db import bunch_insert_on_duplicate_update, execute_scalar
from tasks.config import config

logger = logging.getLogger(__name__)
BASE_DATE = str_2_date('1989-12-01')
LOOP_STEP = 20
TABLE_NAME = 'jq_stock_income'
DTYPE = {
    "id": Integer,
    "company_id": Integer,
    "company_name": String(100),
    "code": String(12),
    "a_code": String(12),
    "b_code": String(12),
    "h_code": String(12),
    "pub_date": Date,
    "start_date": Date,
    "end_date": Date,
    "report_date": Date,
    "report_type": Integer,
    "source_id": Integer,
    "source": String(60),
    "total_operating_revenue": DOUBLE,
    "operating_revenue": DOUBLE,
    "total_operating_cost": DOUBLE,
    "operating_cost": DOUBLE,
    "operating_tax_surcharges": DOUBLE,
    "sale_expense": DOUBLE,
    "administration_expense": DOUBLE,
    "exploration_expense": DOUBLE,
    "financial_expense": DOUBLE,
    "asset_impairment_loss": DOUBLE,
    "fair_value_variable_income": DOUBLE,
    "investment_income": DOUBLE,
    "invest_income_associates": DOUBLE,
    "exchange_income": DOUBLE,
    "other_items_influenced_income": DOUBLE,
    "operating_profit": DOUBLE,
    "subsidy_income": DOUBLE,
    "non_operating_revenue": DOUBLE,
    "non_operating_expense": DOUBLE,
    "disposal_loss_non_current_liability": DOUBLE,
    "other_items_influenced_profit": DOUBLE,
    "total_profit": DOUBLE,
    "income_tax": DOUBLE,
    "other_items_influenced_net_profit": DOUBLE,
    "net_profit": DOUBLE,
    "np_parent_company_owners": DOUBLE,
    "minority_profit": DOUBLE,
    "eps": DOUBLE,
    "basic_eps": DOUBLE,
    "diluted_eps": DOUBLE,
    "other_composite_income": DOUBLE,
    "total_composite_income": DOUBLE,
    "ci_parent_company_owners": DOUBLE,
    "ci_minority_owners": DOUBLE,
    "interest_income": DOUBLE,
    "premiums_earned": DOUBLE,
    "commission_income": DOUBLE,
    "interest_expense": DOUBLE,
    "commission_expense": DOUBLE,
    "refunded_premiums": DOUBLE,
    "net_pay_insurance_claims": DOUBLE,
    "withdraw_insurance_contract_reserve": DOUBLE,
    "policy_dividend_payout": DOUBLE,
    "reinsurance_cost": DOUBLE,
    "non_current_asset_disposed": DOUBLE,
    "other_earnings": DOUBLE,
}


def get_df_iter(date_start, date_end, step, df_len_limit=3000, deep=0):
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
        q = query(finance.STK_INCOME_STATEMENT).filter(
            finance.STK_INCOME_STATEMENT.pub_date > date_2_str(date_from),
            finance.STK_INCOME_STATEMENT.pub_date <= date_2_str(date_to))

        df = finance.run_query(q)
        df_len = df.shape[0]
        if df_len >= df_len_limit:
            if step >= 2:
                logger.warning('%s%d) [%s ~ %s] 包含 %d 条数据，可能已经超越 %d 条提取上限，开始进一步分割日期',
                               '  ' * deep, num, date_from, date_to, df_len, df_len_limit)
                yield from get_df_iter(date_from, date_to, step // 2, deep=deep + 1)
            else:
                logger.warning('%s%d) [%s ~ %s] 包含 %d 条数据，可能已经超越 %d 条提取上限且无法再次分割日期范围，手动需要补充提取剩余数据',
                               '  ' * deep, num, date_from, date_to, df_len, df_len_limit)
                yield df, date_from, date_to
        else:
            logger.debug('%s%d) [%s ~ %s] 包含 %d 条数据', '  ' * deep, num, date_from, date_to, df_len)
            yield df, date_from, date_to


@app.task
def import_jq_stock_income(chain_param=None, ts_code_set=None):
    """
    插入股票日线数据到最近一个工作日-1。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    :return:
    """
    logger.info("更新 %s 开始", TABLE_NAME)
    has_table = engine_md.has_table(TABLE_NAME)
    # 判断表是否已经存在
    if has_table:
        sql_str = f"""select max(pub_date) from {TABLE_NAME}"""
        date_start = execute_scalar(sql_str, engine_md)
        logger.info('查询 %s 数据使用起始日期 %s', TABLE_NAME, date_2_str(date_start))
    else:
        date_start = BASE_DATE
        logger.warning('%s 不存在，使用基础日期 %s', TABLE_NAME, date_2_str(date_start))

    # 查询最新的 pub_date
    date_end = datetime.date.today()
    if date_start >= date_end:
        logger.info('%s 已经是最新数据，无需进一步获取', date_start)
        return
    data_count_tot = 0
    try:
        for num, (df, date_from, date_to) in enumerate(get_df_iter(date_start, date_end, LOOP_STEP)):
            # logger.debug('%d) [%s ~ %s] 包含 %d 条数据', num, date_from, date_to, df.shape[0])
            data_count = bunch_insert_on_duplicate_update(
                df, TABLE_NAME, engine_md,
                dtype=DTYPE, myisam_if_create_table=True,
                primary_keys=['id'], schema=config.DB_SCHEMA_MD)
            data_count_tot += data_count
    finally:
        # 导入数据库
        logging.info("更新 %s 结束 %d 条信息被更新", TABLE_NAME, data_count_tot)


if __name__ == "__main__":
    import_jq_stock_income()
