# -*- coding: utf-8 -*-
"""
Created on 2018/1/17
@author: MG
"""

import logging
import math
from datetime import date, datetime, timedelta
import pandas as pd
from tasks.ifind import invoker
# from ifind_rest.invoke import APIError
from tasks.utils.fh_utils import get_last, get_first, date_2_str, STR_FORMAT_DATE
from sqlalchemy.types import String, Date, Integer, Boolean
from sqlalchemy.dialects.mysql import DOUBLE
from sqlalchemy.exc import ProgrammingError
from tasks.utils.fh_utils import unzip_join
from tasks.utils.db_utils import with_db_session
from tasks.backend import engine_md
from tasks import app

logger = logging.getLogger()
DATE_BASE = datetime.strptime('1990-01-01', STR_FORMAT_DATE).date()
ONE_DAY = timedelta(days=1)
# 标示每天几点以后下载当日行情数据
BASE_LINE_HOUR = 20


def get_private_fund_set(date_fetch):
    date_fetch_str = date_fetch.strftime(STR_FORMAT_DATE)
    fund_df = invoker.THS_DataPool('block', date_fetch_str + ';051010001', 'date:Y,thscode:Y,security_name:Y')
    if fund_df is None:
        logging.warning('%s 获取基金代码失败', date_fetch_str)
        return None
    fund_count = fund_df.shape[0]
    logging.info('get %d stocks on %s', fund_count, date_fetch_str)
    return set(fund_df['THSCODE'])


@app.task
def import_fund_info(ths_code=None, refresh=False):
    """

    :param ths_code:
    :param refresh:
    :return:
    """
    logging.info("更新 iFind_fund_info 开始")
    if ths_code is None:
        # 获取全市场私募基金代码及名称
        if refresh:
            date_fetch = datetime.strptime('1991-02-01', STR_FORMAT_DATE).date()
        else:
            date_fetch = date.today()

        date_end = date.today()
        private_fund_set = set()
        # while date_fetch < date_end:
        #     private_fund_set_sub = get_private_fund_set(date_fetch)
        #     if private_fund_set_sub is not None:
        #         private_fund_set |= private_fund_set_sub
        #     date_fetch += timedelta(days=90)

        private_fund_set_sub = get_private_fund_set(date_end)
        if private_fund_set_sub is not None:
            private_fund_set |= private_fund_set_sub

        ths_code = list(private_fund_set)
        ths_code = ths_code[:5]

    indicator_param_list = [
        ('ths_product_short_name_sp', '', String(10)),
        ('ths_product_full_name_sp', '', String(10)),
        ('ths_trust_category_sp', '', String(10)),
        ('ths_is_structured_product_sp', '', Boolean),
        ('ths_threshold_amt_sp', '', Integer),
        ('ths_low_add_amt_sp', '', Integer),
        ('ths_fore_max_issue_scale_sp', '', String(40)),
        ('ths_actual_issue_scale_sp', '', String(40)),
        ('ths_invest_manager_current_sp', '', String(10)),
        ('ths_mendator_sp', '', String(10)),
        ('ths_recommend_sd_sp', '', Date),
        ('ths_introduction_ed_sp', '', Date),
        ('ths_established_date_sp', '', Date),
        ('ths_maturity_date_sp', '', Date),
        ('ths_found_years_sp', '', Date),
        ('ths_duration_y_sp', '', Integer),
        ('ths_remain_duration_d_sp', '', Integer),
        ('ths_float_manage_rate_sp', '', DOUBLE),
        ('ths_mandate_fee_rate_sp', '', DOUBLE),
        ('ths_subscription_rate_explain_sp', '', String(60)),
        ('ths_redemp_rate_explain_sp', '', String(60)),
        ('ths_opening_period_explain_sp', '', String(60)),
        ('ths_close_period_explain_sp', '', String(60)),
        ('ths_trustee_sp', '', String(10)),
        ('ths_secbroker_sp', '', String(10))
    ]
    # jsonIndicator='THS_BasicData('SM000008.XT','ths_product_short_name_sp;ths_product_full_name_sp;ths_trust_category_sp;ths_is_structured_product_sp;ths_threshold_amt_sp;ths_low_add_amt_sp;ths_fore_max_issue_scale_sp;ths_actual_issue_scale_sp;ths_invest_manager_current_sp;ths_invest_advisor_sp;ths_mendator_sp;ths_recommend_sd_sp;ths_introduction_ed_sp;ths_established_date_sp;ths_maturity_date_sp;ths_found_years_sp;ths_duration_y_sp;ths_remain_duration_d_sp;ths_float_manage_rate_sp;ths_mandate_fee_rate_sp;ths_subscription_rate_explain_sp;ths_redemp_rate_explain_sp;ths_opening_period_explain_sp;ths_close_period_explain_sp;ths_trustee_sp;ths_secbroker_sp'
    # jsonparam=';;;;;;;;;'
    indicator, param = unzip_join([(key, val) for key, val, _ in indicator_param_list], sep=';')
    data_df = invoker.THS_BasicData(ths_code, indicator, param, max_code_num=8000)
    if data_df is None or data_df.shape[0] == 0:
        logging.info("没有可用的 stock info 可以更新")
        return
    # 删除历史数据，更新数据
    with with_db_session(engine_md) as session:
        session.execute(
            "DELETE FROM ifind_private_fund_info WHERE ths_code IN (" + ','.join(
                [':code%d' % n for n in range(len(private_fund_set))]
            ) + ")",
            params={'code%d' % n: val for n, val in enumerate(private_fund_set)})
        session.commit()
    dtype = {key: val for key, _, val in indicator_param_list}
    dtype['ths_code'] = String(20)
    data_count = data_df.shape[0]
    data_df.to_sql('ifind_private_fund_info', engine_md, if_exists='append', index=False, dtype=dtype)
    logging.info("更新 ifind_private_fund_info 完成 存量数据 %d 条", data_count)


@app.task
def import_private_fund_daily(ths_code_set: set = None, begin_time=None):
    """

    :param ths_code_set:
    :param begin_time:
    :return:
    """
    if ths_code_set is None:
        pass
    indicator_param_list = [
        ('netAssetValue', '', DOUBLE),
        ('adjustedNAV', '', DOUBLE),
        ('accumulatedNAV', '', DOUBLE),
        ('premium', '', DOUBLE),
        ('premiumRatio', '', DOUBLE),
        ('estimatedPosition', '', DOUBLE)
    ]
    # jsonIndicator='netAssetValue，adjustedNAV，accumulatedNAV，premium，premiumRatio，estimatedPosition'
    # jsonparam=';;;;;;;;;'
    json_indicator, json_param = unzip_join([(key, val) for key, val, _ in indicator_param_list], sep=';')
    sql_str = """select ths_code, date_frm, if(ths_maturaity_date_sp<end_date, ths_maturaity_date_sp, end_date) date_to
FROM
(
    select info.ths_code, ifnull(trade_date_max_1, ths_ipo_date_stock) date_frm, ths_maturaity_date_sp,
    if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
    from 
        ifind_private_fund_info info 
    left outer join
        (select ths_code, adddate(max(time),1) trade_date_max_1 from ifind_private_fund_daily group by ths_code) daily
    on info.ths_code = daily.ths_code
) tt
where date_frm <= if(ths_maturaity_date_sp<end_date, ths_maturaity_date_sp, end_date) 
order by ths_code"""
    if begin_time is None:
        with with_db_session(engine_md) as session:
            # 获取需要获取日线数据的日期区间
            try:
                table = session.execute(sql_str)
            except ProgrammingError:
                logger.exception('获取历史数据最新交易日期失败，尝试仅适用 ifind_stock_info 表进行计算')
                sql_str = """select ths_code, date_frm, if(ths_maturaity_date_sp<end_date, ths_maturaity_date_sp, end_date) date_to
                                    FROM
                                    (
                                        select info.ths_code, ths_ipo_date_stock date_frm, ths_maturaity_date_sp,
                                        if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
                                        from ifind_stock_info info 
                                    ) tt
                                    where date_frm <= if(ths_maturaity_date_sp<end_date, ths_maturaity_date_sp, end_date) 
                                    order by ths_code"""
                table = session.execute(sql_str)

            #获取每只基金需要获取日线数据的日期区间
            code_date_range_dic = {ths_code: (date_from, date_to)
                                   for ths_code, date_from, date_to in table.fetchall() if
                                   ths_code_set is None or ths_code in ths_code_set}
    # 设置 dtype
    dtype = {key: val for key, _, val in indicator_param_list}
    dtype['ths_code'] = String(20)
    dtype['time'] = Date

    data_df_list, data_count, tot_data_count, code_count = [], 0, 0, len(code_date_range_dic)
    try:
        for num, (ths_code, (begin_time, end_time)) in enumerate(code_date_range_dic.items(), start=1):
            logger.debug('%d/%d) %s [%s - %s]', num, code_count, ths_code, begin_time, end_time)
            data_df = invoker.THS_DateSerial(
                ths_code,
                json_indicator,
                json_param,
                'Days:Tradedays,Fill:Previous,Interval:D',
                begin_time, end_time
            )
            if data_df is not None or data_df.shape[0] > 0:
                data_count += data_df.shape[0]
                data_df_list.append(data_df)
            # 大于阀值有开始插入
            if data_count >= 10000:
                tot_data_df = pd.concat(data_df_list)
                tot_data_df.to_sql('ifind_private_fund_daily', engine_md, if_exists='append', index=False, dtype=dtype)
                tot_data_count += data_count
                data_df_list, data_count = [], 0
    finally:
        if data_count > 0:
            tot_data_df = pd.concat(data_df_list)
            tot_data_df.to_sql('ifind_private_fund_daily', engine_md, if_exists='append', index=False, dtype=dtype)
            tot_data_count += data_count

        logging.info("更新 ifind_private_fund_daily 完成 新增数据 %d 条", tot_data_count)


if __name__ == "__main__":
    ths_code = None  # '600006.SH,600009.SH'
    # 基金基本信息数据加载
    import_fund_info(ths_code)
    # 基金日K数据行情加载
    # import_private_fund_daily(ths_code)
