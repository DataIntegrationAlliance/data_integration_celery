# -*- coding: utf-8 -*-
"""
Created on 2018/1/17
@author: MG
@desc    : 2018-08-29 已经正式运行测试完成，可以正常使用
"""

import logging
from datetime import date, datetime, timedelta
import pandas as pd
from tasks.ifind import invoker
from tasks.utils.fh_utils import get_last, get_first, date_2_str, STR_FORMAT_DATE, str_2_date
from sqlalchemy.types import String, Date, Integer, Text
from sqlalchemy.dialects.mysql import DOUBLE
from tasks.utils.fh_utils import unzip_join
from tasks.utils.db_utils import with_db_session, bunch_insert_on_duplicate_update, alter_table_2_myisam
from tasks.backend import engine_md
from tasks.backend.orm import build_primary_key
from tasks.merge.code_mapping import update_from_info_table
from tasks import app

DEBUG = False
TRIAL = False
logger = logging.getLogger()
DATE_BASE = datetime.strptime('1990-01-01', STR_FORMAT_DATE).date()
ONE_DAY = timedelta(days=1)
# 标示每天几点以后下载当日行情数据
BASE_LINE_HOUR = 20


def get_pub_fund_code_set(date_fetch):
    date_fetch_str = date_fetch.strftime(STR_FORMAT_DATE)
    # 中国公募基金-基金市场类-全部基金(含未成立、已到期):051001039
    stock_df = invoker.THS_DataPool('block', date_fetch_str + ';051001039', 'thscode:Y,security_name:Y')
    if stock_df is None:
        logging.warning('%s 获取基金代码失败', date_fetch_str)
        return None
    stock_count = stock_df.shape[0]
    logging.info('get %d stocks on %s', stock_count, date_fetch_str)
    return set(stock_df['THSCODE'])


@app.task
def import_pub_fund_info(ths_code=None, refresh=False):
    """

    :param ths_code:
    :param refresh:
    :return:
    """
    table_name = 'ifind_pub_fund_info'
    has_table = engine_md.has_table(table_name)
    logging.info("更新 %s 开始", table_name)
    if ths_code is None:
        # 获取全市场公募代码及名称
        if refresh:
            date_fetch = datetime.strptime('1991-02-01', STR_FORMAT_DATE).date()
        else:
            date_fetch = date.today()

        date_end = date.today()
        pub_fund_code_set = set()
        # 直接获取全部基金包含已成立，已到期的基金，因此不再需要循环
        # while date_fetch < date_end:
        #     pub_fund_code_set_sub = get_pub_fund_code_set(date_fetch)
        #     if pub_fund_code_set_sub is not None:
        #         pub_fund_code_set |= pub_fund_code_set_sub
        #     date_fetch += timedelta(days=365)

        pub_fund_code_set_sub = get_pub_fund_code_set(date_end)
        if pub_fund_code_set_sub is not None:
            pub_fund_code_set |= pub_fund_code_set_sub

        ths_code = list(pub_fund_code_set)

        if DEBUG:
            # 该变量仅用于调试期间使用
            ths_code_old = ths_code
            ths_code = ths_code[:40]

    indicator_param_list = [
        ('ths_fund_short_name_fund', '', String(40)),
        ('ths_fund_code_fund', '', String(40)),
        ('ths_fund_thscode_fund', '', String(40)),
        ('ths_fund_full_name_fund', '', String(80)),
        ('ths_invest_objective_fund', '', String(500)),
        ('ths_invest_socpe_fund', '', Text),
        ('ths_perf_comparative_benchmark_fund', '', Text),
        ('ths_fund_listed_exchange_fund', '', String(40)),
        ('ths_fund_td_currency_fund', '', String(60)),
        ('ths_coupon_value_fund', '', String(40)),
        ('ths_fund_manager_current_fund', '', String(40)),
        ('ths_fund_manager_his_fund', '', String(400)),
        ('ths_fund_supervisor_fund', '', String(40)),
        ('ths_fund_mandator_fund', '', String(20)),
        ('ths_fund_sponsor_related_org_fund', '', String(40)),
        ('ths_fund_type_fund', '', String(10)),
        ('ths_fund_invest_type_fund', '', String(10)),
        ('ths_invest_type_first_classi_fund', '', String(40)),
        ('ths_invest_type_second_classi_fund', '', String(40)),
        ('ths_galaxy_classi_fund', '', String(300)),
        ('ths_hts_classi_fund', '', String(100)),
        ('ths_invest_style_fund', '', String(100)),
        ('ths_fund_duration_fund', '', String(40)),
        ('ths_fund_establishment_date_fund', '', Date),
        ('ths_fund_expiry_date_fund', '', Date),
        ('ths_redemp_sd_fund', '', String(40)),
        ('ths_mandate_sd_fund', '', String(40)),
        ('ths_manage_fee_rate_fund', '', String(40)),
        ('ths_mandate_fee_rate_fund', '', String(40)),
        ('ths_sales_service_fee_fund', '', String(40)),
        ('ths_high_pur_fee_rate_fund', '', String(20)),
        ('ths_high_redemp_fee_rate_fund', '', String(40)),
        ('ths_lof_listed_date_fund', '', Date),
        ('ths_lof_listed_td_share_fund', '', String(40)),
        ('ths_pm_fund_code_fund', '', String(40)),
        ('ths_par_short_name_fund', '', String(40)),
        ('ths_online_cash_sell_code_fund', '', String(40)),
        ('ths_online_cash_pur_sd_fund', '', String(40)),
        ('ths_online_cash_pur_ed_fund', '', String(40)),
        ('ths_online_cash_buy_share_ul_fund', '', String(40)),
        ('ths_online_cash_buy_share_dl_fund', '', String(40)),
        ('ths_offline_cash_pur_sd_fund', '', String(40)),
        ('ths_offline_cash_pur_ed_fund', '', String(40)),
        ('ths_offline_stock_pur_sd_fund', '', String(40)),
        ('ths_offline_stock_pur_ed_fund', '', String(40)),
        ('ths_offline_stock_pur_vol_dl_fund', '', String(40)),
        ('ths_fund_shares_convert_date_fund', '', String(40)),
        ('ths_fund_shares_convert_ratio_fund', '', String(40)),
        ('ths_issue_date_fund', '', Date),
        ('ths_issue_object_fund', '', String(100)),
        ('ths_issue_method_fund', '', String(80)),
        ('ths_fund_reg_and_registrant_fund', '', String(40)),
        ('ths_fund_main_underwrite_fund', '', String(40)),
        ('ths_fund_issue_coordinator_fund', '', String(500)),
        ('ths_fund_sales_agent_fund', '', Text),
        ('ths_fund_listing_recommended_fund', '', String(40))
    ]
    # jsonIndicator='ths_fund_short_name_fund;ths_fund_code_fund;ths_fund_thscode_fund;ths_fund_full_name_fund;ths_invest_objective_fund;ths_invest_socpe_fund;ths_perf_comparative_benchmark_fund;ths_fund_listed_exchange_fund;ths_fund_td_currency_fund;ths_coupon_value_fund;ths_fund_manager_current_fund;ths_fund_manager_his_fund;ths_fund_supervisor_fund;ths_fund_mandator_fund;ths_fund_sponsor_related_org_fund;ths_fund_type_fund;ths_fund_invest_type_fund;ths_invest_type_first_classi_fund;ths_invest_type_second_classi_fund;ths_galaxy_classi_fund;ths_hts_classi_fund;ths_invest_style_fund;ths_fund_duration_fund;ths_fund_establishment_date_fund;ths_fund_expiry_date_fund;ths_redemp_sd_fund;ths_mandate_sd_fund;ths_mandate_ed_fund;ths_manage_fee_rate_fund;ths_mandate_fee_rate_fund;ths_sales_service_fee_fund;ths_high_pur_fee_rate_fund;ths_high_redemp_fee_rate_fund;ths_lof_listed_date_fund;ths_lof_listed_td_share_fund;ths_pm_fund_code_fund;ths_par_short_name_fund;ths_online_cash_sell_code_fund;ths_online_cash_pur_sd_fund;ths_online_cash_pur_ed_fund;ths_online_cash_buy_share_ul_fund;ths_online_cash_buy_share_dl_fund;ths_offline_cash_pur_sd_fund;ths_offline_cash_pur_ed_fund;ths_offline_cash_pur_share_dl_fund;ths_offline_stock_pur_sd_fund;ths_offline_stock_pur_ed_fund;ths_offline_stock_pur_vol_dl_fund;ths_fund_shares_convert_date_fund;ths_fund_shares_convert_ratio_fund;ths_issue_date_fund;ths_issue_object_fund;ths_issue_method_fund;ths_fund_reg_and_registrant_fund;ths_fund_main_underwrite_fund;ths_fund_issue_coordinator_fund;ths_fund_sales_agent_fund;ths_fund_listing_recommended_fund'
    # jsonparam=';;;;;;;;;;;'
    indicator, param = unzip_join([(key, val) for key, val, _ in indicator_param_list], sep=';')
    data_df = invoker.THS_BasicData(ths_code, indicator, param, max_code_num=3000)
    if data_df is None or data_df.shape[0] == 0:
        logging.info("没有可用的 pub_fund info 可以更新")
        return
    # 删除历史数据，更新数据
    # table_name_list = engine_md.table_names()
    # if table_name in table_name_list:
    #     with with_db_session(engine_md) as session:
    #         session.execute(
    #             "DELETE FROM {table_name} WHERE ths_code IN (".format(table_name=table_name) + ','.join(
    #                 [':code%d' % n for n in range(len(pub_fund_code_set))]
    #             ) + ")",
    #             params={'code%d' % n: val for n, val in enumerate(pub_fund_code_set)})
    #         session.commit()
    dtype = {key: val for key, _, val in indicator_param_list}
    dtype['ths_code'] = String(20)
    # data_count = data_df.shape[0]
    # data_df.to_sql(table_name, engine_md, if_exists='append', index=False, dtype=dtype)
    data_count = bunch_insert_on_duplicate_update(data_df, table_name, engine_md, dtype)
    logging.info("更新 %s 完成 存量数据 %d 条", table_name, data_count)
    if not has_table and engine_md.has_table(table_name):
        alter_table_2_myisam(engine_md, [table_name])
        build_primary_key([table_name])

    # 更新 code_mapping 表
    update_from_info_table(table_name)


@app.task
def import_pub_fund_daily(ths_code_set: set = None, begin_time=None):
    """
    通过history接口将历史数据保存到 ifind_pub_fund_daily
    :param ths_code_set:
    :param begin_time: 默认为None，如果非None则代表所有数据更新日期不得晚于该日期
    :return:
    """
    table_name = 'ifind_pub_fund_daily'
    has_table = engine_md.has_table(table_name)
    if begin_time is not None and type(begin_time) == date:
        begin_time = str_2_date(begin_time)

    indicator_param_list = [
        ('netAssetValue', '', DOUBLE),
        ('adjustedNAV', '', DOUBLE),
        ('accumulatedNAV', '', DOUBLE)
    ]
    # THS_HistoryQuotes('600006.SH,600010.SH',
    # 'preClose,open,high,low,close,avgPrice,changeRatio,volume,amount,turnoverRatio,transactionAmount,totalShares,totalCapital,floatSharesOfAShares,floatSharesOfBShares,floatCapitalOfAShares,floatCapitalOfBShares,pe_ttm,pe,pb,ps,pcf',
    # 'Interval:D,CPS:1,baseDate:1900-01-01,Currency:YSHB,fill:Previous',
    # '2018-06-30','2018-07-30')
    json_indicator, _ = unzip_join([(key, val) for key, val, _ in indicator_param_list], sep=';')
    if has_table:
        sql_str = """SELECT ths_code, date_frm, if(ths_fund_expiry_date_fund<end_date, ths_fund_expiry_date_fund, end_date) date_to
            FROM
            (
                SELECT info.ths_code, ifnull(trade_date_max_1, ths_lof_listed_date_fund) date_frm, ths_fund_expiry_date_fund,
                if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
                FROM 
                    ifind_pub_fund_info info 
                LEFT OUTER JOIN
                    (SELECT ths_code, adddate(max(time),1) trade_date_max_1 FROM {table_name} GROUP BY ths_code) daily
                ON info.ths_code = daily.ths_code
            ) tt
            WHERE date_frm <= if(ths_fund_expiry_date_fund<end_date, ths_fund_expiry_date_fund, end_date) 
            ORDER BY ths_code""".format(table_name=table_name)
    else:
        logger.warning('%s 不存在，仅使用 ifind_pub_fund_info 表进行计算日期范围', table_name)
        sql_str = """SELECT ths_code, date_frm, if(ths_fund_expiry_date_fund<end_date, ths_fund_expiry_date_fund, end_date) date_to
            FROM
            (
                SELECT info.ths_code, ths_lof_listed_date_fund date_frm, ths_fund_expiry_date_fund,
                if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
                FROM ifind_pub_fund_info info 
            ) tt
            WHERE date_frm <= if(ths_fund_expiry_date_fund<end_date, ths_fund_expiry_date_fund, end_date) 
            ORDER BY ths_code"""

    # 计算每只股票需要获取日线数据的日期区间
    with with_db_session(engine_md) as session:
        # 获取每只股票需要获取日线数据的日期区间
        table = session.execute(sql_str)
        # 计算每只股票需要获取日线数据的日期区间
        code_date_range_dic = {
            ths_code: (date_from if begin_time is None else min([date_from, begin_time]), date_to)
            for ths_code, date_from, date_to in table.fetchall() if
            ths_code_set is None or ths_code in ths_code_set}

    if TRIAL:
        date_from_min = date.today() - timedelta(days=(365 * 5))
        # 试用账号只能获取近5年数据
        code_date_range_dic = {
            ths_code: (max([date_from, date_from_min]), date_to)
            for ths_code, (date_from, date_to) in code_date_range_dic.items() if date_from_min <= date_to}

    # 设置 dtype
    dtype = {key: val for key, _, val in indicator_param_list}
    dtype['ths_code'] = String(20)
    dtype['time'] = Date

    data_df_list, data_count, tot_data_count, code_count = [], 0, 0, len(code_date_range_dic)
    try:
        for num, (ths_code, (begin_time, end_time)) in enumerate(code_date_range_dic.items(), start=1):
            logger.debug('%d/%d) %s [%s - %s]', num, code_count, ths_code, begin_time, end_time)
            data_df = invoker.THS_HistoryQuotes(
                ths_code,
                json_indicator,
                'Interval:D,CPS:1,baseDate:1900-01-01,Currency:YSHB,fill:Previous',
                begin_time, end_time
            )
            if data_df is not None and data_df.shape[0] > 0:
                data_count += data_df.shape[0]
                data_df.rename(columns={col: col.lower() for col in data_df.columns}, inplace=True)
                data_df_list.append(data_df)

            if DEBUG and len(data_df_list) > 1:
                break

            # 大于阀值有开始插入
            if data_count >= 10000:
                tot_data_df = pd.concat(data_df_list)
                data_count = bunch_insert_on_duplicate_update(tot_data_df, table_name, engine_md, dtype)
                tot_data_count += data_count
                data_df_list, data_count = [], 0
    finally:
        if len(data_df_list) > 0:
            tot_data_df = pd.concat(data_df_list)
            data_count = bunch_insert_on_duplicate_update(tot_data_df, table_name, engine_md, dtype)
            tot_data_count += data_count

        logging.info("更新 %s 完成 新增数据 %d 条", table_name, tot_data_count)
        if not has_table and engine_md.has_table(table_name):
            alter_table_2_myisam(engine_md, [table_name])
            build_primary_key([table_name])


if __name__ == "__main__":
    TRIAL = True
    # DEBUG = True
    # ths_code = None  # '600006.SH,600009.SH'
    # 股票基本信息数据加载
    # import_pub_fund_info(ths_code)
    # 股票日K数据加载
    ths_code_set = None  # {'600006.SH', '600009.SH'}
    # 股票日K历史数据加载
    import_pub_fund_daily(ths_code_set)
