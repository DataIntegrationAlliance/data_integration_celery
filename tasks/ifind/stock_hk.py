# -*- coding: utf-8 -*-
"""
Created on 2018/1/17
@author: MG
@contact : mmmaaaggg@163.com
@desc    : 2018-08-20 已经正式运行测试完成，可以正常使用
"""

import logging
from datetime import date, datetime, timedelta
import pandas as pd
from tasks.backend.orm import build_primary_key
from tasks.ifind import invoker
from tasks.utils.fh_utils import STR_FORMAT_DATE, str_2_date
from sqlalchemy.types import String, Date, Integer, Text
from sqlalchemy.dialects.mysql import DOUBLE
from tasks.utils.fh_utils import unzip_join
from tasks.utils.db_utils import with_db_session, add_col_2_table, alter_table_2_myisam
from tasks.backend import engine_md
from tasks import app
from tasks.merge.code_mapping import update_from_info_table
from tasks.utils.db_utils import bunch_insert_on_duplicate_update

DEBUG = False
logger = logging.getLogger()
DATE_BASE = datetime.strptime('1990-01-01', STR_FORMAT_DATE).date()
ONE_DAY = timedelta(days=1)
# 标示每天几点以后下载当日行情数据
BASE_LINE_HOUR = 20
TRIAL = False
# daily_ds 表
INDICATOR_PARAM_LIST_STOCK_HK_DAILY_DS = [
    ('ths_ss_vol_hks', '', DOUBLE),
    ('ths_ss_amt_d_hks', '', String(10)),
    ('ths_trading_status_hks', '', String(120)),
    ('ths_suspen_reason_hks', '', String(300)),
    ('ths_td_currency_d_hks', '', String(10)),
    ('ths_original_currency_hks', '', String(80)),
    ('ths_corp_total_shares_hks', '', DOUBLE),
    ('ths_pe_ttm_hks', '101', DOUBLE),
    ('ths_pcf_operating_cash_flow_ttm_hks', '101', DOUBLE),
    ('ths_pcf_cash_net_flow_ttm_hks', '101', DOUBLE),
    ('ths_ps_ttm_hks', '101', DOUBLE),
    ('ths_market_value_hks', 'HKD', DOUBLE),
]
# 设置 dtype
DTYPE_STOCK_HK_DAILY_DS = {key: val for key, _, val in INDICATOR_PARAM_LIST_STOCK_HK_DAILY_DS}
DTYPE_STOCK_HK_DAILY_DS['ths_code'] = String(20)
DTYPE_STOCK_HK_DAILY_DS['time'] = Date
# daily_his 表
INDICATOR_PARAM_LIST_STOCK_HK_DAILY_HIS = [
    ('preClose', '', DOUBLE),
    ('open', '', DOUBLE),
    ('high', '', DOUBLE),
    ('low', '', DOUBLE),
    ('close', '', DOUBLE),
    ('avgPrice', '', DOUBLE),
    ('changeRatio', '', DOUBLE),
    ('change', '', DOUBLE),
    ('volume', '', DOUBLE),
    ('amount', '', DOUBLE),
    ('turnoverRatio', '', DOUBLE),
]
# 设置 dtype
DTYPE_STOCK_HK_DAILY_HIS = {key: val for key, _, val in INDICATOR_PARAM_LIST_STOCK_HK_DAILY_HIS}
DTYPE_STOCK_HK_DAILY_HIS['ths_code'] = String(20)
DTYPE_STOCK_HK_DAILY_HIS['time'] = Date
# report_date 表
INDICATOR_PARAM_LIST_STOCK_HK_REPORT_DATE = [
    ('ths_perf_briefing_fore_dsclsr_date_hks', '', Date),  # 预披露日期
    ('ths_perf_brief_actual_dd_hks', '', Date),  # 实际披露日期
    ('ths_perf_report_foredsclsr_date_hks', '', Date),  # 预披露日期
    ('ths_perf_report_actual_dd_hks', '', Date),  # 实际披露日期
]
# 设置 dtype
DTYPE_STOCK_HK_REPORT_DATE = {key: val for key, _, val in INDICATOR_PARAM_LIST_STOCK_HK_REPORT_DATE}
DTYPE_STOCK_HK_REPORT_DATE['ths_code'] = String(20)
DTYPE_STOCK_HK_REPORT_DATE['time'] = Date
# fin 表
INDICATOR_PARAM_LIST_STOCK_HK_FIN = [
    ('ths_cce_hks', '2013,100,OC', DOUBLE),
    ('ths_total_liab_hks', '2013,100,OC', DOUBLE),
    ('ths_ebit_ttm_hks', 'OC,101', DOUBLE),
    ('ths_asset_liab_ratio_hks', '', DOUBLE),
    ('ths_current_ratio_hks', '', DOUBLE),
    ('ths_quick_ratio_hks', '', DOUBLE),
    ('ths_ebit_to_interest_fee_hks', '', DOUBLE),
    ('ths_roe_avg_by_ths_hks', '', DOUBLE),
    ('ths_roic_hks', '', DOUBLE),
    ('ths_np_yoy_hks', '', DOUBLE),
    ('ths_net_asset_yoy_hks', '', DOUBLE),
    ('ths_roe_dltd_yoy_hks', '', DOUBLE),
]
# 设置 dtype
DTYPE_STOCK_HK_FIN = {key: val for key, _, val in INDICATOR_PARAM_LIST_STOCK_HK_FIN}
DTYPE_STOCK_HK_FIN['ths_code'] = String(20)
DTYPE_STOCK_HK_FIN['time'] = Date


def get_stock_hk_code_set(date_fetch):
    date_fetch_str = date_fetch.strftime(STR_FORMAT_DATE)
    stock_df = invoker.THS_DataPool('block', date_fetch_str + ';011001012', 'thscode:Y,security_name:Y')
    if stock_df is None:
        logging.warning('%s 获取股票代码失败', date_fetch_str)
        return None
    stock_count = stock_df.shape[0]
    logging.info('get %d ths_code on %s', stock_count, date_fetch_str)
    return set(stock_df['THSCODE'])


@app.task
def import_stock_hk_info(ths_code=None, refresh=False):
    """

    :param ths_code:
    :param refresh:
    :return:
    """
    table_name = 'ifind_stock_hk_info'
    logging.info("更新 %s 开始", table_name)
    if ths_code is None:
        # 获取全市场港股代码及名称
        if refresh:
            date_fetch = datetime.strptime('1991-02-01', STR_FORMAT_DATE).date()
        else:
            date_fetch = date.today()

        date_end = date.today()
        stock_hk_code_set = set()
        while date_fetch < date_end:
            stock_hk_code_set_sub = get_stock_hk_code_set(date_fetch)
            if stock_hk_code_set_sub is not None:
                stock_hk_code_set |= stock_hk_code_set_sub
            date_fetch += timedelta(days=365)

        stock_hk_code_set_sub = get_stock_hk_code_set(date_end)
        if stock_hk_code_set_sub is not None:
            stock_hk_code_set |= stock_hk_code_set_sub

        if DEBUG:
            stock_hk_code_set = list(stock_hk_code_set)[:10]

        ths_code = ','.join(stock_hk_code_set)

    indicator_param_list = [
        ('ths_stock_short_name_hks', '', String(40)),
        ('ths_stock_code_hks', '', String(20)),
        ('ths_isin_code_hks', '', String(40)),
        ('ths_corp_ashare_short_name_hks', '', String(10)),
        ('ths_corp_ashare_code_hks', '', String(60)),
        ('ths_stock_varieties_hks', '', String(40)),
        ('ths_ipo_date_hks', '', Date),
        ('ths_listed_exchange_hks', '', String(60)),
        ('ths_stop_listing_date_hks', '', Date),
        ('ths_corp_cn_name_hks', '', String(120)),
        ('ths_corp_name_en_hks', '', String(120)),
        ('ths_established_date_hks', '', Date),
        ('ths_accounting_date_hks', '', String(20)),
        ('ths_general_manager_hks', '', String(40)),
        ('ths_secretary_hks', '', String(40)),
        ('ths_operating_scope_hks', '', Text),
        ('ths_mo_product_name_hks', '', String(200)),
        ('ths_district_hks', '', String(60)),
        ('ths_reg_address_hks', '', String(200)),
        ('ths_office_address_hks', '', String(200)),
        ('ths_corp_tel_hks', '', String(200)),
        ('ths_corp_fax_hks', '', String(200)),
        ('ths_corp_website_hks', '', String(200)),
        ('ths_auditor_hks', '', String(60)),
        ('ths_legal_counsel_hks', '', String(300)),
        ('ths_hs_industry_hks', '', String(40)),
    ]
    # jsonIndicator='ths_stock_short_name_hks;ths_stock_code_hks;ths_thscode_hks;ths_isin_code_hks;ths_corp_ashare_short_name_hks;ths_corp_ashare_code_hks;ths_stock_varieties_hks;ths_ipo_date_hks;ths_listed_exchange_hks;ths_stop_listing_date_hks;ths_corp_cn_name_hks;ths_corp_name_en_hks;ths_established_date_hks;ths_accounting_date_hks;ths_general_manager_hks;ths_secretary_hks;ths_operating_scope_hks;ths_mo_product_name_hks;ths_district_hks;ths_reg_address_hks;ths_office_address_hks;ths_corp_tel_hks;ths_corp_fax_hks;ths_corp_website_hks;ths_auditor_hks;ths_legal_counsel_hks;ths_hs_industry_hks'
    # jsonparam=';;;;;;;;;;;'
    indicator, param = unzip_join([(key, val) for key, val, _ in indicator_param_list], sep=';')
    param += '100'
    data_df = invoker.THS_BasicData(ths_code, indicator, param)
    if data_df is None or data_df.shape[0] == 0:
        logging.info("没有可用的 stock_hk info 可以更新")
        return
    # 删除历史数据，更新数据
    has_table = engine_md.has_table(table_name)
    if has_table:
        with with_db_session(engine_md) as session:
            session.execute(
                "DELETE FROM {table_name} WHERE ths_code IN (".format(table_name=table_name) + ','.join(
                    [':code%d' % n for n in range(len(stock_hk_code_set))]
                ) + ")",
                params={'code%d' % n: val for n, val in enumerate(stock_hk_code_set)})
            session.commit()
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
def import_stock_hk_daily_ds(ths_code_set: set = None, begin_time=None):
    """
    通过date_serise接口将历史数据保存到 ifind_stock_hk_daily_ds，该数据作为 History数据的补充数据 例如：复权因子af、涨跌停标识、停牌状态、原因等
    :param ths_code_set:
    :param begin_time:
    :return:
    """
    table_name = 'ifind_stock_hk_daily_ds'
    info_table_name = 'ifind_stock_hk_info'
    # jsonIndicator='ths_pre_close_stock;ths_open_price_stock;ths_high_price_stock;ths_low_stock;ths_close_price_stock;ths_chg_ratio_stock;ths_chg_stock;ths_vol_stock;ths_trans_num_stock;ths_amt_stock;ths_turnover_ratio_stock;ths_vaild_turnover_stock;ths_af_stock;ths_up_and_down_status_stock;ths_trading_status_stock;ths_suspen_reason_stock;ths_last_td_date_stock'
    # jsonparam='100;100;100;100;100;;100;100;;;;;;;;;'
    json_indicator, json_param = unzip_join([(key, val) for key, val, _ in INDICATOR_PARAM_LIST_STOCK_HK_DAILY_DS],
                                            sep=';')
    has_table = engine_md.has_table(table_name)
    if has_table:
        sql_str = """SELECT ths_code, date_frm, if(ths_stop_listing_date_hks<end_date, ths_stop_listing_date_hks, end_date) date_to
            FROM
            (
                SELECT info.ths_code, ifnull(trade_date_max_1, ths_ipo_date_hks) date_frm, ths_stop_listing_date_hks,
                if(hour(now())<19, subdate(curdate(),1), curdate()) end_date
                FROM 
                    {info_table_name} info 
                LEFT OUTER JOIN
                    (SELECT ths_code, adddate(max(time),1) trade_date_max_1 FROM {table_name} GROUP BY ths_code) daily
                ON info.ths_code = daily.ths_code
            ) tt
            WHERE date_frm <= if(ths_stop_listing_date_hks<end_date, ths_stop_listing_date_hks, end_date) 
            ORDER BY ths_code""".format(table_name=table_name, info_table_name=info_table_name)
    else:
        sql_str = """SELECT ths_code, date_frm, if(ths_stop_listing_date_hks<end_date, ths_stop_listing_date_hks, end_date) date_to
            FROM
            (
                SELECT info.ths_code, ths_ipo_date_hks date_frm, ths_stop_listing_date_hks,
                if(hour(now())<19, subdate(curdate(),1), curdate()) end_date
                FROM {info_table_name} info 
            ) tt
            WHERE date_frm <= if(ths_stop_listing_date_hks<end_date, ths_stop_listing_date_hks, end_date) 
            ORDER BY ths_code""".format(info_table_name=info_table_name)
        logger.warning('%s 不存在，仅使用 %s 表进行计算日期范围', table_name, info_table_name)
    with with_db_session(engine_md) as session:
        # 获取每只股票需要获取日线数据的日期区间
        table = session.execute(sql_str)

        # 获取每只股票需要获取日线数据的日期区间
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
            if data_df is not None and data_df.shape[0] > 0:
                data_count += data_df.shape[0]
                data_df_list.append(data_df)

            # 仅调试使用
            if DEBUG and len(data_df_list) > 0:
                break

            # 大于阀值有开始插入
            if data_count >= 2000:
                tot_data_df = pd.concat(data_df_list)
                # tot_data_df.to_sql(table_name, engine_md, if_exists='append', index=False, dtype=dtype)
                bunch_insert_on_duplicate_update(tot_data_df, table_name, engine_md, DTYPE_STOCK_HK_DAILY_DS)
                tot_data_count += data_count
                data_df_list, data_count = [], 0

    finally:
        if data_count > 0:
            tot_data_df = pd.concat(data_df_list)
            # tot_data_df.to_sql(table_name, engine_md, if_exists='append', index=False, dtype=dtype)
            bunch_insert_on_duplicate_update(tot_data_df, table_name, engine_md, DTYPE_STOCK_HK_DAILY_DS)
            tot_data_count += data_count

        logging.info("更新 %s 完成 新增数据 %d 条", table_name, tot_data_count)

        if not has_table and engine_md.has_table(table_name):
            alter_table_2_myisam(engine_md, [table_name])
            build_primary_key([table_name])


@app.task
def import_stock_hk_daily_his(ths_code_set: set = None, begin_time=None):
    """
    通过history接口将历史数据保存到 ifind_stock_hk_daily_his
    :param ths_code_set:
    :param begin_time: 默认为None，如果非None则代表所有数据更新日期不得晚于该日期
    :return:
    """
    table_name = 'ifind_stock_hk_daily_his'
    if begin_time is not None and type(begin_time) == date:
        begin_time = str_2_date(begin_time)

    # THS_HistoryQuotes('600006.SH,600010.SH',
    # 'preClose,open,high,low,close,avgPrice,changeRatio,volume,amount,turnoverRatio,transactionAmount,totalShares,totalCapital,floatSharesOfAShares,floatSharesOfBShares,floatCapitalOfAShares,floatCapitalOfBShares,pe_ttm,pe,pb,ps,pcf',
    # 'Interval:D,CPS:1,baseDate:1900-01-01,Currency:YSHB,fill:Previous',
    # '2018-06-30','2018-07-30')
    json_indicator, _ = unzip_join([(key, val) for key, val, _ in INDICATOR_PARAM_LIST_STOCK_HK_DAILY_HIS], sep=';')
    has_table = engine_md.has_table(table_name)
    if has_table:
        sql_str = """SELECT ths_code, date_frm, if(ths_stop_listing_date_hks<end_date, ths_stop_listing_date_hks, end_date) date_to
            FROM
            (
                SELECT info.ths_code, ifnull(trade_date_max_1, ths_ipo_date_hks) date_frm, ths_stop_listing_date_hks,
                if(hour(now())<19, subdate(curdate(),1), curdate()) end_date
                FROM 
                    ifind_stock_hk_info info 
                LEFT OUTER JOIN
                    (SELECT ths_code, adddate(max(time),1) trade_date_max_1 FROM {table_name} GROUP BY ths_code) daily
                ON info.ths_code = daily.ths_code
            ) tt
            WHERE date_frm <= if(ths_stop_listing_date_hks<end_date, ths_stop_listing_date_hks, end_date) 
            ORDER BY ths_code""".format(table_name=table_name)
    else:
        logger.warning('ifind_stock_hk_daily_his 不存在，仅使用 ifind_stock_hk_info 表进行计算日期范围')
        sql_str = """SELECT ths_code, date_frm, if(ths_stop_listing_date_hks<end_date, ths_stop_listing_date_hks, end_date) date_to
            FROM
            (
                SELECT info.ths_code, ths_ipo_date_hks date_frm, ths_stop_listing_date_hks,
                if(hour(now())<19, subdate(curdate(),1), curdate()) end_date
                FROM ifind_stock_hk_info info 
            ) tt
            WHERE date_frm <= if(ths_stop_listing_date_hks<end_date, ths_stop_listing_date_hks, end_date) 
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
                data_df_list.append(data_df)
            # 大于阀值有开始插入
            if data_count >= 10000:
                data_count = save_ifind_stock_hk_daily_his(table_name, data_df_list, DTYPE_STOCK_HK_DAILY_HIS)
                tot_data_count += data_count
                data_df_list, data_count = [], 0
    finally:
        if data_count > 0:
            data_count = save_ifind_stock_hk_daily_his(table_name, data_df_list, DTYPE_STOCK_HK_DAILY_HIS)
            tot_data_count += data_count

        logging.info("更新 %s 完成 新增数据 %d 条", table_name, tot_data_count)

        if not has_table and engine_md.has_table(table_name):
            alter_table_2_myisam(engine_md, [table_name])
            build_primary_key([table_name])


def save_ifind_stock_hk_daily_his(table_name, data_df_list, dtype):
    """保存数据到 table_name"""
    if len(data_df_list) > 0:
        tot_data_df = pd.concat(data_df_list)
        # tot_data_df.to_sql(table_name, engine_md, if_exists='append', index=False, dtype=dtype)
        # data_count = tot_data_df.shape[0]
        data_count = bunch_insert_on_duplicate_update(tot_data_df, table_name, engine_md, dtype)
        # logger.info('保存数据到 %s 成功，包含 %d 条记录', table_name, data_count)
        return data_count
    else:
        return 0


@app.task
def add_new_col_data(col_name, param, db_col_name=None, col_type_str='DOUBLE', ths_code_set: set = None):
    """
    1）修改 daily 表，增加字段
    2）ckpv表增加数据
    3）第二部不见得1天能够完成，当第二部完成后，将ckvp数据更新daily表中
    :param col_name:增加字段名称
    :param param: 参数
    :param dtype: 数据库字段类型
    :param db_col_name: 默认为 None，此时与col_name相同
    :param col_type_str: DOUBLE, VARCHAR(20), INTEGER, etc. 不区分大小写
    :param ths_code_set: 默认 None， 否则仅更新指定 ths_code
    :return:
    """
    table_name = 'ifind_stock_hk_daily_ds'
    if db_col_name is None:
        # 默认为 None，此时与col_name相同
        db_col_name = col_name

    # 检查当前数据库是否存在 db_col_name 列，如果不存在则添加该列
    add_col_2_table(engine_md, table_name, db_col_name, col_type_str)
    # 将数据增量保存到 ckdvp 表
    all_finished = add_data_2_ckdvp(col_name, param, ths_code_set)
    # 将数据更新到 ds 表中
    if all_finished:
        sql_str = """update {table_name} daily, ifind_ckdvp_stock_hk ckdvp
            set daily.{db_col_name} = ckdvp.value
            where daily.ths_code = ckdvp.ths_code
            and daily.time = ckdvp.time
            and ckdvp.key = '{db_col_name}'
            and ckdvp.param = '{param}'""".format(db_col_name=db_col_name, param=param, table_name=table_name)
        with with_db_session(engine_md) as session:
            session.execute(sql_str)
            session.commit()
        logger.info('更新 %s 字段 %s 表', db_col_name, table_name)


def add_data_2_ckdvp(json_indicator, json_param, ths_code_set: set = None, begin_time=None):
    """
    将数据增量保存到 ifind_ckdvp_stock_hk 表，code key date value param 五个字段组合成的表 value 为 Varchar(80)
    该表用于存放各种新增加字段的值
    查询语句举例：
    THS_DateSerial('600007.SH,600009.SH','ths_pe_ttm_stock','101','Days:Tradedays,Fill:Previous,Interval:D','2018-07-31','2018-07-31')
    :param json_indicator:
    :param json_param:
    :param ths_code_set:
    :param begin_time:
    :return: 全部数据加载完成，返回True，否则False，例如数据加载中途流量不够而中断
    """
    all_finished = False
    table_name = 'ifind_ckdvp_stock_hk'
    has_table = engine_md.has_table(table_name)
    if has_table:
        sql_str = """
            select ths_code, date_frm, if(ths_stop_listing_date_hks<end_date, ths_stop_listing_date_hks, end_date) date_to
            FROM
            (
                select info.ths_code, ifnull(trade_date_max_1, ths_ipo_date_hks) date_frm, ths_stop_listing_date_hks,
                    if(hour(now())<19, subdate(curdate(),1), curdate()) end_date
                from 
                    ifind_stock_hk_info info 
                left outer join
                    (select ths_code, adddate(max(time),1) trade_date_max_1 from {table_name} 
                        where {table_name}.key='{json_indicator}' and param='{json_param}' group by ths_code
                    ) daily
                on info.ths_code = daily.ths_code
            ) tt
            where date_frm <= if(ths_stop_listing_date_hks<end_date, ths_stop_listing_date_hks, end_date) 
            order by ths_code""".format(json_indicator=json_indicator, json_param=json_param, table_name=table_name)
    else:
        logger.warning('%s 不存在，仅使用 ifind_stock_hk_info 表进行计算日期范围', table_name)
        sql_str = """
            SELECT ths_code, date_frm, 
                if(ths_stop_listing_date_hks<end_date, ths_stop_listing_date_hks, end_date) date_to
            FROM
            (
                SELECT info.ths_code, ths_ipo_date_hks date_frm, ths_stop_listing_date_hks,
                if(hour(now())<19, subdate(curdate(),1), curdate()) end_date
                FROM ifind_stock_hk_info info 
            ) tt
            WHERE date_frm <= if(ths_stop_listing_date_hks<end_date, ths_stop_listing_date_hks, end_date) 
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

    # 设置 dtype
    dtype = {
        'ths_code': String(20),
        'key': String(80),
        'time': Date,
        'value': String(80),
        'param': String(80),
    }
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
            if data_df is not None and data_df.shape[0] > 0:
                data_df['key'] = json_indicator
                data_df['param'] = json_param
                data_df.rename(columns={json_indicator: 'value'}, inplace=True)
                data_count += data_df.shape[0]
                data_df_list.append(data_df)

            # 大于阀值有开始插入
            if data_count >= 10000:
                tot_data_df = pd.concat(data_df_list)
                # tot_data_df.to_sql(table_name, engine_md, if_exists='append', index=False, dtype=dtype)
                data_count = bunch_insert_on_duplicate_update(tot_data_df, table_name, engine_md, dtype)
                tot_data_count += data_count
                data_df_list, data_count = [], 0

            # 仅调试使用
            if DEBUG and len(data_df_list) > 1:
                break

            all_finished = True
    finally:
        if data_count > 0:
            tot_data_df = pd.concat(data_df_list)
            # tot_data_df.to_sql(table_name, engine_md, if_exists='append', index=False, dtype=dtype)
            data_count = bunch_insert_on_duplicate_update(tot_data_df, table_name, engine_md, dtype)
            tot_data_count += data_count

        if not has_table and engine_md.has_table(table_name):
            create_pk_str = """ALTER TABLE {table_name}
                CHANGE COLUMN `ths_code` `ths_code` VARCHAR(20) NOT NULL ,
                CHANGE COLUMN `time` `time` DATE NOT NULL ,
                CHANGE COLUMN `key` `key` VARCHAR(80) NOT NULL ,
                CHANGE COLUMN `param` `param` VARCHAR(80) NOT NULL ,
                ADD PRIMARY KEY (`ths_code`, `time`, `key`, `param`)""".format(table_name=table_name)
            with with_db_session(engine_md) as session:
                session.execute(create_pk_str)

        logging.info("更新 %s 完成 新增数据 %d 条", table_name, tot_data_count)

    return all_finished


@app.task
def import_stock_hk_report_date(ths_code_set: set = None, begin_time=None, interval='Q'):
    """
    通过date_serise接口将历史财务数据保存到 ifind_stock_fin，国内财务数据按季度发布，因此获取周期为季度（默认）
    :param ths_code_set:
    :param begin_time:
    :param interval: Q 季度 M 月 W 周 D 日
    :return:
    """
    table_name = 'ifind_stock_hk_report_date'
    info_table_name = 'ifind_stock_hk_info'
    has_table = engine_md.has_table(table_name)
    # jsonIndicator='ths_perf_briefing_fore_dsclsr_date_hks;ths_perf_brief_actual_dd_hks;ths_perf_report_foredsclsr_date_hks;ths_perf_report_actual_dd_hks'
    # jsonparam=';'
    json_indicator, json_param = unzip_join([(key, val) for key, val, _ in INDICATOR_PARAM_LIST_STOCK_HK_REPORT_DATE], sep=';')
    if has_table:
        sql_str = """SELECT ths_code, date_frm, if(ths_stop_listing_date_hks<end_date, ths_stop_listing_date_hks, end_date) date_to
            FROM
            (
                SELECT info.ths_code, ifnull(trade_date_max_1, ths_ipo_date_hks) date_frm, ths_stop_listing_date_hks,
                if(hour(now())<19, subdate(curdate(),1), curdate()) end_date
                FROM 
                    {info_table_name} info 
                LEFT OUTER JOIN
                    (SELECT ths_code, adddate(max(time),1) trade_date_max_1 FROM {table_name} GROUP BY ths_code) daily
                ON info.ths_code = daily.ths_code
            ) tt
            WHERE date_frm <= if(ths_stop_listing_date_hks<end_date, ths_stop_listing_date_hks, end_date) 
            ORDER BY ths_code""".format(table_name=table_name, info_table_name=info_table_name)
    else:
        sql_str = """SELECT ths_code, date_frm, if(ths_stop_listing_date_hks<end_date, ths_stop_listing_date_hks, end_date) date_to
            FROM
            (
                SELECT info.ths_code, ths_ipo_date_hks date_frm, ths_stop_listing_date_hks,
                if(hour(now())<19, subdate(curdate(),1), curdate()) end_date
                FROM {info_table_name} info 
            ) tt
            WHERE date_frm <= if(ths_stop_listing_date_hks<end_date, ths_stop_listing_date_hks, end_date) 
            ORDER BY ths_code""".format(info_table_name=info_table_name)
        logger.warning('%s 不存在，仅使用 %s 表进行计算日期范围', table_name, info_table_name)
    with with_db_session(engine_md) as session:
        # 获取每只股票需要获取日线数据的日期区间
        table = session.execute(sql_str)
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

    data_df_list, data_count, tot_data_count, code_count = [], 0, 0, len(code_date_range_dic)
    try:
        for num, (ths_code, (begin_time, end_time)) in enumerate(code_date_range_dic.items(), start=1):
            logger.debug('%d/%d) %s [%s - %s]', num, code_count, ths_code, begin_time, end_time)
            data_df = invoker.THS_DateSerial(
                ths_code,
                json_indicator,
                json_param,
                "Days:Tradedays,Fill:Previous,Interval:{interval}".format(interval=interval),
                begin_time, end_time
            )
            if data_df is not None and data_df.shape[0] > 0:
                data_count += data_df.shape[0]
                data_df_list.append(data_df)
            # 大于阀值有开始插入
            if data_count >= 10000:
                data_df_all = pd.concat(data_df_list)
                # data_df_all.to_sql(table_name, engine_md, if_exists='append', index=False, dtype=dtype)
                data_count = bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md, DTYPE_STOCK_HK_REPORT_DATE)
                tot_data_count += data_count
                data_df_list, data_count = [], 0

            # 仅调试使用
            if DEBUG and len(data_df_list) > 1:
                break
    finally:
        if data_count > 0:
            data_df_all = pd.concat(data_df_list)
            # data_df_all.to_sql(table_name, engine_md, if_exists='append', index=False, dtype=dtype)
            data_count = bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md, DTYPE_STOCK_HK_REPORT_DATE)
            tot_data_count += data_count

        if not has_table and engine_md.has_table(table_name):
            alter_table_2_myisam(engine_md, [table_name])
            build_primary_key([table_name])

        logging.info("更新 %s 完成 新增数据 %d 条", table_name, tot_data_count)


@app.task
def import_stock_hk_fin_quarterly(ths_code_set: set = None, begin_time=None):
    """
    通过date_serise接口将历史数据保存到 import_stock_hk_fin
    该数据作为 为季度获取
    :param ths_code_set:
    :param begin_time:
    :return:
    """
    table_name = 'ifind_stock_hk_fin'
    info_table_name = 'ifind_stock_hk_info'
    # ths_cce_hks;ths_total_liab_hks;ths_ebit_ttm_hks
    # jsonparam='2013,100,OC;2013,100,OC;OC,101'
    json_indicator, json_param = unzip_join([(key, val) for key, val, _ in INDICATOR_PARAM_LIST_STOCK_HK_FIN], sep=';')
    has_table = engine_md.has_table(table_name)
    if has_table:
        sql_str = """SELECT ths_code, date_frm, if(ths_stop_listing_date_hks<end_date, ths_stop_listing_date_hks, end_date) date_to
            FROM
            (
                SELECT info.ths_code, ifnull(trade_date_max_1, ths_ipo_date_hks) date_frm, ths_stop_listing_date_hks,
                if(hour(now())<19, subdate(curdate(),1), curdate()) end_date
                FROM 
                    {info_table_name} info 
                LEFT OUTER JOIN
                    (SELECT ths_code, adddate(max(time),1) trade_date_max_1 FROM {table_name} GROUP BY ths_code) daily
                ON info.ths_code = daily.ths_code
            ) tt
            WHERE date_frm <= if(ths_stop_listing_date_hks<end_date, ths_stop_listing_date_hks, end_date) 
            ORDER BY ths_code""".format(table_name=table_name, info_table_name=info_table_name)
    else:
        sql_str = """SELECT ths_code, date_frm, if(ths_stop_listing_date_hks<end_date, ths_stop_listing_date_hks, end_date) date_to
            FROM
            (
                SELECT info.ths_code, ths_ipo_date_hks date_frm, ths_stop_listing_date_hks,
                if(hour(now())<19, subdate(curdate(),1), curdate()) end_date
                FROM {info_table_name} info 
            ) tt
            WHERE date_frm <= if(ths_stop_listing_date_hks<end_date, ths_stop_listing_date_hks, end_date) 
            ORDER BY ths_code""".format(info_table_name=info_table_name)
        logger.warning('%s 不存在，仅使用 %s 表进行计算日期范围', table_name, info_table_name)
    with with_db_session(engine_md) as session:
        # 获取每只股票需要获取日线数据的日期区间
        table = session.execute(sql_str)
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

    data_df_list, data_count, tot_data_count, code_count = [], 0, 0, len(code_date_range_dic)
    try:
        for num, (ths_code, (begin_time, end_time)) in enumerate(code_date_range_dic.items(), start=1):
            logger.debug('%d/%d) %s [%s - %s]', num, code_count, ths_code, begin_time, end_time)
            data_df = invoker.THS_DateSerial(
                ths_code,
                json_indicator,
                json_param,
                'Days:Tradedays,Fill:Previous,Interval:Q',
                begin_time, end_time
            )
            if data_df is not None and data_df.shape[0] > 0:
                data_count += data_df.shape[0]
                data_df_list.append(data_df)

            # 仅调试使用
            if DEBUG and len(data_df_list) > 0:
                break

            # 大于阀值有开始插入
            if data_count >= 2000:
                tot_data_df = pd.concat(data_df_list)
                # tot_data_df.to_sql(table_name, engine_md, if_exists='append', index=False, dtype=dtype)
                bunch_insert_on_duplicate_update(tot_data_df, table_name, engine_md, DTYPE_STOCK_HK_FIN)
                tot_data_count += data_count
                data_df_list, data_count = [], 0

    finally:
        if data_count > 0:
            tot_data_df = pd.concat(data_df_list)
            # tot_data_df.to_sql(table_name, engine_md, if_exists='append', index=False, dtype=dtype)
            bunch_insert_on_duplicate_update(tot_data_df, table_name, engine_md, DTYPE_STOCK_HK_FIN)
            tot_data_count += data_count

        logging.info("更新 %s 完成 新增数据 %d 条", table_name, tot_data_count)

        if not has_table and engine_md.has_table(table_name):
            alter_table_2_myisam(engine_md, [table_name])
            build_primary_key([table_name])


@app.task
def import_stock_hk_fin_by_report_date_weekly(ths_code_set: set = None, begin_time=None, refresh=False):
    """
    通过date_serise接口将历史数据保存到 import_stock_hk_fin
    该数据作为 为周度获取
    以财务报表发布日期为进准，[ 财务报表发布日-14天 ~ 财务报表发布日]，周度获取财务数据
    :param ths_code_set:
    :param begin_time:
    :param refresh: 全部刷新
    :return:
    """
    table_name = 'ifind_stock_hk_fin'
    info_table_name = 'ifind_stock_hk_info'
    # ths_cce_hks;ths_total_liab_hks;ths_ebit_ttm_hks
    # jsonparam='2013,100,OC;2013,100,OC;OC,101'
    json_indicator, json_param = unzip_join([(key, val) for key, val, _ in INDICATOR_PARAM_LIST_STOCK_HK_FIN], sep=';')
    has_table = engine_md.has_table(table_name)
    ths_code_report_date_str = """select distinct ths_code, subdate(report_date, 14), report_date from
        (
        select ths_code, ths_perf_brief_actual_dd_hks report_date from ifind_stock_hk_report_date
        union
        select ths_code, ths_perf_report_actual_dd_hks report_date from ifind_stock_hk_report_date
        ) tt
        where report_date is not null
        order by ths_code, report_date"""

    if has_table:
        sql_str = """SELECT ths_code, date_frm, if(ths_stop_listing_date_hks<end_date, ths_stop_listing_date_hks, end_date) date_to
            FROM
            (
                SELECT info.ths_code, ifnull(trade_date_max_1, ths_ipo_date_hks) date_frm, ths_stop_listing_date_hks,
                if(hour(now())<19, subdate(curdate(),1), curdate()) end_date
                FROM 
                    {info_table_name} info 
                LEFT OUTER JOIN
                    (SELECT ths_code, adddate(max(time),1) trade_date_max_1 FROM {table_name} GROUP BY ths_code) daily
                ON info.ths_code = daily.ths_code
            ) tt
            WHERE date_frm <= if(ths_stop_listing_date_hks<end_date, ths_stop_listing_date_hks, end_date) 
            ORDER BY ths_code""".format(table_name=table_name, info_table_name=info_table_name)
    else:
        sql_str = """SELECT ths_code, date_frm, if(ths_stop_listing_date_hks<end_date, ths_stop_listing_date_hks, end_date) date_to
            FROM
            (
                SELECT info.ths_code, ths_ipo_date_hks date_frm, ths_stop_listing_date_hks,
                if(hour(now())<19, subdate(curdate(),1), curdate()) end_date
                FROM {info_table_name} info 
            ) tt
            WHERE date_frm <= if(ths_stop_listing_date_hks<end_date, ths_stop_listing_date_hks, end_date) 
            ORDER BY ths_code""".format(info_table_name=info_table_name)
        logger.warning('%s 不存在，仅使用 %s 表进行计算日期范围', table_name, info_table_name)
    with with_db_session(engine_md) as session:
        # 获取报告日-10天到报告日日期范围列表
        table = session.execute(ths_code_report_date_str)
        ths_code_report_date_range_list_dic, ths_code_report_date_range_list_dic_tmp = {}, {}
        for ths_code, date_from, date_to in table.fetchall():
            if ths_code_set is None or ths_code in ths_code_set:
                ths_code_report_date_range_list_dic_tmp.setdefault(ths_code, []).append((date_from, date_to))

        # 获取每只股票需要获取日线数据的日期区间
        if not refresh:
            # 如果全部刷新，则忽略 code_date_range_dic 的日期范围的限制
            table = session.execute(sql_str)
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
        else:
            code_date_range_dic = {}

    # 合并重叠的日期
    for ths_code, date_range_list in ths_code_report_date_range_list_dic_tmp.items():
        if not refresh and ths_code in code_date_range_dic:
            code_date_range = code_date_range_dic[ths_code]
        else:
            code_date_range = None

        # date_range_list 按照 起始日期 顺序排序，下层循环主要作用是将具有重叠日期的日期范围进行合并
        date_range_list_new, date_from_last, date_to_last = [], None, None
        for date_from, date_to in date_range_list:
            if code_date_range is not None:
                # 如果全部刷新，则忽略 code_date_range_dic 的日期范围的限制
                if not refresh and (date_to < code_date_range[0] or code_date_range[1] < date_from):
                    continue

            if date_from_last is None:
                # 首次循环 设置 date_from_last
                date_from_last = date_from
            elif date_from < date_to_last:
                # 日期重叠，需要合并
                pass
            else:
                # 日期未重叠，保存 range
                date_range_list_new.append((date_from_last, date_to_last))
                date_from_last = date_from

            # 循环底部，设置 date_to_last
            date_to_last = date_to

        # 循环结束，保存 range
        if date_from_last is not None and date_to_last is not None:
            date_range_list_new.append((date_from_last, date_to_last))

        if len(date_range_list_new) > 0:
            ths_code_report_date_range_list_dic[ths_code] = date_range_list_new

    data_df_list, data_count, tot_data_count, code_count = [], 0, 0, len(ths_code_report_date_range_list_dic)
    try:
        for num, (ths_code, date_range_list) in enumerate(ths_code_report_date_range_list_dic.items(), start=1):
            for begin_time, end_time in date_range_list:
                logger.debug('%d/%d) %s [%s - %s]', num, code_count, ths_code, begin_time, end_time)
                data_df = invoker.THS_DateSerial(
                    ths_code,
                    json_indicator,
                    json_param,
                    'Days:Tradedays,Fill:Previous,Interval:W',
                    begin_time, end_time
                )
                if data_df is not None and data_df.shape[0] > 0:
                    data_count += data_df.shape[0]
                    data_df_list.append(data_df)

                # 仅调试使用
                if DEBUG and len(data_df_list) > 0:
                    break

                # 大于阀值有开始插入
                if data_count >= 2000:
                    tot_data_df = pd.concat(data_df_list)
                    # tot_data_df.to_sql(table_name, engine_md, if_exists='append', index=False, dtype=dtype)
                    bunch_insert_on_duplicate_update(tot_data_df, table_name, engine_md, DTYPE_STOCK_HK_FIN)
                    tot_data_count += data_count
                    data_df_list, data_count = [], 0

    finally:
        if data_count > 0:
            tot_data_df = pd.concat(data_df_list)
            # tot_data_df.to_sql(table_name, engine_md, if_exists='append', index=False, dtype=dtype)
            bunch_insert_on_duplicate_update(tot_data_df, table_name, engine_md, DTYPE_STOCK_HK_FIN)
            tot_data_count += data_count

        logging.info("更新 %s 完成 新增数据 %d 条", table_name, tot_data_count)

        if not has_table and engine_md.has_table(table_name):
            alter_table_2_myisam(engine_md, [table_name])
            build_primary_key([table_name])


if __name__ == "__main__":
    # DEBUG = True
    TRIAL = True
    # 股票基本信息数据加载
    ths_code = None  # '600006.SH,600009.SH'
    refresh = False
    import_stock_hk_info(ths_code, refresh=refresh)

    # 股票日K历史数据加载
    ths_code_set = None  # {'600006.SH', '600009.SH'}
    import_stock_hk_daily_his(ths_code_set)
    # 股票日K数据加载
    ths_code_set = None  # {'600006.SH', '600009.SH'}
    import_stock_hk_daily_ds(ths_code_set)
    # 添加新字段
    # add_new_col_data('ths_pe_ttm_stock', '101', ths_code_set=ths_code_set)
    # 股票财务报告日期
    interval = 'Q'
    import_stock_hk_report_date(interval=interval)
    # 股票财务数据
    # import_stock_hk_fin_quarterly()
    # 补充股票财务数据
    refresh = True
    import_stock_hk_fin_by_report_date_weekly()
