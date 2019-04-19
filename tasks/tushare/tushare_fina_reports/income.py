"""
Created on 2018/8/31
@author: yby
@desc    : 2018-08-31
"""
import pandas as pd
import logging
from tasks.backend.orm import build_primary_key
from datetime import date, datetime, timedelta
from ibats_utils.mess import try_2_date, STR_FORMAT_DATE, datetime_2_str, split_chunk, try_n_times
from tasks import app
from sqlalchemy.types import String, Date, Integer
from sqlalchemy.dialects.mysql import DOUBLE
from tasks.backend import engine_md
from tasks.merge.code_mapping import update_from_info_table
from ibats_utils.db import with_db_session, add_col_2_table, alter_table_2_myisam, \
    bunch_insert_on_duplicate_update
from tasks.tushare.ts_pro_api import pro
from tasks.config import config

DEBUG = False
logger = logging.getLogger()
DATE_BASE = datetime.strptime('2005-01-01', STR_FORMAT_DATE).date()
ONE_DAY = timedelta(days=1)
# 标示每天几点以后下载当日行情数据
BASE_LINE_HOUR = 16
STR_FORMAT_DATE_TS = '%Y%m%d'

INDICATOR_PARAM_LIST_TUSHARE_STOCK_INCOME = [
    ('ts_code', String(20)),
    ('ann_date', Date),
    ('f_ann_date', Date),
    ('end_date', Date),
    ('report_type', DOUBLE),
    ('comp_type', DOUBLE),
    ('basic_eps', DOUBLE),
    ('diluted_eps', DOUBLE),
    ('total_revenue', DOUBLE),
    ('revenue', DOUBLE),
    ('int_income', DOUBLE),
    ('prem_earned', DOUBLE),
    ('comm_income', DOUBLE),
    ('n_commis_income', DOUBLE),
    ('n_oth_income', DOUBLE),
    ('n_oth_b_income', DOUBLE),
    ('prem_income', DOUBLE),
    ('out_prem', DOUBLE),
    ('une_prem_reser', DOUBLE),
    ('reins_income', DOUBLE),
    ('n_sec_tb_income', DOUBLE),
    ('n_sec_uw_income', DOUBLE),
    ('n_asset_mg_income', DOUBLE),
    ('oth_b_income', DOUBLE),
    ('fv_value_chg_gain', DOUBLE),
    ('invest_income', DOUBLE),
    ('ass_invest_income', DOUBLE),
    ('forex_gain', DOUBLE),
    ('total_cogs', DOUBLE),
    ('oper_cost', DOUBLE),
    ('int_exp', DOUBLE),
    ('comm_exp', DOUBLE),
    ('biz_tax_surchg', DOUBLE),
    ('sell_exp', DOUBLE),
    ('admin_exp', DOUBLE),
    ('fin_exp', DOUBLE),
    ('assets_impair_loss', DOUBLE),
    ('prem_refund', DOUBLE),
    ('compens_payout', DOUBLE),
    ('reser_insur_liab', DOUBLE),
    ('div_payt', DOUBLE),
    ('reins_exp', DOUBLE),
    ('oper_exp', DOUBLE),
    ('compens_payout_refu', DOUBLE),
    ('insur_reser_refu', DOUBLE),
    ('reins_cost_refund', DOUBLE),
    ('other_bus_cost', DOUBLE),
    ('operate_profit', DOUBLE),
    ('non_oper_income', DOUBLE),
    ('non_oper_exp', DOUBLE),
    ('nca_disploss', DOUBLE),
    ('total_profit', DOUBLE),
    ('income_tax', DOUBLE),
    ('n_income', DOUBLE),
    ('n_income_attr_p', DOUBLE),
    ('minority_gain', DOUBLE),
    ('oth_compr_income', DOUBLE),
    ('t_compr_income', DOUBLE),
    ('compr_inc_attr_p', DOUBLE),
    ('compr_inc_attr_m_s', DOUBLE),
    ('ebit', DOUBLE),
    ('ebitda', DOUBLE),
    ('insurance_exp', DOUBLE),
    ('undist_profit', DOUBLE),
    ('distable_profit', DOUBLE),

]
# 设置 dtype
DTYPE_TUSHARE_STOCK_INCOME = {key: val for key, val in INDICATOR_PARAM_LIST_TUSHARE_STOCK_INCOME}


@try_n_times(times=5, sleep_time=2, logger=logger, exception=Exception, exception_sleep_time=5)
def invoke_income(ts_code, start_date, end_date):
    invoke_income = pro.income(ts_code=ts_code, start_date=start_date, end_date=end_date)
    return invoke_income


@app.task
def import_tushare_stock_income(chain_param=None, ts_code_set=None):
    """
    插入股票日线数据到最近一个工作日-1。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    :return:
    """
    table_name = 'tushare_stock_income'
    logging.info("更新 %s 开始", table_name)
    # wind_indictor_str = ",".join([key for key, _ in param_list])
    # rename_col_dic = {key.upper(): key.lower() for key, _ in param_list}
    has_table = engine_md.has_table(table_name)
    # 进行表格判断，确定是否含有tushare_stock_daily
    if has_table:
        sql_str = """
            SELECT ts_code, date_frm, if(delist_date<end_date, delist_date, end_date) date_to
            FROM
            (
                SELECT info.ts_code, ifnull(ann_date, subdate(list_date,365*10)) date_frm, delist_date,
                if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
                FROM 
                  tushare_stock_info info 
                LEFT OUTER JOIN
                    (SELECT ts_code, adddate(max(ann_date),1) ann_date 
                    FROM {table_name} GROUP BY ts_code) income
                ON info.ts_code = income.ts_code
            ) tt
            WHERE date_frm <= if(delist_date<end_date, delist_date, end_date) 
            ORDER BY ts_code""".format(table_name=table_name)
    else:
        sql_str = """
            SELECT ts_code, date_frm, if(delist_date<end_date, delist_date, end_date) date_to
            FROM
              (
                SELECT info.ts_code, subdate(list_date,365*10) date_frm, delist_date,
                if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
                FROM tushare_stock_info info 
              ) tt
            WHERE date_frm <= if(delist_date<end_date, delist_date, end_date) 
            ORDER BY ts_code DESC """
        logger.warning('%s 不存在，仅使用 tushare_stock_info 表进行计算日期范围', table_name)

    with with_db_session(engine_md) as session:
        # 获取每只股票需要获取日线数据的日期区间
        table = session.execute(sql_str)
        # 计算每只股票需要获取日线数据的日期区间
        begin_time = None
        # 获取date_from,date_to，将date_from,date_to做为value值
        code_date_range_dic = {
            ts_code: (date_from if begin_time is None else min([date_from, begin_time]), date_to)
            for ts_code, date_from, date_to in table.fetchall() if
            ts_code_set is None or ts_code in ts_code_set}

    data_df_list, data_count, all_data_count, data_len = [], 0, 0, len(code_date_range_dic)
    logger.info('%d stocks will been import into wind_stock_daily', data_len)
    # 将data_df数据，添加到data_df_list

    Cycles = 1
    try:
        for num, (ts_code, (date_from, date_to)) in enumerate(code_date_range_dic.items(), start=1):
            logger.debug('%d/%d) %s [%s - %s]', num, data_len, ts_code, date_from, date_to)
            df = invoke_income(ts_code=ts_code, start_date=datetime_2_str(date_from, STR_FORMAT_DATE_TS),
                               end_date=datetime_2_str(date_to, STR_FORMAT_DATE_TS))
            data_df = df
            if data_df is not None and len(data_df) > 0:
                while try_2_date(df['ann_date'].iloc[-1]) > date_from:
                    last_date_in_df_last, last_date_in_df_cur = try_2_date(df['ann_date'].iloc[-1]), None
                    df2 = invoke_income(ts_code=ts_code, start_date=datetime_2_str(date_from, STR_FORMAT_DATE_TS),
                                        end_date=datetime_2_str(try_2_date(df['ann_date'].iloc[-1]) - timedelta(days=1),
                                                                STR_FORMAT_DATE_TS))
                    if df2 is not None and df2.shape[0] > 0:
                        last_date_in_df_cur = try_2_date(df2['ann_date'].iloc[-1])
                        if last_date_in_df_cur < last_date_in_df_last:
                            data_df = pd.concat([data_df, df2])
                            df = df2
                        elif last_date_in_df_cur == last_date_in_df_last:
                            break
                    elif df2 is None or df2.shape[0] <= 0:
                        break
            if data_df is None:
                logger.warning('%d/%d) %s has no data during %s %s', num, data_len, ts_code, date_from, date_to)
                continue
            elif data_df is not None:
                logger.info('%d/%d)， %d 条 %s 的利润表数据被提取，起止时间为 %s 和 %s',
                            num, data_len, data_df.shape[0], ts_code, date_from, date_to)
                # # 数据插入数据库
                # data_df_all = data_df
                # data_count = bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md,
                #                                               DTYPE_TUSHARE_STOCK_INCOME)
                # logging.info("成功更新 %s 结束 %d 条信息被更新", table_name, data_count)

            # 把数据攒起来
            if data_df is not None and data_df.shape[0] > 0:
                data_count += data_df.shape[0]
                data_df_list.append(data_df)
            # 大于阀值有开始插入
            if data_count >= 1000 and len(data_df_list) > 0:
                data_df_all = pd.concat(data_df_list)
                bunch_insert_on_duplicate_update(
                    data_df_all, table_name, engine_md, DTYPE_TUSHARE_STOCK_INCOME,
                    myisam_if_create_table=True,
                    primary_keys=['ts_code', 'ann_date', 'end_date'], schema=config.DB_SCHEMA_MD)
                logger.info('%d 条财务指标将数据插入 %s 表', data_count, table_name)
                all_data_count += data_count
                data_df_list, data_count = [], 0
            # 仅调试使用
            Cycles = Cycles + 1
            if DEBUG and Cycles > 10:
                break
    finally:
        # 导入数据库
        if len(data_df_list) > 0:
            data_df_all = pd.concat(data_df_list)
            data_count = bunch_insert_on_duplicate_update(
                data_df_all, table_name, engine_md, DTYPE_TUSHARE_STOCK_INCOME,
                myisam_if_create_table=True,
                primary_keys=['ts_code', 'ann_date', 'end_date'], schema=config.DB_SCHEMA_MD)
            all_data_count = all_data_count + data_count
            logging.info("更新 %s 结束 %d 条信息被更新", table_name, all_data_count)
            # if not has_table and engine_md.has_table(table_name):
            #     alter_table_2_myisam(engine_md, [table_name])
            #     build_primary_key([table_name])


if __name__ == "__main__":
    # DEBUG = True
    # import_tushare_stock_info(refresh=True)
    # 更新每日股票数据
    # ts_code_set = list(['603297.SH'])
    import_tushare_stock_income(ts_code_set=None)
