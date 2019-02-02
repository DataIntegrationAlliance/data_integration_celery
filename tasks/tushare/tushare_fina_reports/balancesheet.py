"""
Created on 2018/9/3
@author: yby
@desc    : 2018-09-3
contact author:ybychem@gmail.com
"""
import pandas as pd
import logging
from tasks.backend.orm import build_primary_key
from datetime import date, datetime, timedelta
from tasks.utils.fh_utils import try_2_date, STR_FORMAT_DATE, datetime_2_str, split_chunk, try_n_times
from tasks import app
from sqlalchemy.types import String, Date, Integer
from sqlalchemy.dialects.mysql import DOUBLE
from tasks.backend import engine_md
from tasks.merge.code_mapping import update_from_info_table
from tasks.utils.db_utils import with_db_session, add_col_2_table, alter_table_2_myisam, \
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

INDICATOR_PARAM_LIST_TUSHARE_STOCK_BALABCESHEET = [
    ('ts_code', String(20)),
    ('ann_date', Date),
    ('f_ann_date', Date),
    ('end_date', Date),
    ('report_type', DOUBLE),
    ('comp_type', DOUBLE),
    ('total_share', DOUBLE),
    ('cap_rese', DOUBLE),
    ('undistr_porfit', DOUBLE),
    ('surplus_rese', DOUBLE),
    ('special_rese', DOUBLE),
    ('money_cap', DOUBLE),
    ('trad_asset', DOUBLE),
    ('notes_receiv', DOUBLE),
    ('accounts_receiv', DOUBLE),
    ('oth_receiv', DOUBLE),
    ('prepayment', DOUBLE),
    ('div_receiv', DOUBLE),
    ('int_receiv', DOUBLE),
    ('inventories', DOUBLE),
    ('amor_exp', DOUBLE),
    ('nca_within_1y', DOUBLE),
    ('sett_rsrv', DOUBLE),
    ('loanto_oth_bank_fi', DOUBLE),
    ('premium_receiv', DOUBLE),
    ('reinsur_receiv', DOUBLE),
    ('reinsur_res_receiv', DOUBLE),
    ('pur_resale_fa', DOUBLE),
    ('oth_cur_assets', DOUBLE),
    ('total_cur_assets', DOUBLE),
    ('fa_avail_for_sale', DOUBLE),
    ('htm_invest', DOUBLE),
    ('lt_eqt_invest', DOUBLE),
    ('invest_real_estate', DOUBLE),
    ('time_deposits', DOUBLE),
    ('oth_assets', DOUBLE),
    ('lt_rec', DOUBLE),
    ('fix_assets', DOUBLE),
    ('cip', DOUBLE),
    ('const_materials', DOUBLE),
    ('fixed_assets_disp', DOUBLE),
    ('produc_bio_assets', DOUBLE),
    ('oil_and_gas_assets', DOUBLE),
    ('intan_assets', DOUBLE),
    ('r_and_d', DOUBLE),
    ('goodwill', DOUBLE),
    ('lt_amor_exp', DOUBLE),
    ('defer_tax_assets', DOUBLE),
    ('decr_in_disbur', DOUBLE),
    ('oth_nca', DOUBLE),
    ('total_nca', DOUBLE),
    ('cash_reser_cb', DOUBLE),
    ('depos_in_oth_bfi', DOUBLE),
    ('prec_metals', DOUBLE),
    ('deriv_assets', DOUBLE),
    ('rr_reins_une_prem', DOUBLE),
    ('rr_reins_outstd_cla', DOUBLE),
    ('rr_reins_lins_liab', DOUBLE),
    ('rr_reins_lthins_liab', DOUBLE),
    ('refund_depos', DOUBLE),
    ('ph_pledge_loans', DOUBLE),
    ('refund_cap_depos', DOUBLE),
    ('indep_acct_assets', DOUBLE),
    ('client_depos', DOUBLE),
    ('client_prov', DOUBLE),
    ('transac_seat_fee', DOUBLE),
    ('invest_as_receiv', DOUBLE),
    ('total_assets', DOUBLE),
    ('lt_borr', DOUBLE),
    ('st_borr', DOUBLE),
    ('cb_borr', DOUBLE),
    ('depos_ib_deposits', DOUBLE),
    ('loan_oth_bank', DOUBLE),
    ('trading_fl', DOUBLE),
    ('notes_payable', DOUBLE),
    ('acct_payable', DOUBLE),
    ('adv_receipts', DOUBLE),
    ('sold_for_repur_fa', DOUBLE),
    ('comm_payable', DOUBLE),
    ('payroll_payable', DOUBLE),
    ('taxes_payable', DOUBLE),
    ('int_payable', DOUBLE),
    ('oth_payable', DOUBLE),
    ('acc_exp', DOUBLE),
    ('deferred_inc', DOUBLE),
    ('st_bonds_payable', DOUBLE),
    ('payable_to_reinsurer', DOUBLE),
    ('rsrv_insur_cont', DOUBLE),
    ('acting_trading_sec', DOUBLE),
    ('acting_uw_sec', DOUBLE),
    ('non_cur_liab_due_1y', DOUBLE),
    ('oth_cur_liab', DOUBLE),
    ('total_cur_liab', DOUBLE),
    ('bond_payable', DOUBLE),
    ('lt_payable', DOUBLE),
    ('specific_payables', DOUBLE),
    ('estimated_liab', DOUBLE),
    ('defer_tax_liab', DOUBLE),
    ('defer_inc_non_cur_liab', DOUBLE),
    ('oth_ncl', DOUBLE),
    ('total_ncl', DOUBLE),
    ('depos_oth_bfi', DOUBLE),
    ('deriv_liab', DOUBLE),
    ('depos', DOUBLE),
    ('agency_bus_liab', DOUBLE),
    ('oth_liab', DOUBLE),
    ('prem_receiv_adva', DOUBLE),
    ('depos_received', DOUBLE),
    ('ph_invest', DOUBLE),
    ('reser_une_prem', DOUBLE),
    ('reser_outstd_claims', DOUBLE),
    ('reser_lins_liab', DOUBLE),
    ('reser_lthins_liab', DOUBLE),
    ('indept_acc_liab', DOUBLE),
    ('pledge_borr', DOUBLE),
    ('indem_payable', DOUBLE),
    ('policy_div_payable', DOUBLE),
    ('total_liab', DOUBLE),
    ('treasury_share', DOUBLE),
    ('ordin_risk_reser', DOUBLE),
    ('forex_differ', DOUBLE),
    ('invest_loss_unconf', DOUBLE),
    ('minority_int', DOUBLE),
    ('total_hldr_eqy_exc_min_int', DOUBLE),
    ('total_hldr_eqy_inc_min_int', DOUBLE),
    ('total_liab_hldr_eqy', DOUBLE),
    ('lt_payroll_payable', DOUBLE),
    ('oth_comp_income', DOUBLE),
    ('oth_eqt_tools', DOUBLE),
    ('oth_eqt_tools_p_shr', DOUBLE),
    ('lending_funds', DOUBLE),
    ('acc_receivable', DOUBLE),
    ('st_fin_payable', DOUBLE),
    ('payables', DOUBLE),
    ('hfs_assets', DOUBLE),
    ('hfs_sales', DOUBLE),
]
# 设置 dtype
DTYPE_TUSHARE_STOCK_BALABCESHEET = {key: val for key, val in INDICATOR_PARAM_LIST_TUSHARE_STOCK_BALABCESHEET}


@try_n_times(times=5, sleep_time=2, logger=logger, exception=Exception, exception_sleep_time=5, timeout=5)
def invoke_balancesheet(ts_code, start_date, end_date):
    invoke_balancesheet = pro.balancesheet(ts_code=ts_code, start_date=start_date, end_date=end_date)
    return invoke_balancesheet


@app.task
def import_tushare_stock_balancesheet(chain_param=None, ts_code_set=None):
    """
    插入股票日线数据到最近一个工作日-1。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    :return:
    """
    table_name = 'tushare_stock_balancesheet'
    logging.info("更新 %s 开始", table_name)

    has_table = engine_md.has_table(table_name)
    # 进行表格判断，确定是否含有tushare_stock_daily
    if has_table:
        sql_str = """
            SELECT ts_code, date_frm, if(delist_date<end_date, delist_date, end_date) date_to
            FROM
            (
                SELECT info.ts_code, ifnull(ann_date, list_date) date_frm, delist_date,
                if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
                FROM 
                  tushare_stock_info info 
                LEFT OUTER JOIN
                    (SELECT ts_code, adddate(max(ann_date),1) ann_date 
                    FROM {table_name} GROUP BY ts_code) balancesheet
                ON info.ts_code = balancesheet.ts_code
            ) tt
            WHERE date_frm <= if(delist_date<end_date, delist_date, end_date) 
            ORDER BY ts_code""".format(table_name=table_name)
    else:
        sql_str = """
            SELECT ts_code, date_frm, if(delist_date<end_date, delist_date, end_date) date_to
            FROM
              (
                SELECT info.ts_code, list_date date_frm, delist_date,
                if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
                FROM tushare_stock_info info 
              ) tt
            WHERE date_frm <= if(delist_date<end_date, delist_date, end_date) 
            ORDER BY ts_code DESC """
        logger.warning('%s 不存在，仅使用 tushare_stock_info 表进行计算日期范围', table_name)
    # ts_code_set = None
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
    logger.info('%d stock balancesheets will been import into tushare_stock_balancesheet', data_len)
    # 将data_df数据，添加到data_df_list

    Cycles = 1
    try:
        for num, (ts_code, (date_from, date_to)) in enumerate(code_date_range_dic.items(), start=1):
            logger.debug('%d/%d) %s [%s - %s]', num, data_len, ts_code, date_from, date_to)
            data_df = invoke_balancesheet(ts_code=ts_code, start_date=datetime_2_str(date_from, STR_FORMAT_DATE_TS),
                                     end_date=datetime_2_str(date_to, STR_FORMAT_DATE_TS))
            # logger.info(' %d data of %s between %s and %s', df.shape[0], ts_code, date_from, date_to)
            # data_df = df
            if len(data_df) > 0 and data_df['ann_date'].iloc[-1] is not None:
                last_date_in_df_last = try_2_date(data_df['ann_date'].iloc[-1])
                while try_2_date(data_df['ann_date'].iloc[-1]) > date_from:
                    df2 = invoke_balancesheet(ts_code=ts_code, start_date=datetime_2_str(date_from, STR_FORMAT_DATE_TS),
                                              end_date=datetime_2_str(try_2_date(data_df['ann_date'].iloc[-1]) - timedelta(days=1),STR_FORMAT_DATE_TS))
                    if len(df2) > 0 and df2['ann_date'].iloc[-1] is not None:
                        last_date_in_df_cur = try_2_date(df2['ann_date'].iloc[-1])
                        if last_date_in_df_cur < last_date_in_df_last:
                            data_df = pd.concat([data_df, df2])
                            # df = df2
                        elif last_date_in_df_cur == last_date_in_df_last:
                            break
                    elif len(df2) <= 0:
                        break
            if data_df is None:
                logger.warning('%d/%d) %s has no data during %s %s', num, data_len, ts_code, date_from, date_to)
                continue
            elif data_df is not None:
                logger.info('整体进度：%d/%d)， %d 条 %s 资产负债表被提取，起止时间为 %s 和 %s', num, data_len, data_df.shape[0], ts_code,date_from, date_to)

            # 把数据攒起来
            if data_df is not None and data_df.shape[0] > 0:
                data_count += data_df.shape[0]
                data_df_list.append(data_df)

            # 大于阀值有开始插入
            if data_count >= 1000 and len(data_df_list) > 0:
                data_df_all = pd.concat(data_df_list)
                bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md,DTYPE_TUSHARE_STOCK_BALABCESHEET)
                logger.info('%d 条资产负债表数据被插入 %s 表', data_count, table_name)
                all_data_count += data_count
                data_df_list, data_count = [], 0
                # # 数据插入数据库
                # data_count = bunch_insert_on_duplicate_update(data_df, table_name, engine_md,
                #                                               DTYPE_TUSHARE_STOCK_BALABCESHEET)
                # logging.info("更新 %s 结束 %d 条信息被更新", table_name, data_count)
                # data_df = []
            # 仅调试使用
            Cycles = Cycles + 1
            if DEBUG and Cycles > 10:
                break
    finally:
        # 导入数据库
        if len(data_df_list) > 0:
            data_df_all = pd.concat(data_df_list)
            data_count = bunch_insert_on_duplicate_update(
                data_df_all, table_name, engine_md, DTYPE_TUSHARE_STOCK_BALABCESHEET,
                primary_keys=['ts_code', 'ann_date'], schema=config.DB_SCHEMA_MD
            )
            all_data_count = all_data_count + data_count
            logging.info("更新 %s 结束 %d 条资产负债表信息被更新", table_name, all_data_count)
            # if not has_table and engine_md.has_table(table_name):
            #     alter_table_2_myisam(engine_md, [table_name])
            #     build_primary_key([table_name])


if __name__ == "__main__":
    # DEBUG = True
    # import_tushare_stock_info(refresh=False)
    # 更新每日股票数据
    import_tushare_stock_balancesheet()

    # 去除重复数据用的
    # sql_str = """SELECT * FROM old_tushare_stock_balancesheet """
    # df=pd.read_sql(sql_str,engine_md)
    # #将数据插入新表
    # data_count = bunch_insert_on_duplicate_update(df, table_name, engine_md, dtype)
    # logging.info("更新 %s 结束 %d 条信息被更新", table_name, data_count)
