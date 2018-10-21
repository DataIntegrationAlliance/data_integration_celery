"""
Created on 2018/8/23
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

DEBUG = False
logger = logging.getLogger()
DATE_BASE = datetime.strptime('2005-01-01', STR_FORMAT_DATE).date()
ONE_DAY = timedelta(days=1)
# 标示每天几点以后下载当日行情数据
BASE_LINE_HOUR = 16
STR_FORMAT_DATE_TS = '%Y%m%d'

INDICATOR_PARAM_LIST_TUSHARE_STOCK_CASHFLOW = [
    ('ts_code', String(20)),
    ('ann_date', Date),
    ('f_ann_date', Date),
    ('end_date', Date),
    ('report_type', DOUBLE),
    ('comp_type', DOUBLE),
    ('net_profit', DOUBLE),
    ('finan_exp', DOUBLE),
    ('c_fr_sale_sg', DOUBLE),
    ('recp_tax_rends', DOUBLE),
    ('n_depos_incr_fi', DOUBLE),
    ('n_incr_loans_cb', DOUBLE),
    ('n_inc_borr_oth_fi', DOUBLE),
    ('prem_fr_orig_contr', DOUBLE),
    ('n_incr_insured_dep', DOUBLE),
    ('n_reinsur_prem', DOUBLE),
    ('n_incr_disp_tfa', DOUBLE),
    ('ifc_cash_incr', DOUBLE),
    ('n_incr_disp_faas', DOUBLE),
    ('n_incr_loans_oth_bank', DOUBLE),
    ('n_cap_incr_repur', DOUBLE),
    ('c_fr_oth_operate_a', DOUBLE),
    ('c_inf_fr_operate_a', DOUBLE),
    ('c_paid_goods_s', DOUBLE),
    ('c_paid_to_for_empl', DOUBLE),
    ('c_paid_for_taxes', DOUBLE),
    ('n_incr_clt_loan_adv', DOUBLE),
    ('n_incr_dep_cbob', DOUBLE),
    ('c_pay_claims_orig_inco', DOUBLE),
    ('pay_handling_chrg', DOUBLE),
    ('pay_comm_insur_plcy', DOUBLE),
    ('oth_cash_pay_oper_act', DOUBLE),
    ('st_cash_out_act', DOUBLE),
    ('n_cashflow_act', DOUBLE),
    ('oth_recp_ral_inv_act', DOUBLE),
    ('c_disp_withdrwl_invest', DOUBLE),
    ('c_recp_return_invest', DOUBLE),
    ('n_recp_disp_fiolta', DOUBLE),
    ('n_recp_disp_sobu', DOUBLE),
    ('stot_inflows_inv_act', DOUBLE),
    ('c_pay_acq_const_fiolta', DOUBLE),
    ('c_paid_invest', DOUBLE),
    ('n_disp_subs_oth_biz', DOUBLE),
    ('oth_pay_ral_inv_act', DOUBLE),
    ('n_incr_pledge_loan', DOUBLE),
    ('stot_out_inv_act', DOUBLE),
    ('n_cashflow_inv_act', DOUBLE),
    ('c_recp_borrow', DOUBLE),
    ('proc_issue_bonds', DOUBLE),
    ('oth_cash_recp_ral_fnc_act', DOUBLE),
    ('stot_cash_in_fnc_act', DOUBLE),
    ('free_cashflow', DOUBLE),
    ('c_prepay_amt_borr', DOUBLE),
    ('c_pay_dist_dpcp_int_exp', DOUBLE),
    ('incl_dvd_profit_paid_sc_ms', DOUBLE),
    ('oth_cashpay_ral_fnc_act', DOUBLE),
    ('stot_cashout_fnc_act', DOUBLE),
    ('n_cash_flows_fnc_act', DOUBLE),
    ('eff_fx_flu_cash', DOUBLE),
    ('n_incr_cash_cash_equ', DOUBLE),
    ('c_cash_equ_beg_period', DOUBLE),
    ('c_cash_equ_end_period', DOUBLE),
    ('c_recp_cap_contrib', DOUBLE),
    ('incl_cash_rec_saims', DOUBLE),
    ('uncon_invest_loss', DOUBLE),
    ('prov_depr_assets', DOUBLE),
    ('depr_fa_coga_dpba', DOUBLE),
    ('amort_intang_assets', DOUBLE),
    ('lt_amort_deferred_exp', DOUBLE),
    ('decr_deferred_exp', DOUBLE),
    ('incr_acc_exp', DOUBLE),
    ('loss_disp_fiolta', DOUBLE),
    ('loss_scr_fa', DOUBLE),
    ('loss_fv_chg', DOUBLE),
    ('invest_loss', DOUBLE),
    ('decr_def_inc_tax_assets', DOUBLE),
    ('incr_def_inc_tax_liab', DOUBLE),
    ('decr_inventories', DOUBLE),
    ('decr_oper_payable', DOUBLE),
    ('incr_oper_payable', DOUBLE),
    ('others', DOUBLE),
    ('im_net_cashflow_oper_act', DOUBLE),
    ('conv_debt_into_cap', DOUBLE),
    ('conv_copbonds_due_within_1y', DOUBLE),
    ('fa_fnc_leases', DOUBLE),
    ('end_bal_cash', DOUBLE),
    ('beg_bal_cash', DOUBLE),
    ('end_bal_cash_equ', DOUBLE),
    ('beg_bal_cash_equ', DOUBLE),
    ('im_n_incr_cash_equ', DOUBLE),

]
# 设置 dtype
DTYPE_TUSHARE_CASHFLOW = {key: val for key, val in INDICATOR_PARAM_LIST_TUSHARE_STOCK_CASHFLOW}


@try_n_times(times=5, sleep_time=2, logger=logger, exception=Exception, exception_sleep_time=5)
def invoke_cashflow(ts_code, start_date, end_date):
    invoke_cashflow = pro.cashflow(ts_code=ts_code, start_date=start_date, end_date=end_date)
    return invoke_cashflow


@app.task
def import_tushare_stock_cashflow(chain_param=None, ts_code_set=None):
    """
    插入股票日线数据到最近一个工作日-1。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    :return:
    """
    table_name = 'tushare_stock_cashflow'
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
                    FROM {table_name} GROUP BY ts_code) cashflow
                ON info.ts_code = cashflow.ts_code
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
            ORDER BY ts_code"""
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

    data_len = len(code_date_range_dic)
    logger.info('%d stocks will been import into wind_stock_daily', data_len)
    # 将data_df数据，添加到data_df_list

    Cycles = 1
    try:
        for num, (ts_code, (date_from, date_to)) in enumerate(code_date_range_dic.items(), start=1):
            logger.debug('%d/%d) %s [%s - %s]', num, data_len, ts_code, date_from, date_to)
            df = invoke_cashflow(ts_code=ts_code, start_date=datetime_2_str(date_from, STR_FORMAT_DATE_TS),
                                 end_date=datetime_2_str(date_to, STR_FORMAT_DATE_TS))
            # logger.info(' %d data of %s between %s and %s', df.shape[0], ts_code, date_from, date_to)
            data_df = df
            if len(data_df) > 0:
                while try_2_date(df['ann_date'].iloc[-1]) > date_from:
                    last_date_in_df_last, last_date_in_df_cur = try_2_date(df['ann_date'].iloc[-1]), None
                    df2 = invoke_cashflow(ts_code=ts_code, start_date=datetime_2_str(date_from, STR_FORMAT_DATE_TS),
                                          end_date=datetime_2_str(
                                              try_2_date(df['ann_date'].iloc[-1]) - timedelta(days=1),
                                              STR_FORMAT_DATE_TS))
                    if len(df2) > 0:
                        last_date_in_df_cur = try_2_date(df2['ann_date'].iloc[-1])
                        if last_date_in_df_cur < last_date_in_df_last:
                            data_df = pd.concat([data_df, df2])
                            df = df2
                        elif last_date_in_df_cur == last_date_in_df_last:
                            break
                        if data_df is None:
                            logger.warning('%d/%d) %s has no data during %s %s', num, data_len, ts_code, date_from,
                                           date_to)
                            continue
                        logger.info('%d/%d) %d data of %s between %s and %s', num, data_len, data_df.shape[0], ts_code,
                                    date_from, date_to)
                    elif len(df2) <= 0:
                        break
                # 数据插入数据库
                data_count = bunch_insert_on_duplicate_update(data_df, table_name, engine_md, DTYPE_TUSHARE_CASHFLOW)
                logging.info("更新 %s 结束 %d 条信息被更新", table_name, data_count)

            # 仅调试使用
            Cycles = Cycles + 1
            if DEBUG and Cycles > 10:
                break
    finally:
        # 导入数据库
        if len(data_df) > 0:
            data_count = bunch_insert_on_duplicate_update(data_df, table_name, engine_md, DTYPE_TUSHARE_CASHFLOW)
            logging.info("更新 %s 结束 %d 条信息被更新", table_name, data_count)
            # if not has_table and engine_md.has_table(table_name):
            #     alter_table_2_myisam(engine_md, [table_name])
            #     build_primary_key([table_name])


if __name__ == "__main__":
    # DEBUG = True
    # import_tushare_stock_info(refresh=False)
    # 更新每日股票数据
    import_tushare_stock_cashflow()

    # sql_str = """SELECT * FROM old_tushare_stock_cashflow """
    # df=pd.read_sql(sql_str,engine_md)
    # #将数据插入新表
    # data_count = bunch_insert_on_duplicate_update(df, table_name, engine_md, dtype)
    # logging.info("更新 %s 结束 %d 条信息被更新", table_name, data_count)

    # df = invoke_cashflow(ts_code='000001.SZ', start_date='19900101', end_date='20180830')
