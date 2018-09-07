"""
Created on 2018/9/3
@author: yby
@desc    : 2018-09-3
contact author:ybychem@gmail.com
"""

import tushare as ts
import pandas as pd
import logging
from tasks.backend.orm import build_primary_key
from datetime import date, datetime, timedelta
from tasks.utils.fh_utils import try_2_date,STR_FORMAT_DATE,datetime_2_str,split_chunk,try_n_times
from tasks import app
from sqlalchemy.types import String, Date, Integer
from sqlalchemy.dialects.mysql import DOUBLE
from tasks.backend import engine_md
from tasks.merge.code_mapping import update_from_info_table
from tasks.utils.db_utils import with_db_session, add_col_2_table, alter_table_2_myisam, \
    bunch_insert_on_duplicate_update

DEBUG = False
logger = logging.getLogger()
pro = ts.pro_api()
DATE_BASE = datetime.strptime('2005-01-01', STR_FORMAT_DATE).date()
ONE_DAY = timedelta(days=1)
# 标示每天几点以后下载当日行情数据
BASE_LINE_HOUR = 16
STR_FORMAT_DATE_TS = '%Y%m%d'

@try_n_times(times=60, sleep_time=0,exception_sleep_time=120)
def invoke_fina_indicator(ts_code,start_date,end_date,fields):
    invoke_fina_indicator=pro.fina_indicator(ts_code=ts_code,start_date=start_date,end_date=end_date,fields=fields)
    return invoke_fina_indicator

# df = invoke_fina_indicator(ts_code='600000.SH',start_date='19980801',end_date='20180820',fields=fields)
# df0=pro.fina_indicator(ts_code='600000.SH',start_date='19980801',end_date='20180820',fields=fields)


@app.task
def import_tushare_stock_fina_indicator(chain_param=None,ts_code_set=None):
    """
    插入股票日线数据到最近一个工作日-1。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    :return:
    """
    table_name = 'tushare_stock_fina_indicator'
    logging.info("更新 %s 开始", table_name)
    param_list = [
        ('ts_code', String(20)),
        ('ann_date', Date),
        ('end_date', Date),
        ('eps', DOUBLE),
        ('dt_eps', DOUBLE),
        ('total_revenue_ps', DOUBLE),
        ('revenue_ps', DOUBLE),
        ('capital_rese_ps', DOUBLE),
        ('surplus_rese_ps', DOUBLE),
        ('undist_profit_ps', DOUBLE),
        ('extra_item', DOUBLE),
        ('profit_dedt', DOUBLE),
        ('gross_margin', DOUBLE),
        ('current_ratio', DOUBLE),
        ('quick_ratio', DOUBLE),
        ('cash_ratio', DOUBLE),
        ('invturn_days', DOUBLE),
        ('arturn_days', DOUBLE),
        ('inv_turn', DOUBLE),
        ('ar_turn', DOUBLE),
        ('ca_turn', DOUBLE),
        ('fa_turn', DOUBLE),
        ('assets_turn', DOUBLE),
        ('op_income', DOUBLE),
        ('valuechange_income', DOUBLE),
        ('interst_income', DOUBLE),
        ('daa', DOUBLE),
        ('ebit', DOUBLE),
        ('ebitda', DOUBLE),
        ('fcff', DOUBLE),
        ('fcfe', DOUBLE),
        ('current_exint', DOUBLE),
        ('noncurrent_exint', DOUBLE),
        ('interestdebt', DOUBLE),
        ('netdebt', DOUBLE),
        ('tangible_asset', DOUBLE),
        ('working_capital', DOUBLE),
        ('networking_capital', DOUBLE),
        ('invest_capital', DOUBLE),
        ('retained_earnings', DOUBLE),
        ('diluted2_eps', DOUBLE),
        ('bps', DOUBLE),
        ('ocfps', DOUBLE),
        ('retainedps', DOUBLE),
        ('cfps', DOUBLE),
        ('ebit_ps', DOUBLE),
        ('fcff_ps', DOUBLE),
        ('fcfe_ps', DOUBLE),
        ('netprofit_margin', DOUBLE),
        ('grossprofit_margin', DOUBLE),
        ('cogs_of_sales', DOUBLE),
        ('expense_of_sales', DOUBLE),
        ('profit_to_gr', DOUBLE),
        ('saleexp_to_gr', DOUBLE),
        ('adminexp_of_gr', DOUBLE),
        ('finaexp_of_gr', DOUBLE),
        ('impai_ttm', DOUBLE),
        ('gc_of_gr', DOUBLE),
        ('op_of_gr', DOUBLE),
        ('ebit_of_gr', DOUBLE),
        ('roe', DOUBLE),
        ('roe_waa', DOUBLE),
        ('roe_dt', DOUBLE),
        ('roa', DOUBLE),
        ('npta', DOUBLE),
        ('roic', DOUBLE),
        ('roe_yearly', DOUBLE),
        ('roa2_yearly', DOUBLE),
        ('roe_avg', DOUBLE),
        ('opincome_of_ebt', DOUBLE),
        ('investincome_of_ebt', DOUBLE),
        ('n_op_profit_of_ebt', DOUBLE),
        ('tax_to_ebt', DOUBLE),
        ('dtprofit_to_profit', DOUBLE),
        ('salescash_to_or', DOUBLE),
        ('ocf_to_or', DOUBLE),
        ('ocf_to_opincome', DOUBLE),
        ('capitalized_to_da', DOUBLE),
        ('debt_to_assets', DOUBLE),
        ('assets_to_eqt', DOUBLE),
        ('dp_assets_to_eqt', DOUBLE),
        ('ca_to_assets', DOUBLE),
        ('nca_to_assets', DOUBLE),
        ('tbassets_to_totalassets', DOUBLE),
        ('int_to_talcap', DOUBLE),
        ('eqt_to_talcapital', DOUBLE),
        ('currentdebt_to_debt', DOUBLE),
        ('longdeb_to_debt', DOUBLE),
        ('ocf_to_shortdebt', DOUBLE),
        ('debt_to_eqt', DOUBLE),
        ('eqt_to_debt', DOUBLE),
        ('eqt_to_interestdebt', DOUBLE),
        ('tangibleasset_to_debt', DOUBLE),
        ('tangasset_to_intdebt', DOUBLE),
        ('tangibleasset_to_netdebt', DOUBLE),
        ('ocf_to_debt', DOUBLE),
        ('ocf_to_interestdebt', DOUBLE),
        ('ocf_to_netdebt', DOUBLE),
        ('ebit_to_interest', DOUBLE),
        ('longdebt_to_workingcapital', DOUBLE),
        ('ebitda_to_debt', DOUBLE),
        ('turn_days', DOUBLE),
        ('roa_yearly', DOUBLE),
        ('roa_dp', DOUBLE),
        ('fixed_assets', DOUBLE),
        ('profit_prefin_exp', DOUBLE),
        ('non_op_profit', DOUBLE),
        ('op_to_ebt', DOUBLE),
        ('nop_to_ebt', DOUBLE),
        ('ocf_to_profit', DOUBLE),
        ('cash_to_liqdebt', DOUBLE),
        ('cash_to_liqdebt_withinterest', DOUBLE),
        ('op_to_liqdebt', DOUBLE),
        ('op_to_debt', DOUBLE),
        ('roic_yearly', DOUBLE),
        ('total_fa_trun', DOUBLE),
        ('profit_to_op', DOUBLE),
        ('q_opincome', DOUBLE),
        ('q_investincome', DOUBLE),
        ('q_dtprofit', DOUBLE),
        ('q_eps', DOUBLE),
        ('q_netprofit_margin', DOUBLE),
        ('q_gsprofit_margin', DOUBLE),
        ('q_exp_to_sales', DOUBLE),
        ('q_profit_to_gr', DOUBLE),
        ('q_saleexp_to_gr', DOUBLE),
        ('q_adminexp_to_gr', DOUBLE),
        ('q_finaexp_to_gr', DOUBLE),
        ('q_impair_to_gr_ttm', DOUBLE),
        ('q_gc_to_gr', DOUBLE),
        ('q_op_to_gr', DOUBLE),
        ('q_roe', DOUBLE),
        ('q_dt_roe', DOUBLE),
        ('q_npta', DOUBLE),
        ('q_opincome_to_ebt', DOUBLE),
        ('q_investincome_to_ebt', DOUBLE),
        ('q_dtprofit_to_profit', DOUBLE),
        ('q_salescash_to_or', DOUBLE),
        ('q_ocf_to_sales', DOUBLE),
        ('q_ocf_to_or', DOUBLE),
        ('basic_eps_yoy', DOUBLE),
        ('dt_eps_yoy', DOUBLE),
        ('cfps_yoy', DOUBLE),
        ('op_yoy', DOUBLE),
        ('ebt_yoy', DOUBLE),
        ('netprofit_yoy', DOUBLE),
        ('dt_netprofit_yoy', DOUBLE),
        ('ocf_yoy', DOUBLE),
        ('roe_yoy', DOUBLE),
        ('bps_yoy', DOUBLE),
        ('assets_yoy', DOUBLE),
        ('eqt_yoy', DOUBLE),
        ('tr_yoy', DOUBLE),
        ('or_yoy', DOUBLE),
        ('q_gr_yoy', DOUBLE),
        ('q_gr_qoq', DOUBLE),
        ('q_sales_yoy', DOUBLE),
        ('q_sales_qoq', DOUBLE),
        ('q_op_yoy', DOUBLE),
        ('q_op_qoq', DOUBLE),
        ('q_profit_yoy', DOUBLE),
        ('q_profit_qoq', DOUBLE),
        ('q_netprofit_yoy', DOUBLE),
        ('q_netprofit_qoq', DOUBLE),
        ('equity_yoy', DOUBLE),
        ('rd_exp', DOUBLE),

    ]

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
                    FROM {table_name} GROUP BY ts_code) fina_indicator
                ON info.ts_code = fina_indicator.ts_code
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
    # 设置 dtype
    dtype = {key: val for key, val in param_list}
    # dtype['ts_code'] = String(20)
    # dtype['trade_date'] = Date

    fields='ts_code','ann_date','end_date','eps','dt_eps','total_revenue_ps','revenue_ps','capital_rese_ps','surplus_rese_ps',\
    'undist_profit_ps','extra_item','profit_dedt','gross_margin','current_ratio','quick_ratio','cash_ratio','invturn_days','arturn_days',\
    'inv_turn','ar_turn','ca_turn','fa_turn','assets_turn','op_income','valuechange_income','interst_income','daa','ebit','ebitda','fcff',\
    'fcfe','current_exint','noncurrent_exint','interestdebt','netdebt','tangible_asset','working_capital','networking_capital','invest_capital',\
    'retained_earnings','diluted2_eps','bps','ocfps','retainedps','cfps','ebit_ps','fcff_ps','fcfe_ps','netprofit_margin','grossprofit_margin',\
    'cogs_of_sales','expense_of_sales','profit_to_gr','saleexp_to_gr','adminexp_of_gr','finaexp_of_gr','impai_ttm','gc_of_gr','op_of_gr',\
    'ebit_of_gr','roe','roe_waa','roe_dt','roa','npta','roic','roe_yearly','roa2_yearly','roe_avg','opincome_of_ebt','investincome_of_ebt',\
    'n_op_profit_of_ebt','tax_to_ebt','dtprofit_to_profit','salescash_to_or','ocf_to_or','ocf_to_opincome','capitalized_to_da','debt_to_assets',\
    'assets_to_eqt','dp_assets_to_eqt','ca_to_assets','nca_to_assets','tbassets_to_totalassets','int_to_talcap','eqt_to_talcapital','currentdebt_to_debt',\
    'longdeb_to_debt','ocf_to_shortdebt','debt_to_eqt','eqt_to_debt','eqt_to_interestdebt','tangibleasset_to_debt','tangasset_to_intdebt',\
    'tangibleasset_to_netdebt','ocf_to_debt','ocf_to_interestdebt','ocf_to_netdebt','ebit_to_interest','longdebt_to_workingcapital','ebitda_to_debt',\
    'turn_days','roa_yearly','roa_dp','fixed_assets','profit_prefin_exp','non_op_profit','op_to_ebt','nop_to_ebt','ocf_to_profit','cash_to_liqdebt',\
    'cash_to_liqdebt_withinterest','op_to_liqdebt','op_to_debt','roic_yearly','total_fa_trun','profit_to_op','q_opincome','q_investincome','q_dtprofit',\
    'q_eps','q_netprofit_margin','q_gsprofit_margin','q_exp_to_sales','q_profit_to_gr','q_saleexp_to_gr','q_adminexp_to_gr','q_finaexp_to_gr',\
    'q_impair_to_gr_ttm','q_gc_to_gr','q_op_to_gr','q_roe','q_dt_roe','q_npta','q_opincome_to_ebt','q_investincome_to_ebt','q_dtprofit_to_profit',\
    'q_salescash_to_or','q_ocf_to_sales','q_ocf_to_or','basic_eps_yoy','dt_eps_yoy','cfps_yoy','op_yoy','ebt_yoy','netprofit_yoy','dt_netprofit_yoy',\
    'ocf_yoy','roe_yoy','bps_yoy','assets_yoy','eqt_yoy','tr_yoy','or_yoy','q_gr_yoy','q_gr_qoq','q_sales_yoy','q_sales_qoq','q_op_yoy','q_op_qoq',\
    'q_profit_yoy','q_profit_qoq','q_netprofit_yoy','q_netprofit_qoq','equity_yoy','rd_exp'

    data_len = len(code_date_range_dic)
    logger.info('%d stocks will been import into wind_stock_daily', data_len)
    # 将data_df数据，添加到data_df_list

    Cycles=1
    try:
        for num, (ts_code, (date_from, date_to)) in enumerate(code_date_range_dic.items(), start=1):
            logger.debug('%d/%d) %s [%s - %s]', num, data_len,ts_code, date_from, date_to)
            df = invoke_fina_indicator(ts_code=ts_code, start_date=datetime_2_str(date_from,STR_FORMAT_DATE_TS),end_date=datetime_2_str(date_to,STR_FORMAT_DATE_TS),fields=fields)
            # logger.info(' %d data of %s between %s and %s', df.shape[0], ts_code, date_from, date_to)
            data_df=df
            if len(data_df)>0:
                while try_2_date(df['ann_date'].iloc[-1]) > date_from:
                    last_date_in_df_last, last_date_in_df_cur = try_2_date(df['ann_date'].iloc[-1]), None
                    df2 = invoke_fina_indicator(ts_code=ts_code,start_date=datetime_2_str(date_from,STR_FORMAT_DATE_TS),
                                    end_date=datetime_2_str(try_2_date(df['ann_date'].iloc[-1])-timedelta(days=1),STR_FORMAT_DATE_TS),fields=fields)
                    if len(df2)>0:
                        last_date_in_df_cur = try_2_date(df2['ann_date'].iloc[-1])
                        if last_date_in_df_cur<last_date_in_df_last:
                            data_df = pd.concat([data_df, df2])
                            df = df2
                        elif last_date_in_df_cur==last_date_in_df_last:
                            break
                        if data_df is None:
                            logger.warning('%d/%d) %s has no data during %s %s', num, data_len, ts_code, date_from, date_to)
                            continue
                        logger.info('%d/%d) %d data of %s between %s and %s', num, data_len, data_df.shape[0], ts_code, date_from,date_to)
                    elif len(df2)<=0:
                        break
                #数据插入数据库
                data_count = bunch_insert_on_duplicate_update(data_df, table_name, engine_md, dtype)
                logging.info("更新 %s 结束 %d 条信息被更新", table_name, data_count)
                data_df=[]
            #仅调试使用
            Cycles=Cycles+1
            if DEBUG and Cycles > 10:
                break
    finally:
        # 导入数据库
        if len(data_df) > 0:

            data_count = bunch_insert_on_duplicate_update(data_df, table_name, engine_md, dtype)
            logging.info("更新 %s 结束 %d 条信息被更新", table_name, data_count)
            # if not has_table and engine_md.has_table(table_name):
            #     alter_table_2_myisam(engine_md, [table_name])
            #     build_primary_key([table_name])



if __name__ == "__main__":
    #DEBUG = True
    #import_tushare_stock_info(refresh=False)
    # 更新每日股票数据
    import_tushare_stock_fina_indicator()

# sql_str = """SELECT * FROM old_tushare_stock_balancesheet """
# df=pd.read_sql(sql_str,engine_md)
# #将数据插入新表
# data_count = bunch_insert_on_duplicate_update(df, table_name, engine_md, dtype)
# logging.info("更新 %s 结束 %d 条信息被更新", table_name, data_count)


#下面代码是生成fields和par的
# sub=pd.read_excel('tasks/tushare/fina_indicator.xlsx',header=0)[['code','types']]
# for a, b in [tuple(x) for x in sub.values]:
#     #print("('%s', %s)," % (a, b))
#     print("'%s'," % (a))
