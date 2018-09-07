import pandas as pd
import logging
from tasks import app
from direstinvoker.utils.fh_utils import date_2_str
from tasks import bunch_insert_on_duplicate_update, alter_table_2_myisam, build_primary_key
from tasks.backend import engine_md
from tasks.utils.db_utils import with_db_session
from sqlalchemy.types import String, Date, Integer, Text
from tasks.utils.fh_utils import is_nan_or_none
from tasks.tushare.stock import DTYPE_TUSHARE_DAILY
from tasks.merge import get_ifind_daily_df, generate_range

logger = logging.getLogger()
DEBUG = False


def get_ifind_daily_df(table_name, date_from) -> pd.DataFrame:
    if date_from is None:
        sql_str = "select * from {table_name}".format(table_name=table_name)
        data_df = pd.read_sql(sql_str, engine_md)  # , index_col='ths_code'
    else:
        sql_str = "select * from {table_name} where time >= %s".format(table_name=table_name)
        data_df = pd.read_sql(sql_str, engine_md, params=[date_from])  # , index_col='ths_code'
    return data_df


def merge_tushare_daily(ths_code_set: set = None, date_from=None):
    table_name = 'tushare_daily'
    logging.info("合成 %s 开始", table_name)
    has_table = engine_md.has_table(table_name)
    if date_from is None and has_table:
        sql_str = "select adddate(max(`time`),1) from {table_name}".format(table_name=table_name)
        with with_db_session(engine_md) as session:
            date_from = date_2_str(session.execute(sql_str).scalar())
    # 獲取各個表格數據
    ifind_his_df = get_ifind_daily_df('tushare_stock_daily', date_from)
    ifind_ds_df = get_ifind_daily_df('tushare_daily_basic', date_from)
    ifind_suspend_df = get_ifind_daily_df('tushare_suspend', None)
    ifind_cashflow_df = get_ifind_daily_df('tushare_stock_cashflow', None)
    ifind_balancesheet_df = get_ifind_daily_df('tushare_stock_balancesheet', None)
    ifind_icome_df = get_ifind_daily_df('tushare_stock_icome', None)
    ifind_indicator_df = get_ifind_daily_df('tushare_stock_fina_indicator', None)

    ifind_his_ds_df = pd.merge(ifind_his_df, ifind_ds_df, how='outer',
                               on=['ts_code', 'trade_date'])
    ifind_merge_df = pd.merge(ifind_cashflow_df, ifind_balancesheet_df, ifind_icome_df, ifind_indicator_df,
                              how='outer')
    ifind_merge_df_g = ifind_merge_df.goupby('ths_code')
    ifind_his_ds_df_g = ifind_his_ds_df.grouptushare_daily_basic('ths_code')
    logging.debug('提取数据完成')

    suspend_date_dic_dic = {}
    # 計算財務紕漏事件
    for suspend_date_g in [ifind_suspend_df.groupby('ts_code','suspend_date')]:
        for num, ((ts_code, suspend_date), date_df) in enumerate(suspend_date_g):
            if ths_code_set is not None and ts_code not in ths_code_set:
                continue
            if is_nan_or_none(suspend_date):
                continue
            suspend_date_dic = suspend_date_dic_dic.setdefault(ts_code, {})
            if ts_code not in ifind_merge_df_g.size():
                logger.error('fin 表中不存在 %s 的財務數據', ts_code)
                continue
            ifind_merge_df_temp = ifind_merge_df_g.get_group(ts_code)
            if suspend_date not in suspend_date_dic_dic:
                ifind_merge_df_temp = ifind_merge_df_temp[ifind_merge_df_temp['time'] <= suspend_date]
                if ifind_merge_df_temp.shape[0] > 0:
                    suspend_date_dic[suspend_date] = ifind_merge_df_temp.sort_values('time').iloc[0]
    # 設置dtype
    dtype = {'suspend_date': Date}


    # # 计算 财报披露时间
    # report_date_dic_dic = {}
    # for report_date_g in [ifind_report_date_df.groupby(['ths_code', 'ths_regular_report_actual_dd_stock'])]:
    #     for num, ((ths_code, report_date), data_df) in enumerate(report_date_g, start=1):
    #         if ths_code_set is not None and ths_code not in ths_code_set:
    #             continue
    #         if is_nan_or_none(report_date):
    #             continue
    #         report_date_dic = report_date_dic_dic.setdefault(ths_code, {})
    #         if ths_code not in ifind_fin_df_g.size():
    #             logger.error('fin 表中不存在 %s 的財務數據', ths_code)
    #             continue
    #         ifind_fin_df_temp = ifind_fin_df_g.get_group(ths_code)
    #         if report_date not in report_date_dic_dic:
    #             ifind_fin_df_temp = ifind_fin_df_temp[ifind_fin_df_temp['time'] <= report_date]
    #             if ifind_fin_df_temp.shape[0] > 0:
    #                 report_date_dic[report_date] = ifind_fin_df_temp.sort_values('time').iloc[0]
    #
    # # # 设置 dtype
    # dtype = {'report_date': Date}拼接後續有nan
    # for dic in [DTYPE_STOCK_DAILY_DS, DTYPE_STOCK_REPORT_DATE, DTYPE_STOCK_DAILY_FIN, DTYPE_STOCK_DAILY_HIS]:
    #     fo# ifind_fin_df_g = ifind_fin_df.groupby('ths_code')
    # # 合并 ds his 数据
    # ifind_his_ds_df = pd.merge(ifind_his_df, ifind_ds_df, how='outer',
    #                            on=['ths_code', 'time'])  # 拼接後續有nan,無數據
    # ifind_his_ds_df_g = ifind_his_ds_df.groupby('ths_code')
    # logging.debug("提取数据完成")r key, val in dic.items():
    #         dtype[key] = val
    #
    # logging.debug("计算财报日期完成")
    # # 整理 data_df 数据
    # tot_data_count, data_count, data_df_list, for_count = 0, 0, [], len(report_date_dic_dic)
    # try:
    #     for num, (ths_code, report_date_dic) in enumerate(report_date_dic_dic.items(), start=1):  # key:ths_code
    #         # TODO: 檢查判斷 ths_code 是否存在在ifind_fin_df_g 裏面,,size暫時使用  以後在驚醒改進
    #         if ths_code not in ifind_his_ds_df_g.size():
    #             logger.error('fin 表中不存在 %s 的財務數據', ths_code)
    #             continue
    #         # open low  等 is NAN 2438
    #         ifind_his_ds_df_cur_ths_code = ifind_his_ds_df_g.get_group(ths_code)  # shape[1] 30
    #         logger.debug('%d/%d) 处理 %s %d 条数据', num, for_count, ths_code, ifind_his_ds_df_cur_ths_code.shape[0])
    #         report_date_list = list(report_date_dic.keys())
    #         report_date_list.sort()
    #         for report_date_from, report_date_to in generate_range(report_date_list):
    #             logger.debug('%d/%d) 处理 %s [%s - %s]',
    #                          num, for_count, ths_code, date_2_str(report_date_from), date_2_str(report_date_to))
    #             # 计算有效的日期范围
    #             if report_date_from is None:
    #                 is_fit = ifind_his_ds_df_cur_ths_code['time'] < report_date_to
    #             elif report_date_to is None:
    #                 is_fit = ifind_his_ds_df_cur_ths_code['time'] >= report_date_from
    #             else:
    #                 is_fit = (ifind_his_ds_df_cur_ths_code['time'] < report_date_to) & (
    #                         ifind_his_ds_df_cur_ths_code['time'] >= report_date_from)
    #             # 获取日期范围内的数据
    #             ifind_his_ds_df_segment = ifind_his_ds_df_cur_ths_code[is_fit].copy()
    #             segment_count = ifind_his_ds_df_segment.shape[0]
    #             if segment_count == 0:
    #                 continue
    #             fin_s = report_date_dic[report_date_from] if report_date_from is not None else None
    #             for key in DTYPE_STOCK_DAILY_FIN.keys():
    #                 if key in ('ths_code', 'time'):
    #                     continue
    #                 ifind_his_ds_df_segment[key] = fin_s[key] if fin_s is not None and key in fin_s else None
    #             ifind_his_ds_df_segment['report_date'] = report_date_from
    #             # 添加数据到列表
    #             data_df_list.append(ifind_his_ds_df_segment)
    #             data_count += segment_count
    #
    #         if DEBUG and len(data_df_list) > 1:
    #             break
    #
    #         # 保存数据库
    #         if data_count > 10000:
    #             # 保存到数据库
    #             data_df = pd.concat(data_df_list)
    #             data_count = bunch_insert_on_duplicate_update(data_df, table_name, engine_md, dtype)
    #             tot_data_count += data_count
    #             data_count, data_df_list = 0, []

    # finally:
    #     # 保存到数据库
    #     if len(data_df_list) > 0:
    #         data_df = pd.concat(data_df_list)
    #         data_count = bunch_insert_on_duplicate_update(data_df, table_name, engine_md, dtype)
    #         tot_data_count += data_count
    #
    #     logger.info('%s 新增或更新记录 %d 条', table_name, tot_data_count)
    #     if not has_table and engine_md.has_table(table_name):
    #         alter_table_2_myisam(engine_md, [table_name])
    #         build_primary_key([table_name])


if __name__ == "__main__":
    # data_df = merge_stock_info()
    # print(data_df)
    ths_code_set = {'600618.SH'}
    merge_tushare_daily(ths_code_set)
