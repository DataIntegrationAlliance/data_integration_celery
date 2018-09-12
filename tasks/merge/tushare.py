import pandas as pd
import logging
# import tushare as ts
from direstinvoker.utils.fh_utils import date_2_str
from tasks.backend import engine_md
from tasks.utils.db_utils import with_db_session
from sqlalchemy.types import Date
from tasks.utils.fh_utils import is_nan_or_none
from tasks.merge import generate_range
from tasks.tushare.tushare_stock_daily.stock import DTYPE_TUSHARE_DAILY
from tasks.tushare.tushare_fina_reports.cashflow import DTYPE_TUSHARE_CASHFLOW
from tasks.tushare.tushare_stock_daily.daily_basic import DTYPE_TUSHARE_DAILY_BASIC
from tasks.tushare.tushare_stock_daily.suspend import DTYPE_TUSHARE_SUSPEND
from tasks import bunch_insert_on_duplicate_update, alter_table_2_myisam, build_primary_key

logger = logging.getLogger()
DEBUG = False


def get_tushare_daily_df(table_name, date_from) -> pd.DataFrame:
    if date_from is None:
        sql_str = "select * from {table_name}".format(table_name=table_name)
        data_df = pd.read_sql(sql_str, engine_md)  # , index_col='ts_code'
    else:
        sql_str = "select * from {table_name} where trade_date >= %s".format(table_name=table_name)
        data_df = pd.read_sql(sql_str, engine_md, params=[date_from])  # , index_col='ts_code'
    return data_df


# def merge_tushare_quarterly(ts_code_set = None, date_from=None, table_param_list = None):
#     # table_name = ""
#     table_param_list = [
#         "tushare_stock_balancesheet" ,"tushare_stock_cashflow" , "tushare_stock_income", "tushare_stock_fina_indicator",
#     ]
#     # logging.INFO("開始合並數據")
#     print(type(table_param_list))
#     # table_name = [table_name for table_name in table_param_list]
#     for table_name in table_param_list:
#         print(table_name)
#         tushare_merge_quarterly_df = get_tushare_daily_df(table_name, date_from)
#         tushare_merge_quarterly_df = pd.merge(tushare_merge_quarterly_df)
#         return tushare_merge_quarterly_df


def merge_tushare_daily(ths_code_set: set = None, date_from=None):
    table_name = 'tushare_daily'
    logging.info("合成 %s 开始", table_name)
    has_table = engine_md.has_table(table_name)
    if date_from is None and has_table:
        sql_str = "select adddate(max(`trade_date`),1) from {table_name}".format(table_name=table_name)
        with with_db_session(engine_md) as session:
            date_from = date_2_str(session.execute(sql_str).scalar())
    # 獲取各個表格數據
    # daily
    tushare_his_df = get_tushare_daily_df('tushare_stock_daily', date_from)
    tushare_ds_df = get_tushare_daily_df('tushare_daily_basic', date_from)
    tushare_suspend_df = get_tushare_daily_df('tushare_suspend', None)
    # quarterly

    tushare_merge_df = get_tushare_daily_df('tushare_stock_cashflow', None)

    # tushare_balancesheet_df = get_tushare_daily_df('tushare_stock_balancesheet', None)
    # tushare_icome_df = get_tushare_daily_df('tushare_stock_income', None)
    # tushare_indicator_df = get_tushare_daily_df('tushare_stock_fina_indicator', None)
    # tushare_merge_quarterly_df = merge_tushare_quarterly(None)
    # #
    # tushare_merge_df_one = pd.merge(tushare_cashflow_df, tushare_balancesheet_df,
    #                                 how='outer', on=['ts_code', 'end_date'])
    # tushare_merge_df_two = pd.merge(tushare_merge_df_one, tushare_icome_df,
    #                                 how='outer', on=['ts_code', 'end_date'])
    # tushare_merge_df = pd.merge(tushare_merge_df_two, tushare_indicator_df,
    #                             how='outer', on=['ts_code', 'end_date'])

    tushare_his_dis_df = pd.merge(tushare_his_df, tushare_ds_df, how='outer',
                                  on=['ts_code', 'trade_date', ])
    tushare_his_ds_df = pd.merge(tushare_his_dis_df, tushare_suspend_df, how='outer', on='ts_code')

    tushare_merge_df_g = tushare_merge_df.groupby('ts_code')
    tushare_his_ds_df_g = tushare_his_ds_df.groupby('ts_code')
    logging.debug('提取数据完成')
    merge_date_dic_dic = {}
    # # 計算財務紕漏事件
    for suspend_date_g in [tushare_merge_df.groupby(['ts_code', 'ann_date'])]:
        for num, ((ts_code, ann_date), date_df) in enumerate(suspend_date_g):
            if ths_code_set is not None and ts_code not in ths_code_set:
                continue
            if is_nan_or_none(ann_date):
                continue
            suspend_date_dic = merge_date_dic_dic.setdefault(ts_code, {})
            if ts_code not in tushare_merge_df_g.size():
                logger.error('hebing 表中不存在 %s 的財務數據', ts_code)
                continue
            tushare_merge_df_temp = tushare_merge_df_g.get_group(ts_code)
            if ann_date not in merge_date_dic_dic:
                tushare_merge_df_temp = tushare_merge_df_temp[tushare_merge_df_temp['f_ann_date'] <= ann_date]
                if tushare_merge_df_temp.shape[0] > 0:
                    suspend_date_dic[ann_date] = tushare_merge_df_temp.sort_values('f_ann_date').iloc[0]
    # # 設置dtype
    dtype = {'ann_date': Date}

    for dic in [DTYPE_TUSHARE_DAILY, DTYPE_TUSHARE_DAILY_BASIC, DTYPE_TUSHARE_SUSPEND,
                # DTYPE_TUSHARE_STOCK_INCOME,
                # DTYPE_TUSHARE_STOCK_BALABCESHEET,
                DTYPE_TUSHARE_CASHFLOW]:
        for key, val in dic.items():  # len(dic)12
            dtype[key] = val
    logging.debug("计算财报日期完成")

    # 整理 data_df 数据
    tot_data_count, data_count, data_df_list, for_count = 0, 0, [], len(merge_date_dic_dic)
    try:
        for num, (ts_code, report_date_dic) in enumerate(merge_date_dic_dic.items(),
                                                         start=1):  # key:ts_code nan 較多  列明: nan
            # TODO: size暫時使用  以後在驚醒改進
            if ts_code not in tushare_his_ds_df_g.size():
                logger.error('suspend 表中不存在 %s 的財務數據', ts_code)
                continue
            tushare_his_ds_df_cur_ths_code = tushare_his_ds_df_g.get_group(ts_code)  # shape[1] 30
            logger.debug('%d/%d) 处理 %s %d 条数据', num, for_count, ts_code, tushare_his_ds_df_cur_ths_code.shape[0])
            report_date_list = list(suspend_date_dic.keys())
            report_date_list.sort()
            for report_date_from, report_date_to in generate_range(report_date_list):
                logger.debug('%d/%d) 处理 %s [%s - %s]',
                             num, for_count, ts_code, date_2_str(report_date_from), date_2_str(report_date_to))
                # 计算有效的日期范围
                if report_date_from is None:
                    is_fit = tushare_his_ds_df_cur_ths_code['trade_date'] < report_date_to
                elif report_date_to is None:
                    is_fit = tushare_his_ds_df_cur_ths_code['trade_date'] >= report_date_from
                else:
                    is_fit = (tushare_his_ds_df_cur_ths_code['trade_date'] < report_date_to) & (
                            tushare_his_ds_df_cur_ths_code['trade_date'] >= report_date_from)
                # 获取日期范围内的数据
                tushare_his_ds_df_segment = tushare_his_ds_df_cur_ths_code[is_fit].copy()
                segment_count = tushare_his_ds_df_segment.shape[0]
                if segment_count == 0:
                    continue
                fin_s = report_date_dic[report_date_from] if report_date_from is not None else None
                #################################################
                for key in (DTYPE_TUSHARE_DAILY.keys() and DTYPE_TUSHARE_DAILY_BASIC.keys() and
                            DTYPE_TUSHARE_SUSPEND.keys() and DTYPE_TUSHARE_CASHFLOW.keys()
                        # and
                        #  DTYPE_TUSHARE_STOCK_BALABCESHEET.keys() and DTYPE_TUSHARE_STOCK_INCOME.keys()
                ):
                    if key in ('ts_code', 'trade_date'):
                        continue
                    tushare_his_ds_df_segment[key] = fin_s[key] if fin_s is not None and key in fin_s else None
                tushare_his_ds_df_segment['ann_date'] = report_date_from
                # 添加数据到列表
                data_df_list.append(tushare_his_ds_df_segment)
                data_count += segment_count

            if DEBUG and len(data_df_list) > 1:
                break

            # 保存数据库
            if data_count > 10000:
                # 保存到数据库
                data_df = pd.concat(data_df_list)
                data_count = bunch_insert_on_duplicate_update(data_df, table_name, engine_md, dtype)
                tot_data_count += data_count
                data_count, data_df_list = 0, []

    finally:
        # 保存到数据库   report_date
        if len(data_df_list) > 0:
            data_df = pd.concat(data_df_list)
            data_count = bunch_insert_on_duplicate_update(data_df, table_name, engine_md, dtype)
            tot_data_count += data_count

        logger.info('%s 新增或更新记录 %d 条', table_name, tot_data_count)
        if not has_table and engine_md.has_table(table_name):
            alter_table_2_myisam(engine_md, [table_name])
            build_primary_key([table_name])
    #
    #         # create_pk_str = """ALTER TABLE {table_name}
    #         #                        CHANGE COLUMN `ts_code` `ts_code` VARCHAR(20) NOT NULL ,
    #         #                        ADD PRIMARY KEY (`ts_code`)""".format(table_name=table_name)
    #         # with with_db_session(engine_md) as session:
    #         #     session.execute(create_pk_str)


if __name__ == "__main__":
    # data_df = merge_stock_info()
    # print(data_df)
    ths_code_set = None
    merge_tushare_daily(ths_code_set)
