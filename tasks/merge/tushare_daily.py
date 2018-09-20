import pandas as pd
import logging
from tasks.backend.orm import build_primary_key
from datetime import date, datetime, timedelta

from tasks.merge import generate_range
from tasks.tushare.tushare_stock_daily.daily_basic import DTYPE_TUSHARE_STOCK_DAILY_BASIC
from tasks.tushare.tushare_stock_daily.stock import DTYPE_TUSHARE_STOCK_DAILY_MD
from tasks.tushare.tushare_stock_daily.adj_factor import DTYPE_TUSHARE_STOCK_DAILY_ADJ_FACTOR
from tasks.tushare.tushare_stock_daily.daily_basic import DTYPE_TUSHARE_STOCK_DAILY_BASIC
from tasks.tushare.tushare_stock_daily.stock import DTYPE_TUSHARE_STOCK_DAILY_MD
from tasks.tushare.tushare_stock_daily.suspend import DTYPE_TUSHARE_SUSPEND
from tasks.utils.fh_utils import try_2_date, STR_FORMAT_DATE, datetime_2_str, split_chunk, is_nan_or_none
from tasks import app, DTYPE_TUSHARE_STOCK_BALABCESHEET
from direstinvoker.utils.fh_utils import date_2_str
from sqlalchemy.types import String, Date, Integer
from sqlalchemy.dialects.mysql import DOUBLE
from tasks.backend import engine_md
from tasks.merge.code_mapping import update_from_info_table
from tasks.utils.db_utils import with_db_session, add_col_2_table, alter_table_2_myisam, \
    bunch_insert_on_duplicate_update
from tasks.utils.fh_utils import is_not_nan_or_none

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


def get_tushare_daily_suspend_df(table_name, date_from) -> pd.DataFrame:
    if date_from is None:
        sql_str = "select * from {table_name}".format(table_name=table_name)
        data_df = pd.read_sql(sql_str, engine_md)
    return data_df


def get_merge(x):
    if x['close_x'] is not None:
        return x['close_x']
    elif x['close_y'] is not None:
        return x['close_y']
    else:
        return None


def merge_tushare_daily(date_from=None):
    """
    合並 adj_factor,daily_basic,stock,suspend 表格數據
    :param date_from:
    :return:
    """
    table_name = 'tushare_stock_daily'
    has_table = engine_md.has_table(table_name)
    if date_from is None and has_table:
        sql_str = "select adddate(max(trade_date),1) from {table_name}".format(table_name=table_name)
        with with_db_session(engine_md) as session:
            date_from = date_2_str(session.execute(sql_str).scalar())
    tushare_adj_factor_df = get_tushare_daily_df('tushare_stock_daily_adj_factor', date_from)
    tushare_daily_basic_df = get_tushare_daily_df('tushare_stock_daily_basic', date_from)
    tushare_stock_daily_md_df = get_tushare_daily_df('tushare_stock_daily_md', date_from)
    # tushare_stock_daily_suspend_df = get_tushare_daily_df('tushare_stock_daily_suspend', date_from)
    tushare_daily_two_form_df = pd.merge(tushare_adj_factor_df, tushare_daily_basic_df, how='outer',
                                         on=['ts_code', 'trade_date'])
    tushare_daily_df = pd.merge(tushare_daily_two_form_df, tushare_stock_daily_md_df,
                                how='outer', on=['ts_code', 'trade_date'])
    # tushare_daily_df = pd.merge(tushare_daily_three_form_df, tushare_stock_daily_suspend_df, how='outer',
    #                             on=['ts_code'])
    # 设置 dtype
    dtype = {}
    for dic in [DTYPE_TUSHARE_SUSPEND, DTYPE_TUSHARE_STOCK_DAILY_BASIC, DTYPE_TUSHARE_STOCK_DAILY_MD]:
        for key, val in dic.items():
            dtype[key] = val
    tushare_daily_df["close"] = tushare_daily_df.apply(get_merge, axis=1)
    tushare_daily_df.drop(['close_x', 'close_y'], axis=1, inplace=True)
    data_count = bunch_insert_on_duplicate_update(tushare_daily_df, table_name, engine_md, dtype)
    logging.info('%s  更新 %d', table_name, data_count)
    # if not has_table and engine_md.has_table(table_name):
    #     alter_table_2_myisam(engine_md, table_name)
    #     build_primary_key([table_name])
    return tushare_daily_df


def merge_daily_balancesheet(ts_code_set: set=None):
    """
    合並 數據日表與 季度balancesheet 表格數據
    :param data_from:
    :return:
    """
    table_name = 'tushare_merge_daily_bala'
    data_balancesheet_df = get_tushare_daily_df('tushare_stock_balancesheet', None)
    tushare_daily_df = get_tushare_daily_df('tushare_stock_daily', None)
    # data_merge_two_df = pd.merge(data_balancesheet_df, tushare_daily_df)
    tushare_daily_df_g = tushare_daily_df.groupby('ts_code')
    data_balancesheet_df_g = data_balancesheet_df.groupby('ts_code')
    report_date_dic_dic = {}

    for report_date_g in [data_balancesheet_df.groupby(['ts_code', "f_ann_date"])]:  # one date one date('ocde'.'open')
        for num, ((ts_code, report_date), data_df) in enumerate(report_date_g, start=1):
            if is_nan_or_none(report_date):
                continue
            if ts_code_set is not None and ts_code not in ts_code_set:
                continue
            report_date_dic = report_date_dic_dic.setdefault(ts_code, {})
            temp = data_balancesheet_df_g.get_group(ts_code)
            report_date_dic[report_date] = temp.sort_values('f_ann_date').iloc[0]

    # report_date = data_balancesheet_df.groupby("f_ann_date").count().index
    # ts_code = [ts_code for ts_code in data_balancesheet_df.groupby("f_ann_date").count()['ts_code']]
    # # 设置 dtype
    dtype = {}
    for dic in [DTYPE_TUSHARE_SUSPEND, DTYPE_TUSHARE_STOCK_DAILY_BASIC, DTYPE_TUSHARE_STOCK_DAILY_MD]:
        for key, val in dic.items():
            dtype[key] = val

    logging.debug('數據整理')
    tot_data_count, data_count, data_df_list, for_count = 0, 0, [], len(report_date_dic_dic)
    try:
        for num, (ts_code, report_date_dic) in enumerate(report_date_dic_dic.items(), start=1):  # key:ts_code
            # open low  等 is NAN 2438
            if ts_code not in tushare_daily_df_g.size():
                continue
            tushare_his_ds_df_cur_ts_code = tushare_daily_df_g.get_group(ts_code)  # shape[1] 30
            logger.debug('%d/%d) 处理 %s %d 条数据', num, for_count, ts_code, tushare_his_ds_df_cur_ts_code.shape[0])
            report_date_list = list(report_date_dic.keys())
            report_date_list.sort()
            for report_date_from, report_date_to in generate_range(report_date_list):
                logger.debug('%d/%d) 处理 %s [%s - %s]',
                             num, for_count, ts_code, date_2_str(report_date_from), date_2_str(report_date_to))
                if report_date_from is None:
                    is_fit = tushare_his_ds_df_cur_ts_code['trade_date'] < report_date_to
                elif report_date_to is None:
                    is_fit = tushare_his_ds_df_cur_ts_code['trade_date'] >= report_date_from
                else:
                    is_fit = (tushare_his_ds_df_cur_ts_code['trade_date'] < report_date_to) & (
                            tushare_his_ds_df_cur_ts_code['trade_date'] >= report_date_from)
                    # 获取日期范围内的数据
                ifind_his_ds_df_segment = tushare_his_ds_df_cur_ts_code[is_fit].copy()
                segment_count = ifind_his_ds_df_segment.shape[0]
                if segment_count == 0:
                    continue
                fin_s = report_date_dic[report_date_from] if report_date_from is not None else None
                for key in DTYPE_TUSHARE_STOCK_BALABCESHEET.keys():
                    if key in ('ths_code', 'time'):
                        continue
                    ifind_his_ds_df_segment[key] = fin_s[key] if fin_s is not None and key in fin_s else None
                ifind_his_ds_df_segment['report_date'] = report_date_from
                # 添加数据到列表
                data_df_list.append(ifind_his_ds_df_segment)

                data_count += segment_count
                if report_date_from is '2018-08-16':
                    break

            if DEBUG and len(data_df_list) > 1:
                break

                # 保存数据库
            if data_count > 1000:
                # 保存到数据库
                data_df = pd.concat(data_df_list)
                data_count = bunch_insert_on_duplicate_update(data_df, table_name, engine_md, dtype)
                tot_data_count += data_count
                data_count, data_df_list = 0, []

    finally:
        # 保存到数据库
        if len(data_df_list) > 0:
            data_df = pd.concat(data_df_list)
            data_count = bunch_insert_on_duplicate_update(data_df, table_name, engine_md, dtype)
            tot_data_count += data_count
        #
        logger.info('%s 新增或更新记录 %d 条', table_name, tot_data_count)
        # if not has_table and engine_md.has_table(table_name):
        #     alter_table_2_myisam(engine_md, [table_name])
        #     build_primary_key([table_name])


if __name__ == '__main__':
    # merge_tushare_daily()
    ts_code_set = {'000002.SZ'}
    merge_daily_balancesheet(ts_code_set)
