"""
@author  : MG
@Time    : 2021/1/18 9:34
@File    : future_min.py
@contact : mmmaaaggg@163.com
@desc    : 用于
"""

import logging
import re
from datetime import datetime, date, timedelta

import pandas as pd
from direstinvoker import APIError
from ibats_utils.db import bunch_insert_on_duplicate_update
from ibats_utils.db import with_db_session
from ibats_utils.mess import STR_FORMAT_DATE, STR_FORMAT_DATETIME, str_2_datetime
from sqlalchemy.dialects.mysql import DOUBLE
from sqlalchemy.types import String, Date, DateTime

from tasks import app
from tasks.backend import engine_md
from tasks.config import config
from tasks.ifind import invoker

logger = logging.getLogger()
RE_PATTERN_MFPRICE = re.compile(r'\d*\.*\d*')
ONE_DAY = timedelta(days=1)
IFIND_VNPY_EXCHANGE_DIC = {
    'SHF': 'SHFE',
    'CZC': 'CZCE',
    'CFE': 'CFFEX',
    'DCE': 'DCE',
    'INE': 'INE'
}
PATTERN_INSTRUMENT_TYPE = re.compile(r'\D+(?=\d{2,4})', re.IGNORECASE)
# 标示每天几点以后下载当日行情数据
BASE_LINE_HOUR = 17
DEBUG = False


@app.task
def import_future_min(chain_param=None, wind_code_set=None, begin_time=None, recent_n_years=None):
    """
    更新期货合约分钟级别行情信息
    请求语句类似于：
    THS_HF('CU2105.SHF','open;high;low;close;volume;amount;change;changeRatio;sellVolume;buyVolume;openInterest',
        'Fill:Original','2021-01-18 09:15:00','2021-01-18 15:15:00')
    :param chain_param:  在celery 中將前面結果做爲參數傳給後面的任務
    :param wind_code_set:  只道 ths_code 集合
    :param begin_time:  最早的起始日期
    :param recent_n_years:  忽略n年前的合约
    :return:
    """
    # global DEBUG
    # DEBUG = True
    table_name = "ifind_future_min"
    logger.info("更新 %s 开始", table_name)
    has_table = engine_md.has_table(table_name)
    param_list = [
        ("open", DOUBLE),
        ("high", DOUBLE),
        ("low", DOUBLE),
        ("close", DOUBLE),
        ("volume", DOUBLE),
        ("amount", DOUBLE),
        ("change", DOUBLE),
        ("changeRatio", DOUBLE),
        ("sellVolume", DOUBLE),
        ("buyVolume", DOUBLE),
        ("openInterest", DOUBLE),
    ]
    ifind_indicator_str = ";".join([key for key, _ in param_list])

    if has_table:
        sql_str = f"""
        select ths_code, date_frm, if(lasttrade_date<end_date, lasttrade_date, end_date) date_to
        FROM
        (
            select fi.ths_code, 
                ifnull(trade_date_max_1, addtime(ths_start_trade_date_future,'09:00:00')) date_frm, 
                addtime(ths_last_td_date_future,'15:00:00') lasttrade_date,
                case 
                    when hour(now())>=23 then DATE_FORMAT(now(),'%Y-%m-%d 23:00:00') 
                    when hour(now())>=15 then DATE_FORMAT(now(),'%Y-%m-%d 15:00:00') 
                    when hour(now())>=12 then DATE_FORMAT(now(),'%Y-%m-%d 12:00:00') 
                    else DATE_FORMAT(now(),'%Y-%m-%d 03:00:00') 
                end end_date
            from ifind_future_info fi 
            left outer join
            (
                select ths_code, addtime(max(trade_datetime),'00:00:01') trade_date_max_1 
                from {table_name} group by ths_code
            ) wfd
            on fi.ths_code = wfd.ths_code
        ) tt
        where date_frm <= if(lasttrade_date<end_date, lasttrade_date, end_date) 
        -- and subdate(curdate(), 360) < if(lasttrade_date<end_date, lasttrade_date, end_date) 
        order by date_to desc, date_frm"""
    else:
        sql_str = """
        SELECT ths_code, date_frm,
            if(lasttrade_date<end_date,lasttrade_date, end_date) date_to
        FROM
        (
            SELECT info.ths_code,
            addtime(ths_start_trade_date_future,'09:00:00') date_frm, 
            addtime(ths_last_td_date_future,'15:00:00')  lasttrade_date,
            case 
                when hour(now())>=23 then DATE_FORMAT(now(),'%Y-%m-%d 23:00:00') 
                when hour(now())>=15 then DATE_FORMAT(now(),'%Y-%m-%d 15:00:00') 
                when hour(now())>=12 then DATE_FORMAT(now(),'%Y-%m-%d 12:00:00') 
                else DATE_FORMAT(now(),'%Y-%m-%d 03:00:00') 
            end end_date
            FROM ifind_future_info info
        ) tt
        WHERE date_frm <= if(lasttrade_date<end_date, lasttrade_date, end_date)
        ORDER BY date_to desc, date_frm"""
        logger.warning('%s 不存在，仅使用 wind_future_info 表进行计算日期范围', table_name)

    with with_db_session(engine_md) as session:
        table = session.execute(sql_str)
        # 获取date_from,date_to，将date_from,date_to做为value值
        future_date_dic = {
            ths_code: (
                str_2_datetime(date_from) if begin_time is None else min([str_2_datetime(date_from), begin_time]),
                str_2_datetime(date_to)
            )
            for ths_code, date_from, date_to in table.fetchall()
            if wind_code_set is None or ths_code in wind_code_set
        }

    # 设置 dtype
    dtype = {key: val for key, val in param_list}
    dtype['ths_code'] = String(20)
    dtype['instrument_id'] = String(20)
    dtype['trade_date'] = Date
    dtype['trade_datetime'] = DateTime

    # 定义统一的插入函数
    def insert_db(df: pd.DataFrame):
        insert_data_count = bunch_insert_on_duplicate_update(
            df, table_name, engine_md, dtype=dtype,
            primary_keys=['ths_code', 'trade_datetime'], schema=config.DB_SCHEMA_MD
        )
        return insert_data_count

    data_df_list = []
    future_count = len(future_date_dic)
    bulk_data_count, tot_data_count = 0, 0
    # 忽略更早的历史合约
    ignore_before = pd.to_datetime(
        date.today() - timedelta(days=int(365 * recent_n_years))) if recent_n_years is not None else None
    try:
        logger.info("%d future instrument will be handled", future_count)
        for num, (ths_code, (date_frm, date_to)) in enumerate(future_date_dic.items(), start=1):
            # 暂时只处理 RU 期货合约信息
            # if ths_code.find('RU') == -1:
            #     continue
            if not(0 <= (date_to - date_frm).days < 800):
                continue

            if ignore_before is not None and pd.to_datetime(date_frm) < ignore_before:
                # 忽略掉 n 年前的合约
                continue
            if isinstance(date_frm, datetime):
                date_frm_str = date_frm.strftime(STR_FORMAT_DATETIME)
            elif isinstance(date_frm, str):
                date_frm_str = date_frm
            else:
                date_frm_str = date_frm.strftime(STR_FORMAT_DATE) + ' 09:00:00'

            # 结束时间到次日的凌晨5点
            if isinstance(date_frm, datetime):
                date_to_str = date_to.strftime(STR_FORMAT_DATETIME)
            elif isinstance(date_to, str):
                date_to_str = date_to
            else:
                date_to += timedelta(days=1)
                date_to_str = date_to.strftime(STR_FORMAT_DATE) + ' 03:00:00'

            logger.info('%d/%d) get %s between %s and %s', num, future_count, ths_code, date_frm_str, date_to_str)
            try:
                data_df = invoker.THS_HighFrequenceSequence(
                    ths_code, ifind_indicator_str, 'Fill:Original', date_frm_str, date_to_str)
            except APIError as exp:
                from tasks.ifind import ERROR_CODE_MSG_DIC
                error_code = exp.ret_dic.setdefault('error_code', 0)
                if error_code in ERROR_CODE_MSG_DIC:
                    logger.error("%d/%d) %s 执行异常 error_code=%d, %s",
                                 num, future_count, ths_code, error_code, ERROR_CODE_MSG_DIC[error_code])
                else:
                    logger.exception("%d/%d) %s 执行异常 error_code=%d",
                                     num, future_count, ths_code, error_code)

                if error_code in (
                        -4210,  # 数据为空
                        -4001,  # 参数错误
                ):
                    continue
                else:
                    break
            if data_df is None:
                logger.warning('%d/%d) %s has no data during %s %s',
                               num, future_count, ths_code, date_frm_str, date_to)
                continue
            logger.info('%d/%d) %d data of %s between %s and %s',
                        num, future_count, data_df.shape[0], ths_code, date_frm_str, date_to)
            # data_df['ths_code'] = ths_code
            data_df.rename(columns={
                'time': 'trade_datetime',
                'thscode': 'ths_code',
            }, inplace=True)
            data_df['trade_date'] = pd.to_datetime(data_df['trade_datetime']).apply(lambda x: x.date())
            data_df.rename(columns={c: str.lower(c) for c in data_df.columns}, inplace=True)
            data_df['instrument_id'] = ths_code.split('.')[0]
            data_df_list.append(data_df)
            bulk_data_count += data_df.shape[0]
            # 仅仅调试时使用
            if DEBUG and len(data_df_list) >= 1:
                break
            if bulk_data_count > 50000:
                logger.info('merge data with %d df %d data', len(data_df_list), bulk_data_count)
                data_df = pd.concat(data_df_list)
                tot_data_count = insert_db(data_df)
                logger.info("更新 %s，累计 %d 条记录被更新", table_name, tot_data_count)
                data_df_list = []
                bulk_data_count = 0
    finally:
        data_df_count = len(data_df_list)
        if data_df_count > 0:
            logger.info('merge data with %d df %d data', len(data_df_list), bulk_data_count)
            data_df = pd.concat(data_df_list)
            tot_data_count += insert_db(data_df)

        logger.info("更新 %s 结束 累计 %d 条记录被更新", table_name, tot_data_count)


def _run_import_future_min():
    # global DEBUG
    # DEBUG = True
    import_future_min()


if __name__ == "__main__":
    _run_import_future_min()
