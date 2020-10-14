#! /usr/bin/env python3
"""
@author  : MG
@Time    : 2020/10/11 下午2:08
@File    : future.py
@contact : mmmaaaggg@163.com
@desc    : 导入 rqdatac 期货行情数据
"""
import logging
import pandas as pd
from datetime import datetime, date, timedelta
import rqdatac
from rqdatac.share.errors import QuotaExceeded
from sqlalchemy.dialects.mysql import DOUBLE, TINYINT, SMALLINT
from tasks import app
from ibats_utils.db import with_db_session
from sqlalchemy.types import String, Date
from ibats_utils.mess import STR_FORMAT_DATE, date_2_str, str_2_date
from tasks.backend.orm import build_primary_key
from tasks.merge.code_mapping import update_from_info_table
from tasks.backend import engine_md
from ibats_utils.db import alter_table_2_myisam
from ibats_utils.db import bunch_insert_on_duplicate_update

logger = logging.getLogger()
ONE_DAY = timedelta(days=1)
# 标示每天几点以后下载当日行情数据
BASE_LINE_HOUR = 17
DEBUG = False


def get_date_iter(date_since, date_until, ndays_step):
    """
    返回日期迭代器，从 date_since 到 date_until，（不包含 date_until），每 ndays_step 天返回一个日期
    """
    while True:
        if date_since >= date_until:
            return
        else:
            date_since += timedelta(days=ndays_step)
            if date_since > date_until:
                date_since = date_until

            yield date_since


@app.task
def import_future_info(chain_param=None):
    """
    更新期货合约列表信息
    :param chain_param:  在celery 中將前面結果做爲參數傳給後面的任務
    :return:
    """
    table_name = "rqdatac_future_info"
    has_table = engine_md.has_table(table_name)
    logger.info("更新 %s 开始", table_name)
    # 获取已存在合约列表
    if has_table:
        sql_str = 'select max(listed_date) from {table_name}'.format(table_name=table_name)
        with with_db_session(engine_md) as session:
            listed_date = session.scalar(sql_str)
            if listed_date is None:
                has_table = False

    ndays_per_update = 60
    # 获取接口参数以及参数列表
    col_name_param_list = [
        ("order_book_id", String(20)),
        # 期货代码，期货的独特的标识符（郑商所期货合约数字部分进行了补齐。例如原有代码'ZC609'补齐之后变为'ZC1609'）。主力连续合约 UnderlyingSymbol+88，例如'IF88' ；指数连续合约命名规则为 UnderlyingSymbol+99
        ("underlying_symbol", String(20)),  # 合约标的名称，例如 IF1005 的合约标的名称为'IF'
        ("market_tplus", TINYINT),  # 交易制度。'0'表示 T+0，'1'表示 T+1，往后顺推
        ("symbol", String(100)),  # 期货的简称，例如'沪深 1005'
        ("margin_rate", DOUBLE),  # 期货合约的最低保证金率
        ("maturity_date", Date),  # 期货到期日。主力连续合约与指数连续合约都为'0000-00-00'
        ("type", String(20)),  # 合约类型，'Future'
        ("trading_code", String(20)),  #
        ("exchange", String(10)),  # 交易所，'DCE' - 大连商品交易所, 'SHFE' - 上海期货交易所，'CFFEX' - 中国金融期货交易所, 'CZCE'- 郑州商品交易所
        ("product", String(20)),  # 合约种类，'Commodity'-商品期货，'Index'-股指期货，'Government'-国债期货
        ("contract_multiplier", SMALLINT),  # 合约乘数，例如沪深 300 股指期货的乘数为 300.0
        ("round_lot", TINYINT),  # 期货全部为 1.0
        ("trading_hours", String(100)),  # 合约交易时间
        ("listed_date", Date),  # 期货的上市日期。主力连续合约与指数连续合约都为'0000-00-00'
        ("industry_name", String(50)),
        ("de_listed_date", Date),  # 目测与 maturity_date 相同
        ("underlying_order_book_id", String(20)),  # 合约标的代码，目前除股指期货(IH, IF, IC)之外的期货合约，这一字段全部为'null'
    ]
    dtype = {key: val for key, val in col_name_param_list}
    if not has_table:
        instrument_info_df = rqdatac.all_instruments(type='Future', market='cn')
    else:
        date_yestoday = date.today() - timedelta(days=1)
        ndays_per_update = 60
        instrument_info_df = None
        for param in get_date_iter(listed_date, date_yestoday, ndays_per_update):
            if instrument_info_df is None:
                instrument_info_df = rqdatac.all_instruments(type='Future', market='cn', date=param)
            else:
                instrument_info_df_tmp = rqdatac.all_instruments(type='Future', market='cn', date=param)
                instrument_info_df.append(instrument_info_df_tmp)

    instrument_info_df.drop_duplicates(inplace=True)
    instrument_info_df.loc[instrument_info_df['underlying_order_book_id'] == 'null', 'underlying_order_book_id'] = None
    instrument_info_df.rename(columns={c: str.lower(c) for c in instrument_info_df.columns}, inplace=True)
    data_count = bunch_insert_on_duplicate_update(instrument_info_df, table_name, engine_md, dtype=dtype)
    logging.info("更新 %s 结束 %d 条信息被更新", table_name, data_count)
    if not has_table and engine_md.has_table(table_name):
        alter_table_2_myisam(engine_md, [table_name])
        build_primary_key([table_name])

    logger.info("更新 wind_future_info 结束 %d 条记录被更新", data_count)
    # update_from_info_table(table_name)


def import_future_min(chain_param=None, order_book_id_set=None, begin_time=None):
    """
    加载商品期货分钟级数据
    """
    table_name = "rqdatac_future_min"
    logger.info("更新 %s 开始", table_name)
    has_table = engine_md.has_table(table_name)
    param_list = [
        ("open", DOUBLE),
        ("high", DOUBLE),
        ("low", DOUBLE),
        ("close", DOUBLE),
        # ("limit_up", DOUBLE),
        # ("limit_down", DOUBLE),
        ("total_turnover", DOUBLE),  # 成交额
        ("volume", DOUBLE),  # 成交量
        # ("num_trades", DOUBLE),  # 成交笔数 （仅限股票日线数据）
        # ("settlement", DOUBLE),  # 结算价 （仅限期货日线数据）
        # ("prev_settlement", DOUBLE),  # 昨日结算价（仅限期货日线数据）
        ("open_interest", DOUBLE),  # 累计持仓量（期货专用）
        ('trading_date', Date,),  # 交易日期（仅限期货分钟线数据），对应期货夜盘的情况
        ('dominant_id', DOUBLE),  # 实际合约的 order_book_id，对应期货 888 系主力连续合约的情况
        # ('strike_price', DOUBLE),  # 行权价，仅限 ETF 期权日线数据
        # ('contract_multiplier', DOUBLE),  # 合约乘数，仅限 ETF 期权日线数据
        # ('iopv', DOUBLE),  # 场内基金实时估算净值
    ]
    field_list = [_[0] for _ in param_list]
    if not has_table:
        # 考虑到流量有限，避免浪费，首次建表的时候只获取一个合约数据进行建表
        sql_str = """
            SELECT order_book_id, date_frm,
                if(lasttrade_date<end_date,lasttrade_date, end_date) date_to
            FROM
            (
                SELECT info.order_book_id,listed_date date_frm, maturity_date lasttrade_date,
                if(hour(now())<16, subdate(curdate(),1), curdate()) end_date,
                maturity_date
                FROM rqdatac_future_info info
            ) tt
            WHERE date_frm <= if(lasttrade_date<end_date, lasttrade_date, end_date)
            ORDER BY maturity_date desc limit 1
         """
        logger.warning('%s 不存在，仅使用 wind_future_info 表进行计算日期范围', table_name)

    else:
        sql_str = """select order_book_id, date_frm, if(lasttrade_date<end_date, lasttrade_date, end_date) date_to
            FROM
            (
                select fi.order_book_id, ifnull(trade_date_max_1, listed_date) date_frm, 
                    maturity_date lasttrade_date,
                    if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
                from rqdatac_future_info fi 
                left outer join
                    (select order_book_id, DATE(adddate(max(trade_date),1)) trade_date_max_1 from {table_name} group by order_book_id) wfd
                on fi.order_book_id = wfd.order_book_id
            ) tt
            where date_frm <= if(lasttrade_date<end_date, lasttrade_date, end_date) 
            -- and subdate(curdate(), 360) < if(lasttrade_date<end_date, lasttrade_date, end_date) 
            order by date_frm desc""".format(table_name=table_name)

    with with_db_session(engine_md) as session:
        table = session.execute(sql_str)
        # 获取date_from,date_to，将date_from,date_to做为value值
        future_date_dic = {
            order_book_id: (date_from if begin_time is None else min([date_from, begin_time]), date_to)
            for order_book_id, date_from, date_to in table.fetchall()
            if order_book_id_set is None or order_book_id in order_book_id_set
        }

    # 设置 dtype
    dtype = {key: val for key, val in param_list}
    dtype['order_book_id'] = String(20)
    data_df_list = []
    data_len = len(future_date_dic)
    try:
        logger.info("%d future instrument will be handled", data_len)
        for num, (order_book_id, (date_frm, date_to)) in enumerate(future_date_dic.items(), start=1):
            if date_frm > date_to:
                continue
            date_frm_str = date_frm.strftime(STR_FORMAT_DATE)
            date_to_str = date_to.strftime(STR_FORMAT_DATE)
            logger.info('%d/%d) get %s between %s and %s', num, data_len, order_book_id, date_frm_str, date_to_str)
            try:
                # get_price(order_book_ids, start_date='2013-01-04', end_date='2014-01-04', frequency='1d', fields=None,
                #           adjust_type='pre', skip_suspended=False, market='cn', expect_df=False)
                data_df = rqdatac.get_price(
                    order_book_id, start_date=date_frm_str, end_date=date_to_str, frequency='1m',
                    adjust_type='none', skip_suspended=False, market='cn')  # fields=field_list,
            except QuotaExceeded:
                logger.exception("获取数据超量")
                break

            if data_df is None or data_df.shape[0] == 0:
                logger.warning('%d/%d) %s has no data during %s %s', num, data_len, order_book_id, date_frm_str,
                               date_to_str)
                continue
            logger.info('%d/%d) %d data of %s between %s and %s', num, data_len, data_df.shape[0], order_book_id,
                        date_frm_str, date_to_str)
            data_df['order_book_id'] = order_book_id
            data_df.index.rename('trade_date', inplace=True)
            data_df.reset_index(inplace=True)
            data_df.rename(columns={c: str.lower(c) for c in data_df.columns}, inplace=True)
            data_df_list.append(data_df)
            # 仅仅调试时使用
            if DEBUG and len(data_df_list) > 1:
                break
    finally:
        data_df_count = len(data_df_list)
        if data_df_count > 0:
            logger.info('merge data with %d df', data_df_count)
            data_df = pd.concat(data_df_list)
            data_count = bunch_insert_on_duplicate_update(data_df, table_name, engine_md, dtype=dtype)
            logger.info("更新 %s 结束 %d 条记录被更新", table_name, data_count)
            if not has_table and engine_md.has_table(table_name):
                alter_table_2_myisam(engine_md, [table_name])
                build_primary_key([table_name])
        else:
            logger.info("更新 %s 结束 0 条记录被更新", table_name)


if __name__ == "__main__":
    # import_future_info()
    import_future_min()
