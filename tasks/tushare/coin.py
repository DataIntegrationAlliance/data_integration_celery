#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/8/21 13:56
@File    : coin.py
@contact : mmmaaaggg@163.com
@desc    : 2018-08-23 已经正式运行测试完成，可以正常使用
"""
import pandas as pd
from tasks import app
from tasks.utils.fh_utils import date_2_str, str_2_date, str_2_datetime, try_n_times
from sqlalchemy.types import String, Date, Integer, DateTime
from sqlalchemy.dialects.mysql import DOUBLE
from tasks.utils.db_utils import with_db_session, bunch_insert_on_duplicate_update, alter_table_2_myisam
from tasks.backend import engine_md
import logging
from datetime import date
from tasks.tushare.ts_pro_api import pro

DEBUG = False
logger = logging.getLogger()
DATE_FORMAT_STR = '%Y%m%d'


# @try_n_times(3, sleep_time=0, logger=logger, exception_sleep_time=60)
# def pro_coinbar(exchange, symbol, freq, start_date, end_date):
#     data_df = pro.coinbar(exchange=exchange, symbol=symbol, freq=freq, start_date=start_date, end_date=end_date)
#     return data_df


@app.task
def import_exchange_info():
    """
    交易所基本信息
    :return:
    """
    table_name = 'tushare_coin_exchange_info'
    has_table = engine_md.has_table(table_name)
    area_en_dic = {
        'ae': '阿联酋',
        'au': '澳大利亚',
        'br': '巴西',
        'by': '白俄罗斯',
        'bz': '伯利兹',
        'ca': '加拿大',
        'cbb': '加勒比',
        'ch': '瑞士',
        'cl': '智利',
        'cn': '中国',
        'cy': '塞浦路斯',
        'dk': '丹麦',
        'ee': '爱沙尼亚',
        'es': '西班牙',
        'hk': '中国香港',
        'id': '印度尼西亚',
        'il': '以色列',
        'in': '印度',
        'jp': '日本',
        'kh': '柬埔寨',
        'kr': '韩国',
        'ky': '开曼群岛',
        'la': '老挝',
        'mn': '蒙古国',
        'mt': '马耳他',
        'mx': '墨西哥',
        'my': '马来西亚',
        'nl': '荷兰',
        'nz': '新西兰',
        'ph': '菲律宾',
        'pl': '波兰',
        'ru': '俄罗斯',
        'sc': '塞舌尔',
        'sg': '新加坡',
        'th': '泰国',
        'tr': '土耳其',
        'tz': '坦桑尼亚',
        'ua': '乌克兰',
        'uk': '英国',
        'us': '美国',
        'vn': '越南',
        'ws': '萨摩亚',
        'za': '南非',
    }
    # 设置 dtype
    dtype = {
        'exchange': String(80),
        'name': String(80),
        'pairs': Integer,
        'area_code': String(10),
        'area_cn': String(80),
        'coin_trade': String(5),
        'fut_trade': String(5),
        'oct_trade': String(5),
    }
    # 获取所在地区为的交易所
    # TODO: 由于目前该接口bug，每一次调用均返回全部接口，因此无需进行循环，但要补充每一个 area_code 对应的中文
    area_en = 'us'
    # for num, (area_en, area_cn) in enumerate(area_en_dic.items()):
    exch_df = pro.coinexchanges(area=area_en)
    if exch_df is not None and exch_df.shape[0] > 0:
        # exch_df['area_en'] = area_en
        exch_df['area_cn'] = exch_df['area_code'].apply(lambda x: area_en_dic.setdefault(x, None))
        data_count = bunch_insert_on_duplicate_update(exch_df, table_name, engine_md, dtype)
        logging.info("更新 %s 完成 新增数据 %d 条", table_name, data_count)

    if not has_table and engine_md.has_table(table_name):
        alter_table_2_myisam(engine_md, [table_name])
        create_pk_str = """ALTER TABLE {table_name}
            CHANGE COLUMN `exchange` `exchange` VARCHAR(60) NOT NULL FIRST,
            ADD PRIMARY KEY (`exchange`)""".format(table_name=table_name)
        with with_db_session(engine_md) as session:
            session.execute(create_pk_str)


@app.task
def import_coin_info():
    """获取全球交易币基本信息"""
    table_name = 'tushare_coin_info'
    has_table = engine_md.has_table(table_name)
    # 设置 dtype
    dtype = {
        'coin': String(60),
        'en_name': String(60),
        'cn_name': String(60),
        'issue_date': Date,
        'amount': DOUBLE,
    }
    coinlist_df = pro.coinlist(start_date='20170101', end_date=date_2_str(date.today(), DATE_FORMAT_STR))
    data_count = bunch_insert_on_duplicate_update(coinlist_df, table_name, engine_md, dtype)
    logging.info("更新 %s 完成 新增数据 %d 条", table_name, data_count)

    if not has_table and engine_md.has_table(table_name):
        alter_table_2_myisam(engine_md, [table_name])
        create_pk_str = """ALTER TABLE {table_name}
            CHANGE COLUMN `coin` `coin` VARCHAR(60) NOT NULL FIRST,
            CHANGE COLUMN `en_name` `en_name` VARCHAR(60) NOT NULL AFTER `coin`,
            ADD PRIMARY KEY (`coin`, `en_name`)""".format(table_name=table_name)
        with with_db_session(engine_md) as session:
            session.execute(create_pk_str)


@app.task
def import_coin_pair_info():
    """获取交易对信息"""
    table_name = 'tushare_coin_pair_info'
    has_table = engine_md.has_table(table_name)
    # 获取 交易所交易对 接口所用交易所列表 https://tushare.pro/document/41?doc_id=51
    exchange_en_list = ['allcoin',
                        'bcex',
                        'bibox',
                        'bigone',
                        'binance',
                        'bitbank',
                        'bitfinex',
                        'bitflyer',
                        'bitflyex',
                        'bithumb',
                        'bitmex',
                        'bitstamp',
                        'bitstar',
                        'bittrex',
                        'bitvc',
                        'bitz',
                        'bleutrade',
                        'btcbox',
                        'btcc',
                        'btccp',
                        'btcturk',
                        'btc_usd_index',
                        'bter',
                        'chbtc',
                        'cobinhood',
                        'coinbase',
                        'coinbene',
                        'coincheck',
                        'coinegg',
                        'coinex',
                        'coinone',
                        'coinsuper',
                        'combine',
                        'currency',
                        'dextop',
                        'digifinex',
                        'exmo',
                        'exx',
                        'fcoin',
                        'fisco',
                        'future_bitmex',
                        'gate',
                        'gateio',
                        'gdax',
                        'gemini',
                        'hadax',
                        'hbus',
                        'hft',
                        'hitbtc',
                        'huobi',
                        'huobiotc',
                        'huobip',
                        'huobix',
                        'idax',
                        'idex',
                        'index',
                        'itbit',
                        'jubi',
                        'korbit',
                        'kraken',
                        'kucoin',
                        'lbank',
                        'lbc',
                        'liqui',
                        'okcn',
                        'okcom',
                        'okef',
                        'okex',
                        'okotc',
                        'okusd',
                        'poloniex',
                        'quoine',
                        'quoinex',
                        'rightbtc',
                        'shuzibi',
                        'simex',
                        'topbtc',
                        'upbit',
                        'viabtc',
                        'yobit',
                        'yuanbao',
                        'yunbi',
                        'zaif',
                        'zb',
                        ]

    # 设置 dtype
    dtype = {
        'ts_pair': String(60),
        'exchange_pair': String(60),
        'exchange': String(60),
        'trade_date_latest_daily': Date,
        'delist_date_daily': Date,
        'trade_date_latest_week': Date,
        'delist_date_week': Date,
    }
    exchange_count = len(exchange_en_list)
    try:
        for num, exchange in enumerate(exchange_en_list, start=1):
            data_df = pro.coinpair(exchange=exchange, trade_date='20180802')
            if data_df is not None and data_df.shape[0] > 0:
                data_df.drop('trade_date', axis=1, inplace=True)
                for freq in ['daily', 'week', '1min', '5min', '15min', '30min', '60min']:
                    data_df['trade_date_latest_{freq}'.format(freq=freq)] = None
                    data_df['delist_date_{freq}'.format(freq=freq)] = None
                data_count = bunch_insert_on_duplicate_update(data_df, table_name, engine_md, dtype)
                logging.info("%d/%d) 更新 %s 完成 新增 %s 数据 %d 条",
                             num, exchange_count, table_name, exchange, data_count)
    finally:
        if not has_table and engine_md.has_table(table_name):
            alter_table_2_myisam(engine_md, [table_name])
            create_pk_str = """ALTER TABLE {table_name}
                CHANGE COLUMN `exchange_pair` `exchange_pair` VARCHAR(60) NOT NULL FIRST,
                CHANGE COLUMN `exchange` `exchange` VARCHAR(60) NOT NULL AFTER `exchange_pair`,
                ADD PRIMARY KEY (`exchange_pair`, `exchange`)""".format(table_name=table_name)
            with with_db_session(engine_md) as session:
                session.execute(create_pk_str)


@app.task
def import_coinbar():
    """获取行情数据"""
    freq_list = ['daily', 'week']
    for freq in freq_list:
        import_coinbar_on_freq_daily(freq)
    # 日内数据 tushare 服务器压力问题，暂停开放 '1min', '5min'
    freq_list = ['15min', '30min', '60min']  # '1min', '5min',
    for freq in freq_list:
        import_coinbar_on_freq_min(freq)


def import_coinbar_on_freq_daily(freq, code_set=None, base_begin_time=None):
    """
    抓取 日级别以上数据[ daily, week ]级别
    :param freq:
    :param code_set:
    :param base_begin_time:
    :return:
    """
    if base_begin_time is not None and not isinstance(base_begin_time, date):
        base_begin_time = str_2_date(base_begin_time)
    table_name = 'tushare_coin_md_' + freq
    has_table = engine_md.has_table(table_name)
    if has_table:
        sql_str = """SELECT exchange, exchange_pair, date_frm, 
                if(delist_date<end_date, delist_date, end_date) date_to
            FROM
            (
                SELECT info.exchange, info.exchange_pair, 
                    ifnull(trade_date_max_1, adddate(trade_date_latest,1)) date_frm, 
                    delist_date,
                    if(hour(now())<8, subdate(curdate(),2), subdate(curdate(),1)) end_date
                FROM 
                (
                    select exchange, exchange_pair,
                    ifnull(trade_date_latest_{freq},'2010-01-01') trade_date_latest,
                    delist_date_{freq} delist_date
                    from tushare_coin_pair_info
                ) info
                LEFT OUTER JOIN
                    (SELECT exchange, symbol, adddate(max(date),1) trade_date_max_1 
                     FROM {table_name} GROUP BY exchange, symbol) daily
                ON info.exchange = daily.exchange
                AND info.exchange_pair = daily.symbol
            ) tt
            WHERE date_frm <= if(delist_date<end_date, delist_date, end_date) 
            ORDER BY exchange, exchange_pair""".format(table_name=table_name, freq=freq)
    else:
        sql_str = """SELECT exchange, exchange_pair, date_frm, 
            if(delist_date<end_date, delist_date, end_date) date_to
        FROM
        (
            SELECT exchange, exchange_pair, 
                ifnull(trade_date_latest_{freq},date('2010-01-01')) date_frm, 
                delist_date_{freq} delist_date, 
                if(hour(now())<8, subdate(curdate(),2), subdate(curdate(),1)) end_date
            FROM tushare_coin_pair_info info 
            ORDER BY exchange, exchange_pair
        ) tt
        WHERE date_frm <= if(delist_date<end_date, delist_date, end_date) 
        ORDER BY exchange, exchange_pair""".format(freq=freq)
        logger.warning('%s 不存在，仅使用 tushare_coin_pair_info 表进行计算日期范围', table_name)

    with with_db_session(engine_md) as session:
        # 获取每只股票需要获取日线数据的日期区间
        table = session.execute(sql_str)
        # 获取每只股票需要获取日线数据的日期区间
        code_date_range_dic = {
            (exchange, symbol):
                (date_from if base_begin_time is None else min([date_from, base_begin_time]),
                 date_to)
            for exchange, symbol, date_from, date_to in table.fetchall() if
            code_set is None or (exchange, symbol) in code_set}

    # 设置 dtype
    dtype = {
        'exchange': String(60),
        'symbol': String(60),
        'date': Date,
        'open': DOUBLE,
        'high': DOUBLE,
        'low': DOUBLE,
        'close': DOUBLE,
        'vol': DOUBLE,
    }

    # 更新 info 表 trade_date_latest 字段
    trade_date_latest_list = []
    update_trade_date_latest_str = """UPDATE tushare_coin_pair_info info
        SET info.trade_date_latest_daily = :trade_date_latest
        WHERE info.exchange = :exchange AND exchange_pair=:exchange_pair"""

    data_df_list, data_count, tot_data_count, code_count = [], 0, 0, len(code_date_range_dic)
    try:
        for num, ((exchange, exchange_pair), (begin_time, end_time)) in enumerate(code_date_range_dic.items(), start=1):
            begin_time_str = date_2_str(begin_time, DATE_FORMAT_STR)
            end_time_str = date_2_str(end_time, DATE_FORMAT_STR)
            logger.debug('%d/%d) %s %s [%s - %s]', num, code_count, exchange, exchange_pair, begin_time, end_time)
            try:
                # data_df = pro.coinbar(exchange='huobi', symbol='gxsbtc', freq='1min', start_date='20180701', end_date='20180801')
                data_df = pro.coinbar(exchange=exchange, symbol=exchange_pair, freq=freq,
                                      start_date=begin_time_str,
                                      end_date=end_time_str)
            except Exception as exp:
                if len(exp.args) >= 1 and exp.args[0] == '系统内部错误':
                    trade_date_latest_list.append({
                        'exchange': exchange,
                        'exchange_pair': exchange_pair,
                        'trade_date_latest': '2020-02-02',
                    })
                    logger.warning(
                        "coinbar(exchange='%s', symbol='%s', freq='%s', start_date='%s', end_date='%s') 系统内部错误",
                        exchange, exchange_pair, freq, begin_time_str, end_time_str)
                    continue
                logger.exception("coinbar(exchange='%s', symbol='%s', freq='%s', start_date='%s', end_date='%s')",
                                 exchange, exchange_pair, freq, begin_time_str, end_time_str)
                raise exp from exp

            if data_df is not None and data_df.shape[0] > 0:
                data_count += data_df.shape[0]
                data_df['exchange'] = exchange
                data_df_list.append(data_df)

            # 记录最新交易日变化
            trade_date_latest_list.append({
                'exchange': exchange,
                'exchange_pair': exchange_pair,
                'trade_date_latest': end_time_str,
            })
            # 大于阀值有开始插入
            if data_count >= 10000:
                data_df_all = pd.concat(data_df_list)
                # data_df_all.to_sql(table_name, engine_md, if_exists='append', index=False, dtype=dtype)
                data_count = bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md, dtype)
                tot_data_count += data_count
                data_df_list, data_count = [], 0

                # 更新 info 表 trade_date_latest 字段
                with with_db_session(engine_md) as session:
                    result = session.execute(update_trade_date_latest_str, params=trade_date_latest_list)
                    update_count = result.rowcount
                    session.commit()
                    logger.info('更新 %d 条交易对的最新交易 %s 信息', update_count, freq)
                trade_date_latest_list = []

            # 仅调试使用
            if DEBUG and len(data_df_list) > 1:
                break
    finally:
        if data_count > 0:
            data_df_all = pd.concat(data_df_list)
            # data_df_all.to_sql(table_name, engine_md, if_exists='append', index=False, dtype=dtype)
            data_count = bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md, dtype)
            tot_data_count += data_count

        # 更新 info 表 trade_date_latest 字段
        if len(trade_date_latest_list) > 0:
            with with_db_session(engine_md) as session:
                result = session.execute(update_trade_date_latest_str, params=trade_date_latest_list)
                update_count = result.rowcount
                session.commit()
                logger.info('更新 %d 条交易对的最新交易日信息', update_count)

        if not has_table and engine_md.has_table(table_name):
            alter_table_2_myisam(engine_md, [table_name])
            # build_primary_key([table_name])
            create_pk_str = """ALTER TABLE {table_name}
                CHANGE COLUMN `exchange` `exchange` VARCHAR(60) NOT NULL FIRST,
                CHANGE COLUMN `symbol` `symbol` VARCHAR(60) NOT NULL AFTER `exchange`,
                CHANGE COLUMN `date` `date` DATE NOT NULL AFTER `symbol`,
                ADD PRIMARY KEY (`exchange`, `symbol`, `date`)""".format(table_name=table_name)
            with with_db_session(engine_md) as session:
                session.execute(create_pk_str)

        logging.info("更新 %s 完成 新增数据 %d 条", table_name, tot_data_count)


def import_coinbar_on_freq_min(freq, code_set=None, base_begin_time=None):
    """
    抓取 日级别以上数据[ 60min, 30min, 15min, 5min, 1min ]级别
    :param freq:
    :param code_set:
    :param base_begin_time:
    :return:
    """
    if base_begin_time is not None and not isinstance(base_begin_time, date):
        base_begin_time = str_2_date(base_begin_time)
    table_name = 'tushare_coin_md_' + freq
    info_table_name = 'tushare_coin_pair_info'
    has_table = engine_md.has_table(table_name)
    if has_table:
        sql_str = """SELECT exchange, exchange_pair, date_frm, 
                if(delist_date<end_date, delist_date, end_date) date_to
            FROM
            (
                SELECT info.exchange, info.exchange_pair, 
                    ifnull(trade_date_max_1, adddate(trade_date_latest,1)) date_frm, 
                    delist_date,
                    if(hour(now())<8, subdate(curdate(),2), subdate(curdate(),1)) end_date
                FROM 
                (
                    select exchange, exchange_pair,
                    ifnull(trade_date_latest_{freq},'2010-01-01') trade_date_latest,
                    delist_date_{freq} delist_date
                    from {info_table_name}
                ) info
                LEFT OUTER JOIN
                    (SELECT exchange, symbol, adddate(max(`date`),1) trade_date_max_1 
                     FROM {table_name} GROUP BY exchange, symbol) daily
                ON info.exchange = daily.exchange
                AND info.exchange_pair = daily.symbol
            ) tt
            WHERE date_frm <= if(delist_date<end_date, delist_date, end_date) 
            ORDER BY exchange, exchange_pair""".format(
            table_name=table_name, info_table_name=info_table_name, freq=freq)
    else:
        sql_str = """SELECT exchange, exchange_pair, date_frm, 
            if(delist_date<end_date, delist_date, end_date) date_to
        FROM
        (
            SELECT exchange, exchange_pair, 
                ifnull(trade_date_latest_{freq},date('2010-01-01')) date_frm, 
                delist_date_{freq} delist_date, 
                if(hour(now())<8, subdate(curdate(),2), subdate(curdate(),1)) end_date
            FROM {info_table_name} info 
            ORDER BY exchange, exchange_pair
        ) tt
        WHERE date_frm <= if(delist_date<end_date, delist_date, end_date) 
        ORDER BY exchange, exchange_pair""".format(info_table_name=info_table_name, freq=freq)
        logger.warning('%s 不存在，仅使用 %s 表进行计算日期范围', table_name, info_table_name)

    with with_db_session(engine_md) as session:
        # 获取每只股票需要获取日线数据的日期区间
        table = session.execute(sql_str)
        # 获取每只股票需要获取日线数据的日期区间
        code_date_range_dic = {
            (exchange, symbol):
                (date_from if base_begin_time is None else min([date_from, base_begin_time]),
                 date_to)
            for exchange, symbol, date_from, date_to in table.fetchall() if
            code_set is None or (exchange, symbol) in code_set}

    # 设置 dtype
    dtype = {
        'exchange': String(60),
        'symbol': String(60),
        'date': Date,
        'datetime': DateTime,
        'open': DOUBLE,
        'high': DOUBLE,
        'low': DOUBLE,
        'close': DOUBLE,
        'vol': DOUBLE,
    }

    # 更新 info 表 trade_date_latest 字段
    trade_date_latest_list = []
    update_trade_date_latest_str = """UPDATE tushare_coin_pair_info info
        SET info.trade_date_latest_daily = :trade_date_latest
        WHERE info.exchange = :exchange AND exchange_pair=:exchange_pair"""

    data_df_list, data_count, tot_data_count, code_count = [], 0, 0, len(code_date_range_dic)
    try:
        for num, ((exchange, exchange_pair), (begin_time, end_time)) in enumerate(code_date_range_dic.items(), start=1):
            begin_time_str = date_2_str(begin_time, DATE_FORMAT_STR)
            end_time_str = date_2_str(end_time, DATE_FORMAT_STR)
            logger.debug('%d/%d) %s %s [%s - %s]', num, code_count, exchange, exchange_pair, begin_time, end_time)
            try:
                # data_df = pro.coinbar(exchange='huobi', symbol='gxsbtc', freq='1min', start_date='20180701', end_date='20180801')
                data_df = pro.coinbar(exchange=exchange, symbol=exchange_pair, freq=freq,
                                      start_date=begin_time_str,
                                      end_date=end_time_str)
            except Exception as exp:
                if len(exp.args) >= 1 and exp.args[0] == '系统内部错误':
                    trade_date_latest_list.append({
                        'exchange': exchange,
                        'exchange_pair': exchange_pair,
                        'trade_date_latest': '2020-02-02',
                    })
                    logger.warning(
                        "coinbar(exchange='%s', symbol='%s', freq='%s', start_date='%s', end_date='%s') 系统内部错误",
                        exchange, exchange_pair, freq, begin_time_str, end_time_str)
                    continue
                logger.exception("coinbar(exchange='%s', symbol='%s', freq='%s', start_date='%s', end_date='%s')",
                                 exchange, exchange_pair, freq, begin_time_str, end_time_str)
                raise exp from exp

            if data_df is not None and data_df.shape[0] > 0:
                data_count += data_df.shape[0]
                data_df['exchange'] = exchange
                data_df['datetime'] = data_df['date']
                data_df['date'] = data_df['date'].apply(lambda x: str_2_datetime(x).date())
                data_df_list.append(data_df)

            # 记录最新交易日变化
            trade_date_latest_list.append({
                'exchange': exchange,
                'exchange_pair': exchange_pair,
                'trade_date_latest': end_time_str,
            })
            # 大于阀值有开始插入
            if data_count >= 10000:
                data_df_all = pd.concat(data_df_list)
                # data_df_all.to_sql(table_name, engine_md, if_exists='append', index=False, dtype=dtype)
                data_count = bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md, dtype)
                tot_data_count += data_count
                data_df_list, data_count = [], 0

                # 更新 info 表 trade_date_latest 字段
                with with_db_session(engine_md) as session:
                    result = session.execute(update_trade_date_latest_str, params=trade_date_latest_list)
                    update_count = result.rowcount
                    session.commit()
                    logger.info('更新 %d 条交易对的最新交易 %s 信息', update_count, freq)
                trade_date_latest_list = []

            # 仅调试使用
            if DEBUG and len(data_df_list) > 1:
                break
    finally:
        if data_count > 0:
            data_df_all = pd.concat(data_df_list)
            # data_df_all.to_sql(table_name, engine_md, if_exists='append', index=False, dtype=dtype)
            data_count = bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md, dtype)
            tot_data_count += data_count

        # 更新 info 表 trade_date_latest 字段
        if len(trade_date_latest_list) > 0:
            with with_db_session(engine_md) as session:
                result = session.execute(update_trade_date_latest_str, params=trade_date_latest_list)
                update_count = result.rowcount
                session.commit()
                logger.info('更新 %d 条交易对的最新交易日信息', update_count)

        if not has_table and engine_md.has_table(table_name):
            alter_table_2_myisam(engine_md, [table_name])
            # build_primary_key([table_name])
            create_pk_str = """ALTER TABLE {table_name}
                CHANGE COLUMN `exchange` `exchange` VARCHAR(60) NOT NULL FIRST,
                CHANGE COLUMN `symbol` `symbol` VARCHAR(60) NOT NULL AFTER `exchange`,
                CHANGE COLUMN `datetime` `datetime` DATETIME NOT NULL AFTER `symbol`,
                ADD PRIMARY KEY (`exchange`, `symbol`, `datetime`)""".format(table_name=table_name)
            with with_db_session(engine_md) as session:
                session.execute(create_pk_str)

        logging.info("更新 %s 完成 新增数据 %d 条", table_name, tot_data_count)


if __name__ == "__main__":
    DEBUG = True
    # 交易所基本信息
    import_exchange_info()
    # 获取全球交易币基本信息
    import_coin_info()
    # 获取交易对信息
    import_coin_pair_info()
    # 获取行情数据
    import_coinbar()
