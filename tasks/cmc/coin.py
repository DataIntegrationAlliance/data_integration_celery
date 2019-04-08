#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/8/27 9:48
@File    : coin.py
@contact : mmmaaaggg@163.com
@desc    : v1 接口将于2018年12月关闭，此后将只能试用pro接口
"""
import datetime
from cryptocmd import CmcScraper
import pandas as pd
from tasks import app
import requests
from cryptocmd.utils import InvalidCoinCode, get_url_data, extract_data, download_coin_data
from sqlalchemy.types import String, Date, Integer
from sqlalchemy.dialects.mysql import DOUBLE, DATETIME
from tasks.backend import engine_md
from ibats_utils.db import with_db_session, alter_table_2_myisam, bunch_insert_on_duplicate_update, execute_sql
from ibats_utils.mess import str_2_date, date_2_str, str_2_datetime
from tasks.config import config
import logging

DEBUG = False
logger = logging.getLogger()
DATE_FORMAT_STR_CMC = '%d-%m-%Y'
DATE_FORMAT_STR = '%Y-%m-%d'
DATETIME_FORMAT_STR = '%Y-%m-%dT%H:%M:%S.%fZ'


def get_coin_ids(coin_code):
    """
    This method fetches the name(id) of currency from the given code
    :param coin_code: coin code of a cryptocurrency e.g. btc
    :return: coin-id for the a cryptocurrency on the coinmarketcap.com
    """
    ids = []
    try:
        url = 'https://api.coinmarketcap.com/v1/ticker/?limit=0'

        json_resp = get_url_data(url).json()

        coin_code = coin_code.upper()

        for coin in json_resp:
            if coin['symbol'] == coin_code:
                ids.append(coin['id'])
        if len(ids) == 0:
            raise InvalidCoinCode('This coin code is unavailable on "coinmarketcap.com"')
    except Exception as e:
        raise e
    return ids


def download_coin_data_by_id(coin_id, start_date, end_date):
    """
    Download HTML price history for the specified cryptocurrency and time range from CoinMarketCap.

    :param coin_id: coin_id of a cryptocurrency e.g. btc
    :param start_date: date since when to scrape data (in the format of dd-mm-yyyy)
    :param end_date: date to which scrape the data (in the format of dd-mm-yyyy)
    :return: returns html of the webpage having historical data of cryptocurrency for certain duration
    """

    if start_date is None:
        # default start date on coinmarketcap.com
        start_date = '28-4-2013'

    if end_date is None:
        yesterday = datetime.date.today() - datetime.timedelta(1)
        end_date = yesterday.strftime('%d-%m-%Y')

    # coin_id = get_coin_id(coin_code)

    # Format the dates as required for the url.
    start_date = datetime.datetime.strptime(start_date, '%d-%m-%Y').strftime('%Y%m%d')
    end_date = datetime.datetime.strptime(end_date, '%d-%m-%Y').strftime('%Y%m%d')

    url = 'https://coinmarketcap.com/currencies/{0}/historical-data/?start={1}&end={2}'.format(coin_id, start_date,
                                                                                               end_date)

    try:
        html = get_url_data(url).text
        return html
    except Exception as e:
        print("Error fetching price data for {} for interval '{}' and '{}'", coin_id, start_date, end_date)

        if hasattr(e, 'message'):
            print('Error message (download_data) :', e.message)
        else:
            print('Error message (download_data) :', e)


class CmcScraperV1(CmcScraper):
    """扩展原有类，支持根据id进行数据下载"""

    def __init__(self, coin_code, coin_id=None, start_date=None, end_date=None, all_time=False):
        """

        :param coin_code: coin code of cryptocurrency e.g. btc
        :param coin_id: default None, somtimes has more than one coin, like:
            ACC has three coins: adcoin, accelerator-network, acchain, with different ids
        :param start_date: date since when to scrape data (in the format of dd-mm-yyyy)
        :param end_date: date to which scrape the data (in the format of dd-mm-yyyy)
        :param all_time: 'True' if need data of all time for respective cryptocurrency
        """
        CmcScraper.__init__(self, coin_code, start_date, end_date, all_time)
        self.coin_id = coin_id

    def _download_data(self, **kwargs):
        """
        This method downloads the data.
        :param forced: (optional) if ``True``, data will be re-downloaded.
        :return:
        """

        forced = kwargs.get('forced')

        if self.headers and self.rows and not forced:
            return

        if self.all_time:
            self.start_date, self.end_date = None, None

        if self.coin_id is None:
            table = download_coin_data(self.coin_code, self.start_date, self.end_date)
        else:
            table = download_coin_data_by_id(self.coin_id, self.start_date, self.end_date)

        # self.headers, self.rows, self.start_date, self.end_date = extract_data(table)
        self.end_date, self.start_date, self.headers, self.rows = extract_data(table)


@app.task
def import_coin_info(chain_param=None, ):
    """插入基础信息数据到 cmc_coin_v1_info"""
    table_name = "cmc_coin_v1_info"
    logging.info("更新 %s 开始", table_name)
    has_table = engine_md.has_table(table_name)
    # url = 'https://api.coinmarketcap.com/v2/listings/'
    # dtype = {
    #     'id': String(60),
    #     'name': String(60),
    #     'symbol': String(20),
    #     'website_slug': String(60),
    # }

    url = 'https://api.coinmarketcap.com/v1/ticker/?limit=0'
    dtype = {
        'id': String(60),
        'name': String(60),
        'symbol': String(20),
        'rank': Integer,
        'price_usd': DOUBLE,
        'price_btc': DOUBLE,
        '24h_volume_usd': DOUBLE,
        'market_cap_usd': DOUBLE,
        'available_supply': DOUBLE,
        'total_supply': DOUBLE,
        'max_supply': DOUBLE,
        'percent_change_1h': DOUBLE,
        'percent_change_24h': DOUBLE,
        'percent_change_7d': DOUBLE,
        'last_updated': DATETIME,
    }
    rsp = requests.get(url)
    if rsp.status_code != 200:
        raise ValueError('请求 listings 相应失败')
    json = rsp.json()
    data_df = pd.DataFrame(json)
    data_df['last_updated'] = data_df['last_updated'].apply(
        lambda x: None if x is None else datetime.datetime.fromtimestamp(float(x)))
    data_count = bunch_insert_on_duplicate_update(data_df, table_name, engine_md, dtype=dtype)
    logging.info("更新 %s 完成 存量数据 %d 条", table_name, data_count)
    if not has_table and engine_md.has_table(table_name):
        alter_table_2_myisam(engine_md, [table_name])
        # build_primary_key([table_name])
        create_pk_str = """ALTER TABLE {table_name}
        CHANGE COLUMN `id` `id` VARCHAR(60) NOT NULL FIRST ,
        ADD PRIMARY KEY (`id`)""".format(table_name=table_name)
        with with_db_session(engine_md) as session:
            session.execute(create_pk_str)

    # 更新 code_mapping 表
    # update_from_info_table(table_name)


def rename_by_dic(name, names):
    """模糊匹配名称，如果找到，则重命名，否则保留原名"""
    name = name.lower()
    for candidate in names:
        if name.find(candidate) != -1:
            return candidate

    return name


@app.task
def import_coin_daily(chain_param=None, id_set=None, begin_time=None):
    """插入历史数据到 cmc_coin_v1_daily 试用 v1 接口，该接口可能在2018年12月底到期"""
    table_name = "cmc_coin_v1_daily"
    info_table_name = "cmc_coin_v1_info"
    logging.info("更新 %s 开始", table_name)
    has_table = engine_md.has_table(table_name)
    if has_table:
        sql_str = """
           SELECT id, symbol, date_frm, if(delist_date<end_date, delist_date, end_date) date_to
           FROM
           (
               SELECT info.id, symbol, ifnull(trade_date,date('2013-04-28')) date_frm, null delist_date,
               if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
               FROM 
                   {info_table_name} info 
               LEFT OUTER JOIN
                   (SELECT id, adddate(max(date),1) trade_date FROM {table_name} GROUP BY id) daily
               ON info.id = daily.id
           ) tt
           WHERE date_frm <= if(delist_date<end_date, delist_date, end_date) 
           ORDER BY id""".format(table_name=table_name, info_table_name=info_table_name)
    else:
        logger.warning('%s 不存在，仅使用 %s 表进行计算日期范围', table_name, info_table_name)
        sql_str = """
           SELECT id, symbol, date_frm, if(delist_date<end_date, delist_date, end_date) date_to
           FROM
           (
               SELECT id, symbol, null date_frm, null delist_date,
               if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
               FROM {info_table_name} info 
           ) tt
           ORDER BY id""".format(info_table_name=info_table_name)

    with with_db_session(engine_md) as session:
        # 获取每只股票需要获取日线数据的日期区间
        table = session.execute(sql_str)
        # 获取每只股票需要获取日线数据的日期区间
        stock_date_dic = {
            (coin_id, symbol): (date_from if begin_time is None else min([date_from, begin_time]), date_to)
            for coin_id, symbol, date_from, date_to in table.fetchall() if
            id_set is None or coin_id in id_set}
    # 设置 dtype
    dtype = {
        'id': String(60),
        'date': Date,
        'open': DOUBLE,
        'high': DOUBLE,
        'low': DOUBLE,
        'close': DOUBLE,
        'volume': DOUBLE,
        'market_cap': DOUBLE,
    }
    col_names = dtype.keys()
    data_df_list = []
    dic_count = len(stock_date_dic)
    data_count = 0
    # 获取接口数据
    logger.info('%d coins will been import into %s', dic_count, table_name)
    try:
        for data_num, ((coin_id, symbol), (date_from, date_to)) in enumerate(stock_date_dic.items(), start=1):
            logger.debug('%d/%d) %s[%s] [%s - %s]', data_num, dic_count, coin_id, symbol, date_from, date_to)
            date_from_str = None
            try:
                if date_from is None:
                    scraper = CmcScraperV1(symbol, coin_id)
                else:
                    date_from_str = date_2_str(str_2_date(date_from, DATE_FORMAT_STR), DATE_FORMAT_STR_CMC)
                    scraper = CmcScraperV1(symbol, coin_id, start_date=date_from_str)
                data_df = scraper.get_dataframe()
            except Exception as exp:
                logger.exception("scraper('%s', '%s', start_date='%s')", symbol, coin_id, date_from_str)
                continue

            if data_df is None or data_df.shape[0] == 0:
                logger.warning('%d/%d) %s has no data during %s %s', data_num, dic_count, coin_id, date_from, date_to)
                continue
            data_df.rename(columns={col_name: rename_by_dic(col_name, col_names) for col_name in data_df.columns},
                           inplace=True)
            data_df.rename(columns={'market cap': 'market_cap'}, inplace=True)
            data_df['market_cap'] = data_df['market_cap'].apply(lambda x: 0 if isinstance(x, str) else x)
            data_df['volume'] = data_df['volume'].apply(lambda x: 0 if isinstance(x, str) else x)
            logger.info('%d/%d) %d data of %s between %s and %s', data_num, dic_count, data_df.shape[0], coin_id,
                        data_df['date'].min(), data_df['date'].max())
            data_df['id'] = coin_id
            data_df_list.append(data_df)
            data_count += data_df.shape[0]
            # 仅供调试使用
            if DEBUG and len(data_df_list) > 10:
                break

            if data_count > 10000:
                data_df_all = pd.concat(data_df_list)
                data_count = bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md, dtype=dtype)
                logging.info("%s %d 条信息被更新", table_name, data_count)
                data_df_list, data_count = [], 0

    finally:
        # 导入数据库 创建
        if len(data_df_list) > 0:
            data_df_all = pd.concat(data_df_list)
            data_count = bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md, dtype=dtype)
            logging.info("更新 %s 结束 %d 条信息被更新", table_name, data_count)

        if not has_table and engine_md.has_table(table_name):
            alter_table_2_myisam(engine_md, [table_name])
            # build_primary_key([table_name])
            create_pk_str = """ALTER TABLE {table_name}
            CHANGE COLUMN `id` `id` VARCHAR(60) NOT NULL FIRST ,
            CHANGE COLUMN `date` `date` DATE NOT NULL AFTER `id`,
            ADD PRIMARY KEY (`id`, `date`)""".format(table_name=table_name)
            with with_db_session(engine_md) as session:
                session.execute(create_pk_str)


@app.task
def import_coin_latest(chain_param=None, ):
    """插入最新价格数据到 cmc_coin_pro_latest """
    table_name = 'cmc_coin_pro_latest'
    has_table = engine_md.has_table(table_name)
    # 设置 dtype
    dtype = {
        'id': Integer,
        'name': String(60),
        'slug': String(60),
        'symbol': String(20),
        'date_added': DATETIME,
        'last_updated': DATETIME,
        'market_cap': DOUBLE,
        'circulating_supply': DOUBLE,
        'max_supply': DOUBLE,
        'num_market_pairs': DOUBLE,
        'percent_change_1h': DOUBLE,
        'percent_change_24h': DOUBLE,
        'percent_change_7d': DOUBLE,
        'price': DOUBLE,
        'total_supply': DOUBLE,
        'volume_24h': DOUBLE,
        'cmc_rank': DOUBLE,
    }

    header = {
        'Content-Type': 'application/json',
        'X-CMC_PRO_API_KEY': config.CMC_PRO_API_KEY
    }
    params = {
        # 'CMC_PRO_API_KEY': config.CMC_PRO_API_KEY,
        'limit': 5000,
        'start': 1
    }
    # https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest?sort=market_cap&start=0&limit=10&cryptocurrency_type=tokens&convert=USD,BTC
    url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
    rsp = requests.get(url=url, params=params, headers=header)
    if rsp.status_code != 200:
        logger.error('获取数据异常[%d] %s', rsp.status_code, rsp.content)
        return
    ret_dic = rsp.json()
    data_list = ret_dic['data']

    data_dic_list = []
    for dic in data_list:
        data_dic = {}
        for key, val in dic.items():
            if key == 'quote':
                for sub_key, sub_val in val['USD'].items():
                    data_dic[sub_key] = sub_val
            else:
                data_dic[key] = val
        data_dic_list.append(data_dic)

    data_df = pd.DataFrame(data_dic_list)
    # 数据整理
    data_df['date_added'] = data_df['date_added'].apply(lambda x:  str_2_datetime(x, DATETIME_FORMAT_STR))
    data_df['last_updated'] = data_df['last_updated'].apply(lambda x: str_2_datetime(x, DATETIME_FORMAT_STR))
    data_count = bunch_insert_on_duplicate_update(data_df, table_name, engine_md, dtype=dtype)
    logging.info("更新 %s 结束 %d 条信息被更新", table_name, data_count)
    if not has_table and engine_md.has_table(table_name):
        alter_table_2_myisam(engine_md, [table_name])
        # build_primary_key([table_name])
        create_pk_str = """ALTER TABLE {table_name}
        CHANGE COLUMN `id` `id` VARCHAR(60) NOT NULL FIRST ,
        CHANGE COLUMN `last_updated` `last_updated` DATETIME NOT NULL AFTER `id`,
        ADD PRIMARY KEY (`id`, `last_updated`)""".format(table_name=table_name)
        execute_sql(engine_md, create_pk_str)


@app.task
def merge_latest(chain_param=None, ):
    """
    将 cmc_coin_v1_daily 历史数据 以及 cmc_coin_pro_latest 最新价格数据 合并到 cmc_coin_merged_latest
    :return:
    """
    table_name = 'cmc_coin_merged_latest'
    logger.info("开始合并数据到 %s 表", table_name)
    has_table = engine_md.has_table(table_name)
    create_sql_str = """CREATE TABLE {table_name} (
      `id` VARCHAR(60) NOT NULL,
      `date` DATE NOT NULL,
      `datetime` DATETIME NULL,
      `name` VARCHAR(60) NULL,
      `symbol` VARCHAR(20) NULL,
      `close` DOUBLE NULL,
      `volume` DOUBLE NULL,
      `market_cap` DOUBLE NULL,
      PRIMARY KEY (`id`, `date`))
    ENGINE = MyISAM""".format(table_name=table_name)
    with with_db_session(engine_md) as session:
        if not has_table:
            session.execute(create_sql_str)
            logger.info("创建 %s 表", table_name)
        session.execute('truncate table {table_name}'.format(table_name=table_name))
        insert_sql_str = """INSERT INTO `{table_name}` 
            (`id`, `date`, `datetime`, `name`, `symbol`, `close`, `volume`, `market_cap`) 
            select daily.id, `date`, `date`, `name`, `symbol`, `close`, `volume`, `market_cap` 
            from cmc_coin_v1_daily daily
            left join cmc_coin_v1_info info
            on daily.id = info.id""".format(table_name=table_name)
        session.execute(insert_sql_str)
        session.commit()
        insert_latest_sql_str = """INSERT INTO `{table_name}` 
            (`id`, `date`, `datetime`, `name`, `symbol`, `close`, `volume`, `market_cap`) 
            select info.id, date(latest.last_updated), latest.last_updated, 
                latest.name, latest.symbol, price, volume_24h, market_cap
            from cmc_coin_pro_latest latest
            left join
            (
                select latest.name, latest.symbol, max(latest.last_updated) last_updated
                from cmc_coin_pro_latest latest
                group by latest.name, latest.symbol
            ) g
            on latest.name = g.name
            and latest.symbol = g.symbol
            and latest.last_updated = g.last_updated
            left outer join cmc_coin_v1_info info
            on latest.name = info.name
            and latest.symbol = info.symbol
            on duplicate key update
                `datetime`=values(`datetime`), 
                `name`=values(`name`), 
                `symbol`=values(`symbol`), 
                `close`=values(`close`), 
                `volume`=values(`volume`), 
                `market_cap`=values(`market_cap`)""".format(table_name=table_name)
        session.execute(insert_latest_sql_str)
        session.commit()
        data_count = session.execute("select count(*) from {table_name}".format(table_name=table_name)).scalar()
        logger.info("%d 条记录插入到 %s", data_count, table_name)


if __name__ == "__main__":
    DEBUG = True
    # import_coin_info()
    import_coin_daily()
    # import_coin_latest()
    # merge_latest()
