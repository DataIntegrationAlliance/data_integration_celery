# -*- coding: utf-8 -*-
"""
Created on 2017/4/14
@author: MG
@desc    : 2018-08-21 已经正式运行测试完成，可以正常使用
"""
import pandas as pd
import logging
from tasks.backend.orm import build_primary_key
from datetime import date, datetime, timedelta
from tasks.wind import invoker
from direstinvoker import APIError
from tasks.utils.fh_utils import STR_FORMAT_DATE, split_chunk
from tasks import app
from sqlalchemy.exc import IntegrityError
from sqlalchemy.types import String, Date, Integer, DateTime
from sqlalchemy.dialects.mysql import DOUBLE
from tasks.backend import engine_md
from tasks.merge.code_mapping import update_from_info_table
from tasks.utils.db_utils import with_db_session, add_col_2_table, alter_table_2_myisam, \
    bunch_insert_on_duplicate_update

logger = logging.getLogger()
DATE_BASE = datetime.strptime('2005-01-01', STR_FORMAT_DATE).date()
ONE_DAY = timedelta(days=1)
DEBUG = False

# 标示每天几点以后下载当日行情数据
BASE_LINE_HOUR = 16


def get_stock_code_set(date_fetch):
    """
     # 通过接口获取股票代码
    :param date_fetch:
    :return:
    """
    date_fetch_str = date_fetch.strftime(STR_FORMAT_DATE)
    stock_df = invoker.wset("sectorconstituent", "date=%s;sectorid=a001010100000000" % date_fetch_str)
    if stock_df is None:
        logging.warning('%s 获取股票代码失败', date_fetch_str)
        return None
    stock_count = stock_df.shape[0]
    logging.info('get %d wind_code on %s', stock_count, date_fetch_str)
    return set(stock_df['wind_code'])


@app.task
def import_wind_stock_info(chain_param=None, refresh=False):
    """
    :param chain_param:  在celery 中將前面結果做爲參數傳給後面的任務
    :param refresh:获取全市场股票代码及名称
    :return:
    """
    table_name = 'wind_stock_info'
    logging.info("更新 %s 开始", table_name)
    has_table = engine_md.has_table(table_name)
    wind_indicator_param_list = [
        ('sec_name', String(20)),
        ('trade_code', String(20)),
        ('ipo_date', Date),
        ('delist_date', Date),
        ('mkt', String(20)),
        ('exch_city', String(20)),
        ('exch_eng', String(20)),
        ('prename', String(2000)),
    ]
    # 获取列属性名，以逗号进行分割 "ipo_date,trade_code,mkt,exch_city,exch_eng"
    param = ",".join([key for key, _ in wind_indicator_param_list])
    # 设置 dtype
    dtype = {key: val for key, val in wind_indicator_param_list}
    dtype['wind_code'] = String(20)
    if refresh:
        date_fetch = datetime.strptime('2005-1-1', STR_FORMAT_DATE).date()
    else:
        date_fetch = date.today()
    date_end = date.today()
    stock_code_set = set()
    # 对date_fetch 进行一个判断，获取stock_code_set
    while date_fetch < date_end:
        stock_code_set_sub = get_stock_code_set(date_fetch)
        if stock_code_set_sub is not None:
            stock_code_set |= stock_code_set_sub
        date_fetch += timedelta(days=365)
    stock_code_set_sub = get_stock_code_set(date_end)
    if stock_code_set_sub is not None:
        stock_code_set |= stock_code_set_sub
    # 获取股票对应上市日期，及摘牌日期
    # w.wss("300005.SZ,300372.SZ,000003.SZ", "ipo_date,trade_code,mkt,exch_city,exch_eng")
    stock_code_list = list(stock_code_set)
    seg_count = 1000
    stock_info_df_list = []

    # 进行循环遍历获取stock_code_list_sub
    for stock_code_list_sub in split_chunk(stock_code_list, seg_count):
        # 尝试将 stock_code_list_sub 直接传递给wss，是否可行
        stock_info_df = invoker.wss(stock_code_list_sub, param)
        stock_info_df_list.append(stock_info_df)
        if DEBUG:
            break
    # 对数据表进行规范整理.整合,索引重命名
    stock_info_all_df = pd.concat(stock_info_df_list)
    stock_info_all_df.index.rename('wind_code', inplace=True)
    logging.info('%d data will be import', stock_info_all_df.shape[0])
    stock_info_all_df.reset_index(inplace=True)
    # data_list = list(stock_info_all_df.T.to_dict().values())
    # 对wind_stock_info表进行数据插入
    # sql_str = "REPLACE INTO {table_name} (wind_code, trade_code, sec_name, ipo_date, delist_date, mkt, exch_city, exch_eng, prename) values (:WIND_CODE, :TRADE_CODE, :SEC_NAME, :IPO_DATE, :DELIST_DATE, :MKT, :EXCH_CITY, :EXCH_ENG, :PRENAME)"
    # 事物提交执行更新
    # with with_db_session(engine_md) as session:
    #     session.execute(sql_str, data_list)
    #     data_count = session.execute('select count(*) from {table_name}').scalar()
    data_count = bunch_insert_on_duplicate_update(stock_info_all_df, table_name, engine_md, dtype=dtype)
    logging.info("更新 %s 完成 存量数据 %d 条", table_name, data_count)
    if not has_table and engine_md.has_table(table_name):
        alter_table_2_myisam(engine_md, [table_name])
        build_primary_key([table_name])

    # 更新 code_mapping 表
    update_from_info_table(table_name)


@app.task
def import_stock_daily(chain_param=None, wind_code_set=None):
    """
    插入股票日线数据到最近一个工作日-1。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    :param chain_param:  在celery 中將前面結果做爲參數傳給後面的任務
    :return:
    """
    table_name = 'wind_stock_daily'
    logging.info("更新 %s 开始", table_name)
    param_list = [
        ('open', DOUBLE),
        ('high', DOUBLE),
        ('low', DOUBLE),
        ('close', DOUBLE),
        ('adjfactor', DOUBLE),
        ('volume', DOUBLE),
        ('amt', DOUBLE),
        ('pct_chg', DOUBLE),
        ('maxupordown', Integer),
        ('swing', DOUBLE),
        ('turn', DOUBLE),
        ('free_turn', DOUBLE),
        ('trade_status', String(30)),
        ('susp_days', Integer),
        ('total_shares', DOUBLE),
        ('free_float_shares', DOUBLE),
        ('ev2_to_ebitda', DOUBLE),
    ]
    wind_indictor_str = ",".join([key for key, _ in param_list])
    rename_col_dic = {key.upper(): key.lower() for key, _ in param_list}
    has_table = engine_md.has_table(table_name)
    # 进行表格判断，确定是否含有wind_stock_daily
    if has_table:
        sql_str = """
            SELECT wind_code, date_frm, if(delist_date<end_date, delist_date, end_date) date_to
            FROM
            (
            SELECT info.wind_code, ifnull(trade_date, ipo_date) date_frm, delist_date,
            if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
            FROM 
                wind_stock_info info 
            LEFT OUTER JOIN
                (SELECT wind_code, adddate(max(trade_date),1) trade_date FROM {table_name} GROUP BY wind_code) daily
            ON info.wind_code = daily.wind_code
            ) tt
            WHERE date_frm <= if(delist_date<end_date, delist_date, end_date) 
            ORDER BY wind_code""".format(table_name=table_name)
    else:
        sql_str = """
            SELECT wind_code, date_frm, if(delist_date<end_date, delist_date, end_date) date_to
            FROM
              (
                SELECT info.wind_code, ipo_date date_frm, delist_date,
                if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
                FROM wind_stock_info info 
              ) tt
            WHERE date_frm <= if(delist_date<end_date, delist_date, end_date) 
            ORDER BY wind_code"""
        logger.warning('%s 不存在，仅使用 wind_stock_info 表进行计算日期范围', table_name)

    with with_db_session(engine_md) as session:
        # 获取每只股票需要获取日线数据的日期区间
        table = session.execute(sql_str)
        # 计算每只股票需要获取日线数据的日期区间
        begin_time = None
        # 获取date_from,date_to，将date_from,date_to做为value值
        code_date_range_dic = {
            wind_code: (date_from if begin_time is None else min([date_from, begin_time]), date_to)
            for wind_code, date_from, date_to in table.fetchall() if
            wind_code_set is None or wind_code in wind_code_set}
    # 设置 dtype
    dtype = {key: val for key, val in param_list}
    dtype['wind_code'] = String(20)
    dtype['trade_date'] = Date

    data_df_list = []
    data_len = len(code_date_range_dic)
    logger.info('%d stocks will been import into wind_stock_daily', data_len)
    # 将data_df数据，添加到data_df_list
    try:
        for num, (wind_code, (date_from, date_to)) in enumerate(code_date_range_dic.items(), start=1):
            logger.debug('%d/%d) %s [%s - %s]', num, data_len, wind_code, date_from, date_to)
            try:
                data_df = invoker.wsd(wind_code, wind_indictor_str, date_from, date_to)
            except APIError as exp:
                logger.exception("%d/%d) %s 执行异常", num, data_len, wind_code)
                if exp.ret_dic.setdefault('error_code', 0) in (
                        -40520007,  # 没有可用数据
                        -40521009,  # 数据解码失败。检查输入参数是否正确，如：日期参数注意大小月月末及短二月
                ):
                    continue
                else:
                    break
            if data_df is None:
                logger.warning('%d/%d) %s has no data during %s %s', num, data_len, wind_code, date_from, date_to)
                continue
            logger.info('%d/%d) %d data of %s between %s and %s', num, data_len, data_df.shape[0], wind_code, date_from,
                        date_to)
            data_df['wind_code'] = wind_code
            data_df_list.append(data_df)
            # 仅调试使用
            if DEBUG and len(data_df_list) > 2:
                break
    finally:
        # 导入数据库
        if len(data_df_list) > 0:
            data_df_all = pd.concat(data_df_list)
            data_df_all.index.rename('trade_date', inplace=True)
            data_df_all.reset_index(inplace=True)
            data_df_all.rename(columns=rename_col_dic, inplace=True)
            # data_df_all.set_index(['wind_code', 'trade_date'], inplace=True)
            # data_df_all.to_sql('wind_stock_daily', engine_md, if_exists='append', dtype=dtype)
            # logging.info("更新 wind_stock_daily 结束 %d 条信息被更新", data_df_all.shape[0])
            data_count = bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md, dtype)
            logging.info("更新 %s 结束 %d 条信息被更新", table_name, data_count)
            if not has_table and engine_md.has_table(table_name):
                alter_table_2_myisam(engine_md, [table_name])
                build_primary_key([table_name])


@app.task
def add_new_col_data(col_name, param, chain_param=None, db_col_name=None, col_type_str='DOUBLE',
                     wind_code_set: set = None):
    """
    1）修改 daily 表，增加字段
    2）wind_ckdvp_stock表增加数据
    3）第二部不见得1天能够完成，当第二部完成后，将wind_ckdvp_stock数据更新daily表中
    :param col_name:增加字段名称
    :param param: 参数
    :param chain_param:  在celery 中將前面結果做爲參數傳給後面的任務
    :param db_col_name: 默认为 None，此时与col_name相同
    :param col_type_str: DOUBLE, VARCHAR(20), INTEGER, etc. 不区分大小写
    :param wind_code_set: 默认 None， 否则仅更新指定 wind_code
    :return:
    """
    if db_col_name is None:
        # 默认为 None，此时与col_name相同
        db_col_name = col_name

    # 检查当前数据库是否存在 db_col_name 列，如果不存在则添加该列
    add_col_2_table(engine_md, 'wind_stock_daily', db_col_name, col_type_str)
    # 将数据增量保存到 wind_ckdvp_stock 表
    all_finished = add_data_2_ckdvp(col_name, param, wind_code_set)
    # 将数据更新到 ds 表中
    # 对表的列进行整合，daily表的列属性值插入wind_ckdvp_stock的value 根据所有条件进行判定
    if all_finished:
        sql_str = """
            update wind_stock_daily daily, wind_ckdvp_stock ckdvp
            set daily.{db_col_name} = ckdvp.value
            where daily.wind_code = ckdvp.wind_code
            and ckdvp.key = '{db_col_name}' and ckdvp.param = '{param}'

            and ckdvp.time = daily.trade_date""".format(db_col_name=db_col_name, param=param)
        # 进行事务提交
        with with_db_session(engine_md) as session:
            rst = session.execute(sql_str)
            data_count = rst.rowcount
            session.commit()
        logger.info('更新 %s 字段 wind_stock_daily 表 %d 条记录', db_col_name, data_count)


def add_data_2_ckdvp(col_name, param, wind_code_set: set = None, begin_time=None):
    """判断表格是否存在，存在则进行表格的关联查询
    :param col_name: 增加的列属性名
    :param param: 参数
    :param wind_code_set: 默认为None
    :param begin_time: 默认为None
    :return:
    """
    table_name = 'wind_ckdvp_stock'
    all_finished = False
    has_table = engine_md.has_table('wind_ckdvp_stock')
    if has_table:
        # 执行语句，表格数据联立
        sql_str = """
            select wind_code, date_frm, if(delist_date<end_date, delist_date, end_date) date_to
            FROM
            (
                select info.wind_code,
                    (ipo_date) date_frm, delist_date,
                    if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
                from wind_stock_info info
                left outer join
                    (select wind_code, adddate(max(time),1) from wind_ckdvp_stock
                    where wind_ckdvp_stock.key='{0}' and param='{1}' group by wind_code
                    ) daily
                on info.wind_code = daily.wind_code
            ) tt
            where date_frm <= if(delist_date<end_date,delist_date, end_date)
            order by wind_code""".format(col_name, param)
    else:
        logger.warning('wind_ckdvp_stock 不存在，仅使用 wind_stock_info 表进行计算日期范围')
        sql_str = """
            SELECT wind_code, date_frm,
                if(delist_date<end_date,delist_date, end_date) date_to
            FROM
            (
                SELECT info.wind_code,ipo_date date_frm, delist_date,
                if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
                FROM wind_stock_info info
            ) tt
            WHERE date_frm <= if(delist_date<end_date, delist_date, end_date)
            ORDER BY wind_code"""
    with with_db_session(engine_md) as session:
        # 获取每只股票需要获取日线数据的日期区间
        table = session.execute(sql_str)
        code_date_range_dic = {
            wind_code: (date_from if begin_time is None else min([date_from, begin_time]), date_to)
            for wind_code, date_from, date_to in table.fetchall() if
            wind_code_set is None or wind_code in wind_code_set}

        # 设置 dtype
        dtype = {
            'wind_code': String(20),
            'key': String(80),
            'time': Date,
            'value': String(80),
            'param': String(80),
        }
        data_df_list, data_count, tot_data_count, code_count = [], 0, 0, len(code_date_range_dic)
        try:
            for num, (wind_code, (date_from, date_to)) in enumerate(code_date_range_dic.items(), start=1):
                logger.debug('%d/%d) %s [%s - %s]', num, code_count, wind_code, date_from, date_to)
                data_df = invoker.wsd(
                    wind_code,
                    col_name,
                    date_from,
                    date_to,
                    param
                )
                if data_df is not None and data_df.shape[0] > 0:
                    # 对我们的表格进行规范整理,整理我们的列名，索引更改
                    data_df['key'] = col_name
                    data_df['param'] = param
                    data_df['wind_code'] = wind_code
                    data_df.rename(columns={col_name.upper(): 'value'}, inplace=True)
                    data_df.index.rename('time', inplace=True)
                    data_df.reset_index(inplace=True)
                    data_count += data_df.shape[0]
                    data_df_list.append(data_df)

                # 大于阀值有开始插入
                if data_count >= 10000:
                    tot_data_df = pd.concat(data_df_list)
                    # tot_data_df.to_sql(table_name, engine_md, if_exists='append', index=False, dtype=dtype)
                    data_count = bunch_insert_on_duplicate_update(tot_data_df, table_name, engine_md, dtype)
                    tot_data_count += data_count
                    data_df_list, data_count = [], 0

                # 仅调试使用
                if DEBUG and len(data_df_list) > 1:
                    break

                all_finished = True
        finally:
            if data_count > 0:
                tot_data_df = pd.concat(data_df_list)
                # tot_data_df.to_sql(table_name, engine_md, if_exists='append', index=False, dtype=dtype)
                data_count = bunch_insert_on_duplicate_update(tot_data_df, table_name, engine_md, dtype)
                tot_data_count += data_count

            if not has_table and engine_md.has_table(table_name):
                create_pk_str = """ALTER TABLE {table_name}
                    CHANGE COLUMN `wind_code` `wind_code` VARCHAR(20) NOT NULL ,
                    CHANGE COLUMN `time` `time` DATE NOT NULL ,
                    CHANGE COLUMN `key` `key` VARCHAR(80) NOT NULL ,
                    CHANGE COLUMN `param` `param` VARCHAR(80) NOT NULL ,
                    ADD PRIMARY KEY (`wind_code`, `time`, `key`, `param`)""".format(table_name=table_name)
                with with_db_session(engine_md) as session:
                    session.execute(create_pk_str)

            logging.info("更新 %s 完成 新增数据 %d 条", table_name, tot_data_count)
            if not has_table and engine_md.has_table(table_name):
                alter_table_2_myisam(engine_md, [table_name])
                build_primary_key([table_name])
        return all_finished


@app.task
def import_stock_tick(wind_code_set: None, chain_param=None, ):
    """
    插入股票日线数据到最近一个工作日-1
    :param chain_param:  在celery 中將前面結果做爲參數傳給後面的任務
    :return:
    """
    import_count = 0
    table_name = 'wind_stock_tick'
    has_table = engine_md.has_table(table_name)
    param_list = [
        ('open', DOUBLE),
        ('high', DOUBLE),
        ('low', DOUBLE),
        ('pre_close', DOUBLE),
        ('last', DOUBLE),
        ('volume', DOUBLE),
        ('bsize1', DOUBLE),
        ('asize1', DOUBLE),
        ('ask1', DOUBLE),
        ('bid1', DOUBLE),
        ('amt', DOUBLE),
    ]
    wind_indictor_str = ",".join([key for key, _ in param_list])
    if has_table:
        sql_str = """
                    SELECT wind_code, date_frm, if(delist_date<end_date, delist_date, end_date) date_to
                    FROM
                    (
                    SELECT info.wind_code, ifnull(trade_date, ipo_date) date_frm, delist_date,
                    if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
                    FROM 
                        wind_stock_info info 
                    LEFT OUTER JOIN
                        (SELECT wind_code, adddate(max(trade_date),1) trade_date FROM {table_name} GROUP BY wind_code) tick
                    ON info.wind_code = tick.wind_code
                    ) tt
                    WHERE date_frm <= if(delist_date<end_date, delist_date, end_date) 
                    ORDER BY wind_code""".format(table_name=table_name)
    else:
        sql_str = """
                    SELECT wind_code, date_frm, if(delist_date<end_date, delist_date, end_date) date_to
                    FROM
                      (
                        SELECT info.wind_code, ipo_date date_frm, delist_date,
                        if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
                        FROM wind_stock_info info 
                      ) tt
                    WHERE date_frm <= if(delist_date<end_date, delist_date, end_date) 
                    ORDER BY wind_code"""
        logger.warning('%s 不存在，仅使用 wind_stock_info 表进行计算日期范围', table_name)
    with with_db_session(engine_md) as session:
        # 获取每只股票需要获取日线数据的日期区间
        table = session.execute(sql_str)
        # 计算每只股票需要获取日线数据的日期区间
        begin_time = None
        # 获取date_from,date_to，将date_from,date_to做为value值
        stock_date_dic = {
            wind_code: (date_from if begin_time is None else min([date_from, begin_time]), date_to)
            for wind_code, date_from, date_to in table.fetchall() if
            wind_code_set is None or wind_code in wind_code_set}

    data_df_list = []
    logger.info('%d stocks tick will been import', len(stock_date_dic))
    try:
        # base_date = min(trade_date_sorted_list)
        data_count = 0
        for stock_num, (wind_code, (date_from, date_to)) in enumerate(stock_date_dic.items()):
            # 获取股票量价等行情数据
            # wind_indictor_str = "ask1,bid1,asize1,bsize1,volume,amt,pre_close,open,high,low,last"
            try:
                data_df = invoker.wst(wind_code, wind_indictor_str, date_from, date_to)
            except APIError as exp:
                data_df = None
                if exp.ret_dic['error_code'] == -40520007:
                    logger.warning('%s[%s - %s] %s', wind_code, date_from, date_to, exp.ret_dic['error_msg'])
                    continue
            if data_df is None:
                logger.warning('%d) %s has no data during %s %s', stock_num, wind_code, date_from, date_to)
                continue
            logger.info('%d) %d data of %s between %s and %s', stock_num, data_df.shape[0], wind_code, date_from,
                        date_to)
            data_df['wind_code'] = wind_code
            data_df_list.append(data_df)
            data_count += data_df.shape[0]
            if data_count >= 10000:
                try:
                    import_count += insert_into_db(data_df_list, engine_md)
                finally:
                    data_df_list = []
                    data_count = 0
                    if DEBUG and stock_num >= 1:
                        break
    finally:
        # 导入数据库
        import_count += insert_into_db(data_df_list, engine_md)
    return import_count


def insert_into_db(data_df_list, engine_md):
    data_count = len(data_df_list)
    table_name = 'wind_stock_tick'
    has_table = engine_md.has_table(table_name)
    param_list = [
        ('datetime', DateTime),
        ('open', DOUBLE),
        ('high', DOUBLE),
        ('low', DOUBLE),
        ('close', DOUBLE),
        ('ask1', DOUBLE),
        ('bid1', DOUBLE),
        ('asize1', DOUBLE),
        ('bsize1', DOUBLE),
        ('volume', DOUBLE),
        ('amount', DOUBLE),
        ('preclose', DOUBLE),
    ]
    dtype = {key: val for key, val in param_list}
    dtype['wind_code'] = String(20)
    if data_count > 0:
        data_df_all = pd.concat(data_df_list)
        data_df_all.index.rename('datetime', inplace=True)
        data_df_all.reset_index(inplace=True)
        bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md, dtype=dtype)
        logger.info('%d data imported', data_df_all.shape[0])
        if not has_table and engine_md.has_table(table_name):
            alter_table_2_myisam(engine_md, [table_name])
            build_primary_key([table_name])

    return data_count


@app.task
def import_stock_quertarly(chain_param=None, wind_code_set=None):
    """
    插入股票日线数据到最近一个工作日-1
    :param chain_param:  在celery 中將前面結果做爲參數傳給後面的任務
    :return:
    """
    logging.info("更新 wind_stock_quertarly 开始")
    table_name = 'wind_stock_quertarly'
    has_table = engine_md.has_table(table_name)
    if has_table:
        sql_str = """
           SELECT wind_code, date_frm, if(delist_date<end_date, delist_date, end_date) date_to
           FROM
           (
               SELECT info.wind_code, ifnull(trade_date, ipo_date) date_frm, delist_date,
               if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
               FROM 
                   wind_stock_info info 
               LEFT OUTER JOIN
                   (SELECT wind_code, adddate(max(trade_date),1) trade_date FROM {table_name} GROUP BY wind_code) quertarly
               ON info.wind_code = quertarly.wind_code
           ) tt
           WHERE date_frm <= if(delist_date<end_date, delist_date, end_date) 
           ORDER BY wind_code;""".format(table_name=table_name)
    else:
        logger.warning('wind_stock_quertarly 不存在，仅使用 wind_stock_info 表进行计算日期范围')
        sql_str = """
           SELECT wind_code, date_frm, if(delist_date<end_date, delist_date, end_date) date_to
           FROM
           (
               SELECT info.wind_code, ipo_date date_frm, delist_date,
               if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
               FROM wind_stock_info info 
           ) tt
           WHERE date_frm <= if(delist_date<end_date, delist_date, end_date) 
           ORDER BY wind_code"""
    with with_db_session(engine_md) as session:
        # 获取每只股票需要获取日线数据的日期区间
        table = session.execute(sql_str)
        # 计算每只股票需要获取日线数据的日期区间
        begin_time = None
        # 获取date_from,date_to，将date_from,date_to做为value值
        stock_date_dic = {
            wind_code: (date_from if begin_time is None else min([date_from, begin_time]), date_to)
            for wind_code, date_from, date_to in table.fetchall() if
            wind_code_set is None or wind_code in wind_code_set}
    # 获取股票量价等行情数据
    param_list = [
        ('roic_ttm', DOUBLE),
        ('yoyprofit', DOUBLE),
        ('ebit', DOUBLE),
        ('ebit2', DOUBLE),
        ('ebit2_ttm', DOUBLE),
        ('surpluscapitalps', DOUBLE),
        ('undistributedps', DOUBLE),
        ('stm_issuingdate', DOUBLE),
    ]

    # 获取参数列表
    wind_indictor_str = ",".join(key for key, _ in param_list)
    dtype = {key: val for key, val in param_list}
    dtype['wind_code'] = String(20)
    dtype['trade_date'] = Date
    # # 标示每天几点以后下载当日行情数据
    # BASE_LINE_HOUR = 16
    # with with_db_session(engine_md) as session:
    #     # 获取每只股票最新交易日数据
    #     sql_str = 'select wind_code, max(Trade_date) from wind_stock_quertarly group by wind_code'
    #     table = session.execute(sql_str)
    #     stock_trade_date_latest_dic = dict(table.fetchall())
    #     # 获取市场有效交易日数据
    #     sql_str = "select trade_date from wind_trade_date where trade_date > '2005-1-1'"
    #     table = session.execute(sql_str)
    #     trade_date_sorted_list = [t[0] for t in table.fetchall()]
    #     trade_date_sorted_list.sort()
    #     # 获取每只股票上市日期、退市日期
    #     table = session.execute('SELECT wind_code, ipo_date, delist_date FROM wind_stock_info')
    #     stock_date_dic = {wind_code: (ipo_date, delist_date if delist_date is None or delist_date > UN_AVAILABLE_DATE else None) for
    #                       wind_code, ipo_date, delist_date in table.fetchall()}
    # date_ending = date.today() - ONE_DAY if datetime.now().hour < BASE_LINE_HOUR else date.today()
    # data_df_list = []
    # logger.info('%d stocks will been import into wind_stock_quertarly', len(stock_date_dic))
    data_df_list = []
    logger.info('%d stocks will been import into wind_stock_quertarly', len(stock_date_dic))

    try:
        for stock_num, (wind_code, (date_from, date_to)) in enumerate(stock_date_dic.items()):
            # 获取股票量价等行情数据
            # w.wsd("002122.SZ", "roic_ttm,yoyprofit,ebit,ebit2,ebit2_ttm,surpluscapitalps,undistributedps,stm_issuingdate", "2012-12-31", "2017-12-06", "unit=1;rptType=1;Period=Q")
            data_df = invoker.wsd(wind_code, wind_indictor_str, date_from, date_to, "unit=1;rptType=1;Period=Q")
            if data_df is None:
                logger.warning('%d) %s has no data during %s %s', stock_num, wind_code, date_from, date_to)
                continue
            data_df.rename(columns={c: str(c).lower() for c in data_df.columns}, inplace=True)
            # 清理掉期间全空的行
            for trade_date in list(data_df.index[:10]):
                is_all_none = data_df.loc[trade_date].apply(lambda x: x is None).all()
                if is_all_none:
                    logger.warning("%s %s 数据全部为空", wind_code, trade_date)
                    data_df.drop(trade_date, inplace=True)
            logger.info('%d) %d data of %s between %s and %s', stock_num, data_df.shape[0], wind_code, date_from,
                        date_to)
            data_df['wind_code'] = wind_code
            data_df.index.rename('trade_date', inplace=True)
            data_df.reset_index(inplace=True)
            data_df_list.append(data_df)
            if DEBUG and len(data_df_list) > 10:
                break
    finally:
        # 导入数据库
        if len(data_df_list) > 0:
            data_df_all = pd.concat(data_df_list)
            bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md, dtype=dtype)
            logging.info("更新 wind_stock_quertarly 结束 %d 条信息被更新", data_df_all.shape[0])
            if not has_table and engine_md.has_table(table_name):
                alter_table_2_myisam(engine_md, [table_name])
                build_primary_key([table_name])


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s [%(name)s:%(funcName)s] %(message)s')
    # DEBUG = True
    import_wind_stock_info(chain_param=None, refresh=False)
    # 更新每日股票数据66
    import_stock_daily(chain_param=None)
    # import_stock_daily_wch()
    # wind_code_set = None
    # add_new_col_data('ev', '', None, wind_code_set=wind_code_set)

    # wind_code_set = None
    # import_count = 1
    # while import_count > 0:
    #     try:
    #         # 更新每日股票数据
    #         import_count = import_stock_tick(wind_code_set, None)
    #     except IntegrityError:
    #         logger.exception("import_stock_tick exception")
    #
    # logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s [%(name)s:%(funcName)s] %(message)s')
    # 更新股票季报数据
    # DEBUG = True
    # wind_code_set = None
    # import_stock_quertarly(wind_code_set, None)

    # 添加某列信息
    # fill_history()
    # fill_col()
