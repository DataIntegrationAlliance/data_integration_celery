from datetime import date, datetime, timedelta
import pandas as pd
import numpy as np
from config_fh import get_db_engine, get_db_session, STR_FORMAT_DATE, UN_AVAILABLE_DATE, WIND_REST_URL
from fh_tools.windy_utils_rest import WindRest, APIError
from fh_tools.fh_utils import get_last, get_first, str_2_date, date_2_str
import logging, os, json
from sqlalchemy.types import String, Date, Integer
from sqlalchemy.dialects.mysql import DOUBLE
logger = logging.getLogger()
DATE_BASE = datetime.strptime('2005-01-01', STR_FORMAT_DATE).date()
ONE_DAY = timedelta(days=1)
# 标示每天几点以后下载当日行情数据
BASE_LINE_HOUR = 16


def fill_col():
    """补充历史col数据"""
    col_name = 'ev2_to_ebitda'
    # 获取每只股票ipo 日期 及 最小的交易日前一天
    #     sql_str = """select si.wind_code, td_from, td_to
    # from wind_stock_info si,
    # (select wind_code, min(trade_date) td_from, max(trade_date) td_to from wind_stock_daily where ev2_to_ebitda is null group by wind_code) sd
    # where si.wind_code = sd.wind_code"""
    sql_str = """select wind_code, if(min_trade_date<'1998-12-31','1998-12-31',min_trade_date) date_from , if(min_date_ev2_to_ebitda<max_trade_date, min_date_ev2_to_ebitda,max_trade_date) date_to, min_date_ev2_to_ebitda, min_trade_date, max_trade_date FROM
(
select wind_code, min(IF(ev2_to_ebitda is null, '2018-01-01', trade_date)) min_date_ev2_to_ebitda, min(trade_date) min_trade_date, max(trade_date) max_trade_date 
from wind_stock_daily group by wind_code
HAVING min_date_ev2_to_ebitda>'1998-12-31'
) aaa
where if(min_trade_date<'1998-12-31','1998-12-31',min_trade_date) < if(min_date_ev2_to_ebitda<max_trade_date, min_date_ev2_to_ebitda,max_trade_date)"""
    w = WindRest(WIND_REST_URL)
    engine = get_db_engine()
    with get_db_session(engine) as session:
        table = session.execute(sql_str)
        stock_trade_date_range_dic = {content[0]: (content[1], content[2]) for content in table.fetchall()}
    data_df_list = []
    try:
        for n, (wind_code, (date_from, date_to)) in enumerate(stock_trade_date_range_dic.items()):
            # 获取股票量价等行情数据
            wind_indictor_str = col_name
            data_df = w.wsd(wind_code, wind_indictor_str, date_from, date_to)
            if data_df is None:
                logger.warning('%d) %s has no data during %s %s', n, wind_code, date_from, date_to)
                continue
            logger.info('%d) %d data of %s between %s and %s', n, data_df.shape[0], wind_code, date_from, date_to)
            data_df['wind_code'] = wind_code
            data_df_list.append(data_df)
            # if n > 5:
            #     break
    finally:
        # 导入数据库
        if len(data_df_list) > 0:
            data_df_all = pd.concat(data_df_list)
            data_df_all.index.rename('trade_date', inplace=True)
            data_df_all.reset_index(inplace=True)
            data_df_all.rename(columns={col_name.upper(): col_name}, inplace=True)
            data_df_all.dropna(inplace=True)
            data_dic_list = data_df_all.to_dict(orient='records')
            sql_str = "update wind_stock_daily set %s=:%s where wind_code=:wind_code and trade_date=:trade_date" % (col_name, col_name)
            with get_db_session(engine) as session:
                table = session.execute(sql_str, params=data_dic_list)
            logger.info('%d data imported', data_df_all.shape[0])
        else:
            logger.warning('no data for update')


def fill_col2():
    """
    向数据库中添加新的列
    :return:
    """
    w = WindRest(WIND_REST_URL)
    table_name = 'wind_stock_daily_wch'
    date_col_name = 'date'

    engine = get_db_engine()
    # 设置添加的字段，wind字段默认全大写
    field_name_col_name_dic = {k.upper(): v for k, v in {
        "PS_TTM": 'PS',
        "VAL_EVTOEBITDA2": "val_evtoebitda2"
    }.items()}
    # 构筑 update 语句
    sql_update_str = "update {0} set {2} where wind_code=:wind_code and {1}=:{1}".format(
        table_name,
        date_col_name,
        ','.join(['%s=:%s' % (col_name, col_name) for col_name in field_name_col_name_dic.values()])
    )
    # 导入历史更新进度文件
    file_name = 'ps_evebitda2.json'
    if os.path.exists(file_name):
        with open(file_name, 'r') as fp:
            param_dic = json.load(fp)
    else:
        param_dic = {}
    where_str = ' and '.join([val + ' is null' for val in field_name_col_name_dic.values()])
    sql_str = """select wind_code, min({1}) trade_date_min, max({1}) trade_date_max 
from {0} where {2}  group by wind_code""".format(table_name, date_col_name, where_str)
    wind_code_date_df = pd.read_sql(sql_str, engine, index_col='wind_code')
    data_len = wind_code_date_df.shape[0]
    invoke_count = 0
    try:
        for data_num, (wind_code, date_dic) in enumerate(wind_code_date_df.to_dict('index').items(), start=1):
            # 起始日期
            if wind_code in param_dic:
                # date_from = max([date_dic['trade_date_min'], str_2_date(param_dic[wind_code])]) + timedelta(days=1)
                date_from = str_2_date(param_dic[wind_code]) + timedelta(days=1)
            else:
                date_from = date_dic['trade_date_min'] + timedelta(days=1)
            # 截止日期
            date_to = date_dic['trade_date_max']
            if date_from > date_to:
                logger.info('%d/%d) %s [%s %s] 无需更新', data_num, data_len, wind_code, date_from, date_to)
                continue
            wind_indictor_str = ','.join(field_name_col_name_dic.keys())
            try:
                other_data_df = w.wsd(wind_code, wind_indictor_str, date_from, date_to)
                if other_data_df is None:
                    logger.warning('%d/%d) %s [%s %s] 缺少行情数据', data_num, data_len, wind_code, date_from, date_to)
                    continue
                logger.info('%d/%d) %s [%s %s]', data_num, data_len, wind_code, date_from, date_to)
                other_data_df['wind_code'] = wind_code
                invoke_count += 1
            except APIError as exp:
                logger.exception("%d/%d) %s 执行异常", data_num, data_len, wind_code)
                if exp.ret_dic.setdefault('error_code', 0) in (
                        -40520007,  # 没有可用数据
                        -40521009,  # 数据解码失败。检查输入参数是否正确，如：日期参数注意大小月月末及短二月
                ):
                    continue
                else:
                    break

            # 字段重命名
            other_data_df.index.rename(date_col_name, inplace=True)
            other_data_df.reset_index(inplace=True)
            other_data_df.rename(columns=field_name_col_name_dic, inplace=True)
            other_data_df.dropna(inplace=True)
            if other_data_df.shape[0] == 0:
                logger.info('%d/%d) %s [%s %s] 没有可更新的数据', data_num, data_len, wind_code, date_from, date_to)
                continue
            data_dic_list = other_data_df.to_dict(orient='records')
            with get_db_session(engine) as session:
                session.execute(sql_update_str, params=data_dic_list)
            logger.info('%d/%d) %s [%s %s] %d 条记录被更新',
                        data_num, data_len, wind_code, date_from, date_to, other_data_df.shape[0])
            param_dic[wind_code] = date_2_str(date_to)
            if invoke_count >= 300:
                break
    finally:
        # 保持进度文件
        if len(param_dic) > 0:
            with open(file_name, 'w') as fp:
                json.dump(param_dic, fp)

        logger.info("更新结束，调用接口 %d 次", invoke_count)


def fill_history():
    """补充历史股票日线数据"""
    # 获取每只股票ipo 日期 及 最小的交易日前一天
    sql_str = """select si.wind_code, ipo_date, td_to
from wind_stock_info si,
(select wind_code, max(trade_date) td_max, date_sub(min(trade_date), interval 1 day) td_to from wind_stock_daily group by wind_code) sd
where si.wind_code = sd.wind_code
and ipo_date < td_to"""
    w = WindRest(WIND_REST_URL)
    engine = get_db_engine()
    with get_db_session(engine) as session:
        table = session.execute(sql_str)
        stock_ipo_trade_date_min_dic = {content[0]: (content[1], content[2]) for content in table.fetchall()}
    data_df_list = []
    try:
        for wind_code, date_pair in stock_ipo_trade_date_min_dic.items():
            date_from, date_to = date_pair
            # 获取股票量价等行情数据
            wind_indictor_str = "open,high,low,close,adjfactor,volume,amt,pct_chg,maxupordown," + \
                                "swing,turn,free_turn,trade_status,susp_days,total_shares,free_float_shares"
            data_df = w.wsd(wind_code, wind_indictor_str, date_from, date_to)
            if data_df is None:
                logger.warning('%s has no data during %s %s', wind_code, date_from, date_to)
                continue
            logger.info('%d data of %s between %s and %s', data_df.shape[0], wind_code, date_from, date_to)
            data_df['wind_code'] = wind_code
            data_df_list.append(data_df)
    finally:
        # 导入数据库
        if len(data_df_list) > 0:
            data_df_all = pd.concat(data_df_list)
            data_df_all.index.rename('trade_date', inplace=True)
            data_df_all.reset_index(inplace=True)
            data_df_all.set_index(['wind_code', 'trade_date'], inplace=True)
            data_df_all.to_sql('wind_stock_daily', engine, if_exists='append',
                               dtype={
                                   'wind_code': String(20),
                                   'trade_date': Date,
                                   'open': DOUBLE,
                                   'high': DOUBLE,
                                   'low': DOUBLE,
                                   'close': DOUBLE,
                                   'adjfactor': DOUBLE,
                                   'volume': DOUBLE,
                                   'amt': DOUBLE,
                                   'pct_chg': DOUBLE,
                                   'maxupordown': Integer,
                                   'swing': DOUBLE,
                                   'turn': DOUBLE,
                                   'free_turn': DOUBLE,
                                   'trade_status': String(20),
                                   'susp_days': Integer,
                                   'total_shares': DOUBLE,
                                   'free_float_shares': DOUBLE,
                               }
                               )
            logger.info('%d data imported', data_df_all.shape[0])


def import_stock_daily():
    """
    插入股票日线数据到最近一个工作日-1。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    :return: 
    """
    logging.info("更新 wind_stock_daily 开始")
    w = WindRest(WIND_REST_URL)
    engine = get_db_engine()
    with get_db_session(engine) as session:
        # 获取每只股票最新交易日数据
        sql_str = 'select wind_code, max(Trade_date) from wind_stock_daily group by wind_code'
        table = session.execute(sql_str)
        stock_trade_date_latest_dic = dict(table.fetchall())
        # 获取市场有效交易日数据
        sql_str = "select trade_date from wind_trade_date where trade_date > '2005-1-1'"
        table = session.execute(sql_str)
        trade_date_sorted_list = [t[0] for t in table.fetchall()]
        trade_date_sorted_list.sort()
        # 获取每只股票上市日期、退市日期
        table = session.execute('SELECT wind_code, ipo_date, delist_date FROM wind_stock_info')
        stock_date_dic = {wind_code: (ipo_date, delist_date if delist_date is None or delist_date > UN_AVAILABLE_DATE else None) for
                          wind_code, ipo_date, delist_date in table.fetchall()}
    date_ending = date.today() - ONE_DAY if datetime.now().hour < BASE_LINE_HOUR else date.today()
    data_df_list = []
    data_len = len(stock_date_dic)
    logger.info('%d stocks will been import into wind_stock_daily', data_len)
    try:
        for data_num, (wind_code, date_pair) in enumerate(stock_date_dic.items()):
            date_ipo, date_delist = date_pair
            # 获取 date_from
            if wind_code in stock_trade_date_latest_dic:
                date_latest_t1 = stock_trade_date_latest_dic[wind_code] + ONE_DAY
                date_from = max([date_latest_t1, DATE_BASE, date_ipo])
            else:
                date_from = max([DATE_BASE, date_ipo])
            date_from = get_first(trade_date_sorted_list, lambda x: x >= date_from)
            # 获取 date_to
            if date_delist is None:
                date_to = date_ending
            else:
                date_to = min([date_delist, date_ending])
            date_to = get_last(trade_date_sorted_list, lambda x: x <= date_to)
            if date_from is None or date_to is None or date_from > date_to:
                continue
            # 获取股票量价等行情数据
            wind_indictor_str = "open,high,low,close,adjfactor,volume,amt,pct_chg,maxupordown," + \
                                "swing,turn,free_turn,trade_status,susp_days,total_shares,free_float_shares,ev2_to_ebitda"
            try:
                data_df = w.wsd(wind_code, wind_indictor_str, date_from, date_to)
            except APIError as exp:
                logger.exception("%d/%d) %s 执行异常", data_num, data_len, wind_code)
                if exp.ret_dic.setdefault('error_code', 0) in (
                        -40520007,  # 没有可用数据
                        -40521009,  # 数据解码失败。检查输入参数是否正确，如：日期参数注意大小月月末及短二月
                ):
                    continue
                else:
                    break
            if data_df is None:
                logger.warning('%d/%d) %s has no data during %s %s', data_num, data_len, wind_code, date_from, date_to)
                continue
            logger.info('%d/%d) %d data of %s between %s and %s', data_num, data_len, data_df.shape[0], wind_code, date_from, date_to)
            data_df['wind_code'] = wind_code
            data_df_list.append(data_df)
    finally:
        # 导入数据库
        if len(data_df_list) > 0:
            data_df_all = pd.concat(data_df_list)
            data_df_all.index.rename('trade_date', inplace=True)
            data_df_all.reset_index(inplace=True)
            data_df_all.set_index(['wind_code', 'trade_date'], inplace=True)
            data_df_all.to_sql('wind_stock_daily', engine, if_exists='append',
                               dtype={
                                   'wind_code': String(20),
                                   'trade_date': Date,
                                   'open': DOUBLE,
                                   'high': DOUBLE,
                                   'low': DOUBLE,
                                   'close': DOUBLE,
                                   'adjfactor': DOUBLE,
                                   'volume': DOUBLE,
                                   'amt': DOUBLE,
                                   'pct_chg': DOUBLE,
                                   'maxupordown': Integer,
                                   'swing': DOUBLE,
                                   'turn': DOUBLE,
                                   'free_turn': DOUBLE,
                                   'trade_status': String(20),
                                   'susp_days': Integer,
                                   'total_shares': DOUBLE,
                                   'free_DOUBLE_shares': DOUBLE,
                                   'ev2_to_ebitda': DOUBLE,
                               }
                               )
            logging.info("更新 wind_stock_daily 结束 %d 条信息被更新", data_df_all.shape[0])


def import_stock_daily_wch():
    """
    插入股票日线数据到最近一个工作日-1。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    :return: 
    """
    logging.info("更新 wind_stock_daily_wch 开始")
    w = WindRest(WIND_REST_URL)
    engine = get_db_engine()
    with get_db_session(engine) as session:
        # 获取每只股票最新交易日数据
        sql_str = 'select wind_code, max(date) from wind_stock_daily_wch group by wind_code'
        table = session.execute(sql_str)
        stock_trade_date_latest_dic = dict(table.fetchall())
        # 获取市场有效交易日数据
        sql_str = "select trade_date from wind_trade_date where trade_date > '2005-1-1'"
        table = session.execute(sql_str)
        trade_date_sorted_list = [t[0] for t in table.fetchall()]
        trade_date_sorted_list.sort()
        # 获取每只股票上市日期、退市日期
        table = session.execute('SELECT wind_code, ipo_date, delist_date FROM wind_stock_info')
        stock_date_dic = {wind_code: (ipo_date, delist_date if delist_date is None or delist_date > UN_AVAILABLE_DATE else None) for
                          wind_code, ipo_date, delist_date in table.fetchall()}
    date_ending = date.today() - ONE_DAY if datetime.now().hour < BASE_LINE_HOUR else date.today()
    data_df_list = []
    data_len = len(stock_date_dic)
    logger.info('%d stocks will been import into wind_stock_daily_wch', data_len)
    try:
        for data_num, (wind_code, date_pair) in enumerate(stock_date_dic.items()):
            date_ipo, date_delist = date_pair
            # 获取 date_from
            if wind_code in stock_trade_date_latest_dic:
                date_latest_t1 = stock_trade_date_latest_dic[wind_code] + ONE_DAY
                date_from = max([date_latest_t1, DATE_BASE, date_ipo])
            else:
                date_from = max([DATE_BASE, date_ipo])
            date_from = get_first(trade_date_sorted_list, lambda x: x >= date_from)
            # 获取 date_to
            if date_delist is None:
                date_to = date_ending
            else:
                date_to = min([date_delist, date_ending])
            date_to = get_last(trade_date_sorted_list, lambda x: x <= date_to)
            if date_from is None or date_to is None or date_from > date_to:
                continue
            logger.debug("%d/%d) 获取股票 %s [%s %s] 行情数据", data_num, data_len, wind_code, date_from, date_to)
            try:
                # 获取股票量价等行情数据
                wind_indictor_str = "open,high,low,close"
                ohlc_df = w.wsd(wind_code, wind_indictor_str, date_from, date_to, "PriceAdj=B")
                if ohlc_df is None:
                    logger.warning('%d/%d) %s [%s %s] 缺少开高低收（后复权）行情数据', data_num, data_len, wind_code, date_from, date_to)
                    continue
                wind_indictor_str = "close,volume,total_shares,free_float_shares,val_pe_deducted_ttm,pb_lf,ev2,ev2_to_ebitda,PS_TTM,VAL_EVTOEBITDA2"
                other_data_df = w.wsd(wind_code, wind_indictor_str, date_from, date_to)
                if other_data_df is None:
                    logger.warning('%d/%d) %s [%s %s] 缺少行情数据', data_num, data_len, wind_code, date_from, date_to)
                    continue
            except APIError as exp:
                logger.exception("%d/%d) %s 执行异常", data_num, data_len, wind_code)
                if exp.ret_dic.setdefault('error_code', 0) in (
                        -40520007,  # 没有可用数据
                        -40521009,  # 数据解码失败。检查输入参数是否正确，如：日期参数注意大小月月末及短二月
                ):
                    continue
                else:
                    break
            other_data_df.rename(columns={'CLOSE': 'CloseUnadj',
                                          'TOTAL_SHARES': 'TotalShare',
                                          'FREE_FLOAT_SHARES': 'FreeFloatShare',
                                          'VAL_PE_DEDUCTED_TTM': 'PE',
                                          'PB_LF': 'PB',
                                          'EV2': 'EV',
                                          'EV2_TO_EBITDA': 'EVEBITDA',
                                          "PS_TTM": 'PS',
                                          "VAL_EVTOEBITDA2": "val_evtoebitda2",
                                          }, inplace=True)
            data_df = ohlc_df.merge(other_data_df, how='outer', left_index=True, right_index=True)
            logger.info('%d/%d) %s [%s %s] 包含 %d 条历史行情数据', data_num, data_len, wind_code, date_from, date_to, data_df.shape[0])
            data_df['wind_code'] = wind_code
            data_df_list.append(data_df)
            # 调试使用
            # if data_num > 1:
            #     break
    finally:
        # 导入数据库
        if len(data_df_list) > 0:
            data_df_all = pd.concat(data_df_list)
            data_df_all.index.rename('date', inplace=True)
            data_df_all.reset_index(inplace=True)
            data_df_all.set_index(['wind_code', 'date'], inplace=True)
            data_df_all.to_sql('wind_stock_daily_wch', engine, if_exists='append')
            logging.info("更新 wind_stock_daily_wch 结束 %d 条信息被更新", data_df_all.shape[0])


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s [%(name)s:%(funcName)s] %(message)s')
    # 更新每日股票数据
    # import_stock_daily()
    import_stock_daily_wch()
    # 添加某列信息
    # fill_history()
    # fill_col2()
