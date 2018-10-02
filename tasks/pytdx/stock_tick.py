"""
Created on 2018/9/27
@author: yby
@desc    : 2018-09-27
contact author:ybychem@gmail.com
"""
import pandas as pd
from pytdx.hq import TdxHq_API
from pytdx.params import TDXParams
from pytdx.config.hosts import hq_hosts
from tasks.utils.fh_utils import try_n_times, datetime_2_str, str_2_datetime
from tasks.utils.db_utils import bunch_insert_on_duplicate_update, execute_sql, with_db_session
from tasks.backend import engine_md
import logging
from sqlalchemy.types import String, Date, Integer, DateTime, Time
from sqlalchemy.dialects.mysql import DOUBLE
from pytdx.errors import TdxConnectionError
from pytdx.config.hosts import hq_hosts

logger = logging.getLogger()
STR_FORMAT_DATE_TS = '%Y%m%d'
api = TdxHq_API(raise_exception=True)
# api.connect('59.173.18.140', 7709)
api.connect('123.125.108.14', 7709)
# api.connect('180.153.18.170', 7709)
# api.connect('202.108.253.130', 7709)



# for ip_add in hq_hosts:
#     try:
#         api.connect(ip_add[1], ip_add[2])#正确的为140
#         break
#     except TdxConnectionError:
#         # pass
#         print('网络连接有问题， 重试')

# 定义提取tick数据函数并将数据转为dataframe
def get_tdx_tick(code, date_str):
    """
    调用pytdx接口获取股票tick数据
    :param code:
    :param date_str:
    :return:
    """
    position, data_list = 0, []
    if code[0] == '6':
        df = api.to_df(api.get_history_transaction_data(TDXParams.MARKET_SH, code, position, 30000, int(date_str)))
    else:
        df = api.to_df(api.get_history_transaction_data(TDXParams.MARKET_SZ, code, position, 30000, int(date_str)))
    data_list.append(df)
    datetime0925 = str_2_datetime(date_str + '09:25', '%Y%m%d%H:%M')
    datetime0930 = str_2_datetime(date_str + '09:30', '%Y%m%d%H:%M')
    while len(df) > 0 and str_2_datetime(date_str + df.time[0], '%Y%m%d%H:%M') > datetime0925:
        position = position + len(df)
        if code[0] == '6':
            df = api.to_df(api.get_history_transaction_data(TDXParams.MARKET_SH, code, position, 30000, int(date_str)))
        else:
            df = api.to_df(api.get_history_transaction_data(TDXParams.MARKET_SZ, code, position, 30000, int(date_str)))
        if df is not None and len(df) > 0:
            # data_df1=data_df
            data_list.append(df)
            if str_2_datetime(date_str + df.time[0], '%Y%m%d%H:%M') == datetime0925 or \
                    str_2_datetime(date_str + df.time[0], '%Y%m%d%H:%M') == datetime0930:
                break
        else:
            break
    if code[0] == 6:
        code = code + '.SH'
    else:
        code = code + '.SZ'
    data_df = pd.concat(data_list)
    trade_date = data_df.time.apply(lambda x: str_2_datetime(date_str + x, '%Y%m%d%H:%M'))

    data_df.insert(0, 'ts_code', code)
    data_df.insert(1, 'date', date_str)
    data_df.insert(2, 'trade_date', trade_date)
    data_df = data_df.sort_values(by='trade_date')
    return data_df


# 再次封包提取函数
@try_n_times(2, sleep_time=0.1, logger=logger, exception_sleep_time=0.5)
def invoke_tdx_tick(code, date_str):
    invoke_tdx_tick = get_tdx_tick(code, date_str)
    return invoke_tdx_tick


INDICATOR_PARAM_LIST_TDX_STOCK_TICK = [
    ('ts_code', String(20)),
    ('date', Date),
    ('trade_date', DateTime),
    ('time', Time),
    ('price', DOUBLE),
    ('vol', DOUBLE),
    ('num', DOUBLE),
    ('buyorsell', DOUBLE),
]
# 设置 dtype
DTYPE_TDX_STOCK_TICK = {key: val for key, val in INDICATOR_PARAM_LIST_TDX_STOCK_TICK}


def import_tdx_tick():
    """
        通过pytdx接口下载tick数据
        :return:
        """
    table_name = 'pytdx_stock_tick'
    has_table = engine_md.has_table(table_name)
    if has_table:
        sql_str = """SELECT md.ts_code, md.trade_date 
                        FROM 
                        tushare_stock_daily_md md 
                        inner join 
                        (
							select ts_code, delist_date from tushare_stock_info where tushare_stock_info.delist_date is null
                        ) info
                        on info.ts_code = md.ts_code
                        
                        left outer join tushare_stock_daily_suspend suspend 
                        on md.ts_code =suspend.ts_code 
                        and md.trade_date =suspend.suspend_date 
                        
                         left outer join
                        (
                            select ts_code,max(trade_date) trade_date_max from {table_name} group by ts_code
                        ) m
                        on md.ts_code = m.ts_code
						where md.trade_date>'2000-01-24' 
                        and suspend.suspend_date is null 
                        and (m.trade_date_max is null or md.trade_date>m.trade_date_max)""".format(
            table_name=table_name)
    else:
        # sql_str = """SELECT ts_code ,trade_date trade_date_list FROM tushare_stock_daily_md where trade_date>'2000-01-24'"""
        sql_str = """
SELECT md.ts_code, md.trade_date 
                        FROM 
                        tushare_stock_daily_md md 
                        inner join 
                        (
							select ts_code, delist_date from tushare_stock_info where tushare_stock_info.delist_date is null
                        ) info
                        on info.ts_code = md.ts_code
                        
                        left outer join tushare_stock_daily_suspend suspend 
                        on md.ts_code =suspend.ts_code 
                        and md.trade_date =suspend.suspend_date
                        where md.trade_date>'2000-01-24' 
                        and suspend.suspend_date is null """

    with with_db_session(engine_md) as session:
        # 获取每只股票需要获取日线数据的日期区间
        table = session.execute(sql_str)
        code_date_range_dic = {}
        for ts_code, trade_date_list in table.fetchall():
            # trade_date_list.sort()
            code_date_range_dic.setdefault(ts_code, []).append(trade_date_list)

    data_df_list, data_count, all_data_count, data_len = [], 0, 0, len(code_date_range_dic)
    logger.info('%d stocks will been import into tushare_stock_daily_md', data_len)
    # 将data_df数据，添加到data_df_list
    Cycles = 1
    try:
        for num, (index_code, trade_date_list) in enumerate(code_date_range_dic.items(), start=1):
            trade_date_list_len = len(trade_date_list)
            for i, trade_date in enumerate(trade_date_list):
                # trade_date=trade_date_list[i]
                logger.debug('%d/%d) %d/%d) %s [%s]', num, data_len, i, trade_date_list_len, index_code, trade_date)
                data_df = invoke_tdx_tick(code=index_code[0:6], date_str=datetime_2_str(trade_date, STR_FORMAT_DATE_TS))
                # 把数据攒起来
                if data_df is not None and data_df.shape[0] > 0:
                    data_count += data_df.shape[0]
                    data_df_list.append(data_df)

                # 大于阀值有开始插入
                if data_count >= 100000:
                    data_df_all = pd.concat(data_df_list)
                    bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md, DTYPE_TDX_STOCK_TICK)
                    all_data_count += data_count
                    data_df_list, data_count = [], 0

    finally:
        # 导入数据库
        if len(data_df_list) > 0:
            data_df_all = pd.concat(data_df_list)
            data_count = bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md, DTYPE_TDX_STOCK_TICK)
            all_data_count = all_data_count + data_count
            logging.info("更新 %s 结束 %d 条信息被更新", table_name, all_data_count)
            # if not has_table and engine_md.has_table(table_name):
            #     alter_table_2_myisam(engine_md, [table_name])
            #     build_primary_key([table_name])


if __name__ == "__main__":
    # date_str, code = '20000125', '000001'
    # df=invoke_tdx_tick(code=code, date_str=date_str)
    import_tdx_tick()
