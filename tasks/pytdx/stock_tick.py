"""
Created on 2018/9/27
@author: yby
@desc    : 2018-09-27
contact author:ybychem@gmail.com
"""
import pandas as pd
import datetime
from pytdx.hq import TdxHq_API
from pytdx.params import TDXParams
from tasks.utils.fh_utils import try_n_times, datetime_2_str, str_2_datetime
from tasks.utils.db_utils import bunch_insert_on_duplicate_update, execute_sql, with_db_session
from tasks.backend import engine_md
import logging
from sqlalchemy.types import String, Date, DateTime, Time,Integer
from sqlalchemy.dialects.mysql import DOUBLE, SMALLINT,TINYINT, FLOAT
from pytdx.errors import TdxConnectionError
from pytdx.config.hosts import hq_hosts
from pytdx.hq import TdxHq_API
from pytdx.pool.ippool import AvailableIPPool
import random
import logging
import pprint
from pytdx.log import DEBUG, log
from functools import partial
import time


## 调用单个接口，重试次数，超过次数则不再重试
DEFAULT_API_CALL_MAX_RETRY_TIMES = 20
## 重试间隔的休眠时间
DEFAULT_API_RETRY_INTERVAL = 0.2

class TdxHqApiCallMaxRetryTimesReachedException(Exception):
    pass

class TdxHqPool_API(object):
    """
    实现一个连接池的机制
    包含：

    1 1个正在进行数据通信的主连接
    2 1个备选连接，备选连接也连接到服务器，通过心跳包维持连接，当主连接通讯出现问题时，备选连接立刻转化为主连接, 原来的主连接返回ip池，并从ip池中选取新的备选连接
    3 m个ip构成的ip池，可以通过某个方法获取列表，列表可以进行排序，如果备选连接缺少的时候，我们根据排序的优先级顺序将其追加到备选连接
    """

    def __init__(self, hq_cls, ippool):
        self.hq_cls = hq_cls
        self.ippool = ippool
        """
        正在通信的客户端连接
        """
        self.api = hq_cls(multithread=True, heartbeat=True)
        """
        备选连接
        """
        self.hot_failover_api = hq_cls(multithread=True, heartbeat=True)

        self.api_call_max_retry_times = DEFAULT_API_CALL_MAX_RETRY_TIMES
        self.api_call_retry_times = 0
        self.api_retry_interval = DEFAULT_API_RETRY_INTERVAL


        # 对hq_cls 里面的get_系列函数进行反射
        log.debug("perform_reflect")
        self.perform_reflect(self.api)

    def perform_reflect(self, api_obj):
        # ref : https://stackoverflow.com/questions/34439/finding-what-methods-an-object-has
        method_names = [attr for attr in dir(api_obj) if callable(getattr(api_obj, attr))]
        for method_name in method_names:
            log.debug("testing attr %s" % method_name)
            if method_name[:3] == 'get' or method_name == "do_heartbeat" or method_name == 'to_df':
                log.debug("set refletion to method: %s", method_name)
                _do_hp_api_call = partial(self.do_hq_api_call, method_name)
                setattr(self, method_name, _do_hp_api_call)

    def do_hq_api_call(self, method_name, *args, **kwargs):
        """
        代理发送请求到实际的客户端
        :param method_name: 调用的方法名称
        :param args: 参数
        :param kwargs: kv参数
        :return: 调用结果
        """
        try:
            result = getattr(self.api, method_name)(*args, **kwargs)
            if result is None:
                log.info("api(%s) call return None" % (method_name,))
        except Exception as e:
            log.info("api(%s) call failed, Exception is %s" % (method_name, str(e)))
            result = None

        # 如果无法获取信息，则进行重试
        if result is None:
            if self.api_call_retry_times >= self.api_call_max_retry_times:
                log.info("(method_name=%s) max retry times(%d) reached" % (method_name, self.api_call_max_retry_times))
                raise TdxHqApiCallMaxRetryTimesReachedException("(method_name=%s) max retry times reached" % method_name)
            old_api_ip = self.api.ip
            new_api_ip = None
            if self.hot_failover_api:
                new_api_ip = self.hot_failover_api.ip
                log.info("api call from init client (ip=%s) err, perform rotate to (ip =%s)..." %(old_api_ip, new_api_ip))
                self.api.disconnect()
                self.api = self.hot_failover_api
            log.info("retry times is " + str(self.api_call_max_retry_times))
            # 从池里再次获取备用ip
            new_ips = self.ippool.get_ips()

            choise_ip = None
            for _test_ip in new_ips:
                if _test_ip[0] == old_api_ip or _test_ip[0] == new_api_ip:
                    continue
                choise_ip = _test_ip
                break

            if choise_ip:
                self.hot_failover_api = self.hq_cls(multithread=True, heartbeat=True)
                self.hot_failover_api.connect(*choise_ip)
            else:
                self.hot_failover_api = None
            # 阻塞0.2秒，然后递归调用自己
            time.sleep(self.api_retry_interval)
            result = self.do_hq_api_call(method_name, *args, **kwargs)
            self.api_call_retry_times += 1

        else:
            self.api_call_retry_times = 0

        return result

    def connect(self, ipandport, hot_failover_ipandport):
        log.debug("setup ip pool")
        self.ippool.setup()
        log.debug("connecting to primary api")
        self.api.connect(*ipandport)
        log.debug("connecting to hot backup api")
        self.hot_failover_api.connect(*hot_failover_ipandport)
        return self

    def disconnect(self):
        log.debug("primary api disconnected")
        self.api.disconnect()
        log.debug("hot backup api  disconnected")
        self.hot_failover_api.disconnect()
        log.debug("ip pool released")
        self.ippool.teardown()

    def close(self):
        """
        disconnect的别名，为了支持 with closing(obj): 语法
        :return:
        """
        self.disconnect()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

#实例化连接池
log.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# def ping(ip):
#     __time1 = datetime.datetime.now()
#     api = TdxHq_API()
#     try:
#         with api.connect(ip, 7709):
#             api.get_history_transaction_data(TDXParams.MARKET_SZ, '000063', 0, 30000, int('20180926'))
#         return float(datetime.datetime.now() - x1)
#     except:
#         return datetime.timedelta(9, 9, 0)
# def select_best_ip():
#     # listx=[]
#     # for i in hq_hosts:
#     #     listx.append(i[1])
#     listx = ['202.108.253.131','180.153.18.170', '202.108.253.130' ]
#     data = [ping(x) for x in listx]
#     return listx[data.index(min(data))]


# add formatter to ch
ch.setFormatter(formatter)
log.addHandler(ch)
selected_hosts = [('上证云北京联通一', '123.125.108.14', 7709), ('上海电信主站Z1', '180.153.18.170', 7709),
                  ('上海电信主站Z2', '180.153.18.171', 7709),
                  ('上海电信主站Z80', '180.153.18.172', 80), ('北京联通主站Z1', '202.108.253.130', 7709),
                  ('北京联通主站Z2', '202.108.253.131', 7709),
                  ('北京联通主站Z80', '202.108.253.139', 80), ('杭州电信主站J1', '60.191.117.167', 7709),
                  ('杭州电信主站J2', '115.238.56.198', 7709),
                  ('杭州电信主站J3', '218.75.126.9', 7709), ('杭州电信主站J4', '115.238.90.165', 7709),
                  ('杭州联通主站J1', '124.160.88.183', 7709),
                  ('杭州联通主站J2', '60.12.136.250', 7709), ('青岛联通主站W1', '218.57.11.101', 7709),
                  ('青岛电信主站W1', '58.58.33.123', 7709),
                  ('深圳电信主站Z1', '14.17.75.71', 7709), ('云行情上海电信Z1', '114.80.63.12', 7709),
                  ('云行情上海电信Z2', '114.80.63.35', 7709),
                  ('上海电信主站Z3', '180.153.39.51', 7709), ('上证云成都电信一', '218.6.170.47', 7709), ]
ips = [(v[1], v[2]) for v in selected_hosts]
# 获取5个随机ip作为ip池
random.shuffle(ips)
ips5 = ips[:5]
ippool = AvailableIPPool(TdxHq_API, ips5)
primary_ip, hot_backup_ip = ippool.sync_get_top_n(2)
print("make pool api")
api = TdxHqPool_API(TdxHq_API, ippool)

logger = logging.getLogger()
STR_FORMAT_DATE_TS = '%Y%m%d'

# 定义提取tick数据函数并将数据转为dataframe
def get_tdx_tick(code, date_str):
    """
    调用pytdx接口获取股票tick数据
    :param code:
    :param date_str:
    :return:
    """
    position, data_list = 0, []
    with api.connect(primary_ip, hot_backup_ip):
        if code[0] == '6':
            df = api.to_df(api.get_history_transaction_data(TDXParams.MARKET_SH, code, position, 30000, int(date_str)))
        else:
            df = api.to_df(api.get_history_transaction_data(TDXParams.MARKET_SZ, code, position, 30000, int(date_str)))
        data_list.insert(0, df)
        datetime0925 = str_2_datetime(date_str + '09:25', '%Y%m%d%H:%M')
        datetime0930 = str_2_datetime(date_str + '09:30', '%Y%m%d%H:%M')
        while len(df) > 0 and str_2_datetime(date_str + df.time[0], '%Y%m%d%H:%M') > datetime0925:
            position = position + len(df)
            if code[0] == '6':
                df = api.to_df(api.get_history_transaction_data(TDXParams.MARKET_SH, code, position, 30000, int(date_str)))
            else:
                df = api.to_df(api.get_history_transaction_data(TDXParams.MARKET_SZ, code, position, 30000, int(date_str)))
            if df is not None and len(df) > 0:
                data_list.insert(0, df)
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
        data_df.index = range(len(data_df))
        data_df.index.rename('index', inplace=True)
        data_df.reset_index(inplace=True)
        return data_df

# zte=get_tdx_tick('000063','20181010')

# 再次封包提取函数
@try_n_times(5, sleep_time=0.01, logger=logger, exception_sleep_time=0.5)
def invoke_tdx_tick(code, date_str):
    invoke_tdx_tick = get_tdx_tick(code, date_str)
    return invoke_tdx_tick


INDICATOR_PARAM_LIST_TDX_STOCK_TICK = [
    ('ts_code', String(12)),
    ('date', Date),
    ('trade_date', DateTime),
    ('time', Time),
    ('price', FLOAT),
    ('vol', Integer),
    ('num', TINYINT),
    ('index',SMALLINT),
    ('buyorsell', TINYINT),
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
                        and (m.trade_date_max is null or md.trade_date>m.trade_date_max)""".format(table_name=table_name)
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
    logger.info('%d stocks will been import into pytdx_stock_tick', data_len)
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
                if data_count >= 300000:
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

