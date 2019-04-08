"""
Created on 2018/9/3
@author: yby
@desc    : 2018-09-3
contact author:ybychem@gmail.com
"""
import pandas as pd
import logging
from tasks.backend.orm import build_primary_key
from datetime import date, datetime, timedelta
from ibats_utils.mess import try_2_date, STR_FORMAT_DATE, datetime_2_str, split_chunk, try_n_times
from tasks import app
from sqlalchemy.types import String, Date, Integer
from sqlalchemy.dialects.mysql import DOUBLE
from tasks.backend import engine_md
from tasks.merge.code_mapping import update_from_info_table
from ibats_utils.db import with_db_session, add_col_2_table, alter_table_2_myisam, \
    bunch_insert_on_duplicate_update
from tasks.tushare.ts_pro_api import pro

DEBUG = False
logger = logging.getLogger()
DATE_BASE = datetime.strptime('2005-01-01', STR_FORMAT_DATE).date()
ONE_DAY = timedelta(days=1)
# 标示每天几点以后下载当日行情数据
BASE_LINE_HOUR = 16
STR_FORMAT_DATE_TS = '%Y%m%d'

INDICATOR_PARAM_LIST_TUSHARE_TMT_TWINCOME = [
    ('item', String(20)),
    ('date', Date),
    ('op_income', DOUBLE),
]
# 设置 dtype
DTYPE_TUSHARE_TMT_TWINCOME = {key: val for key, val in INDICATOR_PARAM_LIST_TUSHARE_TMT_TWINCOME}


def invoke_tmt_twincome(item,start_date,end_date):
    invoke_tmt_twincome=pro.tmt_twincome(item=item,start_date=start_date,end_date=end_date)
    return invoke_tmt_twincome

def tushare_tmt_twincome_info():
    table_name = 'tushare_tmt_twincome_info'
    has_table = engine_md.has_table(table_name)
    indicators_dic = [
        ['1', 'PC', '20110128'],
        ['2', 'NB', '20110128'],
        ['3', '主机板', '20110128'],
        ['4', '印刷电路板', '20110128'],
        ['5', 'IC载板', '20110128'],
        ['6', 'PCB组装', '20110128'],
        ['7', '软板', '20110128'],
        ['8', 'PCB', '20110128'],
        ['9', 'PCB原料', '20110128'],
        ['10', '铜箔基板', '20110128'],
        ['11', '玻纤纱布', '20110128'],
        ['12', 'FCCL', '20110128'],
        ['13', '显示卡', '20110128'],
        ['14', '绘图卡', '20110128'],
        ['15', '电视卡', '20110128'],
        ['16', '泛工业电脑', '20110128'],
        ['17', 'POS', '20110128'],
        ['18', '工业电脑', '20110128'],
        ['19', '光电IO', '20110128'],
        ['20', '监视器', '20110128'],
        ['21', '扫描器', '20110128'],
        ['22', 'PC周边', '20110128'],
        ['23', '储存媒体', '20110128'],
        ['24', '光碟', '20110128'],
        ['25', '硬盘磁盘', '20110128'],
        ['26', '发光二极体', '20110128'],
        ['27', '太阳能', '20110128'],
        ['28', 'LCD面板', '20110128'],
        ['29', '背光模组', '20110128'],
        ['30', 'LCD原料', '20110128'],
        ['31', 'LCD其它', '20110128'],
        ['32', '触控面板', '20110128'],
        ['33', '监控系统', '20110128'],
        ['34', '其它光电', '20110128'],
        ['35', '电子零组件', '20110128'],
        ['36', '二极体整流', '20110128'],
        ['37', '连接器', '20110128'],
        ['38', '电源供应器', '20110128'],
        ['39', '机壳', '20110128'],
        ['40', '被动元件', '20110128'],
        ['41', '石英元件', '20110128'],
        ['42', '3C二次电源', '20110128'],
        ['43', '网路设备', '20110128'],
        ['44', '数据机', '20110128'],
        ['45', '网路卡', '20110128'],
        ['46', '半导体', '20110128'],
        ['47', '晶圆制造', '20110128'],
        ['48', 'IC封测', '20110128'],
        ['49', '特用IC', '20110128'],
        ['50', '记忆体模组', '20110128'],
        ['51', '晶圆材料', '20110128'],
        ['52', 'IC设计', '20110128'],
        ['53', 'IC光罩', '20110128'],
        ['54', '电子设备', '20110128'],
        ['55', '手机', '20110128'],
        ['56', '通讯设备', '20110128'],
        ['57', '电信业', '20110128'],
        ['58', '网路服务', '20110128'],
        ['59', '卫星通讯', '20110128'],
        ['60', '光纤通讯', '20110128'],
        ['61', '3C通路', '20110128'],
        ['62', '消费性电子', '20110128'],
        ['63', '照相机', '20110128'],
        ['64', '软件服务', '20110128'],
        ['65', '系统整合', '20110128'],
    ]
    dtype = {
        'ts_code': String(20),
        'cn_name': String(120),
        'start_date':Date,

    }
    name_list = ['ts_code',  'cn_name','start_date']
    info_df = pd.DataFrame(data=indicators_dic, columns=name_list)
    data_count = bunch_insert_on_duplicate_update(info_df, table_name, engine_md, dtype)
    logger.info('%d 条记录被更新', data_count)
    if not has_table and engine_md.has_table(table_name):
        alter_table_2_myisam(engine_md, [table_name])
        create_pk_str = """ALTER TABLE {table_name}
                CHANGE COLUMN `ts_code` `ts_code` VARCHAR(20) NOT NULL FIRST,
                ADD PRIMARY KEY (`ts_code`)""".format(table_name=table_name)
        with with_db_session(engine_md) as session:
            session.execute(create_pk_str)
        logger.info('%s 表 `wind_code` 主键设置完成', table_name)

@app.task
def import_tushare_tmt_twincome(chain_param=None, ts_code_set=None):
    """
    插入股票日线数据到最近一个工作日-1。
    如果超过 BASE_LINE_HOUR 时间，则获取当日的数据
    :return:
    """
    table_name = 'tushare_tmt_twincome'
    logging.info("更新 %s 开始", table_name)

    has_table = engine_md.has_table(table_name)
    # 进行表格判断，确定是否含有tushare_stock_daily
    if has_table:
        sql_str = """
            SELECT ts_code, date_frm start_date, end_date
            FROM
            (
            SELECT info.ts_code, ifnull(date, start_date) date_frm, 
            if(hour(now())<16, subdate(curdate(),1), curdate()) end_date
            FROM 
                tushare_tmt_twincome_info info 
            LEFT OUTER JOIN
                (SELECT item, adddate(max(date),1) date FROM {table_name} GROUP BY item ) income
            ON info.ts_code = income.item
            ) tt
            order by ts_code""".format(table_name=table_name)
    else:
        sql_str = """SELECT ts_code, start_date ,
            if(hour(now())<16, subdate(curdate(),1), curdate()) end_date 
            FROM tushare_tmt_twincome_info info """
        logger.warning('%s 不存在，仅使用 tushare_tmt_twincome_info 表进行计算日期范围', table_name)


    # ts_code_set = None
    with with_db_session(engine_md) as session:
        # 获取每只股票需要获取日线数据的日期区间
        table = session.execute(sql_str)
        # 计算每只股票需要获取日线数据的日期区间
        begin_time,ts_code_set = None,None
        # 获取date_from,date_to，将date_from,date_to做为value值
        code_date_range_dic = {
            ts_code: (date_from if begin_time is None else min([date_from, begin_time]), date_to)
            for ts_code, date_from, date_to in table.fetchall() if
            ts_code_set is None or ts_code in ts_code_set}

    data_df_list, data_count, all_data_count, data_len = [], 0, 0, len(code_date_range_dic)
    logger.info('%d Taiwan TMT information will been import into tushare_tmt_twincome', data_len)
    # 将data_df数据，添加到data_df_list

    Cycles = 1
    try:
        for num, (ts_code, (start_date, end_date)) in enumerate(code_date_range_dic.items(), start=1):
            logger.debug('%d/%d) %s [%s - %s]', num, data_len, ts_code, start_date, end_date)
            data_df = invoke_tmt_twincome(item=ts_code, start_date=datetime_2_str(start_date, STR_FORMAT_DATE_TS),
                                     end_date=datetime_2_str(end_date, STR_FORMAT_DATE_TS))
            # logger.info(' %d data of %s between %s and %s', df.shape[0], ts_code, start_date, date_to)
            if len(data_df) > 0 and data_df['date'] is not None:
                while try_2_date(data_df['date'].iloc[-1]) > try_2_date(start_date):
                    last_date_in_df_last, last_date_in_df_cur = try_2_date(data_df['date'].iloc[-1]), None
                    df2 = invoke_tmt_twincome(item=ts_code,
                                              start_date=datetime_2_str(start_date, STR_FORMAT_DATE_TS),
                                              end_date=datetime_2_str(try_2_date(data_df['date'].iloc[-1]) - timedelta(days=1),STR_FORMAT_DATE_TS))
                    if len(df2) > 0 and df2['date'] is not None:
                        last_date_in_df_cur = try_2_date(df2['date'].iloc[-1])
                        if last_date_in_df_cur < last_date_in_df_last:
                            data_df = pd.concat([data_df, df2])

                        elif last_date_in_df_cur == last_date_in_df_last:
                            break
                        if data_df is None:
                            logger.warning('%d/%d) %s has no data during %s %s', num, data_len, ts_code, start_date,
                                           end_date)
                            continue
                        logger.info('%d/%d) %d data of %s between %s and %s', num, data_len, data_df.shape[0], ts_code,
                                    start_date, end_date)
                    elif len(df2) <= 0:
                        break
            # 把数据攒起来
            if data_df is not None and data_df.shape[0] > 0:
                data_count += data_df.shape[0]
                data_df_list.append(data_df)
            # 大于阀值有开始插入
            if data_count >= 1000:
                data_df_all = pd.concat(data_df_list)
                bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md, DTYPE_TUSHARE_TMT_TWINCOME)
                all_data_count += data_count
                data_df_list, data_count = [], 0


    finally:
        # 导入数据库
        if len(data_df_list) > 0:
            data_df_all = pd.concat(data_df_list)
            data_count = bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md, DTYPE_TUSHARE_TMT_TWINCOME)
            all_data_count = all_data_count + data_count
            logging.info("更新 %s 结束 %d 条信息被更新", table_name, all_data_count)

if __name__ == "__main__":
    # tushare_tmt_twincome_info()
    import_tushare_tmt_twincome()

# # 下面代码是生成fields和par的
# sub=pd.read_excel('tasks/tushare/tushare_fina_reports/fina_indicator.xlsx',header=0)[['code','types']]
# for a, b in [tuple(x) for x in sub.values]:
#     print("['%s', '%s','20110128']," % (a, b))
#     # print("('%s', %s)," % (a, b))
#     # print("'%s'," % (a))