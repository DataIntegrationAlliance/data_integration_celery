# -*- coding: utf-8 -*-
"""
研究PMI 库存- 订单 与上证指数相关性
Created on 2017/11/11
@author: MG
"""
import logging
from tasks.wind import invoker
from tasks.backend import engine_md
from sqlalchemy.types import String, Date
from datetime import date, timedelta
from sqlalchemy.dialects.mysql import DOUBLE
from tasks.backend.orm import build_primary_key
from direstinvoker import APIError, UN_AVAILABLE_DATE
from tasks.utils.fh_utils import STR_FORMAT_DATE, split_chunk, str_2_date
from tasks.utils.db_utils import with_db_session, add_col_2_table, alter_table_2_myisam, \
    bunch_insert_on_duplicate_update

logger = logging.getLogger()


def import_edb_monthly():
    table_name = 'wind_edb_monthly'
    has_table = engine_md.has_table(table_name)
    PMI_FIELD_CODE_2_CN_DIC = {
        "M0017126": ("PMI", date(2005, 1, 1)),
        "M0017127": ("PMI:生产", date(2005, 1, 1)),
        "M0017128": ("PMI:新订单", date(2005, 1, 1)),
        "M0017129": ("PMI:新出口订单", date(2005, 1, 1)),
        "M0017130": ("PMI:在手订单", date(2005, 1, 1)),
        "M0017131": ("PMI:产成品库存", date(2005, 1, 1)),
        "M0017132": ("PMI:采购量", date(2005, 1, 1)),
        "M0017133": ("PMI:进口", date(2005, 1, 1)),
        "M5766711": ("PMI:出厂价格", date(2005, 1, 1)),
        "M0017134": ("PMI:主要原材料购进价格", date(2005, 1, 1)),
        "M0017135": ("PMI:原材料库存", date(2005, 1, 1)),
        "M0017136": ("PMI:从业人员", date(2005, 1, 1)),
        "M0017137": ("PMI:供货商配送时间", date(2005, 1, 1)),
        "M5207790": ("PMI:生产经营活动预期", date(2005, 1, 1)),
        "M5206738": ("PMI:大型企业", date(2005, 1, 1)),
        "M5206739": ("PMI:中型企业", date(2005, 1, 1)),
        "M5206740": ("PMI:小型企业", date(2005, 1, 1)),

        "M5407921": ("克强指数:当月值", date(2009, 7, 1)),

        "M0000612": ("CPI:当月同比", date(1990, 1, 1)),
        "M0000616": ("CPI:食品:当月同比", date(1990, 1, 1)),
        "M0000613": ("CPI:非食品:当月同比", date(1990, 1, 1)),
        "M0000614": ("CPI:消费品:当月同比", date(1990, 1, 1)),
        "M0000615": ("CPI:服务:当月同比", date(1990, 1, 1)),
        "M0000705": ("CPI:环比", date(1990, 1, 1)),
        "M0000706": ("CPI:食品:环比", date(1990, 1, 1)),
        "M0061581": ("CPI:非食品:环比", date(1990, 1, 1)),
        "M0061583": ("CPI:消费品:环比", date(1990, 1, 1)),

        "M0001227": ("PPI:全部工业品:当月同比", date(1996, 10, 1)),
        "M0061585": ("PPI:全部工业品:环比", date(2002, 1, 1)),
        "M0001228": ("PPI:生产资料:当月同比", date(1996, 10, 1)),
        "M0066329": ("PPI:生产资料:环比", date(2011, 1, 1)),
        "M0001232": ("PPI:生活资料:当月同比", date(1996, 10, 1)),
        "M0066333": ("PPI:生活资料:环比", date(2011, 1, 1)),
    }
    # 设置表属性类型
    param_list = [
        ('field_name', String(45)),
        ('trade_date', Date),
        ('val', DOUBLE),
    ]
    dtype = {key: val for key, val in param_list}
    dtype['field_code'] = String(20)
    data_len = len(PMI_FIELD_CODE_2_CN_DIC)
    if has_table:
        sql_str = """select field_code, max(trade_date) trade_date_max from wind_edb_monthly group by field_code"""

    else:
        sql_str = """
                       CREATE TABLE {table_name } (
                     `field_code` varchar(20) NOT NULL,
                     `field_name` varchar(45) DEFAULT NULL,
                     `trade_date` date NOT NULL,
                     `val` double DEFAULT NULL,
                     PRIMARY KEY (`field_code`,`trade_date`)
                   ) ENGINE=MyISAM DEFAULT CHARSET=utf8 COMMENT='保存wind edb 宏观经济数据';   

               """.format(table_name)
    # 获取数据库中最大日期
    with with_db_session(engine_md) as session:
        table = session.execute(sql_str)
        field_date_dic = {row[0]: row[1] for row in table.fetchall()}
    # 循环更新
    for data_num, (wind_code, (field_name, date_from)) in enumerate(PMI_FIELD_CODE_2_CN_DIC.items(), start=1):
        if wind_code in field_date_dic:
            date_from = field_date_dic[wind_code] + timedelta(days=1)
        date_to = date.today() - timedelta(days=1)
        logger.info('%d/%d) %s %s [%s %s]', data_num, data_len, wind_code, field_name, date_from, date_to)
        try:
            data_df = invoker.edb(wind_code, date_from, date_to, "Fill=Previous")
        except APIError as exp:
            logger.exception("%d/%d) %s 执行异常", data_num, data_len, wind_code)
            if exp.ret_dic.setdefault('error_code', 0) in (
                    -40520007,  # 没有可用数据
                    -40521009,  # 数据解码失败。检查输入参数是否正确，如：日期参数注意大小月月末及短二月
            ):
                continue
            else:
                break
        if data_df is None or data_df.shape[0] == 0:
            continue
        trade_date_max = str_2_date(max(data_df.index))
        if trade_date_max <= date_from:
            continue
        data_df.index.rename('trade_date', inplace=True)
        data_df.reset_index(inplace=True)
        data_df.rename(columns={wind_code.upper(): 'val'}, inplace=True)
        data_df['field_code'] = wind_code
        data_df['field_name'] = field_name
        # data_df.to_sql('wind_edb_monthly', engine_md, if_exists='append', index=False)
        bunch_insert_on_duplicate_update(data_df, table_name, engine_md, dtype=dtype)
        if not has_table and engine_md.has_table(table_name):
            alter_table_2_myisam(engine_md, [table_name])
            build_primary_key([table_name])


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s [%(name)s:%(funcName)s] %(message)s')
    # import_edb_monthly()
