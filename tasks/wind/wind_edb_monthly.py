# -*- coding: utf-8 -*-
"""
研究PMI 库存- 订单 与上证指数相关性
Created on 2017/11/11
@author: MG
"""
from config_fh import get_db_engine, get_db_session, STR_FORMAT_DATE, UN_AVAILABLE_DATE, WIND_REST_URL
from fh_tools.windy_utils_rest import WindRest, APIError
from fh_tools.fh_utils import str_2_date
from datetime import date, timedelta
import pandas as pd
import logging
logger = logging.getLogger()


def import_data():
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
    data_len = len(PMI_FIELD_CODE_2_CN_DIC)
    w = WindRest(WIND_REST_URL)
    sql_str = """select field_code, max(trade_date) trade_date_max from wind_edb_monthly group by field_code"""
    engine = get_db_engine()
    # 获取数据库中最大日期
    with get_db_session(engine) as session:
        table = session.execute(sql_str)
        field_date_dic = {row[0]: row[1] for row in table.fetchall()}
    # 循环更新
    for data_num, (wind_code, (field_name, date_from)) in enumerate(PMI_FIELD_CODE_2_CN_DIC.items(), start=1):
        if wind_code in field_date_dic:
            date_from = field_date_dic[wind_code] + timedelta(days=1)
        date_to = date.today() - timedelta(days=1)
        logger.info('%d/%d) %s %s [%s %s]', data_num, data_len, wind_code, field_name, date_from, date_to)
        try:
            data_df = w.edb(wind_code, date_from, date_to, "Fill=Previous")
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
        data_df.to_sql('wind_edb_monthly', engine, if_exists='append', index=False)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s [%(name)s:%(funcName)s] %(message)s')
    import_data()
