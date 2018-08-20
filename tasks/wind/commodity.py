# -*- coding: utf-8 -*-
"""
Created on 2017/4/14
@author: MG
"""
import pandas as pd
import logging
from datetime import date, datetime, timedelta
from direstinvoker.utils.fh_utils import str_2_date, date_2_str
from tasks.wind import invoker, APIError
from tasks.utils.fh_utils import STR_FORMAT_DATE, split_chunk
from tasks import app
from sqlalchemy.types import String, Date, Integer, Text
from sqlalchemy.dialects.mysql import DOUBLE
from tasks.backend import engine_md
from tasks.utils.db_utils import with_db_session, alter_table_2_myisam, \
    bunch_insert_on_duplicate_update

DEBUG = False
logger = logging.getLogger()
DATE_BASE = datetime.strptime('2005-01-01', STR_FORMAT_DATE).date()
ONE_DAY = timedelta(days=1)
# 标示每天几点以后下载当日行情数据
BASE_LINE_HOUR = 16


def build_commodity_info():
    table_name = 'wind_commodity_info'
    has_table = engine_md.has_table(table_name)
    indicators_dic = [
        #豆粕现货价格
        ["S5006056", "m_price_dg", "东莞豆粕价格", "2009-01-04", None, ''],
        ["S5006045", "m_price_lyg", "连云港豆粕价格", "2009-01-04", None, ''],
        ["S5006038", "m_price_rz", "日照豆粕价格", "2009-01-04", None, ''],
        ["S5006055", "m_price_zj", "湛江豆粕价格", "2009-01-04", None, ''],
        ["S5006038", "m_price_zjg", "张家港豆粕价格", "2009-01-04", None, ''],
        ["S0142909", "m_price_moa", "农业部统计豆粕价格", "2008-02-14", None, ''],
        ["S5006057", "m_price_fcg", "防城港豆粕价格", "2009-01-04", None, ''],
        ["S5006030", "m_price_dl", "大连豆粕价格", "2009-01-04", None, ''],
        ["S5006032", "m_price_tj", "天津豆粕价格", "2009-01-04", None, ''],
        ["S5006044", "m_price_cd", "成都豆粕价格", "2009-01-04", None, ''],
        #豆油现货价格
        ["S0142915", "y_price_average", "豆油平均价格", "2008-03-03", None, '一级豆油现货'],
        ["S5005983", "y_price_hp", "黄埔豆油价格", "2009-01-04", None, '一级豆油现货价格'],
        ["S5005982", "y_price_nb", "宁波豆油价格", "2009-01-04", None, '一级豆油现货价格'],
        ["S5005981", "y_price_zjg", "张家港豆油价格", "2009-01-04", None, '一级豆油现货价格'],
        ["S5005980", "y_price_rz", "日照豆油价格", "2009-01-04", None, '一级豆油现货价格'],
        ["S5005979", "y_price_tj", "天津豆油价格", "2009-01-04", None, '一级豆油现货价格'],
        ["S5005978", "y_price_dl", "大连豆油价格", "2009-01-04", None, '一级豆油现货价格'],
        #豆油出厂价格
        ["S5028743", "y_price_fcg_fab", "防城港豆油出厂价格", "2007-01-04", None, '一级豆油散装出厂价'],
        ["S5028742", "y_price_zj_fab", "湛江豆油出厂价格", "2007-01-04", None, '一级豆油散装出厂价'],
        ["S5028740", "y_price_zjg_fab", "张家港豆油出厂价格", "2007-01-04", None, '一级豆油散装出厂价'],
        ["S5028738", "y_price_rz_fab", "日照豆油出厂价格", "2007-01-04", None, '一级豆油散装出厂价'],
        ["S5028735", "y_price_jingjin_fab", "京津豆油出厂价格", "2007-01-04", None, '一级豆油散装出厂价'],
        ["S5028735", "y_price_dl_fab", "大连豆油出厂价格", "2007-01-04", None, '一级豆油散装出厂价'],
        #菜籽油出厂价格
        ["S5028744", "oi4_price_xy_fab", "河南信阳出厂价格", "2007-01-04", None, '四级菜油出厂价'],
        ["S5028747", "oi4_price_nt_fab", "南通出厂价格", "2007-01-04", None, '四级菜油出厂价'],
        ["S5028750", "oi4_price_wh_fab", "武汉出厂价格", "2007-01-04", None, '四级菜油出厂价'],
        ["S5028751", "oi4_price_jz_fab", "荆州出厂价格", "2007-01-04", None, '四级菜油出厂价'],
        ["S5028753", "oi4_price_cde_fab", "常德出厂价格", "2007-01-04", None, '四级菜油出厂价'],
        ["S5028755", "oi4_price_cdu_fab", "成都出厂价格", "2007-01-04", None, '四级菜油出厂价'],
        ["S5028752", "oi4_price_yy_fab", "岳阳出厂价格", "2007-01-04", None, '四级菜油出厂价'],
        #中国棕榈油价格
        ["S0142919", "p_price_average", "棕榈油平均价格", "2008-02-29", None, '棕榈油农业部统计价格'],
        ["S5006011", "p_price_average_24", "24度棕榈油平均价格", "2009-01-04", None, '现货价'],
        ["S5006005", "p_price_fj", "福建24度棕榈油价格", "2009-01-04", None, '现货价'],
        ["S5006006", "p_price_gd", "广东24度棕榈油价格", "2009-01-04", None, '现货价'],
        ["S5006007", "p_price_nb", "宁波24度棕榈油价格", "2009-01-04", None, '现货价'],
        ["S5006008", "p_price_zjg", "张家港24度棕榈油价格", "2009-01-04", None, '现货价'],
        ["S5006009", "p_price_tj", "天津24度棕榈油价格", "2009-01-04", None, '现货价'],
        ["S5006009", "p_price_rz", "日照24度棕榈油价格", "2009-01-04", None, '现货价'],
        #螺纹钢
        ["S5707798", "rb_price_nation", "全国螺纹钢均价", "2008-12-23", None, 'HRB400 20mm'],
        ["S0033213", "rb_price_bj", "北京螺纹钢均价", "2007-01-08", None, 'HRB400 20mm'],
        ["S0073207", "rb_price_tj", "天津螺纹钢均价", "2007-01-08", None, 'HRB400 20mm'],
        ["S0033217", "rb_price_gz", "广州螺纹钢均价", "2007-01-08", None, 'HRB400 20mm'],
        ["S0033227", "rb_price_sh", "上海螺纹钢均价", "2007-01-08", None, 'HRB400 20mm'],
        ["S5704770", "rb_price_hz", "杭州螺纹钢均价", "2007-04-24", None, 'HRB400 20mm'],
        ["S0033232", "rb_price_wh", "武汉螺纹钢均价", "2007-01-08", None, 'HRB400 20mm'],
        ["S5704771", "rb_price_nj", "南京螺纹钢均价", "2007-04-24", None, 'HRB400 20mm'],
        ["S5704774", "rb_price_fz", "福州螺纹钢均价", "2007-04-24", None, 'HRB400 20mm'],
        ["S5704784", "rb_price_ty", "太原螺纹钢均价", "2007-04-24", None, 'HRB400 20mm'],
        ["S5704788", "rb_price_cd", "成都螺纹钢均价", "2007-04-24", None, 'HRB400 20mm'],

        #外汇市场
        ["M0067855", "us2rmb", "美元兑人民币即期汇率", "1994-01-04", None, '中国货币网'],
        ["M0000185", "us2rmb_mid", "美元兑人民币中间价", "1994-08-31", None, '中国人民银行'],
        ["M0290205", "us2cnh", "美元兑人民币离岸汇率", "2012-04-30", None, '中国货币网'],
        ["M0000271", "usdx", "美元指数", "1971-01-04", None, '倚天财经'],
        #利率市场_上海银行同业拆借
        ["M1001854", "SHIBOR_N", "SHIBOR_N", "2006-10-08", None, '全国银行间同业拆借中心'],
        ["M1001855", "SHIBOR_1W", "SHIBOR_1W", "2006-10-08", None, '全国银行间同业拆借中心'],
        ["M1001856", "SHIBOR_2W", "SHIBOR_2W", "2006-10-08", None, '全国银行间同业拆借中心'],
        ["M1001857", "SHIBOR_1M", "SHIBOR_1M", "2006-10-08", None, '全国银行间同业拆借中心'],
        ["M1001858", "SHIBOR_3M", "SHIBOR_3M", "2006-10-08", None, '全国银行间同业拆借中心'],
        ["M1001859", "SHIBOR_6M", "SHIBOR_6M", "2006-10-08", None, '全国银行间同业拆借中心'],
        ["M1001860", "SHIBOR_9M", "SHIBOR_9M", "2006-10-08", None, '全国银行间同业拆借中心'],
        ["M1001861", "SHIBOR_1Y", "SHIBOR_1Y", "2006-10-08", None, '全国银行间同业拆借中心'],
        #香港同业拆借
        ["M0062945", "HIBOR_N", "HIBOR_N", "2006-01-03", None, '香港同业拆借市场'],
        ["M0062946", "HIBOR_1W", "HIBOR_1W", "2006-01-03", None, '香港同业拆借市场'],
        ["M0062947", "HIBOR_2W", "HIBOR_2W", "2006-01-03", None, '香港同业拆借市场'],
        ["M0062948", "HIBOR_1M", "HIBOR_1M", "2002-03-04", None, '香港同业拆借市场'],
        ["M0062949", "HIBOR_2M", "HIBOR_2M", "2002-03-04", None, '香港同业拆借市场'],
        ["M0062950", "HIBOR_3M", "HIBOR_3M", "2002-03-04", None, '香港同业拆借市场'],
        ["M0062953", "HIBOR_6M", "HIBOR_6M", "2002-03-04", None, '香港同业拆借市场'],
        ["M0062959", "HIBOR_12M", "HIBOR_12M", "2002-03-04", None, '香港同业拆借市场'],
        #伦敦同业
        

        #螺纹钢库存
        ["S0181750", "rb_inventory", "螺纹钢库存", "2010-05-21", None, '含上海全部仓库，单位：万吨'],
        ["S0110142", "wire_inventory", "线材库存", "2006-03-17", None, '单位：万吨'],
        ["S0110143", "hc_inventory", "热卷库存", "2006-03-17", None, '单位：万吨'],
        ["S0110144", "steel_plate_inventory", "中板库存", "2006-03-17", None, '单位：万吨'],
        ["S0110145", "cold_rolled_inventory", "冷轧库存", "2006-03-17", None, '单位：万吨'],
        ["S5708249", "key_steelworks_inventory", "重点钢厂库存", "2009-05-31", None, '单位：万吨'],
        #上海线螺采购量
        ["S5704503", "line_rb_purchase", "上海线螺采购量", "2004-10-17", None, '单位：吨'],

    ]
    dtype = {
        'key': String(20),
        'en_name': String(120),
        'cn_name': String(120),
        'begin_date': Date,
        'end_date': Date,
        'remark': Text,
    }
    name_list = ['key', 'en_name', 'cn_name', 'begin_date', 'end_date', 'remark']
    info_df = pd.DataFrame(data=indicators_dic, columns=name_list)
    data_count = bunch_insert_on_duplicate_update(info_df, table_name, engine_md, dtype)
    logger.info('%d 条记录被更新', data_count)
    if not has_table and engine_md.has_table(table_name):
        alter_table_2_myisam(engine_md, [table_name])
        create_pk_str = """ALTER TABLE {table_name}
            CHANGE COLUMN `key` `key` VARCHAR(20) NOT NULL FIRST,
            ADD PRIMARY KEY (`key`)""".format(table_name=table_name)
        with with_db_session(engine_md) as session:
            session.execute(create_pk_str)
        logger.info('%s 表 `key` 主键设置完成', table_name)


def import_edb(wind_code_set=None):
    """
    通过wind接口获取并导入EDB数据
    :return:
    """
    table_name = 'wind_commodity_edb'
    has_table = engine_md.has_table(table_name)
    logging.info("更新 %s 开始", table_name)
    param_list = [
        ('close', DOUBLE),
     ]
    rename_col_dic = {key.upper(): key.lower() for key, _ in param_list}

    # info_df = pd.read_sql('wind_commodity_info', engine_md)
    # 进行表格判断，确定是否含有 wind_commodity_edb
    if has_table:
        sql_str = """
                SELECT `key`, date_frm, if(end_date<end_date2, end_date, end_date2) date_to
                FROM
                (
                SELECT info.`key`, ifnull(trade_date, begin_date) date_frm, end_date,
                if(hour(now())<16, subdate(curdate(),1), curdate()) end_date2
                FROM 
                    wind_commodity_info info 
                LEFT OUTER JOIN
                    (SELECT `key`, adddate(max(trade_date),1) trade_date FROM {table_name} GROUP BY `key`) daily
                ON info.`key` = daily.`key`
                ) tt
                WHERE date_frm <= if(end_date<end_date2, end_date, end_date2) 
                ORDER BY `key`""".format(table_name=table_name)
    else:
        logger.warning('%s 不存在，仅使用 wind_commodity_info 表进行计算日期范围', table_name)
        sql_str = """
                SELECT `key`, date_frm, if(end_date<end_date2, end_date, end_date2) date_to
                FROM
                  (
                    SELECT info.`key`, begin_date date_frm, end_date,
                    if(hour(now())<16, subdate(curdate(),1), curdate()) end_date2
                    FROM wind_commodity_info info 
                  ) tt
                WHERE date_frm <= if(end_date<end_date2, end_date, end_date2) 
                ORDER BY `key`"""

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
    dtype['key'] = String(20)
    dtype['trade_date'] = Date

    data_df_list = []
    data_len = len(code_date_range_dic)
    logger.info('%d stocks will been import into wind_commodity_edb', data_len)
    # 将data_df数据，添加到data_df_list
    try:
        for num, (key_code, (date_from, date_to)) in enumerate(code_date_range_dic.items(), start=1):
            logger.debug('%d/%d) %s [%s - %s]', num, data_len, key_code, date_from, date_to)
            try:
                data_df = invoker.edb(key_code, date_from, date_to, options='')
            except APIError as exp:
                logger.exception("%d/%d) %s 执行异常", num, data_len, key_code)
                if exp.ret_dic.setdefault('error_code', 0) in (
                        -40520007,  # 没有可用数据
                        -40521009,  # 数据解码失败。检查输入参数是否正确，如：日期参数注意大小月月末及短二月
                ):
                    continue
                else:
                    break
            if data_df is None:
                logger.warning('%d/%d) %s has no data during %s %s', num, data_len, key_code, date_from, date_to)
                continue
            logger.info('%d/%d) %d data of %s between %s and %s', num, data_len, data_df.shape[0], key_code, date_from,
                        date_to)
            data_df['key'] = key_code
            data_df.rename(columns={key_code.upper(): 'value'}, inplace=True)
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
            # data_df_all.rename(columns=rename_col_dic, inplace=True)
            data_count = bunch_insert_on_duplicate_update(data_df_all, table_name, engine_md, dtype)
            logging.info("更新 %s 结束 %d 条信息被更新", table_name, data_count)
            if not has_table and engine_md.has_table(table_name):
                alter_table_2_myisam(engine_md, [table_name])
                # build_primary_key([table_name])
                create_pk_str = """ALTER TABLE {table_name}
                    CHANGE COLUMN `key` `key` VARCHAR(20) NOT NULL FIRST,
                    CHANGE COLUMN `trade_date` `trade_date` DATE NOT NULL AFTER `key`,
                    ADD PRIMARY KEY (`key`, `trade_date`)""".format(table_name=table_name)
                with with_db_session(engine_md) as session:
                    session.execute(create_pk_str)
                logger.info('%s 表 `key` `trade_date` 主键设置完成', table_name)


if __name__ == "__main__":
    # DEBUG = True
    build_commodity_info()
    # 更新每日股票数据
    import_edb()
