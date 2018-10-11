# -*- coding: utf-8 -*-
"""
Created on 2018/4/14
@author: MG
"""
import pandas as pd
import logging
from datetime import date, datetime, timedelta
from tasks.wind import invoker
from direstinvoker import APIError
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
        # 白糖价格
        ["S5023650", "sugar_price_Nanning", "南宁白砂糖价格", "2001-01-03", None, '南宁糖网现货价'],
        ["S5023651", "sugar_price_Liuzhou", "柳州白砂糖价格", "2001-01-03", None, '南宁糖网现货价'],
        ["S5023652", "sugar_price_Kunming", "昆明白砂糖价格", "2001-01-03", None, '南宁糖网现货价'],
        # 油脂油料
        # cbot_soybean
        ["S0069673", "cbot_soybean_close", "CBOT大豆价格", "1973-03-16", None, '单位：美分/蒲式耳'],
        # 国际油脂价格
        ["S5007761", "peanut_oil_price_international", "花生油国际现货价格", "2005-05-06", None, '美分/磅，美国农业部'],
        ["S5007762", "soybean_oil_price_international", "大豆油国际现货价格", "2005-05-06", None, '美分/磅，美国农业部'],
        ["S5007763", "cottonseed_oil_price_international", "棉籽油国际现货价格", "2005-05-06", None, '美分/磅，美国农业部'],
        ["S5007764", "corn_oil_price_international", "玉米油国际现货价格", "2005-05-06", None, '美分/磅，美国农业部'],
        ["S5007765", "linseed_oil_price_international", "亚麻油国际现货价格", "2005-05-06", None, '美分/磅，美国农业部'],
        ["S5007766", "castor_oil_price_international", "蓖麻油国际现货价格", "2005-05-06", None, '美分/磅，美国农业部'],
        ["S5007767", "coconut_oil_price_international", "椰油国际现货价格", "2005-05-06", None, '美分/磅，美国农业部'],
        ["S5007768", "palm_oil_price_international", "棕榈油国际现货价格", "2005-05-06", None, '美分/磅，美国农业部'],
        ["S5007769", "palm_kernel_oil_price_international", "棕榈仁油国际现货价格", "2005-05-06", None, '美分/磅，美国农业部'],
        # 豆粕现货价格
        ["S5006056", "soybean_meal_price_Dongguan", "东莞豆粕价格", "2009-01-04", None, ''],
        ["S5006045", "soybean_meal_price_Lianyungang", "连云港豆粕价格", "2009-01-04", None, ''],
        ["S5006038", "soybean_meal_price_Rizhao", "日照豆粕价格", "2009-01-04", None, ''],
        ["S5006055", "soybean_meal_price_Zhanjiang", "湛江豆粕价格", "2009-01-04", None, ''],
        ["S5028838", "soybean_meal_price_Zhangjiagang", "张家港豆粕价格", "2009-01-04", None, ''],
        ["S0142909", "soybean_meal_price_moa", "农业部统计豆粕价格", "2008-02-14", None, ''],
        ["S5006057", "soybean_meal_price_Fangchenggang", "防城港豆粕价格", "2009-01-04", None, ''],
        ["S5006030", "soybean_meal_price_Dalian", "大连豆粕价格", "2009-01-04", None, ''],
        ["S5006032", "soybean_meal_price_Tianjing", "天津豆粕价格", "2009-01-04", None, ''],
        ["S5006044", "soybean_meal_price_Chengdu", "成都豆粕价格", "2009-01-04", None, ''],

        # 菜粕现货价格
        ["S5028875", "rapeseed_meal_price_Chengdu", "成都菜粕价格", "2007-01-04", None, '国家粮油信息中心'],
        ["S5028874", "rapeseed_meal_price_Jiujiang", "九江菜粕价格", "2007-01-04", None, '国家粮油信息中心'],
        ["S5028871", "rapeseed_meal_price_Jingzhou", "荆州菜粕价格", "2007-01-04", None, '国家粮油信息中心'],
        ["S5028867", "rapeseed_meal_price_Nantong", "南通菜粕价格", "2007-01-04", None, '国家粮油信息中心'],
        ["S5005883", "rapeseed_meal_price_average", "菜粕平均价格", "2009-01-04", None, '国家粮油信息中心'],
        ["S5005872", "rapeseed_meal_price_Huangpu", "黄埔港菜粕价格", "2009-01-04", None, '国家粮油信息中心'],

        # 豆油现货价格
        ["S0142915", "soybean_oil_price_average", "豆油平均价格", "2008-03-03", None, '一级豆油现货'],
        ["S5005983", "soybean_oil_price_Huangpu", "黄埔豆油价格", "2009-01-04", None, '一级豆油现货价格'],
        ["S5005982", "soybean_oil_price_Ningbo", "宁波豆油价格", "2009-01-04", None, '一级豆油现货价格'],
        ["S5005981", "soybean_oil_price_Zhangjiagang", "张家港豆油价格", "2009-01-04", None, '一级豆油现货价格'],
        ["S5005980", "soybean_oil_price_Rizhao", "日照豆油价格", "2009-01-04", None, '一级豆油现货价格'],
        ["S5005979", "soybean_oil_price_Tianjin", "天津豆油价格", "2009-01-04", None, '一级豆油现货价格'],
        ["S5005978", "soybean_oil_price_Dalian", "大连豆油价格", "2009-01-04", None, '一级豆油现货价格'],
        # 豆油出厂价格
        ["S5028743", "soybean_oil_price_fab_Fangchenggang", "防城港豆油出厂价格", "2007-01-04", None, '一级豆油散装出厂价'],
        ["S5028742", "soybean_oil_price_fab_Zhanjiang", "湛江豆油出厂价格", "2007-01-04", None, '一级豆油散装出厂价'],
        ["S5028740", "soybean_oil_price_fab_Zhangjiagang", "张家港豆油出厂价格", "2007-01-04", None, '一级豆油散装出厂价'],
        ["S5028738", "soybean_oil_price_fab_Rizhao", "日照豆油出厂价格", "2007-01-04", None, '一级豆油散装出厂价'],
        ["S5028735", "soybean_oil_price_fab_Jingjin", "京津豆油出厂价格", "2007-01-04", None, '一级豆油散装出厂价'],
        ["S5028735", "soybean_oil_price_fab_Dalian", "大连豆油出厂价格", "2007-01-04", None, '一级豆油散装出厂价'],
        # 菜籽油出厂价格
        ["S5028744", "grade4_rapeseed_oil_price_fab_Xinyang", "河南信阳出厂价格", "2007-01-04", None, '四级菜油出厂价'],
        ["S5028747", "grade4_rapeseed_oil_price_fab_Nantong", "南通出厂价格", "2007-01-04", None, '四级菜油出厂价'],
        ["S5028750", "grade4_rapeseed_oil_price_fab_Wuhan", "武汉出厂价格", "2007-01-04", None, '四级菜油出厂价'],
        ["S5028751", "grade4_rapeseed_oil_price_fab_Jingzhou", "荆州出厂价格", "2007-01-04", None, '四级菜油出厂价'],
        ["S5028753", "grade4_rapeseed_oil_price_fab_Changde", "常德出厂价格", "2007-01-04", None, '四级菜油出厂价'],
        ["S5028755", "grade4_rapeseed_oil_price_fab_Chengdu", "成都出厂价格", "2007-01-04", None, '四级菜油出厂价'],
        ["S5028752", "grade4_rapeseed_oil_price_fab_Yueyang", "岳阳出厂价格", "2007-01-04", None, '四级菜油出厂价'],
        # 马来西亚棕榈油产量和进出口量
        ["S5022944", "palm_oil_production_of_Malaysia", "马来西亚棕榈油产量", "2007-01-01", None, '单位：万吨'],
        ["S5022946", "palm_core_oil_production_of_Malaysia", "马来西亚棕榈仁油产量", "2007-01-01", None, '单位：万吨'],

        ["G0715523", "palm_oil_exporting_of_Malaysia", "马来西亚棕榈油出口量", "2008-01-01", None, '单位：吨'],
        ["G0715524", "palm_oil_importing_of_Malaysia", "马来西亚棕榈油进口量", "2008-01-01", None, '单位：吨'],
        ["G0715525", "Malaysia_palm_oil_export_to_China_cumulative", "马来西亚棕榈油出口中国累积量", "2008-05-01", None, '单位：吨'],
        ["G0715526", "Malaysia_palm_oil_export_to_Europe_cumulative", "马来西亚棕榈油出口欧洲累积量", "2008-05-01", None, '单位：吨'],
        ["G0715527", "Malaysia_palm_oil_export_to_Pakistan_cumulative", "马来西亚棕榈油出口巴基斯坦累积量", "2008-05-01", None, '单位：吨'],
        ["G0715528", "Malaysia_palm_oil_export_to_India_cumulative", "马来西亚棕榈油出口印度累积量", "2008-05-01", None, '单位：吨'],
        ["G0715531", "Malaysia_palm_oil_trade", "马来西亚棕榈油贸易累计量", "2008-01-01", None, '单位：吨'],
        ["G0715533", "Malaysia_palm_oil_inventory", "马来西亚棕榈油月末库存", "2008-01-01", None, '单位：吨'],
        # 马来西亚本地棕榈油价格
        ["G0715534", "palm_oil_price_Malaysia", "马来西亚本地天然棕榈油价格", "2008-01-01", None, '单位：林吉特/吨'],
        # 中国棕榈油价格
        ["S0142919", "palm_oil_price_average", "棕榈油平均价格", "2008-02-29", None, '棕榈油农业部统计价格'],
        ["S5006011", "palm_oil_price_average_24", "24度棕榈油平均价格", "2009-01-04", None, '现货价'],
        ["S5006005", "palm_oil_price_Fujian", "福建24度棕榈油价格", "2009-01-04", None, '现货价'],
        ["S5006006", "palm_oil_price_Guangdong", "广东24度棕榈油价格", "2009-01-04", None, '现货价'],
        ["S5006007", "palm_oil_price_Ningbo", "宁波24度棕榈油价格", "2009-01-04", None, '现货价'],
        ["S5006008", "palm_oil_price_Zhangjiagang", "张家港24度棕榈油价格", "2009-01-04", None, '现货价'],
        ["S5006009", "palm_oil_price_Tianjin", "天津24度棕榈油价格", "2009-01-04", None, '现货价'],
        ["S5006009", "palm_oil_price_Rizhao", "日照24度棕榈油价格", "2009-01-04", None, '现货价'],
        # 棕榈油进口成本和FOB
        ["S5006024", "palm_oil_fob_Malaysia", "棕榈油马来西亚离岸价", "2004-05-08", None, '棕榈油马来西亚离岸价，单位：美元/吨'],
        ["S5006025", "palm_oil_cnf_Malaysia", "棕榈油马来西亚到岸价", "2004-05-08", None, '棕榈油马来西亚到岸价，单位：美元/吨'],
        ["S5006026", "palm_oil_import_cost", "棕榈油马来西亚进口成本价", "2004-05-08", None, '棕榈油马来西亚进口成本价，单位：美元/吨'],
        # 进口棕榈油装船及到港情况
        ["S5006260", "palm_oil_import_shipment", "进口棕榈油装船量当月值", "2008-12-25", None, '商务部，单位：吨'],
        ["S5006259", "palm_oil_import_shipment_Malaysia", "进口马来西亚棕榈油装船量当月值", "2008-12-25", None, '商务部，单位：吨'],
        ["S5006258", "palm_oil_import_shipment_Indonesia", "进口印度尼西亚棕榈油装船量当月值", "2008-12-25", None, '商务部，单位：吨'],
        ["S5006257", "palm_oil_import_arrived", "进口棕榈油到港量当月值", "2009-01-09", None, '商务部，单位：吨'],
        # 进口菜籽油装船及到港情况
        ["S5006241", "rapeseed_oil_import_shipment", "进口菜籽油装船量当月值", "2008-11-09", None, '商务部，单位：吨'],
        ["S5006240", "rapeseed_oil_import_shipment_Canada", "进口加拿大菜籽油装船量当月值", "2008-11-09", None, '商务部，单位：吨'],
        ["S5006238", "rapeseed_oil_import_arrived", "进口菜籽油到港量当月值", "2009-01-09", None, '商务部，单位：吨'],
        # 进口大豆油装船及到港情况
        ["S5006218", "soybean_oil_import_arrived", "进口大豆油到港量当月值", "2008-10-25", None, '商务部，单位：吨'],

        # 棕榈油进口海关统计
        ["S0071260", "palm_oil_import_volume", "海关口径棕榈油进口量当月值", "1991-01-01", None, '海关总署，单位：万吨'],
        ["S0117117", "palm_oil_import_average_price", "海关口径棕榈油进口平均价", "1999-01-01", None, '海关总署，单位：美元/吨'],
        ["S0071397", "palm_oil_import_volume_cumulative", "海关口径棕榈油进口量累计值", "1999-01-01", None, '海关总署，单位：万吨'],
        ["S0071534", "palm_oil_import_volume_cumulative_yoy", "海关口径棕榈油进口量累计同比", "1991-01-01", None, '海关总署，单位：%'],
        # 菜籽油进口商务部统计
        ["S5009840", "rapeseed_oil_import_volume", "海关口径菜籽油进口量", "2005-03-01", None, '商务部，单位：吨'],
        # 菜籽到岸成本
        ["S5005913", "rapeseed_CNF_price", "油菜籽CNF到岸价", "2005-02-01", None, '农业部，单位：美元/吨'],
        ["S5005914", "rapeseed_import_cost", "油菜籽进口成本价", "2005-02-01", None, '农业部，单位：元/吨'],

        # 大豆压榨利润
        ["S5006417", "imported_soybeans_crush_margin_Guangdong", "广东进口大豆压榨利润", "2005-04-04", None, '单位：元/吨'],
        ["S5006416", "imported_soybeans_crush_margin_Jiangsu", "江苏进口大豆压榨利润", "2005-04-04", None, '单位：元/吨'],
        ["S5006415", "imported_soybeans_crush_margin_Shandong", "山东进口大豆压榨利润", "2005-04-04", None, '单位：元/吨'],
        ["S5006413", "imported_soybeans_crush_margin_Tianjin", "天津进口大豆压榨利润", "2005-04-04", None, '单位：元/吨'],
        ["S5006411", "imported_soybeans_crush_margin_Dalian", "大连进口大豆压榨利润", "2005-04-04", None, '单位：元/吨'],
        # 巴西fob运费到岸成本
        ["S5041182", "Brazilian_soybean_premiums_discount_in_recent_month", "巴西大豆近月升贴水", "2013-01-04", None,'单位：美分/蒲式耳'],
        ["S5041183", "Brazilian_soybean_fob_in_recent_month", "巴西大豆近月FOB", "2013-01-04", None, '单位：美元/吨'],
        ["S5041184", "Brazilian_soybean_freight_in_recent_month", "巴西大豆近月运费", "2013-01-04", None, '单位：美元/吨'],
        ["S5041185", "Brazilian_soybean_cnf_in_recent_month", "巴西大豆近月CNF价格", "2013-01-04", None, '单位：美元/吨'],
        ["S5041186", "Brazilian_soybean_onshore_duty_price", "巴西大豆到岸完税价", "2013-01-04", None, '单位：元/吨'],
        # 美豆升贴水运费到岸成本
        ["S0114197", "cost_of_importing_American_soybean_into_China", "进口美国大豆到港成本", "2008-05-07", None, '单位：元/吨'],
        ["S0114199", "freight_of_importing_American_soybean_into_China", "进口美国大豆国际运输成本", "2013-01-04", None, '单位：美元/吨'],
        ["S0114198", "American_soybean_premiums_discount_in_recent_month", "进口美国大豆近月升贴水", "2013-01-04", None,'单位：美分/蒲式耳'],
        # 豆粕库存
        ["S5041144", "soybean_meal_inventory_of_China", "豆粕全国库存", "2012-12-30", None, '单位：万吨'],
        ["S5041138", "soybean_meal_inventory_of_North_China", "华北豆粕库存", "2012-12-30", None, '单位：万吨'],
        ["S5041140", "soybean_meal_inventory_of_East_China", "华东豆粕库存", "2012-12-30", None, '单位：万吨'],
        ["S5041141", "soybean_meal_inventory_of_Guangdong", "广东豆粕库存", "2012-12-30", None, '单位：万吨'],
        ["S5041142", "soybean_meal_inventory_of_Guangxi", "广西豆粕库存", "2012-12-30", None, '单位：万吨'],
        # USDA大豆供需报告
        ["S0112251", "soybean_beginning_stocks", "大豆期初库存", "2000-01-01", None, 'USDA,单位：百万吨'],
        ["S0112252", "soybean_production", "大豆产量", "2000-01-01", None, 'USDA,单位：百万吨'],
        ["S0112253", "soybean_importing", "大豆进口量", "2000-01-01", None, 'USDA,单位：百万吨'],
        ["S0112254", "soybean_crushing", "大豆压榨量", "2000-01-01", None, 'USDA,单位：百万吨'],
        ["S0112255", "soybean_total domestic consumption", "大豆国内消费总计", "2000-01-01", None, 'USDA,单位：百万吨'],
        ["S0112256", "soybean_exporting", "大豆出口量", "2000-01-01", None, 'USDA,单位：百万吨'],
        ["S0112257", "soybean_beginning_stocks", "大豆期末库存", "2000-01-01", None, 'USDA,单位：百万吨'],
        ["S5009779", "soybean_total_suply", "大豆总供给", "2000-01-01", None, 'USDA,单位：百万吨'],
        ["S5009780", "soybean_trade", "大豆贸易量", "2000-01-01", None, 'USDA,单位：百万吨'],

        # 进口大豆船期预报
        ["M0041529", "soybean_imports", "当月大豆进口数量", "1995-01-01", None, '海关总署,单位：万吨'],
        ["S5041160", "forcasting_of_soybean_importing", "全国进口大豆船期预报到港量", "2007-01-01", None, '单位：万吨'],

        # 港口进口大豆库存
        ["S0117164", "soybean_inventory_of_China_port", "中国港口大豆库存", "2008-01-02", None, '单位：万吨'],
        ["S0117165", "soybean_consuming_of_China_port", "进口大豆港口消耗", "2008-01-02", None, '单位：吨'],
        # 豆油商业库存
        ["S5028184", "soybean_oil_business_inventory_of_China", "中国豆油商业库存", "2012-04-02", None, '单位：万吨'],
        # 棕榈油港口库存
        ["S5006381", "palm_oil_inventory_of_China_port", "全国棕榈油港口库存", "2010-04-02", None, '单位：万吨'],
        ["S5006380", "palm_oil_inventory_of_Guangdong", "广东棕榈油港口库存", "2010-04-02", None, '单位：万吨'],
        ["S5006378", "palm_oil_inventory_of_Zhangjiagang", "张家港棕榈油库存", "2010-04-02", None, '单位：万吨'],
        ["S5006376", "palm_oil_inventory_of_Tianjin", "天津棕榈油港口库存", "2010-04-02", None, '单位：万吨'],
        # 棕榈油港口库存
        ["S5029336", "palm_oil_commercial_inventory_of_China", "中国棕榈油商业库存", "2012-02-04", None, '单位：万吨'],
        ["S5029337", "palm_oil_commercial_inventory_of_Tianjin", "天津棕榈油商业库存", "2012-02-04", None, '单位：万吨'],
        ["S5029338", "palm_oil_commercial_inventory_of_Shandong", "山东棕榈油商业库存", "2012-02-04", None, '单位：万吨'],
        ["S5029339", "palm_oil_commercial_inventory_of_East_China", "华东棕榈油商业库存", "2012-02-04", None, '单位：万吨'],
        ["S5029340", "palm_oil_commercial_inventory_of_Guangdong", "广东棕榈油商业库存", "2012-02-04", None, '单位：万吨'],
        ["S5029341", "palm_oil_commercial_inventory_of_Guangxi", "广西棕榈油商业库存", "2012-02-04", None, '单位：万吨'],
        ["S5029342", "palm_oil_commercial_inventory_of_Fujian", "福建棕榈油商业库存", "2012-02-04", None, '单位：万吨'],
        # 马来西亚棕榈油库存
        ["S5022948", "palm_oil_inventory_of_Malaysia", "马来西亚棕榈油期末库存", "2007-01-01", None, '马来西亚棕榈油局，单位：吨'],
        ["S5022950", "palm_kernel_oil_inventory_of_Malaysia", "马来西亚棕榈仁油期末库存", "2007-01-01", None, '马来西亚棕榈油局，单位：吨'],

        # 棕榈油仓单数量
        ["S0149298", "palm_oil_warehouse_orders", "大商所棕榈油仓单数量", "2008-01-04", None, '单位：手'],
        # 能繁母猪
        ["S0114186", "hog_herds", "生猪存栏量", "2008-12-01", None, '数据来自中国政府网，单位：万头'],
        ["S0174665", "hog_herds_mom", "生猪存栏量环比变化", "2012-04-01", None, '数据来自农业部'],
        ["S0174666", "hog_herds_yoy", "生猪存栏量同比变化", "2012-04-01", None, '数据来自农业部'],
        ["S0114187", "breeding_sow_stock", "能繁母猪存栏", "2009-01-01", None, '数据来自中国政府网，单位：万头'],
        ["S0174667", "breeding_sow_stock_mom", "能繁母猪存栏环比变化", "2012-04-01", None, '数据来自农业部'],
        ["S0174668", "breeding_sow_stock_yoy", "能繁母猪存栏同比变化", "2012-04-01", None, '数据来自农业部'],

        # 养殖利润
        ["S5021686", "farming_profit_crude_chicken", "毛鸡养殖利润", "2009-10-23", None, '单位：元/羽'],
        ["S5021692", "farming_profit_boiler_layer", "817肉杂鸡养殖利润", "2010-03-29", None, '单位：元/羽'],
        ["S5021699", "farming_profit_layer", "蛋鸡养殖利润", "2009-10-23", None, '单位：元/羽'],
        ["S5021712", "farming_profit_duck", "肉鸭养殖利润", "2010-03-29", None, '单位：元/羽'],
        ["S5021738", "farming_profit_homebred_pig", "自繁自养生猪养殖利润", "2010-03-29", None, '单位：元/头'],
        ["S5021739", "farming_profit_outsourcing_pig", "外购仔猪养殖利润", "2010-03-29", None, '单位：元/头'],

        # 动力煤港口价格
        ["S5124097", "CCI5500K", "动力煤CCI5500", "2013-10-14", None,
         'CCI1,单位：元/吨，以秦皇岛港发热量5500大卡动力煤的离岸含税价（FOB）为标的物，记录7天至45天后装船的煤炭交易价格，主要采集区域为秦皇岛港、曹妃甸港、京唐港、天津港。'],
        ["S5130993", "CCI5500K", "动力煤CCI5500", "2015-08-10", None, '含税价格，单位：元/吨'],

        ["S5124099", "CCI5500K_imported", "动力煤CCI5500", "2013-10-14", None,
         'CCI8,单位：元/吨，以广州港发热量5500大卡动力煤的到岸不含税价（CIF）为标的物，记录15天至60天后到港的煤炭交易价格，主要采集区域为广州港、防城港、深圳港、厦门港、湛江港和珠江港。'],
        ["S5130997", "CCI5500K_imported", "动力煤CCI5500", "2015-08-10", None, '含税价格，单位：元/吨'],

        ["S5125680", "CCI5000K", "动力煤CCI5000", "2014-06-16", None, 'CC12,5000大卡秦皇岛含税价格，单位：元/吨'],
        ["S5130995", "CCI5000K", "动力煤CCI5000", "2015-08-10", None, '5000大卡秦皇岛含税价价格，单位：元/吨'],

        ["S5125683", "CCI4700K_imported_USD", "动力煤CCI4700", "2014-09-16", None, 'CC17,4700大卡印尼煤南方港口到岸价，单位：美元/吨'],
        ["S5131000", "CCI4700K_imported_USD", "动力煤CCI4700", "2015-08-10", None, '4700大卡印尼煤南方港口到岸价，单位：美元/吨'],
        ["S5125682", "CCI4700K_imported", "动力煤CCI4700", "2014-09-16", None, 'CC17,4700大卡印尼煤南方港口到岸价，单位：元/吨'],
        ["S5130999", "CCI4700K_imported", "动力煤CCI4700", "2015-08-10", None, '4700大卡印尼煤南方港口到岸价，单位：元/吨'],

        ["S5131001", "CCI3800K_imported_RMB", "动力煤CCI3800", "2008-08-10", None, '3800大卡印尼煤南方港口到岸价，单位：元/吨'],
        ["S5131002", "CCI3800K_imported_USD", "动力煤CCI3800", "2015-08-10", None, '3800大卡印尼煤南方港口到岸价，单位：元/吨'],

        ["S5101710", "ARA_thermal_coal_price", "欧洲ARA港动力煤现货价", "2015-08-16", None, '单位：美元/吨'],
        ["S5101711", "RB_thermal_coal_price", "理查德RB港动力煤现货价", "2015-08-16", None, '单位：美元/吨'],
        ["S5101712", "NEWC_thermal_coal_price", "纽卡斯尔NEWC港动力煤现货价", "2015-08-16", None, '单位：美元/吨'],

        # 动力煤坑口价格
        ["S0146017", "coal_mine_price_Datongnanjiao", "大同南郊弱粘煤坑口价", "2006-01-10", None,
         'A10-16%,V28-32%,S0.8%,QQ5500，单位：元/吨'],
        ["S5101817", "coal_mine_price_Yulin", "榆林烟末煤坑口价", "2006-01-10", None, 'A20%,V32-38%,S<1%,QQ5500，单位：元/吨'],
        ["S5101827", "coal_mine_price_Shenmu", "神木烟末煤坑口价", "2008-04-03", None, 'A15%,V28-34%,S<1%,QQ6000，单位：元/吨'],
        ["S5101766", "coal_mine_price_Dongsheng", "东胜原煤坑口价", "2009-07-20", None,
         'A20%,V33-36%,S0.5-0.8%,QQ5200，单位：元/吨'],
        ["S0146021", "coal_mine_price_Datonghunyuan", "大同浑源长焰煤坑口价", "2009-07-20", None,
         'A19%,V30-45%,S0.8%,QQ5600，单位：元/吨'],

        ["S0146192", "coal_board_price_Datonghunyuan", "大同浑源动力煤车板价", "2008-06-10", None,
         'A18-20%,V46%,S1%,QQ5500，单位：元/吨'],
        ["S0146189", "coal_board_price_Datongnanjiao", "大同南郊动力煤车板价", "2008-06-10", None,
         'A12-18%,V28-32%,S1%,QQ5500，单位：元/吨'],
        ["S5101763", "coal_board_price_Baotou", "包头动力煤车板价", "2009-07-27", None, 'A20%,V30%,S0.2%,QQ5000，单位：元/吨'],
        ["S5101820", "coal_board_price_Hancheng", "韩城动力煤车板价", "2009-02-02", None,
         'A20-25%,V17-20%,S0.5%,QQ5000，单位：元/吨'],
        ["S5101762", "coal_board_price_Wuhai", "乌海动力煤车板价", "2009-07-27", None, 'A18%,V>30%,S>1%,QQ5500，单位：元/吨'],

        ["S5104572", "Qinhuangdao_FOB_5500K", "秦皇岛5500大卡FOB价", "2008-07-31", None, '单位：元/吨'],
        ["S5104573", "Qinhuangdao_FOB_5500K", "秦皇岛5000大卡FOB价", "2008-07-31", None, '单位：元/吨'],
        ["S5104574", "Qinhuangdao_FOB_5500K", "秦皇岛4500大卡FOB价", "2008-07-31", None, '单位：元/吨'],

        ["S5101487", "Guangzhou_ex_warehouse_Indonesia_5500K", "广州港5500大卡印尼煤库提价", "2012-01-04", None,
         'A10%,V41%,S0.6%,QQ5500,单位：元/吨'],
        ["S5112201", "Guangzhou_ex_warehouse_South_Africa_6000K", "广州港6000大卡南非煤库提价", "2012-01-04", None,
         'A14%,V26%,S0.6%,QQ6000,单位：元/吨'],
        ["S5101482", "Guangzhou_ex_warehouse_Australia_5500K", "广州港5500大卡澳洲煤库提价", "2011-05-18", None,
         'A20%,V28%,S0.7%,QQ5500,单位：元/吨'],
        ["S5112243", "Guangzhou_ex_warehouse_Shenhun_5500K", "广州港神混1号5500大卡煤库提价", "2012-04-09", None,
         'A12%,V28%,S0.3-0.6%,QQ5500,单位：元/吨'],
        ["S5112245", "Guangzhou_ex_warehouse_Youhun_5500K", "广州港山西优混5500大卡库提价", "2012-01-11", None,
         'A14%,V28%,S0.6%,QQ5500,单位：元/吨'],
        ["S5101484", "Guangzhou_ex_warehouse_Australia_5000K", "广州港5000大卡澳大利亚煤库提价", "2012-02-01", None,
         'A25%,V25%,S0.4%,QQ5000,单位：元/吨'],
        ["S5112254", "Guangzhou_ex_warehouse_Meng_4500K", "广州港4500大卡蒙煤库提价", "2012-04-13", None,
         'A21%,V29%,S0.5%,QQ4500,单位：元/吨'],
        # 沿海和国际煤炭运费
        ["S0167811", "CBCFI", "运费指数", "2011-12-14", None, '中国沿海煤炭运价指数（CBCFI）以2011年9月1日为基期，基期指数为1000点，单位：点'],
        ["S0167812", "CBCFI_coal_Qinhuangdao2Guangzhou", "秦皇岛到广州煤炭运费", "2009-01-06", None, '5-6万DWT单位：元/吨'],
        ["S0167813", "CBCFI_coal_Qinhuangdao2Fuzhou", "秦皇岛到福州煤炭运费", "2009-01-06", None, '3-4万DWT单位：元/吨'],
        ["S0167814", "CBCFI_coal_Qinhuangdao2Ningbo", "秦皇岛到宁波煤炭运费", "2009-01-06", None, '1.5-2万DWT单位：元/吨'],
        ["S0167815", "CBCFI_coal_Qinhuangdao2Shanghai", "秦皇岛到上海煤炭运费", "2009-01-06", None, '4-5万DWT单位：元/吨'],
        ["S0167816", "CBCFI_coal_Qinhuangdao2Zhangjiagang", "秦皇岛到张家港煤炭运费", "2011-12-09", None, '2-3万DWT单位：元/吨'],
        ["S0167817", "CBCFI_coal_Huanghua2Shanghai", "黄骅港到上海煤炭运费", "2009-01-06", None, '3-4万DWT单位：元/吨'],
        ["S0031550", "BDI", "波罗的海干散货指数", "1988-10-19", None, '波罗的海干散货指数单位：元/吨'],


        # 煤炭库存
        ["S5118163", "coal_inventory_Caofeidian", "曹妃甸煤炭库存", "2011-09-27", None, '单位：万吨'],
        ["S5103725", "coal_inventory_Qinhuangdao", "秦皇岛煤炭库存", "2002-01-23", None, '单位：万吨'],
        ["S5103734", "coal_inventory_Jingtanggang", "京唐港国投港区煤炭库存", "2009-06-24", None, '单位：吨'],
        ["S5103728", "coal_inventory_Guangzhougang", "广州港煤炭库存", "2007-04-28", None, '单位：万吨'],
        ["S5133555", "coal_inventory_Changjiangkou", "长江口煤炭库存", "2016-04-29", None, '易煤网，单位：万吨'],
        ["S5118194", "coal_inventory_Tianjin", "天津煤炭库存", "2012-02-29", None, 'wind，单位：万吨'],
        ["S5110861", "coal_inventory_NEWC", "澳大利亚纽卡斯尔港煤炭库存", "2009-11-17", None, 'NVCCC，单位：吨'],
        #焦煤库存
        ["S5118158", "cooking_coal_inventory_Jingtanggang", "京唐港炼焦煤库存", "2011-06-25", None, '单位：万吨'],
        ["S5118159", "cooking_coal_inventory_Rizhaogang", "日照港炼焦煤库存", "2011-06-25", None, '单位：万吨'],
        ["S5118160", "cooking_coal_inventory_Lianyungang", "连云港炼焦煤库存", "2011-06-25", None, '单位：万吨'],

        # 秦皇岛调度
        ["S5104482", "Qinhuangdao_input", "秦皇岛铁路调入量", "2008-09-01", None, '单位：万吨'],
        ["S5104483", "Qinhuangdao_output", "秦皇岛港口吞吐量", "2008-09-01", None, '单位：万吨'],
        ["S5104484", "Qinhuangdao_anchorage_vessels", "秦皇岛锚地船舶数", "2010-08-04", None, '单位：艘'],
        ["S5104485", "Qinhuangdao_prearrival", "秦皇岛船舶预到数", "2010-08-04", None, '单位：艘'],
        # 发电数据
        ["S0027020", "hydropower", "水电发电量当月值", "1986-01-31", None, '单位：亿千瓦时'],
        ["S0027021", "hydropower_yoy", "水电发电量当月同比", "1997-01-31", None, '单位：%'],
        ["S0027022", "hydropower_accumulation", "水电发电量累计值", "1990-01-31", None, '单位：亿千瓦时'],
        ["S0027023", "hydropower_accumulation_yoy", "水电发电量累计同比", "1990-01-31", None, '单位：%'],

        ["S0027016", "thermal_power", "火电发电量当月值", "1990-01-31", None, '单位：亿千瓦时'],
        ["S0027017", "thermal_power_yoy", "火电发电量当月同比", "1997-01-31", None, '单位：%'],
        ["S0027018", "thermal_power_accumulation", "火电发电量累计值", "1990-01-31", None, '单位：亿千瓦时'],
        ["S0027019", "thermal_power_accumulation_yoy", "火电发电量累计同比", "1990-01-31", None, '单位：%'],

        ["S0253036", "wind_power", "风电发电量当月值", "2009-09-30", None, '单位：亿千瓦时'],
        ["S0253038", "wind_power_yoy", "风电发电量当月同比", "2009-09-30", None, '单位：%'],
        ["S0253040", "wind_power_accumulation", "风电发电量累计值", "2009-09-30", None, '单位：亿千瓦时'],
        ["S0253042", "wind_power_accumulation_yoy", "风电发电量累计同比", "2009-09-30", None, '单位：%'],

        ["S0027012", "electricity_generation", "发电量当月值", "1990-01-31", None, '单位：亿千瓦时'],
        ["S0027013", "electricity_generation_yoy", "发电量当月同比", "1990-01-31", None, '单位：%'],
        ["S0027014", "electricity_generation_accumulation", "发电量累计值", "1990-01-31", None, '单位：亿千瓦时'],
        ["S0027015", "electricity_generation_accumulation_yoy", "发电量累计同比", "1990-01-31", None, '单位：%'],

        # 用电数据
        ["S5100021", "electricity_consumption", "全社会用电量当月值", "2006-03-01", None, '发改委能源局，单位：万千瓦时'],
        ["S5100023", "electricity_consumption_primary_industry", "第一产业用电量当月值", "2006-03-01", None, '发改委能源局，单位：万千瓦时'],
        ["S5100024", "electricity_consumption_secondary _industry", "第二产业用电量当月值", "2006-03-01", None, '发改委能源局，单位：万千瓦时'],
        ["S5100025", "electricity_consumption_tertiary_industry", "第三产业用电量当月值", "2006-03-01", None, '发改委能源局，单位：万千瓦时'],

        ["S5100122", "electricity_consumption_yoy", "全社会用电量当月值同比", "2009-07-01", None, '发改委能源局，单位：%'],
        ["S5100124", "electricity_consumption_primary_industry_yoy", "第一产业用电量当月值同比", "2009-07-01", None, '发改委能源局，单位：%'],
        ["S5100125", "electricity_consumption_secondary _industry_yoy", "第二产业用电量当月值同比", "2009-07-01", None, '发改委能源局，单位：%'],
        ["S5100126", "electricity_consumption_tertiary_industry_yoy", "第三产业用电量当月值同比", "2009-07-01", None, '发改委能源局，单位：%'],

        ["S0048389", "electricity_consumption_accumulation", "全社会用电量累计值", "2003-05-01", None, '中国电力企业联合会，单位：亿千瓦时'],
        ["S0048390", "electricity_consumption_primary_industry_accumulation", "第一产业用电量累计值", "2003-05-01", None, '中国电力企业联合会，单位：亿千瓦时'],
        ["S0048391", "electricity_consumption_secondary _industry_accumulation", "第二产业用电量累计值", "2003-05-01", None, '中国电力企业联合会，单位：亿千瓦时'],
        ["S0048392", "electricity_consumption_tertiary_industry_accumulation", "第三产业用电量累计值", "2003-05-01", None, '中国电力企业联合会，单位：亿千瓦时'],

        ["S0048397", "electricity_consumption_accumulation_yoy", "全社会用电量累计同比", "2003-05-01", None, '中国电力企业联合会，单位：%'],
        ["S0048398", "electricity_consumption_primary_industry_accumulation_yoy", "第一产业用电量累计同比", "2003-05-01", None, '中国电力企业联合会，单位：%'],
        ["S0048399", "electricity_consumption_secondary _industry_accumulation_yoy", "第二产业用电量累计同比", "2003-05-01", None, '中国电力企业联合会，单位：%'],
        ["S0048400", "electricity_consumption_tertiary_industry_accumulation_yoy", "第三产业用电量累计同比", "2003-05-01", None,'中国电力企业联合会，单位：%'],

        # 重点和六大电厂库存及日耗
        ["S5116614", "six_power_consumption", "六大电厂日均耗煤量", "2009-10-01", None, '单位：万吨'],
        ["S5116621", "six_power_inventory", "六大电厂库存", "2009-10-01", None, '单位：万吨'],
        ["S5116622", "six_power_usable_days", "六大电厂可用天数", "2009-10-01", None, '单位：天'],
        ["S5103913", "key_power_inventory", "重点电厂库存直供统计", "2004-09-01", None, '单位：万吨'],
        # 三峡流量
        ["S5110944", "Sanxia_inflow", "三峡入库流量", "2003-06-01", None, '单位：立方米/秒'],
        ["S5110945", "Sanxia_outflow", "三峡出库流量", "2003-06-01", None, '单位：立方米/秒'],

        # 原油价格
        ["S0031526", "OPEC_package_oil_price", "opec一揽子原油价格", "1990-12-31", None, '美元/桶'],
        ["S5111905", "Brent_oil_price", "英国布伦特原油价格", "2001-12-26", None, '美元/桶'],
        ["S0031528", "Shengli_ring_pacific_oil_price", "中国胜利环太平洋原油价格", "2001-12-26", None, '美元/桶'],
        ["S0031529", "Daqing_ring_pacific_oil_price", "中国大庆环太平洋原油价格", "2001-12-26", None, '美元/桶'],
        ["S0031530", "Dubai_pacific_rim_oil_price", "阿联酋迪拜环太平洋原油价格", "2001-12-25", None, '美元/桶'],
        ["S0031532", "Malaysia_pacific_rim_tapes_oil_price", "马来西亚塔皮斯环太平洋原油价格", "2001-12-25", None, '美元/桶'],
        ["S0031533", "Indonesia_minas_pacific_rim_oil_price", "印尼米纳斯环太平洋原油价格", "2001-12-25", None, '美元/桶'],
        ["S0031534", "Indonesia_sinta_pacific_rim_oil_price", "印尼辛塔环太平洋原油价格", "2001-12-27", None, '美元/桶'],
        ["S0031535", "Indonesia_aldouri_pacific_rim_oil_price", "印尼杜里环太平洋原油价格", "2001-12-26", None, '美元/桶'],
        ["S5111903", "WTI_light_oil_price", "美国西德克萨斯中级轻质原油价格", "2002-11-25", None, '美元/桶'],
        ["S5111904", "Oman_rim_pacific_oil_price", "阿曼环太平洋原油价格", "2001-12-26", None, '美元/桶'],
        ["S5111925", "WTI_cushing_light_oil_fob_price", "西德克萨斯中级轻质原油库欣FOB价格", "1986-01-02", None, '美元/桶'],
        ["S5111926", "Brent_oil_europe_fob_price", "英国布伦特FOB欧洲原油价格", "1987-05-20", None, '美元/桶'],
        ["S5104915", "Brent_oil_north_sea_fob_price", "英国布伦特北海离岸原油价格", "1997-01-03", None, '美元/桶'],
        ["S5104912", "Louisiana_low_sulphur_St_James_deliverapeseed_oil_price", "路易斯安那低硫圣詹姆斯交割价格", "1997-01-03", None,
         '美元/桶'],
        ["S5111925", "WTI_cushing_light_oil_deliverapeseed_oil_price", "西德克萨斯中级轻质原油库欣交割价格", "1997-01-03", None, '美元/桶'],
        ["S5104914", "Dubai_middle_east_fob_price", "迪拜中东离岸价格", "2008-12-19", None, '美元/桶'],
        ["S5110987", "Kuwait_Energy_Corp_oil_price", "科威特能源公司原油价格", "2001-12-26", None, '美元/桶'],

        # 螺纹钢
        ["S5707798", "rebar_price_nation", "全国螺纹钢均价", "2008-12-23", None, 'HRB400 20mm'],
        ["S0033213", "rebar_price_Beijing", "北京螺纹钢均价", "2007-01-08", None, 'HRB400 20mm'],
        ["S0073207", "rebar_price_Tianjin", "天津螺纹钢均价", "2007-01-08", None, 'HRB400 20mm'],
        ["S0033217", "rebar_price_Guangzhou", "广州螺纹钢均价", "2007-01-08", None, 'HRB400 20mm'],
        ["S0033227", "rebar_price_Shanghai", "上海螺纹钢均价", "2007-01-08", None, 'HRB400 20mm'],
        ["S5704770", "rebar_price_Hangzhou", "杭州螺纹钢均价", "2007-04-24", None, 'HRB400 20mm'],
        ["S0033232", "rebar_price_Wuhan", "武汉螺纹钢均价", "2007-01-08", None, 'HRB400 20mm'],
        ["S5704771", "rebar_price_Nanjing", "南京螺纹钢均价", "2007-04-24", None, 'HRB400 20mm'],
        ["S5704774", "rebar_price_Fuzhou", "福州螺纹钢均价", "2007-04-24", None, 'HRB400 20mm'],
        ["S5704784", "rebar_price_Taiyuan", "太原螺纹钢均价", "2007-04-24", None, 'HRB400 20mm'],
        ["S5704788", "rebar_price_Chengdu", "成都螺纹钢均价", "2007-04-24", None, 'HRB400 20mm'],
        # 钢坯价格
        ["S0143493", "square_billet_Tangshan", "唐山方坯价格", "2007-01-31", None, '单位：元/吨'],
        ["S0143492", "square_billet_Shandong", "山东方坯价格", "2007-01-31", None, '单位：元/吨'],
        ["S0143491", "square_billet_Jiangsu", "江苏方坯价格", "2007-01-31", None, '单位：元/吨'],
        ["S0143494", "square_billet_Shanxi", "山西方坯价格", "2007-01-31", None, '单位：元/吨'],
        ["S0143504", "square_billet_MnSi_Tangshan", "唐山MnSi方坯价格", "2007-01-31", None, '单位：元/吨,20MnSi'],
        # 螺纹钢库存
        ["S0181750", "rebar_inventory", "螺纹钢库存", "2010-05-21", None, '含上海全部仓库，单位：万吨'],
        ["S0110142", "wire_inventory", "线材库存", "2006-03-17", None, '单位：万吨'],
        ["S0110143", "hot_rolled_coils_inventory", "热卷库存", "2006-03-17", None, '单位：万吨'],
        ["S0110144", "steel_plate_inventory", "中板库存", "2006-03-17", None, '单位：万吨'],
        ["S0110145", "cold_rolled_inventory", "冷轧库存", "2006-03-17", None, '单位：万吨'],
        ["S5708249", "key_steelworks_inventory", "重点钢厂库存", "2009-05-31", None, '单位：万吨'],
        # 上海线螺采购量
        ["S5704503", "line_rebar_purchase", "上海线螺采购量", "2004-10-17", None, '单位：吨'],

        # 货币供应
        ["M0001380", "M0", "M0", "1990-12-31", None, '中国人民银行，单位：亿元'],
        ["M0001381", "M0_yoy", "M0_yoy", "1990-12-31", None, '中国人民银行'],
        ["M0001382", "M1", "M1", "1990-12-31", None, '中国人民银行，单位：亿元'],
        ["M0001383", "M1_yoy", "M1_yoy", "1990-12-31", None, '中国人民银行'],
        ["M0001384", "M2", "M2", "1990-12-31", None, '中国人民银行，单位：亿元'],
        ["M0001385", "M2_yoy", "M2_yoy", "1990-12-31", None, '中国人民银行'],
        ["M0001386", "net_cash_release", "当月现金净投放", "1996-01-31", None, '中国人民银行'],
        ["M0010131", "money_multiplier", "货币乘数", "1997-12-31", None, '中国人民银行'],
        ["M0060451", "M0_yoy_tail_raising_factor", "M0同比翘尾因素", "2003-01-31", None, '中国人民银行'],
        ["M0060452", "M1_yoy_tail_raising_factor", "M1同比翘尾因素", "2003-01-31", None, '中国人民银行'],
        ["M0060453", "M2_yoy_tail_raising_factor", "M2同比翘尾因素", "2003-01-31", None, '中国人民银行'],

        # 外汇市场
        ["M0067855", "us2rmb", "美元兑人民币即期汇率", "1994-01-04", None, '中国货币网'],
        ["M0000185", "us2rmb_mid", "美元兑人民币中间价", "1994-08-31", None, '中国人民银行'],
        ["M0290205", "us2cnh", "美元兑人民币离岸汇率", "2012-04-30", None, '中国货币网'],
        ["M0000271", "usdx", "美元指数", "1971-01-04", None, '倚天财经'],
        ["G0007431", "Brazilian_real_buy", "巴西雷亚尔买入价", "1994-07-01", None, '巴西央行'],
        ["G0007432", "Brazilian_real_sell", "巴西雷亚尔卖出价", "1994-07-01", None, '巴西央行'],
        ["G0007068", "Argentine_peso", "阿根廷比索", "1992-01-01", None, '阿根廷央行'],
        ["M0000204", "Australian_dollar", "澳元兑美元", "1978-10-02", None, '倚天财经'],
        ["G0002335", "Malaysian_ringgit", "美元兑林吉特", "1971-01-04", None, '美联储'],
        ["G0004377", "Indonesia_rupiah_buy", "美元兑印尼卢比", "2001-01-24", None, '印尼央行'],

        # 利率市场_上海银行同业拆借
        ["M1001854", "SHIBOR_N", "SHIBOR_N", "2006-10-08", None, '全国银行间同业拆借中心'],
        ["M1001855", "SHIBOR_1W", "SHIBOR_1W", "2006-10-08", None, '全国银行间同业拆借中心'],
        ["M1001856", "SHIBOR_2W", "SHIBOR_2W", "2006-10-08", None, '全国银行间同业拆借中心'],
        ["M1001857", "SHIBOR_1M", "SHIBOR_1M", "2006-10-08", None, '全国银行间同业拆借中心'],
        ["M1001858", "SHIBOR_3M", "SHIBOR_3M", "2006-10-08", None, '全国银行间同业拆借中心'],
        ["M1001859", "SHIBOR_6M", "SHIBOR_6M", "2006-10-08", None, '全国银行间同业拆借中心'],
        ["M1001860", "SHIBOR_9M", "SHIBOR_9M", "2006-10-08", None, '全国银行间同业拆借中心'],
        ["M1001861", "SHIBOR_1Y", "SHIBOR_1Y", "2006-10-08", None, '全国银行间同业拆借中心'],
        # 票据贴现利率
        ["M0061577", "direct_discount_rate_pearl_river_delta_6m", "珠三角票据直贴利率", "2007-03-16", None, '中国货币网'],
        ["M0061578", "direct_discount_rate_yangze_river_delta_6m", "长三角票据直贴利率", "2007-03-16", None, '中国货币网'],
        ["M0061575", "direct_discount_rate_midwestern_6m", "中西部票据直贴利率", "2007-03-16", None, '中国货币网'],
        ["M0061576", "direct_discount_rate_bohai_rim_6m", "环渤海票据直贴利率", "2007-03-16", None, '中国货币网'],
        ["M0061579", "rediscount_rate_6m", "票据转贴利率", "2007-03-16", None, '中国货币网'],
        # 温州民间借贷利率
        ["M5447740", "Wenzhou_private_finace_index_1m", "温州民间融资综合利率1个月", "2013-01-04", None, '温州金融办'],
        ["M5447741", "Wenzhou_private_finace_index_3m", "温州民间融资综合利率3个月", "2013-01-04", None, '温州金融办'],
        ["M5447742", "Wenzhou_private_finace_index_6m", "温州民间融资综合利率6个月", "2013-01-04", None, '温州金融办'],
        ["M5447743", "Wenzhou_private_finace_index_1y", "温州民间融资综合利率1年", "2013-01-04", None, '温州金融办'],
        # 中企债AAA
        ["M1000368", "china_bond_corporate_bond_yield_1y_AAA", "1年期中债企业债到期收益率", "2006-03-01", None, '债券交易中心'],
        ["M1004552", "china_bond_corporate_bond_yield_3m_AAA", "3月期中债企业债到期收益率", "2006-03-01", None, '债券交易中心'],
        ["M1006941", "china_bond_corporate_bond_yield_1m_AAA", "1月期中债企业债到期收益率", "2006-03-01", None, '债券交易中心'],
        ["M1000370", "china_bond_corporate_bond_yield_3y_AAA", "3年期中债企业债到期收益率", "2006-03-01", None, '债券交易中心'],
        ["M1000372", "china_bond_corporate_bond_yield_5y_AAA", "5年期中债企业债到期收益率", "2006-03-01", None, '债券交易中心'],
        ["M1000376", "china_bond_corporate_bond_yield_10y_AAA", "10年期中债企业债到期收益率", "2006-03-01", None, '债券交易中心'],
        ["M1000373", "china_bond_corporate_bond_yield_6y_AAA", "6年期中债企业债到期收益率", "2006-03-01", None, '债券交易中心'],
        # 中企债AAA-
        ["M1005088", "china_bond_corporate_bond_yield_1y_AAA-", "1年期中债企业债到期收益率_AAA-", "2008-08-21", None, '债券交易中心'],
        ["M1005086", "china_bond_corporate_bond_yield_3m_AAA-", "3月期中债企业债到期收益率_AAA-", "2012-02-21", None, '债券交易中心'],
        ["M1006944", "china_bond_corporate_bond_yield_1m_AAA-", "1月期中债企业债到期收益率_AAA-", "2013-05-03", None, '债券交易中心'],
        ["M1005090", "china_bond_corporate_bond_yield_3y_AAA-", "3年期中债企业债到期收益率_AAA-", "2008-08-21", None, '债券交易中心'],
        ["M1005092", "china_bond_corporate_bond_yield_5y_AAA-", "5年期中债企业债到期收益率_AAA-", "2008-08-21", None, '债券交易中心'],
        ["M1005096", "china_bond_corporate_bond_yield_10y_AAA-", "10年期中债企业债到期收益率_AAA-", "2008-08-21", None, '债券交易中心'],
        ["M1005093", "china_bond_corporate_bond_yield_6y_AAA-", "6年期中债企业债到期收益率_AAA-", "2008-08-21", None, '债券交易中心'],

        # 中企债AA+
        ["S0059843", "china_bond_corporate_bond_yield_1y_AA+", "1年期中债企业债到期收益率_AA+", "2007-10-11", None, '债券交易中心'],
        ["M1004554", "china_bond_corporate_bond_yield_3m_AA+", "3月期中债企业债到期收益率_AA+", "2007-10-11", None, '债券交易中心'],
        ["M1006947", "china_bond_corporate_bond_yield_1m_AA+", "1月期中债企业债到期收益率_AA+", "2007-10-11", None, '债券交易中心'],
        ["S0059845", "china_bond_corporate_bond_yield_3y_AA+", "3年期中债企业债到期收益率_AA+", "2007-10-11", None, '债券交易中心'],
        ["S0059846", "china_bond_corporate_bond_yield_5y_AA+", "5年期中债企业债到期收益率_AA+", "2007-10-11", None, '债券交易中心'],
        ["S0059848", "china_bond_corporate_bond_yield_10y_AA+", "10年期中债企业债到期收益率_AA+", "2007-10-11", None, '债券交易中心'],
        ["M0057983", "china_bond_corporate_bond_yield_6y_AA+", "6年期中债企业债到期收益率_AA+", "2007-10-11", None, '债券交易中心'],
        # 中企债AA
        ["S0059761", "china_bond_corporate_bond_yield_1y_AA", "1年期中债企业债到期收益率_AA", "2007-05-25", None, '债券交易中心'],
        ["M1004556", "china_bond_corporate_bond_yield_3m_AA", "3月期中债企业债到期收益率_AA", "2007-05-25", None, '债券交易中心'],
        ["M1006950", "china_bond_corporate_bond_yield_1m_AA", "1月期中债企业债到期收益率_AA", "2007-05-25", None, '债券交易中心'],
        ["S0059763", "china_bond_corporate_bond_yield_3y_AA", "3年期中债企业债到期收益率_AA", "2007-05-25", None, '债券交易中心'],
        ["S0059764", "china_bond_corporate_bond_yield_5y_AA", "5年期中债企业债到期收益率_AA", "2007-05-25", None, '债券交易中心'],
        ["S0059766", "china_bond_corporate_bond_yield_10y_AA", "10年期中债企业债到期收益率_AA", "2007-05-25", None, '债券交易中心'],
        ["M0057978", "china_bond_corporate_bond_yield_6y_AA", "6年期中债企业债到期收益率_AA", "2007-05-25", None, '债券交易中心'],
        # 中企债AA-
        ["M1005118", "china_bond_corporate_bond_yield_1y_AA-", "1年期中债企业债到期收益率_AA-", "2008-08-21", None, '债券交易中心'],
        ["M1005116", "china_bond_corporate_bond_yield_3m_AA-", "3月期中债企业债到期收益率_AA-", "2012-02-21", None, '债券交易中心'],
        ["M1006953", "china_bond_corporate_bond_yield_1m_AA-", "1月期中债企业债到期收益率_AA-", "2013-05-03", None, '债券交易中心'],
        ["M1005120", "china_bond_corporate_bond_yield_3y_AA-", "3年期中债企业债到期收益率_AA-", "2008-08-21", None, '债券交易中心'],
        ["M1005122", "china_bond_corporate_bond_yield_5y_AA-", "5年期中债企业债到期收益率_AA-", "2008-08-21", None, '债券交易中心'],
        ["M1005123", "china_bond_corporate_bond_yield_10y_AA-", "10年期中债企业债到期收益率_AA-", "2008-08-21", None, '债券交易中心'],
        ["M1005126", "china_bond_corporate_bond_yield_6y_AA-", "6年期中债企业债到期收益率_AA-", "2008-08-21", None, '债券交易中心'],

        # 中企债A+
        ["S0059890", "china_bond_corporate_bond_yield_1y_A+", "1年期中债企业债到期收益率_A+", "2008-01-07", None, '债券交易中心'],
        ["M1004560", "china_bond_corporate_bond_yield_3m_A+", "3月期中债企业债到期收益率_A+", "2008-01-07", None, '债券交易中心'],
        ["M1006956", "china_bond_corporate_bond_yield_1m_A+", "1月期中债企业债到期收益率_A+", "2008-01-07", None, '债券交易中心'],
        ["S0059892", "china_bond_corporate_bond_yield_3y_A+", "3年期中债企业债到期收益率_A+", "2008-01-07", None, '债券交易中心'],
        ["S0059893", "china_bond_corporate_bond_yield_5y_A+", "5年期中债企业债到期收益率_A+", "2008-01-07", None, '债券交易中心'],
        ["S0059895", "china_bond_corporate_bond_yield_10y_A+", "10年期中债企业债到期收益率_A+", "2008-01-07", None, '债券交易中心'],
        ["M0057971", "china_bond_corporate_bond_yield_6y_A+", "6年期中债企业债到期收益率_A+", "2008-01-07", None, '债券交易中心'],
        # 城投债AAA
        ["M0048432", "china_bond_local_government_bond_yield_1y_AAA", "1年期中债城投债到期收益率_AAA", "2008-08-21", None,
         '债券估值中心'],
        ["M0048434", "china_bond_local_government_bond_yield_3y_AAA", "3年期中债城投债到期收益率_AAA", "2008-08-21", None,
         '债券估值中心'],
        ["M0048435", "china_bond_local_government_bond_yield_5y_AAA", "5年期中债城投债到期收益率_AAA", "2008-08-21", None,
         '债券估值中心'],
        ["M0057985", "china_bond_local_government_bond_yield_6y_AAA", "6年期中债城投债到期收益率_AAA", "2008-08-21", None,
         '债券估值中心'],
        ["M0048437", "china_bond_local_government_bond_yield_10y_AAA", "10年期中债城投债到期收益率_AAA", "2008-08-21", None,
         '债券估值中心'],
        ["M0048438", "china_bond_local_government_bond_yield_15y_AAA", "15年期中债城投债到期收益率_AAA", "2008-08-21", None,
         '债券估值中心'],
        ["M0048439", "china_bond_local_government_bond_yield_20y_AAA", "20年期中债城投债到期收益率_AAA", "2008-08-21", None,
         '债券估值中心'],
        ["M1006901", "china_bond_local_government_bond_yield_1m_AAA", "1月期中债城投债到期收益率_AAA", "2013-05-03", None,
         '债券估值中心'],
        ["M1004553", "china_bond_local_government_bond_yield_3m_AAA", "3月年期中债城投债到期收益率_AAA", "2012-02-21", None,
         '债券估值中心'],
        # 城投债AA+
        ["M0048422", "china_bond_local_government_bond_yield_1y_AA+", "1年期中债城投债到期收益率_AA+", "2008-08-19", None,
         '债券估值中心'],
        ["M0048424", "china_bond_local_government_bond_yield_3y_AA+", "3年期中债城投债到期收益率_AA+", "2008-08-19", None,
         '债券估值中心'],
        ["M0048425", "china_bond_local_government_bond_yield_5y_AA+", "5年期中债城投债到期收益率_AA+", "2008-08-19", None,
         '债券估值中心'],
        ["M0057981", "china_bond_local_government_bond_yield_6y_AA+", "6年期中债城投债到期收益率_AA+", "2008-08-19", None,
         '债券估值中心'],
        ["M0048427", "china_bond_local_government_bond_yield_10y_AA+", "10年期中债城投债到期收益率_AA+", "2008-08-19", None,
         '债券估值中心'],
        ["M0048428", "china_bond_local_government_bond_yield_15y_AA+", "15年期中债城投债到期收益率_AA+", "2008-08-19", None,
         '债券估值中心'],
        ["M0048429", "china_bond_local_government_bond_yield_20y_AA+", "20年期中债城投债到期收益率_AA+", "2008-08-19", None,
         '债券估值中心'],
        ["M1006904", "china_bond_local_government_bond_yield_1m_AA+", "1月期中债城投债到期收益率_AA+", "2013-05-03", None,
         '债券估值中心'],
        ["M1004555", "china_bond_local_government_bond_yield_3m_AA+", "3月年期中债城投债到期收益率_AA+", "2012-02-21", None,
         '债券估值中心'],

        # 城投债AA
        ["M0048412", "china_bond_local_government_bond_yield_1y_AA", "1年期中债城投债到期收益率_AA", "2008-08-28", None,
         '债券估值中心'],
        ["M0048414", "china_bond_local_government_bond_yield_3y_AA", "3年期中债城投债到期收益率_AA", "2008-08-28", None,
         '债券估值中心'],
        ["M0048415", "china_bond_local_government_bond_yield_5y_AA", "5年期中债城投债到期收益率_AA", "2008-08-28", None,
         '债券估值中心'],
        ["M0057974", "china_bond_local_government_bond_yield_6y_AA", "6年期中债城投债到期收益率_AA", "2008-08-28", None,
         '债券估值中心'],
        ["M0048417", "china_bond_local_government_bond_yield_10y_AA", "10年期中债城投债到期收益率_AA", "2008-08-28", None,
         '债券估值中心'],
        ["M0048418", "china_bond_local_government_bond_yield_15y_AA", "15年期中债城投债到期收益率_AA", "2008-08-28", None,
         '债券估值中心'],
        ["M0048419", "china_bond_local_government_bond_yield_20y_AA", "20年期中债城投债到期收益率_AA", "2008-08-28", None,
         '债券估值中心'],
        ["M1006907", "china_bond_local_government_bond_yield_1m_AA", "1月期中债城投债到期收益率_AA", "2013-05-03", None,
         '债券估值中心'],
        ["M1004557", "china_bond_local_government_bond_yield_3m_AA", "3月年期中债城投债到期收益率_AA", "2012-02-21", None,
         '债券估值中心'],
        # 香港同业拆借
        ["M0062945", "HIBOR_N", "HIBOR_N", "2006-01-03", None, '香港同业拆借市场'],
        ["M0062946", "HIBOR_1W", "HIBOR_1W", "2006-01-03", None, '香港同业拆借市场'],
        ["M0062947", "HIBOR_2W", "HIBOR_2W", "2006-01-03", None, '香港同业拆借市场'],
        ["M0062948", "HIBOR_1M", "HIBOR_1M", "2002-03-04", None, '香港同业拆借市场'],
        ["M0062949", "HIBOR_2M", "HIBOR_2M", "2002-03-04", None, '香港同业拆借市场'],
        ["M0062950", "HIBOR_3M", "HIBOR_3M", "2002-03-04", None, '香港同业拆借市场'],
        ["M0062953", "HIBOR_6M", "HIBOR_6M", "2002-03-04", None, '香港同业拆借市场'],
        ["M0062959", "HIBOR_12M", "HIBOR_12M", "2002-03-04", None, '香港同业拆借市场'],
        # 伦敦同业

        # 美国国债收益率
        ["G0000886", "yield_on_us_treasury_bonds_1y", "一年期美国国债收益率", "1970-08-20", None, '美联储'],
        ["G0000891", "yield_on_us_treasury_bonds_10y", "十年期美国国债收益率", "1970-08-20", None, '美联储'],
        ["G0000884", "yield_on_us_treasury_bonds_3m", "三月期美国国债收益率", "1982-01-04", None, '美联储'],
        ["G0005428", "yield_on_us_treasury_bonds_tips_3m", "10年期美国国债收益率_通胀指数国债", "1982-01-04", None, '美联储'],
        # 中国国债收益率
        ["M1004964", "yield_on_cn_treasury_bonds_1y", "一年期中国国债收益率", "2011-01-07", None, '中国货币网'],
        ["M1004965", "yield_on_cn_treasury_bonds_3y", "三年期中国国债收益率", "2011-01-07", None, '中国货币网'],
        ["M1004966", "yield_on_cn_treasury_bonds_5y", "五年期中国国债收益率", "2011-01-07", None, '中国货币网'],
        ["M1004967", "yield_on_cn_treasury_bonds_7y", "七年期中国国债收益率", "2011-01-07", None, '中国货币网'],
        ["M1004968", "yield_on_cn_treasury_bonds_10y", "十年期中国国债收益率", "2011-01-07", None, '中国货币网'],
        ["M1004969", "yield_on_cn_treasury_bonds_20y", "二十年期中国国债收益率", "2011-01-07", None, '中国货币网'],
        ["M1004970", "yield_on_cn_treasury_bonds_30y", "三十年期中国国债收益率", "2011-01-07", None, '中国货币网'],

    ]
    dtype = {
        'wind_code': String(20),
        'en_name': String(120),
        'cn_name': String(120),
        'begin_date': Date,
        'end_date': Date,
        'remark': Text,
    }
    name_list = ['wind_code', 'en_name', 'cn_name', 'begin_date', 'end_date', 'remark']
    info_df = pd.DataFrame(data=indicators_dic, columns=name_list)
    data_count = bunch_insert_on_duplicate_update(info_df, table_name, engine_md, dtype)
    logger.info('%d 条记录被更新', data_count)
    if not has_table and engine_md.has_table(table_name):
        alter_table_2_myisam(engine_md, [table_name])
        create_pk_str = """ALTER TABLE {table_name}
            CHANGE COLUMN `wind_code` `wind_code` VARCHAR(20) NOT NULL FIRST,
            ADD PRIMARY KEY (`wind_code`)""".format(table_name=table_name)
        with with_db_session(engine_md) as session:
            session.execute(create_pk_str)
        logger.info('%s 表 `wind_code` 主键设置完成', table_name)


@app.task
def import_edb(chain_param=None, wind_code_set=None):
    """
    通过wind接口获取并导入EDB数据
    :param chain_param:  在celery 中將前面結果做爲參數傳給後面的任務
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
                SELECT `wind_code`, date_frm, if(end_date<end_date2, end_date, end_date2) date_to
                FROM
                (
                SELECT info.`wind_code`, ifnull(trade_date, begin_date) date_frm, end_date,
                if(hour(now())<16, subdate(curdate(),1), curdate()) end_date2
                FROM 
                    wind_commodity_info info 
                LEFT OUTER JOIN
                    (SELECT `wind_code`, adddate(max(trade_date),1) trade_date FROM {table_name} GROUP BY `wind_code`) daily
                ON info.`wind_code` = daily.`wind_code`
                ) tt
                WHERE date_frm <= if(end_date<end_date2, end_date, end_date2) 
                ORDER BY `wind_code`""".format(table_name=table_name)
    else:
        logger.warning('%s 不存在，仅使用 wind_commodity_info 表进行计算日期范围', table_name)
        sql_str = """
                SELECT `wind_code`, date_frm, if(end_date<end_date2, end_date, end_date2) date_to
                FROM
                  (
                    SELECT info.`wind_code`, begin_date date_frm, end_date,
                    if(hour(now())<16, subdate(curdate(),1), curdate()) end_date2
                    FROM wind_commodity_info info 
                  ) tt
                WHERE date_frm <= if(end_date<end_date2, end_date, end_date2) 
                ORDER BY `wind_code`"""

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
    logger.info('%d commodities will been import into wind_commodity_edb', data_len)
    # 将data_df数据，添加到data_df_list
    try:
        for num, (wind_code, (date_from, date_to)) in enumerate(code_date_range_dic.items(), start=1):
            logger.debug('%d/%d) %s [%s - %s]', num, data_len, wind_code, date_from, date_to)
            try:
                data_df = invoker.edb(wind_code, date_from, date_to, options='')
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
            data_df.rename(columns={wind_code.upper(): 'value'}, inplace=True)
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
                    CHANGE COLUMN `wind_code` wind_code` VARCHAR(20) NOT NULL FIRST,
                    CHANGE COLUMN `trade_date` `trade_date` DATE NOT NULL AFTER `wind_code`,
                    ADD PRIMARY KEY (`wind_code`, `trade_date`)""".format(table_name=table_name)
                with with_db_session(engine_md) as session:
                    session.execute(create_pk_str)
                logger.info('%s 表 `wind_code` `trade_date` 主键设置完成', table_name)


if __name__ == "__main__":
    # DEBUG = True
    build_commodity_info()
    # 更新每日股票数据
    import_edb(chain_param=None)
