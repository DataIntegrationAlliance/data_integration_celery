#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/8/5 11:09
@File    : __init__.py.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from direstinvoker.iwind import WindRestInvoker

from tasks.config import config

invoker = WindRestInvoker(config.WIND_REST_URL)

# 以下语句不能够提前，将会导致循环引用异常
# from tasks.wind.bonus_import import *
from tasks.wind.commodity import *
from tasks.wind.convertible_bond import *
from tasks.wind.future import *
from tasks.wind.private_fund import *
from tasks.wind.pub_fund import *
from tasks.wind.stock import *
from tasks.wind.stock_hk import *
from tasks.wind.trade_date import *
from tasks.wind.index import *
from tasks.wind.index_constituent import *
from tasks.wind.macroeconomy import *
from tasks.wind.sectorconstituent import *
from tasks.wind.smfund import *

# 日级别加载的程序
wind_daily_task = (
        import_edb.s() |
        import_future_daily.s() |
        # import_index_daily.s() |
        # import_index_constituent_all.s() |
        import_macroeconom_edb.s() |
        # import_private_fund_nav_daily.s() |
        # import_pub_fund_daily.s() |
        # import_sectorconstituent_all.s() |
        # import_smfund_daily.s() |
        import_stock_daily.s()
    # import_stock_daily_hk.s() |
    # import_stock_quertarly_hk.s() |
    # import_stock_daily_hk.s()
)
# 周级别加载的程序
wind_weekly_task = (
    # import_cb_info.s() |
        import_future_info.s() |
        # import_private_fund_info.s()
        # import_pub_fund_info.s() |
        # import_smfund_info.s() |
        import_wind_stock_info.s()
    # import_stock_info_hk.s()
)
# 一次性加载的程序
wind_codes = ['HSCEI.HI', 'HSI.HI', 'HSML25.HI', 'HSPI.HI', '000001.SH', '000016.SH',
              '000300.SH', '000905.SH', '037.CS', '399001.SZ', '399005.SZ', '399006.SZ',
              '399101.SZ', '399102.SZ',
              ]
wind_import_once = (
        import_macroeconomy_info.s() |
        import_index_info.s(wind_codes)
)
# if __name__ == "__main__":
#     # 仅供接口测试使用
#     # df = invoker.wset("sectorconstituent", "date=2018-01-04;sectorid=a001010100000000")
#     # print(df)

ERROR_CODE_MSG_DIC = {
    -40520008: "Start登陆超时，请重新登陆",
    -40520007: "没有可用数据",
    -40521009: "数据解码失败。检查输入参数是否正确，如：日期参数注意大小月月末及短二月",
    -40520004: "登陆失败，请确保Wind终端和编程程序以相同的windows权限启动",
}
