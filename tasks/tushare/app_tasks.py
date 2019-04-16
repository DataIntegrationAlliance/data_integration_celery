#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 19-4-15 下午2:00
@File    : app_tasks.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
# from tasks.tushare.coin import import_coinbar, import_coin_info, import_coin_pair_info
from ibats_utils.mess import decorator_timer
from tasks.tushare.trade_cal import import_trade_date
from tasks.tushare.tushare_stock_daily.stock import import_tushare_stock_daily, import_tushare_stock_info
from tasks.tushare.tushare_stock_daily.adj_factor import import_tushare_adj_factor
from tasks.tushare.tushare_stock_daily.daily_basic import import_tushare_daily_basic
from tasks.tushare.tushare_stock_daily.ggt_top10 import import_tushare_ggt_top10
from tasks.tushare.tushare_stock_daily.hsgt_top10 import import_tushare_hsgt_top10
from tasks.tushare.tushare_stock_daily.margin import import_tushare_margin
from tasks.tushare.tushare_stock_daily.margin_detail import import_tushare_margin_detail
from tasks.tushare.tushare_stock_daily.moneyflow_hsgt import import_tushare_moneyflow_hsgt
from tasks.tushare.tushare_stock_daily.suspend import import_tushare_suspend
from tasks.tushare.tushare_stock_daily.index_daily import import_tushare_stock_index_daily
from tasks.tushare.tushare_stock_daily.top_list import import_tushare_top_list
from tasks.tushare.tushare_stock_daily.top_list_detail import import_tushare_top_inst
from tasks.tushare.tushare_fina_reports.balancesheet import import_tushare_stock_balancesheet
from tasks.tushare.tushare_fina_reports.cashflow import import_tushare_stock_cashflow
from tasks.tushare.tushare_fina_reports.fina_audit import import_tushare_stock_fina_audit
from tasks.tushare.tushare_fina_reports.fina_indicator import import_tushare_stock_fina_indicator
from tasks.tushare.tushare_fina_reports.fina_mainbz import import_tushare_stock_fina_mainbz
from tasks.tushare.tushare_fina_reports.income import import_tushare_stock_income
# from tasks.tushare.tushare_fina_reports.patch_balancesheet import *
# from tasks.tushare.tushare_fina_reports.patch_cashflow import *
# from tasks.tushare.tushare_fina_reports.patch_fina_indicator import *
from tasks.tushare.tushare_fina_reports.top10_holders import import_tushare_stock_top10_holders
from tasks.tushare.tushare_fina_reports.top10_floatholders import import_tushare_stock_top10_floatholders
from tasks.tushare.tushare_fina_reports.forecast import import_tushare_stock_forecast
from tasks.tushare.tushare_fina_reports.express import import_tushare_stock_express
from tasks.tushare.tushare_fina_reports.dividend import import_tushare_dividend
from tasks.tushare.tushare_stock_daily.index_basic import import_tushare_index_basic
from tasks.tushare.tushare_stock_daily.block_trade import import_tushare_block_trade
import logging

from tasks.utils.to_sqlite import transfer_mysql_to_sqlite

logger = logging.getLogger(__name__)

# 日级别加载的程序
tushare_daily_task = (
        import_tushare_adj_factor.s() |
        import_tushare_daily_basic.s() |
        import_tushare_ggt_top10.s() |
        import_tushare_hsgt_top10.s() |
        import_tushare_margin.s() |
        import_tushare_margin_detail.s() |
        import_tushare_moneyflow_hsgt.s() |
        import_tushare_stock_daily.s() |
        import_tushare_suspend.s() |
        import_tushare_index_basic.s() |
        import_tushare_stock_index_daily.s() |
        import_tushare_top_list.s() |
        import_tushare_top_inst.s() |
        import_tushare_block_trade.s()
    # import_coinbar.s()
)
# 周级别加载的程序
tushare_weekly_task = (
        import_tushare_stock_info.s() |
        import_tushare_stock_balancesheet.s() |
        import_tushare_stock_cashflow.s() |
        import_tushare_stock_fina_audit.s() |
        import_tushare_stock_fina_indicator.s() |
        import_tushare_stock_fina_mainbz.s() |
        import_tushare_stock_income.s() |
        import_tushare_stock_top10_holders.s() |
        import_tushare_stock_top10_floatholders.s() |
        import_tushare_stock_forecast.s() |
        import_tushare_stock_express.s() |
        import_tushare_dividend.s()
    # import_coin_info.s() |
    # import_coin_pair_info.s()
)
# 一次性加载的程序
tushare_import_once = (
        import_tushare_stock_info.s() |
        import_trade_date.s()
)


def run_once_job_local():
    import_trade_date()
    import_tushare_stock_info()


def run_weekly_job_local():
    import_tushare_stock_info()
    import_tushare_stock_balancesheet()
    import_tushare_stock_cashflow()
    import_tushare_stock_fina_audit()
    import_tushare_stock_fina_indicator()
    import_tushare_stock_fina_mainbz()
    import_tushare_stock_income()
    import_tushare_stock_top10_holders()
    import_tushare_stock_top10_floatholders()
    import_tushare_stock_forecast()
    import_tushare_stock_express()
    import_tushare_dividend()


def run_daily_job_local():
    import_tushare_adj_factor()
    import_tushare_daily_basic()
    import_tushare_ggt_top10()
    import_tushare_hsgt_top10()
    import_tushare_margin()
    import_tushare_margin_detail()
    import_tushare_moneyflow_hsgt()
    import_tushare_stock_daily()
    import_tushare_suspend()
    import_tushare_index_basic()
    import_tushare_stock_index_daily()
    import_tushare_top_list()
    import_tushare_top_inst()
    import_tushare_block_trade()


@decorator_timer
def run_job_on_pool():
    """
    采用线程池方式并行执行任务
    :return:
    """
    from tasks.config import config
    from concurrent.futures import ThreadPoolExecutor, as_completed

    tasks = [
        import_tushare_stock_info,
        import_tushare_stock_balancesheet,
        import_tushare_stock_cashflow,
        # import_tushare_stock_fina_audit,
        import_tushare_stock_fina_indicator,
        # import_tushare_stock_fina_mainbz,
        import_tushare_stock_income,
        # import_tushare_stock_top10_holders,
        # import_tushare_stock_top10_floatholders,
        # import_tushare_stock_forecast,
        # import_tushare_stock_express,
        # import_tushare_dividend,
        import_tushare_adj_factor,
        import_tushare_daily_basic,
        # import_tushare_ggt_top10,
        # import_tushare_hsgt_top10,
        # import_tushare_margin,
        # import_tushare_margin_detail,
        # import_tushare_moneyflow_hsgt,
        import_tushare_stock_daily,
        # import_tushare_suspend,
        import_tushare_index_basic,
        import_tushare_stock_index_daily,
        # import_tushare_top_list,
        # import_tushare_top_inst,
        import_tushare_block_trade,
    ]

    with ThreadPoolExecutor(max_workers=config.TUSHARE_THREADPOOL_WORKERS) as executor:
        tasks_result = [executor.submit(task) for task in tasks]

        for num, result in enumerate(as_completed(tasks_result)):
            try:
                result.result()
                logger.info("%s 执行完成", tasks[num].__name__)
            except:
                logger.exception("%s 执行异常", tasks[num].__name__)


if __name__ == "__main__":
    logger.info("本地执行 tushare 任务")
    mysql_to_sqlite = True
    # run_once_job_local()
    # run_weekly_job_local()
    # run_daily_job_local()
    run_job_on_pool()
    if mysql_to_sqlite:
        transfer_mysql_to_sqlite()
