"""
@author  : MG
@Time    : 2021/1/27 10:26
@File    : future_tasks.py
@contact : mmmaaaggg@163.com
@desc    : 用于
"""
import logging

from ibats_utils.db import with_db_session

from tasks.backend import engine_md
from tasks.ifind import import_future_min, import_future_info, import_future_daily_his
from tasks.ifind.future.to_model_server import daily_2_model_server
from tasks.ifind.future.to_vnpy import min_to_vnpy_increment

logger = logging.getLogger()


def get_main_secondary_contract_by_instrument_types(instrument_types=None):
    if instrument_types is None or len(instrument_types) == 0:
        sql_str = """SELECT t.instrument_type, t.instrument_id_main, t.instrument_id_secondary 
            FROM ifind_future_adj_factor t
            inner join 
            (
                SELECT instrument_type, max(trade_date) trade_date_max 
                FROM ifind_future_adj_factor group by instrument_type
            ) latest
            on t.instrument_type = latest.instrument_type
            and t.trade_date = latest.trade_date_max"""
    else:
        sql_str = f"""SELECT t.instrument_type, t.instrument_id_main, t.instrument_id_secondary 
            FROM ifind_future_adj_factor t
            inner join 
            (
                SELECT instrument_type, max(trade_date) trade_date_max 
                FROM ifind_future_adj_factor 
                where instrument_type in ('{"','".join(instrument_types)}')
                group by instrument_type
            ) latest
            on t.instrument_type = latest.instrument_type
            and t.trade_date = latest.trade_date_max"""

    with with_db_session(engine_md) as session:
        table = session.execute(sql_str)
        code_list = []
        for instrument_type, main, secondary in table.fetchall():
            if main is not None:
                code_list.append(main)
            if secondary is not None:
                code_list.append(secondary)

        return code_list


def run_daily_min_task():
    """完整的日线、分钟线数据更新任务"""
    from tasks.wind.future_reorg.reversion_rights_factor import task_save_adj_factor
    # DEBUG = True
    wind_code_set = None
    # import_future_info_hk(chain_param=None)
    # update_future_info_hk(chain_param=None)
    import_future_info(chain_param=None)
    # 导入期货每日行情数据
    import_future_daily_his(None, wind_code_set)
    try:
        # 同步到 阿里云 RDS 服务器
        daily_2_model_server()
    except:
        logger.exception("同步到 阿里云 RDS 服务器失败")
    # 根据商品类型将对应日线数据插入到 vnpy dbbardata 表中
    # _run_daily_to_vnpy()
    # 重新计算复权数据
    task_save_adj_factor()
    # 导入期货分钟级行情数据
    import_future_min(None, wind_code_set, recent_n_years=1)
    min_to_vnpy_increment(None)


def run_daily_only():
    """仅完成日级别数据更新任务"""
    wind_code_set = None
    # 导入期货每日行情数据
    import_future_daily_his(None, wind_code_set)
    try:
        # 同步到 阿里云 RDS 服务器
        daily_2_model_server()
    except:
        logger.exception("同步到 阿里云 RDS 服务器失败")
    # 根据商品类型将对应日线数据插入到 vnpy dbbardata 表中
    # _run_daily_to_vnpy()


def run_daily_to_model_server_db():
    """同步日线数据到 model server"""
    daily_2_model_server(instrument_types=['rb', 'hc', 'i', 'j', 'jm'])


def run_mid_day_task():
    """
    中午数据更新任务
    时间紧迫，仅更新指定品种分钟级数据
    :return:
    """
    wind_code_set = None
    # wind_code_set = (
    #     'RB2105.SHF', 'RB2110.SHF',
    #     'HC2105.SHF', 'HC2110.SHF',
    # )
    instrument_types = ['rb', 'hc', 'i', 'j', 'jm', 'jd', 'ap', 'a', 'p', 'm', 'y', 'b', 'jd', 'ru', 'bu', 'ma', 'cu',
                        'fg', 'cj', 'oi', 'rm', 'rs', 'sr', 'cf']
    wind_code_set = get_main_secondary_contract_by_instrument_types(instrument_types=instrument_types)
    import_future_min(wind_code_set=wind_code_set, )
    min_to_vnpy_increment(
        # instrument_types=['rb', 'i', 'hc']
    )


if __name__ == "__main__":
    run_mid_day_task()
