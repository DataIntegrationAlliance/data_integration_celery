# -*- coding: utf-8 -*-
"""
Created on Fri Feb 17 10:17:38 2017

@author: Administrator
"""
import pandas as pd
import logging
# from logging.handlers import RotatingFileHandler
from tasks import app
from tasks.backend.orm import build_primary_key
from tasks.merge.code_mapping import update_from_info_table
from tasks.backend import engine_md
from tasks.utils.db_utils import alter_table_2_myisam
from tasks.utils.db_utils import bunch_insert_on_duplicate_update
from tasks.wind import invoker
from sqlalchemy.dialects.mysql import DOUBLE
from direstinvoker.ifind import APIError
from sqlalchemy.types import String, Date
from datetime import datetime, date, timedelta
from tasks.utils.db_utils import with_db_session
from tasks.utils.fh_utils import STR_FORMAT_DATE, date_2_str, str_2_date

DEBUG = False
logger = logging.getLogger()


@app.task
def wind_fund_info_import(table_name, get_df=False):
    # 初始化服务器接口，用于下载万得数据
    # table_name = 'fund_info'
    has_table = engine_md.has_table(table_name)
    types = {u'股票多头策略': 1000023122000000,
             u'股票多空策略': 1000023123000000,
             u'其他股票策略': 1000023124000000,
             u'阿尔法策略': 1000023125000000,
             u'其他市场中性策略': 1000023126000000,
             u'事件驱动策略': 1000023113000000,
             u'债券策略': 1000023114000000,
             u'套利策略': 1000023115000000,
             u'宏观策略': 1000023116000000,
             u'管理期货': 1000023117000000,
             u'组合基金策略': 1000023118000000,
             u'货币市场策略': 1000023119000000,
             u'多策略': 100002312000000,
             u'其他策略': 1000023121000000}
    df = pd.DataFrame()
    today = date.today().strftime('%Y-%m-%d')
    param_list = [
        ('FUND_SETUPDATE', Date),
        ('FUND_MATURITYDATE', Date),
        ('FUND_MGRCOMP', String(800)),
        ('FUND_EXISTINGYEAR', String(500)),
        ('FUND_PTMYEAR', String(30)),
        ('FUND_TYPE', String(20)),
        ('FUND_FUNDMANAGER', String(500))
    ]
    col_name_dic = {col_name.upper(): col_name.lower() for col_name, _ in param_list}
    # 获取列表名
    col_name_list = [col_name.lower() for col_name in col_name_dic.keys()]
    param_str = ",".join(col_name_list)
    # 设置dtype类型
    dtype = {key.lower(): val for key, val in param_list}
    dtype['wind_code'] = String(20)
    dtype['sec_name'] = String(200)
    dtype['strategy_type'] = String(200)
    dtype['trade_date_latest'] = String(200)
    for i in types.keys():
        temp = invoker.wset("sectorconstituent", "date=%s;sectorid=%s" % (today, str(types[i])))
        temp['strategy_type'] = i
        df = pd.concat([df, temp], axis=0)
        if DEBUG and len(df) > 1000:
            break
    # 插入数据库
    # 初始化数据库engine
    # 整理数据
    fund_types_df = df[['wind_code', 'sec_name', 'strategy_type']]
    fund_types_df.set_index('wind_code', inplace=True)
    # 获取基金基本面信息
    code_list = list(fund_types_df.index)  # df['wind_code']
    code_count = len(code_list)
    seg_count = 5000
    info_df = pd.DataFrame()
    for n in range(int(code_count / seg_count) + 1):
        num_start = n * seg_count
        num_end = (n + 1) * seg_count
        num_end = num_end if num_end <= code_count else code_count
        if num_start <= code_count:
            codes = ','.join(code_list[num_start:num_end])
            # 分段获取基金成立日期数据
            info2_df = invoker.wss(codes, param_str)
            logging.info('%05d ) [%d %d]' % (n, num_start, num_end))
            info_df = info_df.append(info2_df)
            if DEBUG and len(info_df) > 1000:
                break
        else:
            break
            # 整理数据插入数据库)
    info_df['FUND_SETUPDATE'] = info_df['FUND_SETUPDATE'].apply(lambda x: str_2_date(x))
    info_df['FUND_MATURITYDATE'] = info_df['FUND_MATURITYDATE'].apply(lambda x: str_2_date(x))
    info_df = fund_types_df.join(info_df, how='right')
    info_df.rename(columns=col_name_dic, inplace=True)
    info_df['trade_date_latest'] = None
    info_df.index.names = ['wind_code']
    info_df.reset_index(inplace=True)
    info_df.drop_duplicates(inplace=True)
    bunch_insert_on_duplicate_update(info_df, table_name, engine_md, dtype=dtype)
    logging.info('%d funds inserted' % len(info_df))
    if not has_table and engine_md.has_table(table_name):
        alter_table_2_myisam(engine_md, [table_name])
        build_primary_key([table_name])

    # 更新 code_mapping 表
    update_from_info_table(table_name)
    if get_df:
        return info_df


def fund_nav_df_2_sql(table_name, fund_nav_df, engine_md, is_append=True):
    col_name_param_list = [
        ('NAV_DATE', Date),
        ('NAV', DOUBLE),
        ('NAV_ACC', DOUBLE),

    ]
    col_name_dic = {col_name.upper(): col_name.lower() for col_name, _ in col_name_param_list}
    dtype = {col_name.lower(): val for col_name, val in col_name_param_list}
    dtype['wind_code'] = String(200)
    dtype['trade_date'] = Date
    #    print('reorg dfnav data[%d, %d]' % fund_nav_df.shape)
    try:
        fund_nav_df['NAV_DATE'] = pd.to_datetime(fund_nav_df['NAV_DATE']).apply(lambda x: x.date())
    except Exception as exp:
        logger.exception(str(fund_nav_df['NAV_DATE']))
        return None
    trade_date_s = pd.to_datetime(fund_nav_df.index)
    trade_date_latest = trade_date_s.max().date()
    fund_nav_df['trade_date'] = trade_date_s
    fund_nav_df.rename(columns=col_name_dic, inplace=True)
    # fund_nav_df['trade_date'] = trade_date_s
    fund_nav_df.set_index(['wind_code', 'trade_date'], inplace=True)
    fund_nav_df.reset_index(inplace=True)
    # action_str = 'append' if is_append else 'replace'
    #    print('df--> sql fundnav table if_exists="%s"' % action_str)
    bunch_insert_on_duplicate_update(fund_nav_df, table_name, engine_md, dtype=dtype)

    # fund_nav_df.to_sql(table_name, engine_md, if_exists=action_str, index_label=['wind_code', 'trade_date'],
    #                    dtype={
    #                        'wind_code': String(200),
    #                        'nav_date': Date,
    #                        'trade_date': Date,
    #                    })  # , index=False
    logger.info('%d data inserted', fund_nav_df.shape[0])
    return trade_date_latest


def update_trade_date_latest(wind_code_trade_date_latest):
    """
    设置 fund_info 表 trade_date_latest 字段为最近的交易日
    :param wind_code_trade_date_latest:
    :return:
    """
    logger.info("开始设置 fund_info 表 trade_date_latest 字段为最近的交易日")
    if len(wind_code_trade_date_latest) > 0:
        params = [{'wind_code': wind_code, 'trade_date_latest': trade_date_latest}
                  for wind_code, trade_date_latest in wind_code_trade_date_latest.items()]
        with with_db_session(engine_md) as session:
            session.execute(
                'update fund_info set trade_date_latest = :trade_date_latest where wind_code = :wind_code',
                params)
        logger.info('%d 条基金信息记录被更新', len(wind_code_trade_date_latest))


@app.task
def update_wind_fund_nav(get_df=False, wind_code_list=None):
    table_name = 'wind_fund_nav'
    # 初始化数据下载端口
    # 初始化数据库engine
    # 链接数据库，并获取fundnav旧表
    # with get_db_session(engine) as session:
    #     table = session.execute('select wind_code, ADDDATE(max(trade_date),1) from wind_fund_nav group by wind_code')
    #     fund_trade_date_begin_dic = dict(table.fetchall())
    # 获取wind_fund_info表信息
    has_table = engine_md.has_table(table_name)
    if has_table:
        fund_info_df = pd.read_sql_query(
            """SELECT DISTINCT fi.wind_code as wind_code, 
               IFNULL(trade_date_from, if(trade_date_latest BETWEEN '1900-01-01' and ADDDATE(CURDATE(), -1), ADDDATE(trade_date_latest,1) , fund_setupdate) ) date_from,
               if(fund_maturitydate BETWEEN '1900-01-01' and ADDDATE(CURDATE(), -1),fund_maturitydate,ADDDATE(CURDATE(), -1)) date_to 
               from fund_info fi
               LEFT JOIN
               (
               select wind_code, ADDDATE(max(trade_date),1) trade_date_from from wind_fund_nav
               GROUP BY wind_code
               ) wfn
               on fi.wind_code = wfn.wind_code""", engine_md)
    else:

        logger.warning('wind_fund_nav 不存在，仅使用 fund_info 表进行计算日期范围')
        fund_info_df = pd.read_sql_query("""SELECT DISTINCT fi.wind_code as wind_code, 
                     fund_setupdate date_from,
                    if(fund_maturitydate BETWEEN '1900-01-01' and ADDDATE(CURDATE(), -1),fund_maturitydate,ADDDATE(CURDATE(), -1)) date_to 
                    from fund_info fi order by wind_code""", engine_md)
    wind_code_date_frm_to_dic = {wind_code: (str_2_date(date_from), str_2_date(date_to)) for
                                 wind_code, date_from, date_to in
                                 zip(fund_info_df['wind_code'], fund_info_df['date_from'], fund_info_df['date_to'])}
    fund_info_df.set_index('wind_code', inplace=True)
    if wind_code_list is None:
        wind_code_list = list(fund_info_df.index)
    else:
        wind_code_list = list(set(wind_code_list) & set(fund_info_df.index))
    # 结束时间
    date_last_day = date.today() - timedelta(days=1)
    # date_end_str = date_end.strftime(STR_FORMAT_DATE)

    fund_nav_all_df = []
    no_data_count = 0
    code_count = len(wind_code_list)
    # 对每个新获取的基金名称进行判断，若存在 fundnav 中，则只获取部分净值
    wind_code_trade_date_latest_dic = {}
    date_gap = timedelta(days=10)
    try:
        for num, wind_code in enumerate(wind_code_list):
            date_begin, date_end = wind_code_date_frm_to_dic[wind_code]

            # if date_end > date_last_day:
            #     date_end = date_last_day
            if date_begin > date_end:
                continue
            # 设定数据获取的起始日期
            # wind_code_trade_date_latest_dic[wind_code] = date_to
            # if wind_code in fund_trade_date_begin_dic:
            #     trade_latest = fund_trade_date_begin_dic[wind_code]
            #     if trade_latest > date_end:
            #         continue
            #     date_begin = max([date_begin, trade_latest])
            # if date_begin is None:
            #     continue
            # elif isinstance(date_begin, str):
            #     date_begin = datetime.strptime(date_begin, STR_FORMAT_DATE).date()

            if isinstance(date_begin, date):
                if date_begin.year < 1900:
                    continue
                if date_begin > date_end:
                    continue
                date_begin_str = date_begin.strftime('%Y-%m-%d')
            else:
                logger.error("%s date_begin:%s", wind_code, date_begin)
                continue

            if isinstance(date_end, date):
                if date_begin.year < 1900:
                    continue
                if date_begin > date_end:
                    continue
                date_end_str = date_end.strftime('%Y-%m-%d')
            else:
                logger.error("%s date_end:%s", wind_code, date_end)
                continue
            # 尝试获取 fund_nav 数据
            for k in range(2):
                try:
                    fund_nav_tmp_df = invoker.wsd(codes=wind_code, fields='nav,NAV_acc,NAV_date',
                                                  beginTime=date_2_str(date_begin_str),
                                                  endTime=date_2_str(date_end_str), options='Fill=Previous')
                    trade_date_latest = datetime.strptime(date_end_str, '%Y-%m-%d').date() - date_gap
                    wind_code_trade_date_latest_dic[wind_code] = trade_date_latest
                    break
                except APIError as exp:
                    # -40520007z
                    if exp.ret_dic.setdefault('error_code', 0) == -40520007:
                        trade_date_latest = datetime.strptime(date_end_str, '%Y-%m-%d').date() - date_gap
                        wind_code_trade_date_latest_dic[wind_code] = trade_date_latest
                    logger.error("%s Failed, ErrorMsg: %s" % (wind_code, str(exp)))
                    continue
                except Exception as exp:
                    logger.error("%s Failed, ErrorMsg: %s" % (wind_code, str(exp)))
                    continue
            else:
                fund_nav_tmp_df = None

            if fund_nav_tmp_df is None:
                logger.info('%s No data', wind_code)
                # del wind_code_trade_date_latest_dic[wind_code]
                no_data_count += 1
                logger.warning('%d funds no data', no_data_count)
            else:
                fund_nav_tmp_df.dropna(how='all', inplace=True)
                df_len = fund_nav_tmp_df.shape[0]
                if df_len == 0:
                    continue
                fund_nav_tmp_df['wind_code'] = wind_code
                # 此处删除 trade_date_latest 之后再加上，主要是为了避免因抛出异常而导致的该条数据也被记录更新
                # del wind_code_trade_date_latest_dic[wind_code]
                trade_date_latest = fund_nav_df_2_sql(table_name, fund_nav_tmp_df, engine_md, is_append=True)
                if trade_date_latest is None:
                    logger.error('%s[%d] data insert failed', wind_code)
                else:
                    wind_code_trade_date_latest_dic[wind_code] = trade_date_latest
                    logger.info('%d) %s updated, %d funds left', num, wind_code, code_count - num)
                    if get_df:
                        fund_nav_all_df = fund_nav_all_df.append(fund_nav_tmp_df)
            if DEBUG and num > 4:  # 调试使用
                break
    finally:
        # import_wind_fund_nav_to_fund_nav()
        # # update_trade_date_latest(wind_code_trade_date_latest_dic)
        # try:
        #     # update_fund_mgrcomp_info()
        # except:
        #     # 新功能上线前由于数据库表不存在，可能导致更新失败，属于正常现象
        logger.exception('新功能上线前由于数据库表不存在，可能导致更新失败，属于正常现象')
        if not has_table and engine_md.has_table(table_name):
            alter_table_2_myisam(engine_md, [table_name])
            build_primary_key([table_name])
    return fund_nav_all_df


def import_wind_fund_nav_to_nav():
    """
    将 wind_fund_nav 数据导入到 fund_nav 表中
    :return:
    """
    table_name = 'fund_nav'
    has_table = engine_md.has_table(table_name)
    logger.info("开始将 wind_fund_nav_daily 数据导入到 fund_nav_tmp_df")

    create_sql_str = """CREATE TABLE {table_name} (
          `wind_code` varchar(20) NOT NULL COMMENT '基金代码',
          `nav_date` date NOT NULL COMMENT '净值日期',
          `nav` double DEFAULT NULL COMMENT '净值',
          `nav_acc` double DEFAULT NULL COMMENT '累计净值',
          PRIMARY KEY (`wind_code`,`nav_date`)
        ) ENGINE=MyISAM DEFAULT CHARSET=utf8""".format(table_name=table_name)

    # TODO: 需要对 Group by 子句进行跳转
    sql_str = """insert into fund_nav(wind_code, nav_date, nav, nav_acc)
    select wfn.wind_code, wfn.nav_date, wfn.nav, wfn.nav_acc
    from
    (
        select wind_code, nav_date, max(nav) nav, max(nav_acc) nav_acc
        from wind_fund_nav_daily
        group by wind_code, nav_date
    ) as wfn
    left outer join
        fund_nav fn
    on 
        wfn.wind_code = fn.wind_code and 
        wfn.nav_date = fn.nav_date
    where fn.nav_date is null"""
    with with_db_session(engine_md) as session:
        if not has_table:
            session.execute(create_sql_str)
            logger.info("创建 %s 表", table_name)

        session.execute(sql_str)
        logger.info('导入结束')
    # 更新 name_date_rr，每次执行更新前删除近1个月的结果重新计算
    #     update_name_date_rr()


@app.task
def import_wind_fund_nav_daily(wind_code_list=None):
    table_name = 'wind_fund_nav_daily'
    # 初始化数据下载端口
    # 初始化数据库engine
    # 链接数据库，并获取fundnav旧表
    # with get_db_session(engine) as session:
    #     table = session.execute('select wind_code, ADDDATE(max(trade_date),1) from wind_fund_nav group by wind_code')
    #     fund_trade_date_begin_dic = dict(table.fetchall())
    # 获取wind_fund_info表信息
    col_name_param_list = [
        ('trade_date', Date),
        ('nav', DOUBLE),
        ('nav_acc', DOUBLE),
        ('nav_date', Date),
    ]
    dtype = {col_name: val for col_name, val in col_name_param_list}
    dtype['wind_code'] = String(200)
    has_table = engine_md.has_table(table_name)
    if has_table:
        fund_info_df = pd.read_sql_query(
            """SELECT DISTINCT fi.wind_code as wind_code, 
                IFNULL(fund_setupdate, if(trade_date_latest BETWEEN '1900-01-01' and ADDDATE(CURDATE(), -1), ADDDATE(trade_date_latest,1) , fund_setupdate) ) date_from,
                if(fund_maturitydate BETWEEN '1900-01-01' and ADDDATE(CURDATE(), -1),fund_maturitydate,ADDDATE(CURDATE(), -1)) date_to 
                from fund_info fi
                LEFT JOIN
                (
                select wind_code, ADDDATE(max(trade_date),1) trade_date_from from wind_fund_nav_daily
                GROUP BY wind_code
                ) wfn
                on fi.wind_code = wfn.wind_code""",
            engine_md)
    else:
        fund_info_df = pd.read_sql_query(
            """SELECT DISTINCT fi.wind_code as wind_code, 
                IFNULL(fund_setupdate, if(trade_date_latest BETWEEN '1900-01-01' and ADDDATE(CURDATE(), -1), ADDDATE(trade_date_latest,1) , fund_setupdate) ) date_from,
                if(fund_maturitydate BETWEEN '1900-01-01' and ADDDATE(CURDATE(), -1),fund_maturitydate,ADDDATE(CURDATE(), -1)) date_to 
                from fund_info fi
                ORDER BY wind_code;""",
            engine_md)

        wind_code_date_frm_to_dic = {wind_code: (str_2_date(date_from), str_2_date(date_to)) for
                                     wind_code, date_from, date_to in
                                     zip(fund_info_df['wind_code'], fund_info_df['date_from'], fund_info_df['date_to'])}
        fund_info_df.set_index('wind_code', inplace=True)
        if wind_code_list is None:
            wind_code_list = list(fund_info_df.index)
        else:
            wind_code_list = list(set(wind_code_list) & set(fund_info_df.index))
        # 结束时间
        date_last_day = date.today() - timedelta(days=1)
        # date_end_str = date_end.strftime(STR_FORMAT_DATE)

        fund_nav_all_df = []
        no_data_count = 0
        code_count = len(wind_code_list)
        # 对每个新获取的基金名称进行判断，若存在 fundnav 中，则只获取部分净值
        wind_code_trade_date_latest_dic = {}
        date_gap = timedelta(days=10)
        try:
            for num, wind_code in enumerate(wind_code_list):
                date_begin, date_end = wind_code_date_frm_to_dic[wind_code]

                # if date_end > date_last_day:
                #     date_end = date_last_day
                if date_begin > date_end:
                    continue
                # 设定数据获取的起始日期
                # wind_code_trade_date_latest_dic[wind_code] = date_to
                # if wind_code in fund_trade_date_begin_dic:
                #     trade_latest = fund_trade_date_begin_dic[wind_code]
                #     if trade_latest > date_end:
                #         continue
                #     date_begin = max([date_begin, trade_latest])
                # if date_begin is None:
                #     continue
                # elif isinstance(date_begin, str):
                #     date_begin = datetime.strptime(date_begin, STR_FORMAT_DATE).date()

                if isinstance(date_begin, date):
                    if date_begin.year < 1900:
                        continue
                    if date_begin > date_end:
                        continue
                    date_begin_str = date_begin.strftime('%Y-%m-%d')
                else:
                    logger.error("%s date_begin:%s", wind_code, date_begin)
                    continue

                if isinstance(date_end, date):
                    if date_begin.year < 1900:
                        continue
                    if date_begin > date_end:
                        continue
                    date_end_str = date_end.strftime('%Y-%m-%d')
                else:
                    logger.error("%s date_end:%s", wind_code, date_end)
                    continue
                # 尝试获取 fund_nav 数据
                for k in range(2):
                    try:
                        fund_nav_tmp_df = invoker.wsd(codes=wind_code, fields='nav,NAV_acc,NAV_date',
                                                      beginTime=date_begin_str,
                                                      endTime=date_end_str, options='Fill=Previous')
                        trade_date_latest = datetime.strptime(date_end_str, '%Y-%m-%d').date() - date_gap
                        wind_code_trade_date_latest_dic[wind_code] = trade_date_latest
                        break
                    except APIError as exp:
                        # -40520007z
                        if exp.ret_dic.setdefault('error_code', 0) == -40520007:
                            trade_date_latest = datetime.strptime(date_end_str, '%Y-%m-%d').date() - date_gap
                            wind_code_trade_date_latest_dic[wind_code] = trade_date_latest
                        logger.error("%s Failed, ErrorMsg: %s" % (wind_code, str(exp)))
                        continue
                    except Exception as exp:
                        logger.error("%s Failed, ErrorMsg: %s" % (wind_code, str(exp)))
                        continue
                else:
                    fund_nav_tmp_df = None

                if fund_nav_tmp_df is None:
                    logger.info('%s No data', wind_code)
                    # del wind_code_trade_date_latest_dic[wind_code]
                    no_data_count += 1
                    logger.warning('%d funds no data', no_data_count)
                else:
                    fund_nav_tmp_df.dropna(how='all', inplace=True)
                    df_len = fund_nav_tmp_df.shape[0]
                    if df_len == 0:
                        continue
                    fund_nav_tmp_df['wind_code'] = wind_code
                    # 此处删除 trade_date_latest 之后再加上，主要是为了避免因抛出异常而导致的该条数据也被记录更新
                    # del wind_code_trade_date_latest_dic[wind_code]
                    trade_date_latest = fund_nav_df_2_sql(table_name, fund_nav_tmp_df, engine_md, is_append=True)
                    if trade_date_latest is None:
                        logger.error('%s[%d] data insert failed', wind_code)
                    else:
                        wind_code_trade_date_latest_dic[wind_code] = trade_date_latest
                        logger.info('%d) %s updated, %d funds left', num, wind_code, code_count - num)

                if DEBUG and num > 1:  # 调试使用
                    break
            #
        finally:
            import_wind_fund_nav_to_nav()
            update_trade_date_latest(wind_code_trade_date_latest_dic)
            if not has_table and engine_md.has_table(table_name):
                alter_table_2_myisam(engine_md, [table_name])
                build_primary_key([table_name])
        return fund_nav_all_df


def clean_fund_nav(date_str):
    """
    wind数据库中存在部分数据净值记录前后不一致的问题
    比如：某日记录净值 104，次一后期净值变为 1.04 导致净值收益率走势出现偏差
    此脚本主要目的在于对这种偏差进行修正
    :param date_str:
    :return:
    """
    sql_str = """select fn_before.wind_code, fn_before.nav_date nav_date_before, fn_after.nav_date nav_date_after, fn_before.nav_acc nav_acc_before, fn_after.nav_acc nav_acc_after, fn_after.nav_acc / fn_before.nav_acc nav_acc_pct
from
fund_nav fn_before,
fund_nav fn_after,
(
select wind_code, max(if(nav_date<%s, nav_date, null)) nav_date_before, min(if(nav_date>=%s, nav_date, null)) nav_date_after
from fund_nav group by wind_code
having nav_date_before is not null and nav_date_after is not null
) fn_date
where fn_before.nav_date = fn_date.nav_date_before and fn_before.wind_code = fn_date.wind_code
and fn_after.nav_date = fn_date.nav_date_after and fn_after.wind_code = fn_date.wind_code
and fn_after.nav_acc / fn_before.nav_acc < 0.5
    """
    data_df = pd.read_sql(sql_str, engine_md, params=[date_str, date_str])
    data_count = data_df.shape[0]
    if data_count == 0:
        logger.info('no data for clean on %s', date_str)
        return
    logger.info('\n%s', data_df)
    data_list = data_df.to_dict(orient='records')
    with with_db_session(engine_md) as session:
        for content in data_list:
            wind_code = content['wind_code']
            nav_date_before = content['nav_date_before']
            logger.info('update wind_code=%s nav_date<=%s', wind_code, nav_date_before)
            sql_str = "update fund_nav set nav = nav/100, nav_acc = nav_acc/100 where wind_code = :wind_code and nav_date <= :nav_date"
            session.execute(sql_str, params={'wind_code': wind_code, 'nav_date': nav_date_before})


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s [%(name)s:%(funcName)s] %(message)s')
    DEBUG = True
    # fund_info 表 及 fund_nav表更新完成后，更新及插入 fund_mgrcomp_info 表相关统计信息
    # update_fund_mgrcomp_info()
    table_name = "fund_info"
    # 调用wind接口更新基金净值
    # update_wind_fund_nav(get_df=False)  # , wind_code_list=['XT1513361.XT']
    wind_fund_info_import(table_name, get_df=False)
    import_wind_fund_nav_to_nav()
    wind_code_list = ['XT1513361.XT']
    update_wind_fund_nav()  # , wind_code_list=['XT1513361.XT']
