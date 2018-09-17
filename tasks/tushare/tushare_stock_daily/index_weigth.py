df = pro.index_weight(index_code='399300.SZ',trade_date='',start_date='20180903',end_date='20180913')

df=pro.index_daily(ts_code='000124.SH', start_date='20160903',end_date='20180913')

sql_str="""SELECT ts_code,trade_date FROM md_integration.tushare_stock_index_daily_md """
with with_db_session(engine_md) as session:
    # 获取每只股票需要获取日线数据的日期区间
    table = session.execute(sql_str)
    # trddate = list(row[0] for row in table.fetchall())
    # ts_code_list=list(row[1] for row in table.fetchall())
    # 计算每只股票需要获取日线数据的日期区间
    begin_time = None
    ts_code_set=None
    # 获取date_from,date_to，将date_from,date_to做为value值
    code_date_range_dic = {
        ts_code: (trade_date if begin_time is None else min([trade_date, begin_time]))
        for ts_code, trade_date in table.fetchall() if
        ts_code_set is None or ts_code in ts_code_set}

    for stock_num, (ts_code, trade_date) in enumerate(code_date_range_dic.items()):
        print(stock_num, (ts_code, trade_date))