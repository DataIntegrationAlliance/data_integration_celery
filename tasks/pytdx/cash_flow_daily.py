"""
Created on 2018/9/3
@author: yby
@desc    : 2018-09-3
contact author:ybychem@gmail.com
"""
import pandas as pd
from tasks.backend import engine_md


def get_cash_flow_daily(trade_date, ts_code):
    sql_str = """SELECT * FROM md_integration.pytdx_stock_tick where date=%s and ts_code=%s"""
    # sql_str="""SELECT * FROM md_integration.pytdx_stock_tick where date={trade_date} and ts_code={ts_code}""".format(trade_date="""'20181010'""",ts_code="""'000002.SZ'""")
    df = pd.read_sql(sql_str, engine_md, params=[trade_date, ts_code])
    trade_date = df.date[0]
    net_buy_vol = df[df.vol > 500][df[df.vol > 500].buyorsell == 0].vol.sum() - df[df.vol > 500][
        df[df.vol > 500].buyorsell == 1].vol.sum()
    net_buy_account = (df[df.vol > 500][df[df.vol > 500].buyorsell == 0].vol * 100 * df[df.vol > 500][
        df[df.vol > 500].buyorsell == 0].price).sum() - (
                                  df[df.vol > 500][df[df.vol > 500].buyorsell == 1].vol * 100 * df[df.vol > 500][
                              df[df.vol > 500].buyorsell == 1].price).sum()
    data_df = pd.DataFrame([df.ts_code[0], trade_date, net_buy_vol, net_buy_account],
                           index=['ts_code', 'trade_date', 'net_buy_vol', 'net_buy_account']).T
    return data_df


get_cash_flow_daily(trade_date='20181010', ts_code='000002.SZ')
