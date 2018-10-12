"""
Created on 2018/9/3
@author: yby
@desc    : 2018-09-3
contact author:ybychem@gmail.com
"""
import pandas as pd
from tasks.backend import engine_md


def get_cash_flow_daily(trade_date, ts_code,method='amount',threshold=1000000):
    sql_str = """SELECT * FROM md_integration.pytdx_stock_tick where date=%s and ts_code=%s"""
    df = pd.read_sql(sql_str, engine_md, params=[trade_date, ts_code])
    total_amount=(df.vol*df.price).sum()
    total_buy=((df[df.buyorsell == 0].price * df[df.buyorsell == 0].vol * 100).sum() -
     (df[df.buyorsell == 1].price * df[df.buyorsell == 1].vol * 100).sum()) / 10000
    if method=='amount':
        big_trade=df[df.vol*df.price*100>=threshold]
        net_big_buy_vol=big_trade[big_trade.buyorsell==0].vol.sum()-big_trade[big_trade.buyorsell==1].vol.sum()
        net_big_buy_amount=(big_trade[big_trade.buyorsell == 0].vol*100*big_trade[big_trade.buyorsell == 0].price).sum() - \
                        (big_trade[big_trade.buyorsell == 1].vol*100*big_trade[big_trade.buyorsell == 1].price).sum()
        big_amount_rate=net_big_buy_amount/total_amount

        trade_date = df.date[0]
    elif method=='volume':
        big_trade=df[df.vol>=threshold]

        big_trade = df[df.vol * df.price * 100 >= threshold]
        net_big_buy_vol = big_trade[big_trade.buyorsell == 0].vol.sum() - big_trade[big_trade.buyorsell == 1].vol.sum()
        net_big_buy_amount = (big_trade[big_trade.buyorsell == 0].vol * 100 * big_trade[big_trade.buyorsell == 0].price).sum() - \
                             (big_trade[big_trade.buyorsell == 1].vol * 100 * big_trade[big_trade.buyorsell == 1].price).sum()
        big_amount_rate = net_big_buy_amount / total_amount
    data_df = pd.DataFrame([df.ts_code[0], trade_date, total_buy,net_big_buy_vol, net_big_buy_amount/10000,big_amount_rate],
                           index=['ts_code', 'trade_date', 'total_buy','net_big_buy_vol', 'net_big_buy_amount','big_amount_rate']).T
    return data_df

get_cash_flow_daily(trade_date='20181010', ts_code='000002.SZ',method='amount',threshold=1000000)
get_cash_flow_daily(trade_date='20181010', ts_code='000005.SZ',method='volume',threshold=500)

def cash_flow_daily_import():


