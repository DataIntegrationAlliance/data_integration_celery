import tushare as ts
cons = ts.get_apis()
df_day =ts.bar("RB1801",conn=cons,asset='X',freq='D')
df_5min =ts.bar("IF1801",conn=cons,asset='X',freq='5min')
