"""
Created on 2018/9/27
@author: yby
@desc    : 2018-09-27
contact author:ybychem@gmail.com
"""
"""

##pip install pytdx
import pandas as pd
from pytdx.hq import TdxHq_API
from pytdx.params import TDXParams
from datetime import datetime
api = TdxHq_API()
api.connect('59.173.18.140', 7709)
print("获取股票行情")


date_str,code='20180926', '000001'
def get_tdx_tick(code,date_str):
    api.connect('59.173.18.140', 7709)
    position,data_list=0,[]
    df=api.to_df(api.get_history_transaction_data(TDXParams.MARKET_SZ, code, position, 30000, int(date_str)))
    data_list.append(df)
    while datetime.strptime(date_str+df.time[0],'%Y%m%d%H:%M')>datetime.strptime(date_str+'09:25', '%Y%m%d%H:%M') or datetime.strptime(date_str+df.time[0],'%Y%m%d%H:%M')>datetime.strptime(date_str+'09:30', '%Y%m%d%H:%M'):
        position = position + len(df)
        df = api.to_df(api.get_history_transaction_data(TDXParams.MARKET_SZ, code, position, 30000, int(date_str)))
        if len(df)>0:
            # data_df1=data_df
            data_list.append(df)
            if datetime.strptime(date_str+df.time[0],'%Y%m%d%H:%M')==datetime.strptime(date_str+'09:25', '%Y%m%d%H:%M') or datetime.strptime(date_str+df.time[0],'%Y%m%d%H:%M')==datetime.strptime(date_str+'09:30', '%Y%m%d%H:%M'):
                break
        else:
            break
    if code[0]==6:
        code=code+'.SH'
    else:
        code=code+'.SZ'
    data_df=pd.concat(data_list)
    trade_date=data_df.time.apply(lambda x: datetime.strptime(date_str + x, '%Y%m%d%H:%M'))

    data_df.insert(0,'ts_code',code)
    data_df.insert(1,'date',date_str)
    data_df.insert(2,'trade_date',trade_date)
    data_df=data_df.sort_values(by='trade_date')
    return data_df

get_tdx_tick(code='000002',date_str='20000125')