"""
Created on 2018/9/27
@author: yby
@desc    : 2018-09-27
contact author:ybychem@gmail.com
"""
import pandas as pd
from pytdx.hq import TdxHq_API
from pytdx.params import TDXParams
from tasks.utils.fh_utils import str_2_datetime
import logging

logger = logging.getLogger()
api = TdxHq_API()
api.connect('59.173.18.140', 7709)


def get_tdx_tick(code, date_str):
    """
    调用pytdx接口获取股票tick数据
    :param code:
    :param date_str:
    :return:
    """
    position, data_list = 0, []
    df = api.to_df(api.get_history_transaction_data(TDXParams.MARKET_SZ, code, position, 30000, int(date_str)))
    data_list.append(df)
    datetime0925 = str_2_datetime(date_str + '09:25', '%Y%m%d%H:%M')
    datetime0930 = str_2_datetime(date_str + '09:30', '%Y%m%d%H:%M')
    while str_2_datetime(date_str + df.time[0], '%Y%m%d%H:%M') > datetime0925:
        position = position + len(df)
        df = api.to_df(api.get_history_transaction_data(TDXParams.MARKET_SZ, code, position, 30000, int(date_str)))
        if df is not None and len(df) > 0:
            # data_df1=data_df
            data_list.append(df)
            if str_2_datetime(date_str + df.time[0], '%Y%m%d%H:%M') == datetime0925 or \
                    str_2_datetime(date_str + df.time[0], '%Y%m%d%H:%M') == datetime0930:
                break
        else:
            break
    if code[0] == 6:
        code = code + '.SH'
    else:
        code = code + '.SZ'
    data_df = pd.concat(data_list)
    trade_date = data_df.time.apply(lambda x: str_2_datetime(date_str + x, '%Y%m%d%H:%M'))

    data_df.insert(0, 'ts_code', code)
    data_df.insert(1, 'date', date_str)
    data_df.insert(2, 'trade_date', trade_date)
    data_df = data_df.sort_values(by='trade_date')
    return data_df


if __name__ == "__main__":
    date_str, code = '20180926', '000001'
    get_tdx_tick(code=code, date_str=date_str)
