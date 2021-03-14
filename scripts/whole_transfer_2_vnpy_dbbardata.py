"""
@author  : MG
@Time    : 2021/3/13 4:57
@File    : whole_transfer_2_vnpy_dbbardata.py
@contact : mmmaaaggg@163.com
@desc    : 用于全量的将历史期货分钟级数据导入到vnpy数据库中
使用 rqdata 以及 wind 数据进行结合
"""
from tasks.rqdatac.future import min_to_vnpy
from tasks.wind.future import min_to_vnpy_whole


def transfer():
    min_to_vnpy_whole(lasttrade_date_lager_than_n_days_before=1000)
    min_to_vnpy()


if __name__ == "__main__":
    transfer()
