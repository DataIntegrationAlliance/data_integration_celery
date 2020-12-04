"""
@author  : MG
@Time    : 2020/12/3 10:47
@File    : future_mid_day_task.py
@contact : mmmaaaggg@163.com
@desc    : 用于
"""
from tasks.wind.future import import_future_min, min_to_vnpy


def run_mid_day_task():
    import_future_min()
    min_to_vnpy()


if __name__ == "__main__":
    run_mid_day_task()
