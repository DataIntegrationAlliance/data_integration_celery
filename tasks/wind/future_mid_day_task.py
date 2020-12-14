"""
@author  : MG
@Time    : 2020/12/3 10:47
@File    : future_mid_day_task.py
@contact : mmmaaaggg@163.com
@desc    : 用于
"""
from tasks.wind.future import import_future_min, min_to_vnpy


def run_mid_day_task():
    import_future_min(
        # wind_code_set=('RB2101.SHF', 'RB2105.SHF', 'RB2110.SHF', 'HC2101.SHF', 'HC2105.SHF', 'HC2110.SHF'),
    )
    min_to_vnpy(
        # instrument_types=['rb', 'i', 'hc']
    )


if __name__ == "__main__":
    run_mid_day_task()
