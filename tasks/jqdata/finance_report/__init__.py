#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2019/2/26 17:38
@File    : __init__.py.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from tasks.jqdata import finance, query
import logging
from datetime import date
from tasks.utils.fh_utils import str_2_date, date_2_str, iter_2_range, range_date
from tasks.backend import engine_md
from tasks.utils.db_utils import bunch_insert_on_duplicate_update, execute_scalar
from tasks.config import config


class FinanceReportSaver:

    def __init__(self, table_name, dtype, statement):
        self.logger = logging.getLogger(__name__)
        self.BASE_DATE = str_2_date('1989-12-01')
        self.loop_step = 20
        self.dtype = dtype
        self.table_name = table_name
        self.statement = statement

    def get_df_iter(self, date_start, date_end, step, df_len_limit=3000, deep=0):
        """
        获取日期范围内的数据，当数据记录大于上限条数时，将日期范围进行二分法拆分，迭代进行查询
        :param date_start:
        :param date_end:
        :param step:
        :param df_len_limit:
        :param deep:
        :return:
        """
        for num, (date_from, date_to) in enumerate(iter_2_range(range_date(
                date_start, date_end, step), has_left_outer=False, has_right_outer=False), start=1):
            q = query(self.statement).filter(
                self.statement.pub_date > date_2_str(date_from),
                self.statement.pub_date <= date_2_str(date_to))

            df = finance.run_query(q)
            df_len = df.shape[0]
            if df_len >= df_len_limit:
                if step >= 2:
                    self.logger.warning('%s%d) [%s ~ %s] 包含 %d 条数据，可能已经超越 %d 条提取上限，开始进一步分割日期',
                                        '  ' * deep, num, date_from, date_to, df_len, df_len_limit)
                    yield from self.get_df_iter(date_from, date_to, step // 2, deep=deep + 1)
                else:
                    self.logger.warning('%s%d) [%s ~ %s] 包含 %d 条数据，可能已经超越 %d 条提取上限且无法再次分割日期范围，手动需要补充提取剩余数据',
                                        '  ' * deep, num, date_from, date_to, df_len, df_len_limit)
                    yield df, date_from, date_to
            else:
                self.logger.debug('%s%d) [%s ~ %s] 包含 %d 条数据', '  ' * deep, num, date_from, date_to, df_len)
                yield df, date_from, date_to

    def save(self):
        self.logger.info("更新 %s 开始", self.table_name)
        has_table = engine_md.has_table(self.table_name)
        # 判断表是否已经存在
        if has_table:
            sql_str = f"""select max(pub_date) from {self.table_name}"""
            date_start = execute_scalar(engine_md, sql_str)
            self.logger.info('查询 %s 数据使用起始日期 %s', self.table_name, date_2_str(date_start))
        else:
            date_start = self.BASE_DATE
            self.logger.warning('%s 不存在，使用基础日期 %s', self.table_name, date_2_str(date_start))

        # 查询最新的 pub_date
        date_end = date.today()
        if date_start >= date_end:
            self.logger.info('%s 已经是最新数据，无需进一步获取', date_start)
            return
        data_count_tot = 0
        try:
            for num, (df, date_from, date_to) in enumerate(self.get_df_iter(date_start, date_end, self.loop_step)):
                # logger.debug('%d) [%s ~ %s] 包含 %d 条数据', num, date_from, date_to, df.shape[0])
                if df is not None and df.shape[0] > 0:
                    data_count = bunch_insert_on_duplicate_update(
                        df, self.table_name, engine_md,
                        dtype=self.dtype, myisam_if_create_table=True,
                        primary_keys=['id'], schema=config.DB_SCHEMA_MD)
                    data_count_tot += data_count
        finally:
            # 导入数据库
            logging.info("更新 %s 结束 %d 条信息被更新", self.table_name, data_count_tot)
