# -*- coding: utf-8 -*-
"""
Created on Thu Apr 13 16:41:56 2017

@author: Yupeng Guo
"""

import logging
import pandas as pd
from tasks import app
from tasks.wind import invoker
from datetime import date, timedelta
from sqlalchemy.types import String, Date
from tasks.utils.db_utils import with_db_session, alter_table_2_myisam
from tasks.backend import engine_md
from tasks.utils.db_utils import bunch_insert_on_duplicate_update
from tasks.backend.orm import build_primary_key


@app.task
def wind_CS_sector_update(chain_param=None):
    """
    :param chain_param:  在celery 中將前面結果做爲參數傳給後面的任務
    :return:
    """
    dic = {u'CS石油石化': 'b101000000000000',
           u'CS煤炭': 'b102000000000000',
           u'CS有色金属': 'b103000000000000',
           u'CS电力及公用事业': 'b104000000000000',
           u'CS钢铁': 'b105000000000000',
           u'CS基础化工': 'b106000000000000',
           u'CS建筑': 'b107000000000000',
           u'CS建材': 'b108000000000000',
           u'CS轻工制造': 'b109000000000000',
           u'CS机械': 'b10a000000000000',
           u'CS电力设备': 'b10b000000000000',
           u'CS国防军工': 'b10c000000000000',
           u'CS汽车': 'b10d000000000000',
           u'CS商贸零售': 'b10e000000000000',
           u'CS餐饮旅游': 'b10f000000000000',
           u'CS家电': 'b10g000000000000',
           u'CS纺织服装': 'b10h000000000000',
           u'CS医药': 'b10i000000000000',
           u'CS食品饮料': 'b10j000000000000',
           u'CS农林牧鱼': 'b10k000000000000',
           u'CS银行': 'b10l000000000000',
           u'CS证券II': 'b10m010000000000',
           u'CS保险II': 'b10m020000000000',
           u'CS信托及其他': 'b10m030000000000',
           u'CS房地产': 'b10n000000000000',
           u'CS交通运输': 'b10o000000000000',
           u'CS电子元器件': 'b10p000000000000',
           u'CS通信': 'b10q000000000000',
           u'CS计算机': 'b10r000000000000',
           u'CS传媒': 'b10s000000000000',
           u'CS综合': 'b10t000000000000'}
    table_name = 'wind_CS_sector'
    has_table = engine_md.has_table(table_name)
    param_list = [
        ('sector', String(20)),
        ('date', Date),
    ]
    dtype = {col_name: val for col_name, val in param_list}
    dtype['wind_code'] = String(20)
    info = pd.read_sql_query('select sector, max(date) as last_date from wind_CS_sector group by sector', engine_md)
    info.set_index('sector', inplace=True)
    for sector in info.index:
        begin_date = info.loc[sector, 'last_date'] + timedelta(days=1)
        week_ends = invoker.tdays(beginTime=begin_date, endTime=date.today(), options='Period=W').Times  ##

    for week_end in week_ends:
        for sector in dic.keys():
            df = invoker.wset("sectorconstituent",
                              "date=%s;sectorid=%s" % (week_end.date().strftime('%Y-%m-%d'), dic[sector]))
            if len(df) == 0:
                continue
            df['sector'] = sector
            df.set_index(['sector', 'date', 'wind_code'], inplace=True)
            bunch_insert_on_duplicate_update(df, table_name, engine_md, dtype=dtype)
            logging.info('Success import %s - %s' % (week_end, sector))
            if not has_table and engine_md.has_table(table_name):
                alter_table_2_myisam(engine_md, [table_name])
                build_primary_key([table_name])


temp = pd.read_sql_table('factors_d', engine_md)
