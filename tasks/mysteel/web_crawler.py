import logging
import datetime
import time
from decimal import Decimal

import pandas as pd
from selenium import webdriver
from sqlalchemy.types import String, Date
from sqlalchemy.dialects.mysql import DOUBLE

from ibats_utils.db import bunch_insert_on_duplicate_update
from tasks.config import config
from tasks import app
from tasks.backend import engine_dic


logger = logging.getLogger()
engine_model = engine_dic[config.DB_SCHEMA_MODEL]


# 此爬虫仅限使用Chrome浏览器，使用前下载Chrome对应版本的driver并在config文件中修改路径
# driver下载地址：http://chromedriver.storage.googleapis.com/index.html
@app.task
def start_web_crawler(date_from=None):
    chrome_opt = webdriver.ChromeOptions()
    prefs = {"profile.managed_default_content_settings.images": 2}
    chrome_opt.add_experimental_option("prefs", prefs)

    browser = webdriver.Chrome(executable_path=config.DRIVER_PATH, chrome_options=chrome_opt)
    browser.get('https://gc.mysteel.com/huizong/index.html')
    time.sleep(1)

    # 点击登录
    elem = browser.find_element_by_css_selector("[class = 'loginBtn btn']")
    elem.click()

    # 输入用户名
    elem = browser.find_element_by_class_name("userName")
    elem.send_keys(config.MYSTEEL_USERNAME)

    # 输入密码
    elem = browser.find_element_by_css_selector("[class = 'pasd pasdPsd hide']")
    elem.send_keys(config.MYSTEEL_PASSWORD)

    # 提交表单
    elem = browser.find_element_by_class_name("loginBtnBig")
    elem.click()
    time.sleep(1)

    # 点击进入下一个链接
    elem = browser.find_element_by_xpath("/html/body/div[7]/div[1]/div[2]/ul[1]/li[1]/a")
    elem.click()
    browser.switch_to.window(browser.window_handles[1])
    time.sleep(1)

    # 获取数据发布日期
    date_str = browser.find_element_by_id("publishtime").text
    date_time = datetime.datetime.strptime(date_str.split(" ")[0], '%Y-%m-%d')

    # 将表格数据储存在一个dataframe中
    array = []
    table = browser.find_element_by_xpath("/html/body/div[8]/div/div[3]/table/tbody")
    trs = table.find_elements_by_tag_name("tr")
    for tr in trs:
        tds = tr.find_elements_by_tag_name("td")
        row_array = []
        for td in tds:
            row_array.append(td.text)
        array.append(row_array)
    df = pd.DataFrame(array)

    df1 = df.head(6).T.tail(df.shape[1]-1)
    df1['datetime'] = date_time
    df1.columns = ['city', 'price', 'growth_rate', 'producer', 'wighting_mode', '30ds_price_avg', 'publish_date']

    df2 = df.head(1)
    df2 = df2.append(df.tail(5))
    df2 = df2.T.tail(df.shape[1]-1)
    df2['datetime'] = date_time
    df2.columns = ['city', 'price', 'growth_rate', 'producer', 'wighting_mode', '30ds_price_avg', 'publish_date']

    df1['price'] = df1['price'].map(lambda x: Decimal(x))
    df1['growth_rate'] = df1['growth_rate'].map(lambda x: Decimal(x))
    df1['30ds_price_avg'] = df1['30ds_price_avg'].map(lambda x: Decimal(x))

    df2['price'] = df2['price'].map(lambda x: Decimal(x))
    df2['growth_rate'] = df2['growth_rate'].map(lambda x: Decimal(x))
    df2['30ds_price_avg'] = df2['30ds_price_avg'].map(lambda x: Decimal(x))

    table_name1 = "mysteel_hrb400_12mm"
    table_name2 = "mysteel_hrb400_20mm"

    param_list = [
        ('city', String(50)),
        ('price', DOUBLE),
        ('growth_rate', DOUBLE),
        ('producer', String(50)),
        ('wighting_mode', String(50)),
        ('30ds_price_avg', DOUBLE),
        ('publish_date', Date,)
    ]
    # 设置 dtype
    dtype = {key: val for key, val in param_list}
    create_pk_str = """ALTER TABLE %s ADD PRIMARY KEY (`city`, `publish_date`)"""

    logger.info("更新 %s 开始", table_name1)
    has_table1 = engine_model.has_table(table_name1)
    bunch_insert_on_duplicate_update(df1, table_name1, engine_model, dtype=dtype)
    if not has_table1:
        engine_model.execute(create_pk_str % table_name1)

    logger.info("更新 %s 开始", table_name2)
    has_table2 = engine_model.has_table(table_name1)
    bunch_insert_on_duplicate_update(df2, table_name2, engine_model, dtype=dtype)
    if not has_table2:
        engine_model.execute(create_pk_str % table_name2)

    browser.close()


if __name__ == "__main__":
    start_web_crawler()



