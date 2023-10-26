# -*- encoding: utf-8 -*-
'''
@File    :   down_currency.py
@Time    :   2023/10/25 10:13:30
@Author  :   cep 
'''
from tools.config import *
from tools.duckC import *


sql_rate = """SELECT from_currency_code as currency,rate_month as paymonth,rate FROM yibai_system.yibai_currency_rate
 WHERE rate_month >= 202208 AND to_currency_code = 'CNY';"""


engine3 = create_engine(con3)


df = pd.read_sql(sql_rate,engine3)

c = DuckDbTools().con_duck().cursor()

c.sql("create table currency as select * from df")

c.sql("from currency").show()

c.close()


