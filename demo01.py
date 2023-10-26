# -*- encoding: utf-8 -*-
'''
@File    :   duckdbdemo.py
@Time    :   2023/10/17 17:31:14
@Author  :   cep 
'''
from clickhouse_driver import Client
from tools.config import *
from tools.duckC import *
import pandas as pd

# 连接duckdb
# c = DuckDbTools().con_duck()
# c.sql("show tables").show()
# c.sql("insert into listing_his select * from df")S
# c.sql("from listing_his").show()





# 账号数据
account_sheet = pd.read_excel("./Duckdb/account_data.xlsx")
account_li = account_sheet['erp_id'].to_list()

task_list = []
for i in account_li:
    sql_cl = f"""
    WITH
    (select '2022-08-01') AS begin_date,
    (select '2023-09-01') AS end_date,
    (select {i} ) AS ac_id,
    -- 匹配捆绑表
    listing as (
    select account_id, seller_sku, toDateTime(open_date) as open_date
    from yibai_product_kd_sync.yibai_amazon_listing_alls 
      WHERE account_id = ac_id AND open_date >= begin_date AND open_date < end_date AND fulfillment_channel = 'DEF'
      AND status = 1 AND add_delete != 'del'
      ),
    map_data as (
    select account_id ,seller_sku , sku from yibai_product_kd_sync.yibai_amazon_sku_map where account_id = ac_id 
    )
    select a.account_id, a.seller_sku, c.sku,a.open_date from listing a
    left join map_data c on a.account_id = c.account_id and a.seller_sku = c.seller_sku
    settings max_memory_usage = 40000000000
    """

    # columns = ['account_id', 'seller_sku', 'sku', 'open_date']
    # data = con.execute(sql_cl)
    # df = pd.DataFrame(data=data, columns=columns)
    # print(df)
    task_list.append(sql_cl)

def fun(sql_x):
    con = Client(host=host_ck, port=9001, user=username_ck,
                 password=password_ck, database=database_ck, send_receive_timeout=3000)

    columns = ['account_id', 'seller_sku', 'sku', 'open_date']
    data = con.execute(sql_x)
    df = pd.DataFrame(data=data, columns=columns)
    # print(df)
    # 数据写入dk
    writeDuck(df) # 调用函数写入

def writeDuck(data):
    local_con  = DuckDbTools().con_duck().cursor()
    local_con.sql("insert into listing_his select * from data")
    local_con.close()  # 用完关闭
    

# print(len(task_list))
# g = DownData()
# g.get_data_down(task_list=task_list, func_do=fun)


