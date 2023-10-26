# -*- encoding: utf-8 -*-
'''
@File    :   down_orders.py
@Time    :   2023/10/19 15:20:50
@Author  :   cep 
'''

from tools.config import *
from tools.duckC import *

con = Client(host=host_ck, port=9001, user=username_ck,
             password=password_ck, database=database_ck, send_receive_timeout=3000)


# 账号数据
account_sheet = pd.read_excel("./Duckdb/account_data.xlsx")
account_li = account_sheet['erp_id'].to_list()

# 初始化订单下载任务
local_con  = DuckDbTools().con_duck().cursor()
local_con.sql("truncate order_his")
local_con.close()
print("历史数据已经清理")


task_list = []
start_time = '2022-08-01 00:00:00'
for i in range(440):
    t0 = datetime.datetime.strptime(start_time, '%Y-%m-%d 00:00:00') + datetime.timedelta(days=i)
    t1 = datetime.datetime.strptime(start_time, '%Y-%m-%d 00:00:00') + datetime.timedelta(days=i + 1)

    # print(t0, t1)
    sql_order = f"""
    with 
    oms_order as 
    (select platform_code ,
    account_id ,
    payment_time , 
    order_id ,
    currency ,
    total_price
    from yibai_oms_sync.yibai_oms_order 
    where payment_time >= '{str(t0)}' 
    AND payment_time < '{str(t1)}' 
    AND platform_code='AMAZON'
    AND order_type IN ( 1, 2, 5, 6, 8 )
    AND order_status NOT IN (25,40,80)
    AND payment_status = 1
    AND order_id  NOT LIKE '%%-RE%%'
    ),
    oms_order_detail as (
    select order_id, total_price , quantity , seller_sku, item_id, quantity 
    from yibai_oms_sync.yibai_oms_order_detail 
    WHERE platform_code = 'AMAZON' AND create_time >= '{str(t0)}' AND create_time < '{str(t1)}'
    ),
    order_sku as (
    select order_id ,sku from yibai_oms_sync.yibai_oms_order_sku where order_id in (select order_id from oms_order)
    )
    select 
    a.order_id,
    a.account_id,
    b.seller_sku,
    c.sku,
    a.payment_time,
    formatDateTime(a.payment_time,'%Y%m') as paymonth,
    a.currency,
    toFloat64(a.total_price) as total_price
    from oms_order a
    left join oms_order_detail b
    ON a.order_id = b.order_id
    left join order_sku c
    on a.order_id = c.order_id
    where a.total_price > 0
    settings max_memory_usage = 40000000000
    """

    # print(sql_order)
    # con = Client(host=host_ck, port=9001, user=username_ck, password=password_ck, database=database_ck, send_receive_timeout=3000)

    # columns = ['order_id', 'account_id', 'seller_sku', 'sku', 'payment_time', 'paymonth',
    #         'currency', 'total_price']
    # data = con.execute(sql_order)
    # df = pd.DataFrame(data=data, columns=columns)
    # print(df)

    task_list.append(sql_order)


# 订单数据下载函数构建
def fun(sql_x):
    con = Client(host=host_ck, port=9001, user=username_ck,
                 password=password_ck, database=database_ck, send_receive_timeout=3000)

    columns = ['order_id', 'account_id', 'seller_sku', 'sku', 'payment_time', 'paymonth',
            'currency', 'total_price']
    data = con.execute(sql_x)
    df = pd.DataFrame(data=data, columns=columns)
    # print(df)
    # 数据写入dk
    writeDuck(df) # 调用函数写入

def writeDuck(data):
    data_insert = data.copy()
    local_con  = DuckDbTools().con_duck().cursor()
    # print(data_insert)
    local_con.sql("insert into order_his select * from data_insert")
    local_con.close()  # 用完关闭
    


# task_list = task_list[:5]
print(len(task_list))
g = DownData()
g.queue_num = 5
g.get_data_down(task_list=task_list, func_do=fun)
