

from clickhouse_driver import Client
from sqlalchemy import create_engine
import pandas as pd
import datetime



# 配置文件数据库连接

# clickhouse information
host_ck='121.37.30.78'
username_ck='chenerping'
password_ck='cheneRpin_g95012703'
database_ck='yb_datacenter'

# Amazon销售中台数据库连接
host_amzt = '121.37.214.103'
username_amzt = 'W00575'
password_amzt = 'YIJ8CxjJbC'
database_amzt = "yibai_sale_center_amazon"

# Amazon数据分析系统
host_am1 = '124.71.220.143'
username_am1 = 'fanziyu'
password_am1 = 'FANzy#dgj46fh'
database_am1 = "yibai_product"
con3 = f"mysql+pymysql://{username_am1}:{password_am1}@{host_am1}:3306/{database_am1}"

# Localhost数据分析系统
host_loc = '172.16.8.130'  # win11主机IP
username_loc = 'root'
password_loc = 'test123'
database_loc = "test_java"
loc_con = f"mysql+pymysql://{username_loc}:{password_loc}@{host_loc}:3306/{database_loc}"