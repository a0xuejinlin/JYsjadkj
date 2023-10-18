
import time
import baostock as bs
import pandas as pd
from sqlalchemy import create_engine

db_conn = create_engine('sqlite:///JYsjadkj/mystock.db')
lg = bs.login()

rs1 = bs.query_history_k_data_plus("sz.000001",
    "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
    start_date='2017-07-01', end_date='2017-12-31',
    frequency="d", adjustflag="2")
rs=bs.query_all_stock('2023-10-12')

data_list = []
while (rs.error_code == '0') & rs.next():
    row = rs.get_row_data()
    current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
    print(current_time)
    row.append(current_time)
    
    data_list.append(row)
#    data_list.append(rs.get_row_data())
    print(row)
db_conn.execute(r'''
    INSERT OR REPLACE INTO allstock VALUES (?,?,?,?)
    ''', data_list)
bs.logout()


