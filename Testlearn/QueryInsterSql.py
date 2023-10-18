import baostock as bs
import pandas as pd
from sqlalchemy import create_engine

def refresh_all_stock(current_date = "2020-03-27"):
    db_conn = create_engine('sqlite:///mystock.db')
    lg = bs.login()
    rs = bs.query_all_stock(day=current_date)

    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    db_conn.execute(r'''
    INSERT OR REPLACE INTO allstock VALUES (?, ?, ?)
    ''', data_list)
    print(data_list)
    bs.logout()