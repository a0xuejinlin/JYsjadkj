from sqlalchemy import create_engine
import pandas as pd
import datetime

db = create_engine('sqlite:///JYsjadkj/mystock.db')
sql_cmd = "SELECT * FROM stock_day_k where code='sh.000001' order by date desc limit 0,251"
datash = pd.read_sql(sql_cmd, db)
print(datash[1])