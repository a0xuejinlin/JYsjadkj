import baostock as bs
import pandas as pd
from sqlalchemy import create_engine
from PPSET import DBAS

# 参数含义：

# code：股票代码，sh或sz.+6位数字代码，或者指数代码，如：sh.601398。sh：上海；sz：深圳。此参数不可为空；
# fields：指示简称，支持多指标输入，以半角逗号分隔，填写内容作为返回类型的列。详细指标列表见历史行情指标参数章节，日线与分钟线参数不同。此参数不可为空；
# start：开始日期（包含），格式“YYYY-MM-DD”，为空时取2015-01-01；
# end：结束日期（包含），格式“YYYY-MM-DD”，为空时取最近一个交易日；
# frequency：数据类型，默认为d，日k线；d=日k线、w=周、m=月、5=5分钟、15=15分钟、30=30分钟、60=60分钟k线数据，不区分大小写；指数没有分钟线数据；周线每周最后一个交易日才可以获取，月线每月最后一个交易日才可以获取。
# adjustflag：复权类型，默认不复权：3；1：后复权；2：前复权。已支持分钟线、日线、周线、月线前后复权。 BaoStock提供的是涨跌幅复权算法复权因子，具体介绍见：复权因子简介或者BaoStock复权因子简介。
# 注意：

# 股票停牌时，对于日线，开、高、低、收价都相同，且都为前一交易日的收盘价，成交量、成交额为0，换手率为空。
# 如果需要将换手率转为float类型，可使用如下方法转换：result["turn"] = [0 if x == "" else float(x) for x in result["turn"]]

# 关于复权数据的说明：

# BaoStock使用“涨跌幅复权法”进行复权，详细说明参考上文“复权因子简介”。不同系统间采用复权方式可能不一致，导致数据不一致。

# “涨跌幅复权法的”优点：可以计算出资金收益率，确保初始投入的资金运用率为100%，既不会因为分红而导致投资减少，也不会因为配股导致投资增加。

#Scode股票代码,Sd开始事件,Ed结束事件,Sl数据类型（日周月分）,IFCQ（是否除权）

def TEstRSDay(Scode,Sd,Ed,Sl,IFCQ):
    db_conn = create_engine(DBAS)
    ###登录系统###
    lg =bs.login()
    #显示登陆返回信息
    print('login respond error_code:'+lg.error_code)
    print('longin respond error_msg:'+lg.error_msg)

    ###获取历史日K线数据
    rs=bs.query_history_k_data_plus(Scode,"date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST",start_date=Sd,end_date=Ed,frequency="d",adjustflag=IFCQ)
    if rs.error_code=='0':
        print('query_history_k_data_plus respond error_code:'+rs.error_code)
        print('query_history_k_data_plus respond error_msg:'+rs.error_msg)
    else:
        print('query_history_k_data_plus failed')

    ###打印结果集###
    data_list = []
    while (rs.error_code=='0')& rs.next():
        data_list.append(rs.get_row_data())
    db_conn.execute(r'''
        INSERT OR REPLACE INTO daystock (date, code, open, high, low, close, preclose, 
            volume, amount, adjustflag, turn, tradestatus,  
            pctChg, peTTM, psTTM, pcfNcfTTM, pbMRQ, isST)  
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ''', data_list)
    #登出
    bs.logout()
    #月周数据获取
def TEstRSMon(Scode,Sd,Ed,Sl,IFCQ):
    db_conn = create_engine(DBAS)
    ###登录系统###
    lg =bs.login()
    #显示登陆返回信息
    print('login respond error_code:'+lg.error_code)
    print('longin respond error_msg:'+lg.error_msg)

    ###获取历史月K线数据
    rs=bs.query_history_k_data_plus(Scode,"date, code, open, high, low, close, volume, amount,          adjustflag, turn, pctChg",start_date=Sd,end_date=Ed,frequency=Sl,adjustflag=IFCQ)
    if rs.error_code=='0':
        print('query_history_k_data_plus respond error_code:'+rs.error_code)
        print('query_history_k_data_plus respond error_msg:'+rs.error_msg)
    else:
        print('query_history_k_data_plus failed')

    ###打印结果集###
    data_list = []
    while (rs.error_code=='0')& rs.next():
        data_list.append(rs.get_row_data())
    if Sl=="m":
        db_conn.execute(r'''
        INSERT OR REPLACE INTO MonthStock (date, code, open, high, low, close, volume, amount,  
           adjustflag, turn, pctChg)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)
        ''', data_list)
    elif Sl=="w":
        print("test-test-test-test-test-test-test")
        print(data_list)
        db_conn.execute(r'''
        INSERT OR REPLACE INTO weekStock (date, code, open, high, low, close, volume, amount,  
           adjustflag, turn, pctChg)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)
        ''', data_list)   
    else:
        print("错误的数据类型"+Sl)
    #登出
    bs.logout()

    #分钟数据获取
def TEstRSMin(Scode,Sd,Ed,Sl,IFCQ):
    db_conn = create_engine(DBAS)
    ###登录系统###
    lg =bs.login()
    #显示登陆返回信息
    print('login respond error_code:'+lg.error_code)
    print('longin respond error_msg:'+lg.error_msg)

    ###获取历史分钟K线数据
    rs=bs.query_history_k_data_plus(Scode,"date, time, code, open, high, low, close, volume, amount, adjustflag",start_date=Sd,end_date=Ed,frequency=Sl,adjustflag=IFCQ)
    if rs.error_code=='0':
        print('query_history_k_data_plus respond error_code:'+rs.error_code)
        print('query_history_k_data_plus respond error_msg:'+rs.error_msg)
    else:
        print('query_history_k_data_plus failed')

    ###打印结果集###
    data_list = []
    while (rs.error_code=='0')& rs.next():
        data_list.append(rs.get_row_data())
    if Sl=='5':
        db_conn.execute(r'''
            INSERT OR REPLACE INTO minutestock5 (date, time, code, open, high, low, close, volume, amount, adjustflag)  
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', data_list)
    elif Sl=='15':
        db_conn.execute(r'''
            INSERT OR REPLACE INTO minutestock15 (date, time, code, open, high, low, close, volume, amount, adjustflag)  
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', data_list)
    elif Sl=='30':
        db_conn.execute(r'''
            INSERT OR REPLACE INTO minutestock30 (date, time, code, open, high, low, close, volume, amount, adjustflag)  
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', data_list)
    elif Sl=='60':
        db_conn.execute(r'''
            INSERT OR REPLACE INTO minutestock60 (date, time, code, open, high, low, close, volume, amount, adjustflag)  
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', data_list)
    else:
        print("错误的数据类型"+Sl)
    #登出
    bs.logout()