####createDB.py####
import baostock as bs
import pandas as pd
import sqlite3
from RetrieveData.PPSET import CJBD



def createDB():
    conn = sqlite3.connect(CJBD)
    cursor = conn.cursor()

    # create three tables
    #创建日线指标参数（包含停牌证券）
    sql_daystock = "CREATE TABLE DayStock(id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT, code TEXT, open REAL, high REAL, low REAL, close REAL, preclose REAL, volume INTEGER, amount REAL, adjustflag TEXT, turn REAL, tradestatus INTEGER, pctChg REAL, peTTM REAL, psTTM REAL, pcfNcfTTM REAL, pbMRQ REAL, isST INTEGER)"
    #创建周、月线指标参数
    sql_monthstock = "CREATE TABLE MonthStock(id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT, code TEXT, open REAL, high REAL, low REAL, close REAL, volume INTEGER, amount REAL, adjustflag TEXT, turn REAL, pctChg REAL)"
    sql_weekstock = "CREATE TABLE weekStock(id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT, code TEXT, open REAL, high REAL, low REAL, close REAL, volume INTEGER, amount REAL, adjustflag TEXT, turn REAL, pctChg REAL)"
    #创建5、15、30、60分钟线指标参数（不包含指数）
    sql_minutestock5 = "CREATE TABLE MinuteStock5(id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT, time TEXT, code TEXT, open REAL, high REAL, low REAL, close REAL, volume INTEGER, amount REAL, adjustflag TEXT)"
    sql_minutestock15 = "CREATE TABLE MinuteStock15(id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT, time TEXT, code TEXT, open REAL, high REAL, low REAL, close REAL, volume INTEGER, amount REAL, adjustflag TEXT)"
    sql_minutestock30 = "CREATE TABLE MinuteStock30(id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT, time TEXT, code TEXT, open REAL, high REAL, low REAL, close REAL, volume INTEGER, amount REAL, adjustflag TEXT)"
    sql_minutestock60 = "CREATE TABLE MinuteStock60(id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT, time TEXT, code TEXT, open REAL, high REAL, low REAL, close REAL, volume INTEGER, amount REAL, adjustflag TEXT)"    
    cursor.execute(sql_daystock)
    cursor.execute(sql_monthstock)
    cursor.execute(sql_weekstock)    
    cursor.execute(sql_minutestock5)  
    cursor.execute(sql_minutestock15) 
    cursor.execute(sql_minutestock30) 
    cursor.execute(sql_minutestock60)   
    cursor.close()
    conn.commit()
    conn.close()

if __name__=='__main__':
    createDB()