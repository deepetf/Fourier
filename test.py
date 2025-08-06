from xtquant import xtdata
import time
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine,text,Table, MetaData
import os
from urllib.parse import quote
import numpy as np
import yaml
from datetime import datetime
import json



def get_data_1m(stock_code, period, start_time, end_time):
    xtdata.download_history_data(stock_code, period, start_time=start_time, end_time=end_time)
    data = xtdata.get_market_data_ex([],[stock_code], '1m', "","")
    #data['123200.SZ'].to_excel('test_1m.xlsx')

def get_data_tick(stock_code, period, start_time, end_time):
    xtdata.download_history_data(stock_code, period, start_time=start_time, end_time=end_time)
    data = xtdata.get_market_data_ex([],[stock_code], 'tick', "","")
    #data['123200.SZ'].to_excel('test_tick.xlsx')

def conv_time(ct):
    '''
    conv_time(1476374400000) --> '20161014000000.000'
    '''
    local_time = time.localtime(ct / 1000)
    data_head = time.strftime('%Y%m%d%H%M%S', local_time)
    data_secs = (ct - int(ct)) * 1000
    time_stamp = '%s.%03d' % (data_head, data_secs)
    return time_stamp

# 直接调用即可，无需登录步骤
#xtdata.download_history_data(stock_code, period, start_time='', end_time='', incrementally = None)

xtdata.download_history_data("123200.SZ", 'tick', start_time='20230801', end_time='20250801')
data = xtdata.get_market_data_ex([],['123200.SZ'], 'tick', "","")
#data['123200.SZ'].to_excel('test_tick.xlsx')

df = data['123200.SZ']

df.index = df.index.rename('trade_time')
df['code'] = '123200.SZ'

df.reset_index(inplace=True)

cols_to_convert = ['askPrice', 'bidPrice', 'askVol', 'bidVol']

for col in cols_to_convert:
    df[col] = df[col].apply(json.dumps)

# 编码连接字符串中的特殊字符
password = quote('Happy$4ever')


table_name = 'CB_TICK'

engine = create_engine(f'mysql+mysqlconnector://root:{password}@192.168.8.78:3306/KDATA')
conn = engine.connect()

'''
with engine.connect() as conn:
    conn.execute(text("TRUNCATE TABLE CB_TICK;"))

with engine.connect() as conn:
    conn.execute(text("TRUNCATE TABLE CB_TICK;"))
'''

try:
    with engine.connect() as conn:  
            df.to_sql(table_name, conn, if_exists='append', index=False)
except Exception as e:
    print("写入数据库失败:", e)
    conn.rollback()
finally:
    with engine.connect() as conn:
            df.to_sql(table_name, conn, if_exists='append', index=False)
    conn.close()


#xtdata.download_cb_data()
#data = xtdata.get_cb_info('123200.SZ')


