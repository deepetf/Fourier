import mysql.connector
from LidoDBClass import LidoCBData
import pandas as pd
from sqlalchemy import create_engine,text,Table, MetaData
import os
from urllib.parse import quote
import numpy as np
import yaml
from datetime import datetime
import json
from CommonFunctions import addSuffix


password = quote('Happy$4ever')

print("开始从数据库中获取数据...")
TABLE_NAME = 'CB_STOCK_LINK'              
engine = create_engine(f'mysql+mysqlconnector://root:{password}@www.deepweiqi.cn:3306/KDATA')
with engine.connect() as conn:
    df = pd.read_sql_table(TABLE_NAME,conn)
    print("获取到{}条数据".format(len(df)))
    df.to_excel('CB_STOCK_LINK.xlsx',index=False)


'''
start_date = '2024-07-01'
print("开始从数据库中获取数据...")
lido_db = LidoCBData()
cbdf = lido_db.GetCBData(start_date)
print("获取到{}条数据，开始处理...".format(len(cbdf)))

cbdf = cbdf[['trade_date','code','code_stk','conv_price','list_days','is_call']]

# 排除强赎的转债
is_call_exclude = ['已满足强赎条件','公告实施强赎','公告提示强赎']
cbdf = cbdf[~cbdf['is_call'].isin(is_call_exclude)]

# 排除上市时间小于3天的转债
cbdf = cbdf[cbdf['list_days'] > 3]

print("处理后数据条数：{}".format(len(cbdf)))

print(cbdf['code_stk'].isna().sum())   # 看看有多少 None
#cbdf['code_stk'] = cbdf['code_stk'].fillna('').astype(str)
#cbdf['code_stk'] = cbdf['code_stk'].apply(addSuffix)


password = quote('Happy$4ever')

TABLE_NAME = 'CB_STOCK_LINK'              
engine = create_engine(f'mysql+mysqlconnector://root:{password}@www.deepweiqi.cn:3306/KDATA')


print("写入数据库...")
with engine.connect() as conn:
    # 将cbdf分为10个部分写入数据库
    parts = np.array_split(cbdf, 10)
    for i, part in enumerate(parts):
        print(f"正在写入第{i+1}/10部分，数据量：{len(part)}")
        # 显式转换为DataFrame，防止因swapaxes警告
        part = pd.DataFrame(part)
        part = part.transpose().transpose()  # 用transpose替代swapaxes，虽然这里其实是冗余的，但可防止某些内部调用swapaxes
        if part.empty:
            print("第{}部分为空，跳过...".format(i+1))
            continue
        part.to_sql(
            TABLE_NAME,
            conn,
            if_exists='append',
            index=False
        )
        conn.commit()

'''