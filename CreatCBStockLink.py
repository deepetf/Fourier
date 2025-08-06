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


start_date = '2025-07-01'
print("开始从数据库中获取数据...")
lido_db = LidoCBData()
cbdf = lido_db.GetCBData(start_date)

cbdf = cbdf[['trade_date','code','code_stk','conv_price']]

print(cbdf['code_stk'].isna().sum())   # 看看有多少 None
cbdf['code_stk'] = cbdf['code_stk'].fillna('').astype(str)
cbdf['code_stk'] = cbdf['code_stk'].apply(addSuffix)


password = quote('Happy$4ever')

TABLE_NAME = 'CB_STOCK_LINK'              
engine = create_engine(f'mysql+mysqlconnector://root:{password}@www.deepweiqi.cn:3306/KDATA')

with engine.connect() as conn:

    cbdf.to_sql(
                TABLE_NAME,
                conn,
                if_exists='append',
                index=False
                )
    conn.commit()