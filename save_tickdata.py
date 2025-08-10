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


def tickdata_pre_process(df,code):
    df.index = df.index.rename('trade_time')
    df['code'] = code

    df = df.drop(columns=['openInt', 'lastSettlementPrice', 'settlementPrice'])
    df.reset_index(inplace=True)

    cols_to_convert = ['askPrice', 'bidPrice', 'askVol', 'bidVol']

    for col in cols_to_convert:
        df[col] = df[col].apply(json.dumps)

    return df

def tickdata_check(df):
    

    df.columns = df.columns.str.strip()

    # 检查基于主键的重复项
    # keep=False 会标记出所有重复项（包括第一次出现的）
    duplicates = df.duplicated(subset=['code', 'trade_time'], keep=False)
    num_of_total_duplicate_rows = duplicates.sum()

    # .duplicated(keep='first') 只会标记第二次、第三次...出现的重复项
    num_of_rows_to_drop = df.duplicated(subset=['code', 'trade_time'], keep='first').sum()


    print("\n--- 重复数据诊断报告 ---")
    print(f"基于主键 (code, trade_time)，文件中需要被丢弃的重复记录数量为: {num_of_rows_to_drop}")

    if num_of_rows_to_drop > 0:
        print("\n以下是部分重复数据的示例（主键相同，但其他列可能不同）:")
        print(df[duplicates].sort_values(by=['code', 'trade_time']).head(10).to_string())



def tickdata_to_db(df_source,engine,table_name):
    

    CHUNK_SIZE = 2000                   # 每次插入的行数，可根据内存和网络情况调整
    TABLE_NAME = table_name 

    try:


        # 获取源数据中的真实列名，并清理可能存在的前后空格
        df_source.columns = df_source.columns.str.strip()
        actual_columns = list(df_source.columns)
        #print(f"[*] 源数据包含的列: {actual_columns}")

        # --- 步骤 2: 数据预处理 (关键步骤) ---
        # 统一主键的数据类型和格式，为合并做准备。
        print("[*] 正在进行数据预处理...")
        
        # a) 强制将 trade_time 列转换为标准字符串格式，消除任何格式差异
        df_source['trade_time'] = pd.to_datetime(df_source['trade_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # b) 清理主键列的前后空格，确保匹配的精确性
        df_source['code'] = df_source['code'].str.strip()
        df_source['trade_time'] = df_source['trade_time'].str.strip()


        with engine.connect() as conn:
            # a) 读取数据库中已存在的所有主键
            query = f"SELECT code, trade_time FROM {TABLE_NAME}"
            df_existing = pd.read_sql(query, conn)
            print(f"[*] 数据库中已存在 {len(df_existing)} 条记录。")

            # b) 同样清理数据库中读取出的主键，确保与源数据格式一致
            if not df_existing.empty:
                df_existing['code'] = df_existing['code'].str.strip()
                df_existing['trade_time'] = df_existing['trade_time'].str.strip()

            # c) 使用 'left' merge 来找出只存在于源文件中的新记录
            # 这是整个增量更新逻辑的核心，远比 isin() 方法可靠。
            merged_df = df_source.merge(
                df_existing,
                on=['code', 'trade_time'],
                how='left',
                indicator=True
            )
            df_new = merged_df[merged_df['_merge'] == 'left_only'].copy()
            df_new.drop(columns=['_merge'], inplace=True)
            
            # --- 步骤 4: 执行分块插入 ---
            if df_new.empty:
                print("\n[✓] 所有数据均已存在，无需插入新记录。程序正常结束。")
                return

            print(f"\n[*] 发现 {len(df_new)} 行新记录，准备开始分块插入...")
           
            # 只选择源文件中真实存在的列进行插入
            df_to_insert = df_new[actual_columns]
            tickdata_check(df_to_insert)

            num_chunks = int(np.ceil(len(df_to_insert) / CHUNK_SIZE))

            try:        
                    for i, chunk in enumerate(np.array_split(df_to_insert, num_chunks)):
                        print(f"    -> 正在插入块 {i + 1}/{num_chunks} (包含 {len(chunk)} 行)...")
                        chunk.to_sql(
                            TABLE_NAME,
                            conn,
                            if_exists='append',
                            index=False
                        )
                    
                    print("\n[✓] 所有新数据块均已成功插入！事务将自动提交。")
                    # 当 with conn.begin() 块正常结束时，事务会自动提交
                    conn.commit()
                    print("[✓] 事务已显式提交！数据已永久保存。")

                    '''
                    # --- 步骤 5: 回读验证 (新增部分) ---
                    print("\n[*] 正在执行回读验证...")
                    
                    # a) 查询当前总行数
                    total_rows_query = f"SELECT COUNT(*) FROM {TABLE_NAME}"
                    total_rows = pd.read_sql(total_rows_query, conn).iloc[0, 0]
                    print(f"    -> 验证成功：数据库 '{TABLE_NAME}' 表中当前总行数为: {total_rows}")

                    # b) 查询并显示时间最晚的5条记录
                    latest_records_query = f"SELECT * FROM {TABLE_NAME} ORDER BY trade_time DESC LIMIT 5"
                    df_latest = pd.read_sql(latest_records_query, conn)
                    print("    -> 数据库中时间最晚的5条记录如下:")
                    print("-------------------------------------------------")
                    print(df_latest.to_string())
                    print("-------------------------------------------------")    
                    '''
            except Exception as e:
                    print(f"\n[✗] 错误：在插入过程中发生异常！")
                    print(f"    错误详情: {e}")
                    print(f"    事务将自动回滚，本次运行不会向数据库写入任何新数据。")
                    # 异常发生时，with conn.begin() 会自动回滚事务
                    # 可以选择重新抛出异常，让外部知道发生了错误
                    raise e
                    
    except KeyError as e:
        print(f"[✗] 错误: 缺少必要的列名 {e}。请检查Excel文件的列标题是否与预期一致。")
    except Exception as e:
        print(f"\n[✗] 程序发生未预料的严重错误: {e}")


    conn.close()
    engine.dispose()

def get_cb_stock_link_df(engine):
    print("开始获取CB_STOCK_LINK数据...")
    TABLE_NAME = 'CB_STOCK_LINK'
    query = f"SELECT code FROM {TABLE_NAME}"
    conn = engine.connect()
    df = pd.read_sql_table(TABLE_NAME,conn)
    print(f"获取到{len(df)}条数据")
    return df

def save_cb_tick_data(df,engine):

    table_name = 'CB_TICK'
    end_date = df['trade_date'].max()
    end_date = pd.to_datetime(end_date).strftime('%Y%m%d')
    # 计算start_date为end_date之前一年，格式为字符串
    start_date = (pd.to_datetime(end_date) - pd.DateOffset(years=1)).strftime('%Y%m%d')
    start_date = (datetime.now().replace(year=datetime.now().year - 1)).strftime('%Y%m%d')

    cs_link_df = df[df['trade_date'] == end_date]
    cb_codes = cs_link_df['code'].tolist()
    for cb_code in cb_codes:
        print(f"正在获取{cb_code}的tick数据...")
        xtdata.download_history_data(cb_code, 'tick', start_time=start_date, end_time=end_date)
        data = xtdata.get_market_data_ex([],[cb_code], 'tick', "","")
        #data['123200.SZ'].to_excel('test_tick.xlsx')
        df = data[cb_code]
        print(f"获取到{cb_code}的tick数据，存入数据库...")
        df = tickdata_pre_process(df,cb_code)
        tickdata_to_db(df,engine,table_name)

def main():

    password = quote('Happy$4ever')

            # 您要插入数据的表名
    #engine = create_engine(f'mysql+mysqlconnector://root:{password}@www.deepweiqi.cn:3306/KDATA')
    print("开始连接数据库...","192.168.8.78")
    engine = create_engine(f'mysql+mysqlconnector://root:{password}@192.168.8.78:3306/KDATA')

    df = get_cb_stock_link_df(engine)
    save_cb_tick_data(df,engine)

    ''' 

    xtdata.download_history_data("123200.SZ", 'tick', start_time='20230801', end_time='20250801')
    data = xtdata.get_market_data_ex([],['123200.SZ'], 'tick', "","")
    #data['123200.SZ'].to_excel('test_tick.xlsx')

    df = data['123200.SZ']
    df = tickdata_pre_process(df)
    tickdata_to_db(df,engine)
    '''

if __name__ == "__main__":
    main()