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
import argparse



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


def data_pre_process_tick(df,code):
    df.index = df.index.rename('trade_time')
    df['code'] = code

    df = df.drop(columns=['openInt', 'lastSettlementPrice', 'settlementPrice'])
    df.reset_index(inplace=True)

    cols_to_convert = ['askPrice', 'bidPrice', 'askVol', 'bidVol']

    for col in cols_to_convert:
        df[col] = df[col].apply(json.dumps)

    return df

def data_pre_process_1m(df,code):
    df.index = df.index.rename('trade_time')
    df['code'] = code
    df = df.drop(columns=[ 'settelementPrice'])
    
    df.reset_index(inplace=True)

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



def tick_1m_data_to_db(df_source, conn, period, contract):
    

    if period == 'tick':
        CHUNK_SIZE = 2000                   # 每次插入的行数，可根据内存和网络情况调整
        if contract == 'cb':
            TABLE_NAME = 'CB_TICK'
        elif contract == 'stock':
            TABLE_NAME = 'STOCK_TICK'

    elif period == '1m':
        CHUNK_SIZE = 3000                   # 每次插入的行数，可根据内存和网络情况调整
        if contract == 'cb':
            TABLE_NAME = 'CB_1M'
        elif contract == 'stock':
            TABLE_NAME = 'STOCK_1M'

    try:

        '''
        # 获取源数据中的真实列名，并清理可能存在的前后空格
        df_source.columns = df_source.columns.str.strip()
        actual_columns = list(df_source.columns)
        #print(f"[*] 源数据包含的列: {actual_columns}")
        '''
        # --- 步骤 2: 数据预处理 (关键步骤) ---
        # 统一主键的数据类型和格式，为合并做准备。
        
        print("[*] 正在进行数据预处理...")
        
        # a) 强制将 trade_time 列转换为标准字符串格式，消除任何格式差异
        df_source['trade_time'] = pd.to_datetime(df_source['trade_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # b) 清理主键列的前后空格，确保匹配的精确性
        df_source['code'] = df_source['code'].str.strip()
        df_source['trade_time'] = df_source['trade_time'].str.strip()

        print(f"\n[*] {len(df_source)} 行新记录，准备开始分块插入...")
       
        # 只选择源文件中真实存在的列进行插入
        df_to_insert = df_source
        # 检查是否有重复键值
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
            conn.commit()
            print("[✓] 事务已显式提交！数据已永久保存。")

        except Exception as e:
            print(f"\n[✗] 错误：在插入过程中发生异常！")
            print(f"    错误详情: {e}")
            print(f"    事务将自动回滚，本次运行不会向数据库写入任何新数据。")
            try:
                conn.rollback()
            except Exception:
                pass
            raise e
                    
    except KeyError as e:
        print(f"[✗] 错误: 缺少必要的列名 {e}。请检查Excel文件的列标题是否与预期一致。")
    except Exception as e:
        print(f"\n[✗] 程序发生未预料的严重错误: {e}")


    # 连接与引擎的生命周期由调用方管理，这里不做关闭

def get_cb_stock_link_df(conn):
    print("开始获取CB_STOCK_LINK数据...")
    TABLE_NAME = 'CB_STOCK_LINK'
    query = f"SELECT code FROM {TABLE_NAME}"
    df = pd.read_sql_table(TABLE_NAME, conn)
    print(f"获取到{len(df)}条数据")
    return df

def save_cb_tick_1m_data(df, conn, period):

    # period 支持 'tick' 与 '1m'
    end_date = df['trade_date'].max()
    end_date = pd.to_datetime(end_date).strftime('%Y%m%d')
    # 计算start_date为end_date之前一年，格式为字符串
    start_date = (pd.to_datetime(end_date) - pd.DateOffset(years=1)).strftime('%Y%m%d')
    start_date = (datetime.now().replace(year=datetime.now().year - 1)).strftime('%Y%m%d')

    cs_link_df = df[df['trade_date'] == end_date]
    cb_codes = cs_link_df['code'].tolist()
    for cb_code in cb_codes:
        print(f"正在获取{cb_code}的{period}数据...")
        xtdata.download_history_data(cb_code, period, start_time=start_date, end_time=end_date)
        data = xtdata.get_market_data_ex([],[cb_code], period, "","")
        #data['123200.SZ'].to_excel('test_tick.xlsx')
        df = data[cb_code]
        print(f"获取到{cb_code}的{period}数据，存入数据库...")
        if period == 'tick':
            df = data_pre_process_tick(df, cb_code)
        elif period == '1m':
            df = data_pre_process_1m(df, cb_code)
        tick_1m_data_to_db(df, conn, period,'cb')

def parse_args():
    parser = argparse.ArgumentParser(description='Save market data to DB')
    parser.add_argument('--contract', choices=['cb', 'stock'], default='cb', help='合约类型: cb 或 stock')
    parser.add_argument('--period', choices=['tick', '1m'], default='1m', help='周期: tick 或 1m')
    return parser.parse_args()

def main():
    args = parse_args()
    contract = args.contract
    period = args.period

    password = quote('Happy$4ever')

    #NUC 使用
    host =  '192.168.8.78'    

    #笔记本使用
    #host = 'www.deepweiqi.cn'
    
    print("开始连接数据库...", host)
    engine = create_engine(f'mysql+mysqlconnector://root:{password}@{host}:3306/KDATA')

    conn = engine.connect()
    try:
        if contract == 'cb':
            df = get_cb_stock_link_df(conn)
            save_cb_tick_1m_data(df, conn, period)
        else:
            print(f"[!] 合约类型 '{contract}' 的处理暂未实现，已跳过。")
    finally:
        conn.close()
        engine.dispose()

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