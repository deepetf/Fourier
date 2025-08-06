import mysql.connector
import pandas as pd
import sys
from sqlalchemy import create_engine
from urllib.parse import quote

# --- 配置 ---
# 请在这里填入您的数据库连接信息
# 编码连接字符串中的特殊字符
password = quote('Happy$4ever')
DATABASE_CONNECTION_STRING = "mysql+mysqlconnector://root:{password}@192.168.8.78:3306/CB_HISTORY" # 示例，请替换
# ========================================================================

EXCEL_FILE_NAME = 'test_tick.xlsx'
TABLE_NAME = 'CB_TICK' # 您要插入的表名
LAST_SUCCESSFUL_TIME = '20250801133827'

def find_failing_row_one_by_one(df_to_check, engine):
    """
    通过逐行插入并自动提交的方式，找到导致失败的确切行。
    """
    print(f"\n--- 开始对 {len(df_to_check)} 行数据进行逐行独立插入测试 ---")
    
    # 使用一个支持 autocommit 的连接
    with engine.connect().execution_options(autocommit=True) as conn:
        for index, row in df_to_check.iterrows():
            # 将单行数据转换为一个小的 DataFrame
            single_row_df = pd.DataFrame([row])
            
            try:
                # 尝试只插入这一行
                single_row_df.to_sql(TABLE_NAME, conn, if_exists='append', index=False)
                # 打印进度，方便观察
                if (index + 1) % 50 == 0:
                    print(f"已成功插入 {index + 1 - df_to_check.index[0]} 行...")

            except Exception as e:
                # 一旦出错，这里就是问题的根源！
                print("\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                print(f"!!! 定位到最终的失败行 !!!")
                print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                print(f"\n【失败位置】")
                print(f"  - Excel中的行号（大约）: {index + 2}")
                print(f"  - DataFrame中的索引: {index}")
                
                print("\n【数据库返回的原始错误信息】")
                print(f"  - 错误类型: {type(e).__name__}")
                print(f"  - 错误详情: {e}")
                
                print("\n--- 导致失败的完整行数据如下 ---")
                print(single_row_df.to_string())
                print("\n请仔细检查这行数据，特别是其中的特殊字符或格式。")
                return # 找到后即停止

    print("\n--- 诊断完成 ---")
    print("✅ 所有行都已成功独立插入。请检查数据库确认。")


# --- 主程序 ---
try:
    print("--- 终极诊断脚本 (逐行炮轰) 正在运行 ---")
    
    # 编码连接字符串中的特殊字符
    password = quote('Happy$4ever')


    table_name = 'CB_TICK'

    engine = create_engine(f'mysql+mysqlconnector://root:{password}@192.168.8.78:3306/CB_HISTORY')

    # 创建数据库引擎
    #engine = create_engine(DATABASE_CONNECTION_STRING)
    
    print(f"\n[步骤 1/3] 正在读取文件: '{EXCEL_FILE_NAME}'...")
    df = pd.read_excel(EXCEL_FILE_NAME, sheet_name=0)
    print(f"文件加载成功。")

    # 清理列名，去除可能的前后空格
    df.columns = df.columns.str.strip()

    df['trade_time'] = df['trade_time'].astype(str)
    start_index = df[df['trade_time'] == LAST_SUCCESSFUL_TIME].index[0] + 1
    print(f"\n[步骤 2/3] 将从索引 {start_index} 开始检查...")
    df_to_check = df.iloc[start_index:].copy()

    # 只保留与数据库表匹配的列
    # 这是根据您之前运行脚本的输出得到的真实列名
    actual_columns = ['code', 'trade_time', 'time', 'lastPrice', 'open', 'high', 'low', 'lastClose', 'amount', 'volume', 'pvolume', 'tickvol', 'stockStatus', 'openInt', 'lastSettlementPrice', 'askPrice', 'bidPrice', 'askVol', 'bidVol', 'settlementPrice', 'transactionNum', 'pe']
    df_to_check = df_to_check[actual_columns]

    print(f"\n[步骤 3/3] 开始执行逐行插入测试...")
    find_failing_row_one_by_one(df_to_check, engine)

except Exception as e:
    print(f"\n发生了未预料的严重错误: {e}")