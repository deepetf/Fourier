import pandas as pd
import sys

# --- 配置 ---
EXCEL_FILE_NAME = 'test_tick.xlsx'
LAST_SUCCESSFUL_TIME = '20250801133827'

# 根据您Excel文件中的【真实列名】修正后的SCHEMA
# 注意：这里已经删除了不存在的 'stk_code' 等列
SCHEMA = {
    'code': {'type': 'string', 'limit': 10},
    'trade_time': {'type': 'string', 'limit': 16},
    'time': {'type': 'numeric'},
    'lastPrice': {'type': 'numeric'},
    'open': {'type': 'numeric'},
    'high': {'type': 'numeric'},
    'low': {'type': 'numeric'},
    'lastClose': {'type': 'numeric'},
    'amount': {'type': 'numeric'},
    'volume': {'type': 'numeric'},
    'pvolume': {'type': 'numeric'},
    'tickvol': {'type': 'numeric'},
    'stockStatus': {'type': 'numeric'},
    'openInt': {'type': 'numeric'},
    'lastSettlementPrice': {'type': 'numeric'},
    'askPrice': {'type': 'string', 'limit': 255},
    'bidPrice': {'type': 'string', 'limit': 255},
    'askVol': {'type': 'string', 'limit': 255},
    'bidVol': {'type': 'string', 'limit': 255},
    'settlementPrice': {'type': 'numeric'},
    'transactionNum': {'type': 'numeric'},
    'pe': {'type': 'numeric'}
}

def print_error_and_exit(error_type, index, col_name, value, full_row, limit=None):
    """打印详细的错误信息并退出程序。"""
    print("\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    print(f"!!! 定位到错误：{error_type} !!!")
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    print(f"\n【错误位置】")
    print(f"  - Excel中的行号（大约）: {index + 2}")
    print(f"  - DataFrame中的索引: {index}")
    print(f"  - 出错的列名: '{col_name}'")
    
    print("\n【错误详情】")
    if limit is not None:
        print(f"  - 数据库限制: 长度不能超过 {limit}")
        print(f"  - 实际数据的长度: {len(str(value))}")
    
    print(f"  - 出错的具体数据: '{value}' (数据类型: {type(value).__name__})")
    
    print("\n--- 导致错误的完整行数据如下 ---")
    print(full_row.to_string())
    print("\n请修正此行数据后重试。")
    sys.exit()

# --- 主程序 ---
try:
    print("--- 修正版诊断脚本正在运行 ---")
    print(f"\n[步骤 1/3] 正在读取文件: '{EXCEL_FILE_NAME}'...")
    df = pd.read_excel(EXCEL_FILE_NAME, sheet_name=0)
    print(f"文件加载成功，共 {len(df)} 行。")

    df['trade_time'] = df['trade_time'].astype(str)
    start_index = df[df['trade_time'] == LAST_SUCCESSFUL_TIME].index[0] + 1
    print(f"\n[步骤 2/3] 将从索引 {start_index} 开始检查数据...")
    df_to_check = df.iloc[start_index:].copy()

    print(f"\n[步骤 3/3] 开始对 {len(df_to_check)} 行数据进行逐行、逐列的内容检查...")
    for index, row in df_to_check.iterrows():
        for col_name, constraints in SCHEMA.items():
            value = row[col_name]
            if constraints['type'] == 'string' and value is not None and not pd.isna(value):
                if len(str(value)) > constraints['limit']:
                    print_error_and_exit("字符串超长", index, col_name, value, row, constraints['limit'])
            if constraints['type'] == 'numeric' and isinstance(value, str):
                try:
                    float(value)
                except ValueError:
                    print_error_and_exit("数值格式错误", index, col_name, value, row)
    
    print("\n--- 诊断完成 ---")
    print("✅ 在所有自动化检查中，未发现明显的数据内容错误。")

except Exception as e:
    print(f"发生了未预料的错误: {e}")