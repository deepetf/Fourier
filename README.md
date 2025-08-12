## 项目简介

`Fourier` 项目中的 `save_tickdata.py` 用于批量抓取可转债/股票的行情数据（支持 tick 与 1 分钟线 1m），进行必要的字段预处理后，按分块方式写入 MySQL 数据库。

### 关键点概览
- **数据来源**: `xtquant.xtdata`
- **支持合约类型**: 可转债 `cb`、股票 `stock`
- **支持周期**: `tick`、`1m`
- **数据库**: MySQL（通过 SQLAlchemy 连接）
- **去重/校验**: 基于主键 `(code, trade_time)` 的重复检查（日志输出诊断）
- **插入策略**: 分块 `to_sql` 插入 + 正常提交/异常回滚


## 功能特性

- **批量下载并入库**
  - 从基表 `CB_STOCK_LINK` 读取标的清单（见下方表结构要求）。
  - 按传入的 `contract` 与 `period` 下载相应数据。
  - 执行预处理：
    - `tick` 数据：重命名索引为 `trade_time`、新增 `code` 列、删除不需要的列（如 `openInt`、`lastSettlementPrice`、`settlementPrice`）、将委买/委卖价量数组 JSON 序列化、重置索引。
    - `1m` 数据：重命名索引为 `trade_time`、新增 `code` 列、删除不需要的列、重置索引。
  - 分块插入到目标表（见“表路由”）。

- **表路由（contract × period → 表名）**
  - `cb × tick` → `CB_TICK`
  - `cb × 1m` → `CB_1M`
  - `stock × tick` → `STOCK_TICK`
  - `stock × 1m` → `STOCK_1M`


## 数据库要求与表结构

- 需要一个 MySQL 数据库（示例库名 `KDATA`）。
- 需要存在以下基础表：
  - `CB_STOCK_LINK`（作为标的清单来源），至少包含：
    - `code`（可转债代码，如 `123001.SZ`）
    - `code_stk`（正股代码，如 `000001.SZ`）
    - `trade_date`（交易日期，脚本会以该列的最大值作为当日，筛选当日的标的清单）

- 需要存在（或允许 `to_sql` 自动创建）如下数据表：
  - `CB_TICK`、`CB_1M`、`STOCK_TICK`、`STOCK_1M`
  - 若希望更严格的字段/索引控制，建议预建表，并在 `(code, trade_time)` 上建立唯一键以保障数据一致性。


## 环境依赖

请使用 Python 3.9+，并安装以下依赖：

```bash
pip install pandas numpy sqlalchemy mysql-connector-python pyyaml
```

此外需要已安装并正确配置的 `xtquant` 环境，以保证 `from xtquant import xtdata` 可用。


## 配置

- 数据库连接配置位于 `save_tickdata.py` 的 `main()`：

```python
# 密码示例（请按需修改或改为读取环境变量）
password = quote('Happy$4ever')

# 主机地址（按需切换）：
host = '192.168.8.78'          # NUC 使用
# host = 'www.deepweiqi.cn'    # 笔记本使用

engine = create_engine(
    f'mysql+mysqlconnector://root:{password}@{host}:3306/KDATA'
)
```

- 建议将敏感信息（数据库密码、主机等）改为用环境变量读取，或配置文件方式加载。


## 使用方法

脚本提供命令行参数用于选择合约类型与周期。

- **参数**
  - `--contract {cb, stock}`：合约类型，默认 `cb`
  - `--period {tick, 1m}`：周期，默认 `1m`

- **运行示例**
  - 使用默认（`cb + 1m`）：
    ```bash
    python save_tickdata.py
    ```
  - 指定可转债 tick：
    ```bash
    python save_tickdata.py --contract cb --period tick
    ```
  - 指定股票 1m：
    ```bash
    python save_tickdata.py --contract stock --period 1m
    ```


## 运行流程摘要

1. 连接 MySQL（在 `main()` 中创建一次连接，并在结束统一关闭）。
2. 从 `CB_STOCK_LINK` 读取标的清单，选择 `trade_date` 最大值对应日期的标的。
3. 按 `contract` 与 `period` 下载数据（时间跨度默认为：`end_date` 前推一年）。
4. 预处理数据（规范 `trade_time`、`code`、列清洗等）。
5. 分块写入目标表；正常提交，异常回滚。


## 常见问题

- **锁等待或连接报错**
  - 若出现数据库锁等待或“需要先回滚再重连”类错误，请检查是否有长事务未提交；建议在目标表上建立合理索引，或降低并发。

- **字段不一致/找不到列**
  - 若报缺少列，请核对 `CB_STOCK_LINK` 是否包含 `code`、`code_stk`、`trade_date`，以及返回的数据列是否与预处理函数中删除的列名一致。

- **时区与时间格式**
  - 代码将 `trade_time` 统一格式化为 `%Y-%m-%d %H:%M:%S` 字符串；如需时区处理或毫秒精度，可在预处理中调整。


## 目录结构（节选）

```
Fourier/
  save_tickdata.py        # 主脚本（下载+预处理+入库）
  CreatCBStockLink.py     # 生成/维护 CB_STOCK_LINK 的辅助脚本
  CB_STOCK_LINK.xlsx      # 可能的参考/样例数据
  Wavelet_Test/           # 其他实验性内容
```


## 免责声明

本脚本用于研究与数据处理示例，请在合法合规前提下使用，且务必对数据库写入的表结构、索引、权限进行审慎管理。


