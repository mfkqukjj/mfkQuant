# mfkQuant 使用说明

## foDataFetcher 简介

`foDataFetcher` 是一个用于获取中金所、上交所、深交所期货/期权持仓排名、历史行情、ETF期权风险指标等数据的 Python 类。支持自动下载、解压、合并、清洗数据，输出标准化的 pandas DataFrame。

---

## 接口功能总览

| 接口函数名                    | 数据来源 | 数据类型         | 主要输入参数                                   | 主要输出字段示例                                  |
|------------------------------|-------------|------------------|------------------------------------------------|---------------------------------------------------|
| get_cffex_position_rank      | 中金所      | 期货/期权持仓排名 | symbol, start_date, end_date                   | date, 合约类型, 合约代码, 排名, 成交量, 买单, 卖单等 |
| get_cffex_trade_data            | 中金所      | 期权历史行情     | symbol, start_date, end_date                   | date, 合约代码, symbol, 其余行情字段               |
| get_sh_option_risk           | 上交所      | ETF期权风险指标   | symbol, start_date, end_date                   | date, 合约简称, 交易代码, 跟踪ETF, ETF代码, 多空类型等 |
| get_sz_option_risk           | 深交所      | ETF期权风险指标   | start_date, end_date                           | date, 合约简称, 合约代码, 跟踪ETF, ETF代码, 多空类型等 |
| get_sz_etf_op_market         | 深交所      | ETF期权市场持仓   | start_date, end_date                           | date, 深交所日度持仓相关字段                       |

- **symbol** 参数详见各函数说明，支持如 'IF', 'IO', '50ETF' 等。
- **start_date/end_date** 格式为 `YYYYMMDD` 或 `YYYYMM`，详见各函数说明。
- 所有接口输出均为 pandas DataFrame，字段自动标准化。

---

## 主要接口与功能

### 1. 期货/期权持仓排名数据

- **函数**：`get_cffex_position_rank(symbol, start_date=None, end_date=None)`
- **数据来源**：中金所官网  
  `http://www.cffex.com.cn/sj/ccpm/{ym}/{day}/{symbol}_1.csv`
- **参数说明**：
  - `symbol`：品种代码，如 `'IF'`, `'IC'`, `'IM'`, `'IH'`, `'IO'`, `'MO'`, `'HO'` 等
  - `start_date`/`end_date`：字符串，格式为 `YYYYMMDD`，默认最近30天
- **输出**：DataFrame，含日期、合约类型、合约代码、排名、成交量、买单、卖单等字段

---

### 2. 中金所期权历史行情数据

- **函数**：`get_cffex_trade_data(symbol=None, start_date=None, end_date=None)`
- **数据来源**：中金所官网  
  `http://www.cffex.com.cn/sj/historysj/{ym}/zip/{ym}.zip`
- **参数说明**：
  - `symbol`：期权品种（如 `'IO'`, `'MO'`, `'HO'`），不填则保留全部（剔除“小计”“合计”）
  - `start_date`/`end_date`：字符串，格式为 `YYYYMM`，默认上月和本月
- **输出**：DataFrame，含日期、合约代码、symbol（自动提取）、其余行情字段

---

### 3. 上交所ETF期权风险指标

- **函数**：`get_sh_option_risk(symbol="全部", start_date=None, end_date=None)`
- **数据来源**：上交所官网  
  `https://query.sse.com.cn/derivative/downloadRisk.do?trade_date={YYYYMMDD}&productType={symbol}`
- **参数说明**：
  - `symbol`：ETF品种（"全部"、"50ETF"、"300ETF"、"500ETF"、"科创50"、"科创板50"），默认"全部"
  - `start_date`/`end_date`：字符串，格式为 `YYYYMMDD`，默认最近30天
- **输出**：DataFrame，含日期、合约简称、交易代码、跟踪ETF、ETF代码、多空类型等

---

### 4. 深交所ETF期权风险指标

- **函数**：`get_sz_option_risk(start_date=None, end_date=None)`
- **数据来源**：深交所官网  
  `https://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=option_hyfxzb&TABKEY=tab1&txtSearchDate={YYYY-MM-DD}`
- **参数说明**：
  - `start_date`/`end_date`：字符串，格式为 `YYYYMMDD`，默认最近30天
- **输出**：DataFrame，含日期、合约简称、合约代码、跟踪ETF、ETF代码、多空类型等

---

### 5. 深交所ETF期权市场日度持仓统计

- **函数**：`get_sz_etf_op_market(start_date=None, end_date=None)`
- **数据来源**：深交所官网  
  `https://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=ysprdzb&TABKEY=tab1&txtQueryDate={YYYY-MM-DD}`
- **参数说明**：
  - `start_date`/`end_date`：字符串，格式为 `YYYYMMDD`，默认最近30天
- **输出**：DataFrame，含日期及深交所日度持仓相关字段

---

## 输出格式

所有接口均返回 pandas DataFrame，字段自动标准化，字符串字段去除首尾空白。部分接口自动新增：
- `symbol`：合约代码前2字符中的英文部分
- `跟踪ETF`：合约简称中“购”或“沽”前的部分
- `ETF代码`：交易代码前6位
- `多空类型`：合约简称含“购”或“多”为“购”，含“沽”或“空”为“沽”

---

## 示例代码

```python
from package.getOptionData import foDataFetcher

fetcher = foDataFetcher()

# 获取中金所期货/期权持仓排名数据
df_rank = fetcher.get_cffex_position_rank('IF', start_date='20250601', end_date='20250605')

# 获取中金所期权历史行情数据
df_op = fetcher.get_cffex_trade_data(symbol='IO', start_date='202406', end_date='202407')

# 获取上交所ETF期权风险指标
df_sh = fetcher.get_sh_option_risk(symbol="50ETF", start_date="20250601", end_date="20250605")

# 获取深交所ETF期权风险指标
df_sz_risk = fetcher.get_sz_option_risk(start_date="20250601", end_date="20250605")

# 获取深交所ETF期权市场每日持仓数据
df_sz_market = fetcher.get_sz_etf_op_market(start_date="20250601", end_date="20250605")
```

---

## 依赖

- pandas
- requests
- tqdm
- zipfile（标准库）
- openpyxl

---

## 注意事项

- 网络接口数据如有变动，需适当调整字段或URL。
- 建议在国内网络环境下使用，部分接口可能需科学上网。
- 若遇到 openpyxl 的 warning，可忽略，不影响数据读取。

---