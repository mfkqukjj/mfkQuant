# mfkQuant 使用说明

## opDataFetcher 简介

`opDataFetcher` 是一个用于获取中金所、上交所、深交所期货/期权持仓排名、历史行情、ETF期权风险指标等数据的 Python 类。支持自动下载、解压、合并、清洗数据，输出标准化的 pandas DataFrame。

---

## 主要接口与功能

### 1. 期货/期权持仓排名数据

- **函数**：`get_position_rank(symbol, start_date=None, end_date=None)`
- **数据来源**：中金所官网  
  `http://www.cffex.com.cn/sj/ccpm/{ym}/{day}/{symbol}_1.csv`
- **参数说明**：
  - `symbol`：品种代码，如 `'IF'`, `'IC'`, `'IM'`, `'IH'`, `'IO'`, `'MO'`, `'HO'` 等
  - `start_date`/`end_date`：字符串，格式为 `YYYYMMDD`，默认最近30天
- **输出**：DataFrame，含日期、合约类型、合约代码、排名、成交量、买单、卖单等字段

---

### 2. 中金所期权历史行情数据

- **函数**：`get_op_data(symbol=None, start_date=None, end_date=None)`
- **数据来源**：中金所官网  
  `http://www.cffex.com.cn/sj/historysj/{ym}/zip/{ym}.zip`
- **参数说明**：
  - `symbol`：期权品种（如 `'IO'`, `'MO'`, `'HO'`），不填则保留全部（剔除“小计”“合计”）
  - `start_date`/`end_date`：字符串，格式为 `YYYYMM`，默认上月和本月
- **输出**：DataFrame，含日期、合约代码、symbol（自动提取）、其余行情字段

---

### 3. 上交所ETF期权风险指标

- **函数**：`get_etf_op_data(symbol="全部", start_date=None, end_date=None)`
- **数据来源**：上交所官网  
  `https://query.sse.com.cn/derivative/downloadRisk.do?trade_date={YYYYMMDD}&productType={symbol}`
- **参数说明**：
  - `symbol`：ETF品种（"全部"、"50ETF"、"300ETF"、"500ETF"、"科创50"、"科创板50"），默认"全部"
  - `start_date`/`end_date`：字符串，格式为 `YYYYMMDD`，默认最近30天
- **输出**：DataFrame，含日期、合约简称、交易代码、跟踪ETF、ETF代码、多空类型等

---

### 4. 深交所ETF期权市场日度持仓统计

- **函数**：`get_etf_op_market_sz(start_date=None, end_date=None)`
- **数据来源**：深交所官网  
  `https://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=ysprdzb&TABKEY=tab1&txtQueryDate={YYYY-MM-DD}`
- **参数说明**：
  - `start_date`/`end_date`：字符串，格式为 `YYYYMMDD`，默认最近30天
- **输出**：DataFrame，含日期及深交所日度持仓相关字段

---

### 5. 深交所ETF期权风险指标

- **函数**：`get_sz_option_risk(start_date=None, end_date=None)`
- **数据来源**：深交所官网  
  `https://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=option_hyfxzb&TABKEY=tab1&txtSearchDate={YYYY-MM-DD}`
- **参数说明**：
  - `start_date`/`end_date`：字符串，格式为 `YYYYMMDD`，默认最近30天
- **输出**：DataFrame，含日期、合约简称、合约代码、跟踪ETF、ETF代码、多空类型等

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
from package.getOptionData import o'pDataFetcher

fetcher = opDataFetcher()

# 获取中金所期货持仓排名
df_fut = fetcher.get_position_rank('IF', start_date='20250601', end_date='20250625')

# 获取中金所期权历史行情
df_op = fetcher.get_op_data(symbol='IO', start_date='202406', end_date='202407')

# 获取上交所ETF期权风险指标
df_etf = fetcher.get_etf_op_data(symbol="50ETF", start_date="20250625", end_date="20250702")

# 获取深交所ETF期权市场日度持仓统计
df_sz = fetcher.get_etf_op_market_sz(start_date="20250625", end_date="20250702")

# 获取深交所ETF期权风险指标
df_sz_risk = fetcher.get_sz_option_risk(start_date="20250625", end_date="20250702")
```

---

## 依赖

- pandas
- requests
- tqdm
- zipfile
- openpyxl

---

## 注意事项

- 网络接口数据如有变动，需适当调整字段或URL。
- 建议在国内网络环境下使用，部分接口可能需科学上网。
- 若遇到 openpyxl 的 warning，可忽略，不影响数据读取。

---