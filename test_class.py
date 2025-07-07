from package.getOptionData import foDataFetcher

mk = foDataFetcher()

# 1. 获取中金所期货/期权持仓排名数据
df = mk.get_cffex_position_rank(symbol='IF', start_date='20250701', end_date='20250705')
print("中金所持仓排名数据：")
print(df.head())

# 1.1 整理排名数据到不同样式
from package.foDataProcessor import FoDataProcessor
dp=FoDataProcessor()
# 按公司、合约、统计类型（成交量/持买单量/持卖单量）转化为结构化数据
df_1=dp.process_by_company(df)
# 按公司、合约类型转为行数据
df_2=dp.process_original_format(df)

# 2. 获取中金所期权\期货历史行情数据
df = mk.get_cffex_trade_data(symbol='IO', start_date='202406', end_date='202407')
print("中金所期权历史行情数据：")
print(df.head())

# 3. 获取上交所ETF期权风险指标
df = mk.get_sh_option_risk(start_date='20250601', end_date='20250605')
print("上交所ETF期权风险指标：")
print(df.head())

# 3.1 获取上交所ETF期权风险指标（指定ETF）
df = mk.get_sh_option_risk(symbol='50ETF', start_date='20250601', end_date='20250605')


# 4. 获取深交所ETF期权风险指标
df = mk.get_sz_option_risk(start_date='20250601', end_date='20250605')
print("深交所ETF期权风险指标：")
print(df.head())

# 5. 获取深交所ETF期权市场每日持仓数据
df = mk.get_sz_etf_op_market(start_date='20250601', end_date='20250605')
print("深交所ETF期权市场每日持仓数据：")
print(df.head())


