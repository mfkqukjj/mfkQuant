from package.getOptionData import opDataFetcher
mk=opDataFetcher()


# 测试获取期货数据

df_posi_rank = mk.get_position_rank('IF', start_date='20250627', end_date='20250627')
print("期货数据示例：")
print(df_posi_rank)

# 测试获取期权数据
df_opt = mk.get_op_data('IO','202501','202503')
print("期权数据示例：")
print(df_opt)

df=mk.get_etf_op_data(symbol="50ETF", start_date="20250629", end_date="20250702")