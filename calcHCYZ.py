import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from package.readLocalData import LocalMarketData
from tqdm import tqdm

# 1. 读取数据
base_dir = '/Users/dremind/Nutstore Files/.symlinks/坚果云/mfkQuant/Parquet数据/Astock_hfq_parquet_by_year'
market_data = LocalMarketData(base_dir)
cols = ['date', 'code', 'open', 'close']
df = market_data.read_hfq_data(2024, 2025, columns=cols)

# 2. 计算returns（简单收益率）
df = df.sort_values(['code', 'date'])
df['returns'] = df.groupby('code')['close'].pct_change()

# 3. 计算sum(open, 5)和sum(returns, 5)
df['sum_open_5'] = df.groupby('code')['open'].rolling(window=5, min_periods=5).sum().reset_index(level=0, drop=True)
df['sum_ret_5'] = df.groupby('code')['returns'].rolling(window=5, min_periods=5).sum().reset_index(level=0, drop=True)
df['prod_sum'] = df['sum_open_5'] * df['sum_ret_5']

# 4. delay(prod_sum, 10)
df['prod_sum_delay10'] = df.groupby('code')['prod_sum'].shift(10)

# 5. alpha因子
df['alpha_custom_001'] = -1 * df.groupby('date').apply(
    lambda x: x['prod_sum'] - x['prod_sum_delay10']
).reset_index(level=0, drop=True).groupby(df['date']).rank()

# 6. 计算后N天收益率
N_list = [1,2,3,4,5,7,10,12,15,20,30,45,60]
df['open_t1'] = df.groupby('code')['open'].shift(-1)
for N in N_list:
    df[f'close_t{N}'] = df.groupby('code')['close'].shift(-N)
    df[f'ret_t{N}'] = (df[f'close_t{N}'] / df['open_t1'] - 1).round(4)

# 7. 整理后N天分组和收益率为数组
df['后N天分组'] = [N_list] * len(df)
df['后N天收益率'] = df[[f'ret_t{N}' for N in N_list]].values.tolist()

# 8. 计算IC和IR
def calc_ic_ir(sub_df):
    # 取未来5日收益率与因子做IC
    if sub_df['alpha_custom_001'].isnull().all():
        return pd.Series({'IC': np.nan, 'IR': np.nan})
    ic = sub_df['alpha_custom_001'].corr(sub_df['ret_t5'])
    ir = ic / sub_df['ret_t5'].std() if sub_df['ret_t5'].std() != 0 else np.nan
    return pd.Series({'IC': ic, 'IR': ir})

ic_ir_df = df.groupby('date').apply(calc_ic_ir).reset_index()
df = df.merge(ic_ir_df, on='date', how='left')

# 9. 生成最终结果表
result = df[['date', 'code', 'alpha_custom_001', 'IC', 'IR', '后N天分组', '后N天收益率']].copy()
result['策略因子ID'] = 'alpha_custom_001'
result['入库日期'] = datetime.now().strftime('%Y-%m-%d')

# 10. 调整字段顺序
result = result[['date', 'code', '策略因子ID', 'alpha_custom_001', 'IC', 'IR', '后N天分组', '后N天收益率', '入库日期']]

# 11. 输出结果
result.to_parquet('/Users/dremind/Nutstore Files/.symlinks/坚果云/mfkQuant/alpha_custom_001_result.parquet', index=False)
print(result.head())