import os
import pandas as pd
from glob import glob

csv_dir = '/Users/dremind/Nutstore Files/.symlinks/坚果云/mfkQuant/历史行情数据/ETF期权历史行情'
pkl_dir = os.path.join(csv_dir, 'pkl文件')
os.makedirs(pkl_dir, exist_ok=True)

# 获取所有sh_option_risk开头的csv文件
csv_files = sorted(glob(os.path.join(csv_dir, 'sh_option_risk*.csv')))

# 合并所有csv为一个DataFrame
df_all = []
for file in csv_files:
    df = pd.read_csv(file, encoding='utf-8-sig')
    if 'date' not in df.columns or 'ETF代码' not in df.columns or '跟踪ETF' not in df.columns:
        continue
    df['date'] = pd.to_datetime(df['date'])
    df_all.append(df)
if not df_all:
    print("无有效数据")
    exit(0)
df_all = pd.concat(df_all, ignore_index=True)

# 统计每个ETF代码对应的最常见跟踪ETF
etf_map = (
    df_all.groupby(['ETF代码', '跟踪ETF'])
    .size()
    .reset_index(name='count')
    .sort_values(['ETF代码', 'count'], ascending=[True, False])
)
etfcode2name = etf_map.groupby('ETF代码').first()['跟踪ETF'].to_dict()

# 按年份和ETF代码分组并保存
for (year, etf_code), group in df_all.groupby([df_all['date'].dt.year, 'ETF代码']):
    etf_name = etfcode2name.get(etf_code, 'Unknown')
    etf_safe = str(etf_name).replace('/', '_').replace('\\', '_').replace(' ', '').replace('*', '')
    code_safe = str(etf_code).replace('/', '_').replace('\\', '_').replace(' ', '').replace('*', '')
    pkl_path = os.path.join(pkl_dir, f"sh_option_risk_{etf_safe}_{code_safe}_{year}.pkl")
    group.to_pickle(pkl_path)
    print(f"{year}年 {etf_name}({etf_code}) 数据已保存到 {pkl_path}")