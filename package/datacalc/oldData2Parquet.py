import os
import glob
import pandas as pd
from tqdm import tqdm
import shutil

data_dir = '/Users/dremind/Documents/量化体系搭建/PKL格式数据'
output_dir = '/Users/dremind/Nutstore Files/.symlinks/坚果云/mfkQuant/Parquet数据'
parquet_dir = os.path.join(output_dir, 'parquet_by_year')

# 清空parquet_dir目录
if os.path.exists(parquet_dir):
    shutil.rmtree(parquet_dir)
os.makedirs(parquet_dir, exist_ok=True)

pkl_files = glob.glob(os.path.join(data_dir, '*.pkl'))
batch_size = 100
stock_dates = []

def process_files(files):
    dfs = []
    for file in files:
        df = pd.read_pickle(file)
        code = os.path.basename(file)[:6]
        df['code'] = str(code)
        # 检查'date'是否在列中
        if 'date' not in df.columns and 'date' not in df.index.names:
            print(f"文件 {file} 缺少'date'列，已跳过。")
            continue
        if 'date' not in df.index.names:
            df = df.set_index('date')
        stock_dates.append({
            'code': code,
            'min_date': df.index.min(),
            'max_date': df.index.max()
        })
        dfs.append(df)
    if dfs:
        batch_df = pd.concat(dfs)
        batch_df = batch_df.reset_index().set_index(['date', 'code'])
        batch_df = batch_df.reset_index()
        batch_df['year'] = pd.to_datetime(batch_df['date']).dt.year.astype(str)
        return batch_df
    return None

for i in tqdm(range(0, len(pkl_files), batch_size)):
    batch_files = pkl_files[i:i+batch_size]
    batch_df = process_files(batch_files)
    if batch_df is not None:
        batch_df.to_parquet(
            parquet_dir,
            partition_cols=['year'],
            index=False,
            engine='pyarrow'
        )

# 保存股票日期范围
stock_dates_df = pd.DataFrame(stock_dates)
stock_dates_df = stock_dates_df.sort_values('code')
stock_dates_df.to_csv(os.path.join(output_dir, 'stock_date_range.csv'), index=False)

print("数据已分批汇总并按年份分区保存为parquet，股票日期范围已保存为CSV。")