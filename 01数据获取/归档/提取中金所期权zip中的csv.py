import os
import zipfile
import pandas as pd
from tqdm import tqdm

save_dir = '/Users/dremind/Documents/量化体系搭建/期权数据/中金所期权历史行情'
output_parquet = os.path.join(save_dir, 'cffex_option_all.parquet')

all_zips = [f for f in os.listdir(save_dir) if f.endswith('.zip')]
all_dfs = []

for zip_name in tqdm(all_zips, desc="解压并读取csv"):
    zip_path = os.path.join(save_dir, zip_name)
    with zipfile.ZipFile(zip_path, 'r') as zf:
        for csv_file in zf.namelist():
            if csv_file.endswith('.csv'):
                with zf.open(csv_file) as f:
                    df = None
                    for enc in ['utf-8', 'gbk', 'latin1']:
                        try:
                            df = pd.read_csv(f, encoding=enc)
                            break
                        except pd.errors.EmptyDataError:
                            print(f"警告：{csv_file} 文件为空，已跳过。")
                            df = None
                            break
                        except Exception:
                            f.seek(0)  # 重新定位到文件开头
                            continue
                    if df is not None and not df.empty:
                        base_name = os.path.basename(csv_file)
                        date_part = base_name.split('_')[0]
                        df['date'] = date_part
                        all_dfs.append(df)
                    elif df is None:
                        print(f"警告：{csv_file} 读取失败，已跳过。")

if not all_dfs:
    print("没有找到任何csv文件。")
else:
    all_df = pd.concat(all_dfs, ignore_index=True)
    if '合约代码' not in all_df.columns:
        print("警告：未找到'合约代码'字段，请检查csv文件结构。")
    all_df = all_df.set_index(['date', '合约代码'])
    all_df.to_parquet(output_parquet)
    print(f"全部数据已合并并保存为Parquet：{output_parquet}")