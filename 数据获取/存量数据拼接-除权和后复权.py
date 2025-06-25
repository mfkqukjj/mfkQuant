import os
import pandas as pd
from tqdm import tqdm

parquet_dir = r"f:\金融数据\A股历史行情数据（除权）"
hfq_dir = r"f:\金融数据\A股历史行情数据（后复权）"
output_dir = r"f:\金融数据\A股历史行情数据（合并）"
os.makedirs(output_dir, exist_ok=True)

# 获取所有parquet文件及其大小，并按大小排序
parquet_files = [
    (f, os.path.getsize(os.path.join(parquet_dir, f)))
    for f in os.listdir(parquet_dir) if f.endswith('.parquet')
]
parquet_files.sort(key=lambda x: x[1])  # 按文件大小升序排序

for parquet_file, _ in tqdm(parquet_files, desc="处理parquet文件"):
    parquet_path = os.path.join(parquet_dir, parquet_file)
    df_parquet = pd.read_parquet(parquet_path).reset_index()
    # 只保留close字段并重命名
    df_parquet = df_parquet.copy()
    df_parquet = df_parquet.drop(columns=[col for col in ['open', 'high', 'low'] if col in df_parquet.columns])
    if 'close' in df_parquet.columns:
        df_parquet = df_parquet.rename(columns={'close': 'close_Ex_right'})
    else:
        continue  # 没有close字段跳过

    # 处理每个code
    codes = df_parquet['code'].unique()
    dfs = []
    for code in codes:
        df_code = df_parquet[df_parquet['code'] == code].copy()
        stock_name = df_code['name'].iloc[0] if 'name' in df_code.columns else ''
        print(f"正在处理: {str(code).zfill(6)} {stock_name}")
        # 匹配前六位等于code的csv文件
        hfq_csv_file = None
        for fname in os.listdir(hfq_dir):
            if fname.startswith(str(code).zfill(6)) and fname.endswith('.csv'):
                hfq_csv_file = fname
                break
        if not hfq_csv_file:
            tqdm.write(f"未找到后复权csv: code={code}")
            continue
        hfq_csv_path = os.path.join(hfq_dir, hfq_csv_file)
        df_hfq = pd.read_csv(hfq_csv_path)
        df_hfq['code'] = code
        if 'date' in df_hfq.columns:
            df_hfq['date'] = pd.to_datetime(df_hfq['date'])
        else:
            continue
        if 'date' in df_code.columns:
            df_code['date'] = pd.to_datetime(df_code['date'])
        hfq_cols = ['open', 'high', 'low', 'close']
        hfq_rename = {col: f"{col}_hfq" for col in hfq_cols}
        df_hfq = df_hfq[['date', 'code'] + hfq_cols].rename(columns=hfq_rename)
        df_merged = pd.merge(df_code, df_hfq, left_on=['date', 'code'], right_on=['date', 'code'], how='left')
        if 'close_Ex_right' in df_merged.columns and 'outstanding_share' in df_merged.columns:
            df_merged['Circulation_market_value'] = df_merged['close_Ex_right'] * df_merged['outstanding_share']
        else:
            df_merged['Circulation_market_value'] = None
        dfs.append(df_merged)

    if dfs:
        df_all = pd.concat(dfs, ignore_index=True)
        col_order = [
            'close_Ex_right', 'open_hfq', 'high_hfq', 'low_hfq', 'close_hfq',
            'volume', 'amount', 'outstanding_share', 'turnover', 'name', 'Circulation_market_value'
        ]
        final_cols = [col for col in col_order if col in df_all.columns]
        df_all = df_all.set_index(['date', 'code'])
        df_all = df_all[final_cols]
        output_path = os.path.join(output_dir, parquet_file.replace('.parquet', '_merged.parquet'))
        df_all.to_parquet(output_path)