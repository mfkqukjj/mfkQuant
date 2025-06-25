import akshare as ak
import pandas as pd
import os
import time
import re
from tqdm import tqdm

def get_existing_prefixes(output_dir):
    """获取已下载的股票代码前3位列表"""
    existing_prefixes = set()
    if os.path.exists(output_dir):
        for filename in os.listdir(output_dir):
            if filename.endswith('.parquet'):
                prefix_match = re.match(r'(\d{3})_.*\.parquet', filename)
                if prefix_match:
                    existing_prefixes.add(prefix_match.group(1))
    return existing_prefixes

def get_stock_list():
    """获取A股列表"""
    try:
        stock_info = ak.stock_info_a_code_name()
        stock_info['code'] = stock_info['code'].astype(str).str.zfill(6)
        return stock_info
    except Exception as e:
        print(f"获取股票列表失败: {e}")
        return None

def clean_filename(filename):
    filename = filename.replace('*ST', 'ST')
    invalid_chars = [':', '/', '\\', '?', '"', '<', '>', '|']
    for char in invalid_chars:
        filename = filename.replace(char, '')
    return filename

def get_stock_history(code, name):
    """获取单个股票的历史数据，返回DataFrame"""
    try:
        if code.startswith('6'):
            prefix = 'sh'
        elif code.startswith(('4', '8')):
            prefix = 'bj'
        else:
            prefix = 'sz'
        full_code = f"{prefix}{code}"
        df = ak.stock_zh_a_daily(symbol=full_code) #除权数据
        if df is not None and not df.empty:
            df = df.copy()
            df['code'] = code
            df['name'] = name
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
            else:
                df = df.reset_index().rename(columns={'index': 'date'})
                df['date'] = pd.to_datetime(df['date'])
            return df
        else:
            return None
    except Exception as e:
        print(f"\n获取 {code} {name} 数据失败: {e}")
        return None

def main():
    output_dir = r"F:\金融数据\A股历史行情数据（除权）"
    os.makedirs(output_dir, exist_ok=True)

    print("正在获取A股列表...")
    stock_info = get_stock_list()
    if stock_info is None:
        return

    # 按股票代码前3位分组
    stock_info['prefix'] = stock_info['code'].str[:3]
    prefix_groups = stock_info.groupby('prefix')

    total = len(prefix_groups)
    print(f"需要处理的前3位分组数量: {total}")

    with tqdm(total=total, desc="总进度", position=0) as pbar:
        for prefix, group in prefix_groups:
            parquet_path = os.path.join(output_dir, f"{prefix}_stocks.parquet")
            if os.path.exists(parquet_path):
                pbar.update(1)
                continue  # 已存在则跳过

            dfs = []
            for _, row in group.iterrows():
                code = row['code']
                name = row['name']
                tqdm.write(f"正在处理: {code} {name}")
                df = get_stock_history(code, name)
                if df is not None:
                    dfs.append(df)
                time.sleep(0.2)  # 防止请求过快

            if dfs:
                merged = pd.concat(dfs, ignore_index=True)
                merged = merged.drop_duplicates(subset=['date', 'code'])
                merged = merged.set_index(['date', 'code'])
                merged.to_parquet(parquet_path, index=True)
            pbar.update(1)

    print("全部处理完成！")

if __name__ == "__main__":
    main()