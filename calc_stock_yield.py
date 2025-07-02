import pandas as pd
import numpy as np
from datetime import datetime
from package.readLocalData import LocalMarketData
import hashlib
from tqdm import tqdm
import os

print("开始运行，当前系统时间:", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

base_dir = '/Users/dremind/Nutstore Files/.symlinks/坚果云/mfkQuant/Parquet数据/Astock_hfq_parquet_by_year'
cols = ['date', 'code', 'open', 'close', 'high', 'low']
market_data = LocalMarketData(base_dir)

period_days_short = [1,2,3,4,5]
period_days_middle = [7,10,12,15,20]
period_days_long = [30,45,60,120,250]
periods_dict = {
    'short': period_days_short,
    'middle': period_days_middle,
    'long': period_days_long
}

years = list(range(2017, 2025))  # 只到2024，最后一年单独处理
for period_tag, period_days in periods_dict.items():
    period_names = [str(x) for x in period_days]
    print(f"开始计算周期段: {period_tag}, 周期: {period_names}")
    output_dir = f'/Users/dremind/Nutstore Files/.symlinks/坚果云/mfkQuant/Parquet数据/stock_yield_results_{period_tag}'
    os.makedirs(output_dir, exist_ok=True)

    for i, year in enumerate(years):
        # 读取当前年和下一年数据
        next_year = year + 1
        print(f"处理年份窗口: {year}-{next_year}")
        df = market_data.read_hfq_data(year, next_year, columns=cols)
        df = df.sort_values(['code', 'date']).reset_index(drop=True)

        # 计算是否涨跌停
        df['pre_close'] = df.groupby('code')['close'].shift(1)
        def limit_status(row):
            if row['high'] == row['low']:
                if row['pre_close'] is not None and not np.isnan(row['pre_close']):
                    if row['high'] >= round(row['pre_close'] * 1.044, 2):
                        return 1
                    elif row['low'] <= round(row['pre_close'] * 0.956, 2):
                        return -1
            return 0
        df['是否涨跌停'] = df.apply(limit_status, axis=1)

        # 计算未来收益率
        def calc_future_returns_and_limit(sub_df, period_days):
            result = []
            limit_flags = sub_df['是否涨跌停'].values
            open_prices = sub_df['open'].values
            close_prices = sub_df['close'].values
            for idx in range(len(sub_df)):
                base_idx = idx + 1
                while base_idx < len(sub_df) and limit_flags[base_idx-1] == 1:
                    base_idx += 1
                if base_idx >= len(sub_df):
                    result.append([np.nan]*len(period_days))
                    continue
                base_open = open_prices[base_idx]
                buy_cost = base_open * (1 + 0.0005)
                rets = []
                for offset in period_days:
                    target_idx = idx + offset
                    if target_idx < len(sub_df):
                        if limit_flags[idx] == 1:
                            rets.append(0.0)
                        else:
                            if buy_cost is None or buy_cost == 0 or np.isnan(buy_cost):
                                ret = np.nan
                            else:
                                ret = round(close_prices[target_idx] / buy_cost - 1, 4)
                            rets.append(ret)
                    else:
                        rets.append(np.nan)
                result.append(rets)
            rets_df = pd.DataFrame(result, columns=[str(x) for x in period_days])
            return rets_df

        future_rets = []
        for code, sub_df in tqdm(df.groupby('code'), desc=f'计算未来收益率-{period_tag}-{year}'):
            sub_df = sub_df.reset_index(drop=True)
            rets = calc_future_returns_and_limit(sub_df, period_days)
            future_rets.append(rets)
        future_rets_df = pd.concat(future_rets, ignore_index=True)
        df[period_names] = future_rets_df

        # 只保留当前年份的数据
        df['年份'] = pd.to_datetime(df['date']).dt.year.astype(str)
        df_this_year = df[df['年份'] == str(year)].copy()
        if df_this_year.empty:
            continue

        # 组装结果表
        result = pd.DataFrame()
        result['股票代码'] = df_this_year['code']
        result['日期'] = df_this_year['date']
        result['股价后复权'] = df_this_year['close']
        result['是否涨跌停'] = df_this_year['是否涨跌停']
        result['未来收益周期'] = [period_names] * len(df_this_year)
        result['未来收益率'] = df_this_year[period_names].values.tolist()

        # 版本哈希与入库时间
        def get_hash(row):
            s = f"{row['股票代码']}_{row['日期']}_{row['股价后复权']}_{row['未来收益率']}"
            return hashlib.md5(s.encode('utf-8')).hexdigest()
        result['版本哈希'] = result.apply(get_hash, axis=1)
        result['入库时间'] = datetime.now().strftime('%Y-%m-%d %H%M%S')
        result['年份'] = pd.to_datetime(result['日期']).dt.year.astype(str)

        # 设置索引
        result = result.set_index(['日期', '股票代码'])

        # 按年份分区追加保存
        print(f"即将写入{period_tag} {year}，数据量：", result.shape)
        result.to_parquet(
            output_dir,
            partition_cols=['年份'],
            index=True,
            engine='pyarrow'
        )
        print(f"{period_tag} {year}段写入完成，当前系统时间:", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

print("全部运行结束，当前系统时间:", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
