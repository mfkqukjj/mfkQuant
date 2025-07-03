import os
import time
from datetime import datetime, timedelta
import pandas as pd
from tqdm import tqdm
from package.getOptionData import foDataFetcher


save_dir = '/Users/dremind/Nutstore Files/.symlinks/坚果云/mfkQuant/历史行情数据/ETF期权历史行情'
os.makedirs(save_dir, exist_ok=True)

fetcher = foDataFetcher()

# --------- 配置区 ---------
# ETF期权相关数据
# start_date = datetime.strptime('20150209', '%Y%m%d') # 上证50ETF期权上市日
# start_date = datetime.strptime('20221212', '%Y%m%d') # 深证100ETF期权上市日

# 持仓排名数据
rank_start_date = datetime.strptime('20100416', '%Y%m%d')  # 中金所持仓排名数据起始日
rank_end_date = datetime.today()

# 其他数据
data_start_date = datetime.strptime('20150209', '%Y%m%d')
data_end_date = datetime.today()

# 可选：要抓取的函数及保存前缀
# options: 'sh_option_risk', 'sz_option_risk', 'sz_etf_op_market', 'cffex_position_rank'
fetch_func = 'cffex_position_rank'  # 修改此处即可切换接口

if fetch_func == 'sh_option_risk':
    func = fetcher.get_sh_option_risk
    file_prefix = 'sh_option_risk'
    s_date = data_start_date
    e_date = data_end_date
elif fetch_func == 'sz_option_risk':
    func = fetcher.get_sz_option_risk
    file_prefix = 'sz_option_risk'
    s_date = data_start_date
    e_date = data_end_date
elif fetch_func == 'sz_etf_op_market':
    func = fetcher.get_sz_etf_op_market
    file_prefix = 'sz_etf_op_market'
    s_date = data_start_date
    e_date = data_end_date
elif fetch_func == 'cffex_position_rank':
    func = fetcher.get_cffex_position_rank
    file_prefix = 'cffex_position_rank'
    s_date = rank_start_date
    e_date = rank_end_date
else:
    raise ValueError("fetch_func 仅支持 'sh_option_risk', 'sz_option_risk', 'sz_etf_op_market', 'cffex_position_rank'")

# 计算需要抓取的月份数
month_list = []
cur = s_date
while cur < e_date:
    month_start = cur.replace(day=1)
    next_month = (month_start + timedelta(days=32)).replace(day=1)
    month_end = min(next_month - timedelta(days=1), e_date)
    month_list.append((month_start, month_end))
    cur = next_month

for month_start, month_end in tqdm(month_list, desc="抓取进度"):
    s_str = month_start.strftime('%Y%m%d')
    e_str = month_end.strftime('%Y%m%d')
    print(f"正在获取 {s_str} 至 {e_str} 的数据...")
    try:
        if fetch_func == 'cffex_position_rank':
            # 可根据需要循环symbol，这里以'IF'为例，如需全部symbol可自行扩展
            for symbol in ['IF', 'IH'] #, 'IC', 'IM', 'TS', 'TF', 'T', 'TL', 'IO', 'MO', 'HO']:
                df = func(symbol=symbol, start_date=s_str, end_date=e_str)
                if not df.empty:
                    csv_path = os.path.join(save_dir, f"{file_prefix}_{symbol}_{s_str}_{e_str}.csv")
                    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
                    print(f"已保存：{csv_path}")
                else:
                    print(f"{symbol} {s_str}-{e_str} 无数据")
        elif fetch_func == 'sh_option_risk':
            df = func(start_date=s_str, end_date=e_str)
            if not df.empty:
                csv_path = os.path.join(save_dir, f"{file_prefix}_{s_str}_{e_str}.csv")
                df.to_csv(csv_path, index=False, encoding='utf-8-sig')
                print(f"已保存：{csv_path}")
            else:
                print(f"{s_str}-{e_str} 无数据")
        else:
            df = func(start_date=s_str, end_date=e_str)
            if not df.empty:
                csv_path = os.path.join(save_dir, f"{file_prefix}_{s_str}_{e_str}.csv")
                df.to_csv(csv_path, index=False, encoding='utf-8-sig')
                print(f"已保存：{csv_path}")
            else:
                print(f"{s_str}-{e_str} 无数据")
    except Exception as e:
        print(f"{s_str}-{e_str} 获取失败: {e}")
    time.sleep(3)  # 避免请求过快