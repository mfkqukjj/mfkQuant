import os
import time
from datetime import datetime, timedelta
import pandas as pd
from tqdm import tqdm
from package.getOptionData import foDataFetcher

save_dir = '/Users/dremind/Nutstore Files/.symlinks/坚果云/mfkQuant/历史行情数据/ETF期权历史行情'
os.makedirs(save_dir, exist_ok=True)

fetcher = foDataFetcher()

start_date = datetime.strptime('20150209', '%Y%m%d')
end_date = datetime.today()

# 计算需要抓取的月份数
month_list = []
cur = start_date
while cur < end_date:
    month_start = cur.replace(day=1)
    next_month = (month_start + timedelta(days=32)).replace(day=1)
    month_end = min(next_month - timedelta(days=1), end_date)
    month_list.append((month_start, month_end))
    cur = next_month

for month_start, month_end in tqdm(month_list, desc="抓取进度"):
    s_str = month_start.strftime('%Y%m%d')
    e_str = month_end.strftime('%Y%m%d')
    print(f"正在获取 {s_str} 至 {e_str} 的上交所ETF期权风险指标数据...")
    try:
        df = fetcher.get_sh_option_risk(start_date=s_str, end_date=e_str)
        if not df.empty:
            csv_path = os.path.join(save_dir, f"sh_option_risk_{s_str}_{e_str}.csv")
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print(f"已保存：{csv_path}")
        else:
            print(f"{s_str}-{e_str} 无数据")
    except Exception as e:
        print(f"{s_str}-{e_str} 获取失败: {e}")
    time.sleep(3)  # 避免请求过快