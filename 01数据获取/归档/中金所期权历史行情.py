import os
import requests
from tqdm import tqdm
from datetime import datetime

save_dir = '/Users/dremind/Documents/量化体系搭建/期权数据/中金所期权历史行情'
os.makedirs(save_dir, exist_ok=True)

start_year, start_month = 2010, 4
end_year, end_month = 2025, 6

def gen_year_months(start_y, start_m, end_y, end_m):
    ym_list = []
    y, m = start_y, start_m
    while (y < end_y) or (y == end_y and m <= end_m):
        ym_list.append(f"{y}{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1
    return ym_list

all_months = gen_year_months(start_year, start_month, end_year, end_month)

fail_list = []
for ym in tqdm(all_months, desc="下载中金所期权历史行情"):
    url = f"http://www.cffex.com.cn/sj/historysj/{ym}/zip/{ym}.zip"
    local_path = os.path.join(save_dir, f"{ym}.zip")
    if os.path.exists(local_path):
        tqdm.write(f"{ym} 已存在，跳过。")
        continue
    success = False
    for attempt in range(3):  # 最多重试3次
        try:
            tqdm.write(f"正在下载月份：{ym}，尝试{attempt+1}/3")
            resp = requests.get(url, timeout=30)
            if resp.status_code == 200:
                with open(local_path, 'wb') as f:
                    f.write(resp.content)
                success = True
                break
            else:
                tqdm.write(f"{ym} 下载失败，状态码：{resp.status_code}")
        except Exception as e:
            tqdm.write(f"{ym} 下载异常：{e}")
    if not success:
        fail_list.append(ym)

tqdm.write("全部下载完成。")

# 记录下载日志
log_path = os.path.join(save_dir, "download_log.txt")
with open(log_path, "a", encoding="utf-8") as logf:
    logf.write(f"\n下载时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    if fail_list:
        logf.write("失败文件：\n")
        for fail in fail_list:
            logf.write(f"{fail}\n")
    else:
        logf.write("全部文件下载成功。\n")