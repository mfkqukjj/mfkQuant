import os
import requests
import zipfile
import pandas as pd
from tqdm import tqdm
from io import StringIO
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

class CFFEXDataFetcher:
    """
    用于获取中金所期货、期权持仓排名数据和期权历史行情数据
    """
    FUTURE_SYMBOLS = ['IF', 'IC', 'IM', 'IH','TS','TF','T','TL']
    OPTION_SYMBOLS = ['IO', 'MO', 'HO']

    def __init__(self):
        self.base_url = "http://www.cffex.com.cn/sj/ccpm/{ym}/{day}/{symbol}_1.csv"
        self.option_zip_url = "http://www.cffex.com.cn/sj/historysj/{ym}/zip/{ym}.zip"

    def get_position_rank(self, symbol, start_date=None, end_date=None):
        """
        获取期货或期权持仓排名数据
        :param symbol: 品种名称，如'IF', 'IC', 'IM', 'IH', 'TS','TF','T','TL', 'IO', 'MO', 'HO'
        :param start_date: 'YYYYMMDD'字符串，默认最近30天
        :param end_date: 'YYYYMMDD'字符串，默认最近30天
        :return: DataFrame，date和合约类型为普通列
        """
        # 日期处理
        if end_date is None:
            end = datetime.today()
        else:
            end = datetime.strptime(end_date, "%Y%m%d")
        if start_date is None:
            start = end - timedelta(days=29)
        else:
            start = datetime.strptime(start_date, "%Y%m%d")

        date_list = []
        cur = start
        while cur <= end:
            date_list.append(cur.strftime("%Y%m%d"))
            cur += timedelta(days=1)

        all_dfs = []
        for date_str in date_list:
            ym = date_str[:6]
            day = date_str[6:8]
            url = self.base_url.format(ym=ym, day=day, symbol=symbol)
            try:
                resp = requests.get(url, timeout=10)
                if resp.status_code != 200 or len(resp.content) < 10:
                    continue
                # 用GBK解码，防止中文乱码
                lines = resp.content.decode('gbk', errors='ignore').splitlines()
                if len(lines) < 2:
                    continue
                data_lines = lines[2:]
                columns = [
                    'date', '合约代码', '排名',
                    '成交量-会员简称', '成交量-成交量', '成交量-比上一交易日增减',
                    '买单-会员简称', '买单-持买单量', '买单-比上一交易日增减',
                    '卖单-会员简称', '卖单-持卖单量', '卖单-比上一交易日增减'
                ]
                csv_text = ','.join(columns) + '\n' + '\n'.join(data_lines)
                df = pd.read_csv(StringIO(csv_text))
                df['date'] = date_str
                df['合约类型'] = symbol  # 新增品种类型列
                all_dfs.append(df)
            except Exception as e:
                print(f"{date_str} {symbol} 下载或解析失败: {e}")

        if all_dfs:
            result = pd.concat(all_dfs, ignore_index=True)
            return result
        else:
            print("未获取到任何数据")
            return pd.DataFrame(columns=[
                'date', '合约类型', '合约代码', '排名',
                '成交量-会员简称', '成交量-成交量', '成交量-比上一交易日增减',
                '买单-会员简称', '买单-持买单量', '买单-比上一交易日增减',
                '卖单-会员简称', '卖单-持卖单量', '卖单-比上一交易日增减'
            ])

    def get_op_data(self, symbol=None, start_date=None, end_date=None):
        """
        下载中金所期权历史行情zip文件并提取所有csv数据，合并为一个DataFrame
        :param symbol: 品种名称，如'IO', 'MO', 'HO'，默认为'IO'
        【IO】:沪深300股指期权,【HO】:上证50股指期权,【MO】:中证1000股指期权。
        :param start_date: 'YYYYMMDD'字符串，默认最近30天
        :param end_date: 'YYYYMMDD'字符串，默认最近30天
        :return: 合并后的DataFrame
        """
        # 日期处理
        today = datetime.today()
        if end_date is None:
            end = datetime(today.year, today.month, 1)
        else:
            end = datetime.strptime(end_date, "%Y%m")
        if start_date is None:
            # 上月
            first_this_month = datetime(today.year, today.month, 1)
            last_month = first_this_month - timedelta(days=1)
            start = datetime(last_month.year, last_month.month, 1)
        else:
            start = datetime.strptime(start_date, "%Y%m")

        # 生成年月列表
        ym_list = []
        y, m = start.year, start.month
        end_y, end_m = end.year, end.month
        while (y < end_y) or (y == end_y and m <= end_m):
            ym_list.append(f"{y}{m:02d}")
            m += 1
            if m > 12:
                m = 1
                y += 1

        # 下载zip文件到临时目录
        import tempfile
        save_dir = tempfile.mkdtemp(prefix='cffex_op_zip_')

        fail_list = []
        for ym in tqdm(ym_list, desc="下载中金所期权历史行情"):
            url = self.option_zip_url.format(ym=ym)
            local_path = os.path.join(save_dir, f"{ym}.zip")
            if os.path.exists(local_path):
                tqdm.write(f"{ym} 已存在，跳过。")
                continue
            success = False
            for attempt in range(3):
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

        # 解压并读取所有csv
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
                                    f.seek(0)
                                    continue
                            if df is not None and not df.empty:
                                base_name = os.path.basename(csv_file)
                                date_part = base_name.split('_')[0]
                                df['date'] = date_part
                                # 提取symbol字段：合约代码前2字符中的英文部分
                                if '合约代码' in df.columns:
                                    df['symbol'] = df['合约代码'].astype(str).str.extract(r'^([A-Za-z]+)')
                                else:
                                    df['symbol'] = None
                                all_dfs.append(df)
                            elif df is None:
                                print(f"警告：{csv_file} 读取失败，已跳过。")
        if not all_dfs:
            print("没有找到任何csv文件。")
            return pd.DataFrame()
        else:
            all_df = pd.concat(all_dfs, ignore_index=True)
            # 剔除合约代码为“小计”“合计”的数据
            if '合约代码' in all_df.columns:
                all_df = all_df[~all_df['合约代码'].astype(str).str.strip().isin(['小计', '合计'])]
            # 如指定symbol则只保留对应symbol的数据，否则全部保留（已剔除小计、合计）
            if symbol:
                all_df = all_df[all_df['symbol'] == symbol]
            # 只保留日期范围内的数据
            all_df['date'] = pd.to_datetime(all_df['date'], errors='coerce')
            mask = (all_df['date'] >= start) & (all_df['date'] <= end)
            return all_df.loc[mask].reset_index(drop=True)

    def get_etf_op_data(self, symbol="全部", start_date=None, end_date=None):
        """
        获取上交所ETF期权风险指标数据
        :param symbol: ETF品种，如"全部"、"50ETF"、"300ETF"、"500ETF"、"科创50"、"科创板50"
        :param start_date: 'YYYYMMDD'字符串，默认最近30天
        :param end_date: 'YYYYMMDD'字符串，默认最近30天
        :return: 合并后的DataFrame，含"跟踪ETF"和"ETF代码"两列
        """
        import time

        # 日期处理
        if end_date is None:
            end = datetime.today()
        else:
            end = datetime.strptime(end_date, "%Y%m%d")
        if start_date is None:
            start = end - timedelta(days=29)
        else:
            start = datetime.strptime(start_date, "%Y%m%d")

        # symbol参数映射
        symbol_map = {
            "全部": "%E5%85%A8%E9%83%A8",
            "50ETF": "50ETF",
            "300ETF": "300ETF",
            "500ETF": "500ETF",
            "科创50": "%E7%A7%91%E5%88%9B50",
            "科创板50": "%E7%A7%91%E5%88%9B%E6%9D%BF50"
        }
        product_type = symbol_map.get(symbol, "%E5%85%A8%E9%83%A8")

        # 生成日期列表
        date_list = []
        cur = start
        while cur <= end:
            date_list.append(cur.strftime("%Y%m%d"))
            cur += timedelta(days=1)

        all_dfs = []
        for trade_date in tqdm(date_list, desc="下载ETF期权风险指标"):
            url = f"https://query.sse.com.cn/derivative/downloadRisk.do?trade_date={trade_date}&productType={product_type}"
            headers = {
                "Referer": "https://www.sse.com.cn/",
                "User-Agent": "Mozilla/5.0"
            }
            try:
                resp = requests.get(url, headers=headers, timeout=10)
                if resp.status_code != 200 or len(resp.content) < 100:
                    continue
                # 读取csv内容，尝试多种编码
                df = None
                for enc in ['utf-8', 'gb2312', 'gbk', 'latin1']:
                    try:
                        df = pd.read_csv(StringIO(resp.content.decode(enc, errors='ignore')), index_col=False)
                        # 字段名标准化
                        df.columns = [c.strip().replace('\ufeff', '') for c in df.columns]
                        break
                    except Exception:
                        df = None
                        continue
                if df is None or df.empty:
                    continue
                df['date'] = trade_date
                all_dfs.append(df)
            except Exception as e:
                print(f"{trade_date} {symbol} 下载或解析失败: {e}")
            time.sleep(0.2)  # 防止被封

        if not all_dfs:
            print("未获取到任何数据")
            return pd.DataFrame()

        # 合并前过滤掉没有“合约简称”列的df
        all_dfs = [df for df in all_dfs if "合约简称" in df.columns]

        if not all_dfs:
            print("未获取到任何含“合约简称”字段的数据")
            return pd.DataFrame()

        result = pd.concat(all_dfs, ignore_index=True)
        # 合并后，去除所有字符串字段的首尾空白字符
        for col in result.select_dtypes(include='object').columns:
            result[col] = result[col].astype(str).str.strip()
   
        # 新增“跟踪ETF”列：为'合约简称'中"购"或"沽"字符前面的部分（不含购、沽）
        def extract_etf_name(x):
            if pd.isna(x):
                return ""
            idx = x.find("购")
            if idx == -1:
                idx = x.find("沽")
            if idx == -1:
                return ""
            return x[:idx]
        result["跟踪ETF"] = result["合约简称"].astype(str).apply(extract_etf_name)
       
       # 新增“ETF代码”列：为'交易代码'前六位
        if "交易代码" in result.columns:
            result["ETF代码"] = result["交易代码"].astype(str).str[:6]
        else:
            result["ETF代码"] = ""

        # 新增“多空类型”列
        def extract_long_short(x):
            if pd.isna(x):
                return ""
            if ("购" in x) or ("多" in x):
                return "购"
            elif ("沽" in x) or ("空" in x):
                return "沽"
            else:
                return ""
        result["多空类型"] = result["合约简称"].astype(str).apply(extract_long_short)

        return result 
    
    def get_etf_op_market_sz(self, start_date=None, end_date=None):
        """
        获取深交所ETF期权市场每日持仓数据
        :param start_date: 'YYYYMMDD'字符串，默认最近30天
        :param end_date: 'YYYYMMDD'字符串，默认最近30天
        :return: 合并后的DataFrame
        """
        import time

        # 日期处理
        if end_date is None:
            end = datetime.today()
        else:
            end = datetime.strptime(end_date, "%Y%m%d")
        if start_date is None:
            start = end - timedelta(days=29)
        else:
            start = datetime.strptime(start_date, "%Y%m%d")

        # 生成日期列表
        date_list = []
        cur = start
        while cur <= end:
            date_list.append(cur.strftime("%Y-%m-%d"))
            cur += timedelta(days=1)

        all_dfs = []
        for trade_date in tqdm(date_list, desc="下载深交所ETF期权市场日度持仓统计"):
            url = f"https://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=ysprdzb&TABKEY=tab1&txtQueryDate={trade_date}"
            headers = {
                "Referer": "https://www.szse.cn/",
                "User-Agent": "Mozilla/5.0"
            }
            try:
                resp = requests.get(url, headers=headers, timeout=15)
                if resp.status_code != 200 or len(resp.content) < 100:
                    continue
                df = None
                for enc in ['utf-8', 'gbk', 'gb2312', 'latin1']:
                    try:
                        df = pd.read_excel(pd.io.common.BytesIO(resp.content), dtype=str)
                        # 字段名标准化
                        df.columns = [c.strip().replace('\ufeff', '') for c in df.columns]
                        break
                    except Exception:
                        df = None
                        continue
                if df is None or df.empty:
                    continue
                df['date'] = trade_date
                all_dfs.append(df)
            except Exception as e:
                print(f"{trade_date} 下载或解析失败: {e}")
            time.sleep(0.2)  # 防止被封

        if not all_dfs:
            print("未获取到任何数据")
            return pd.DataFrame()

        result = pd.concat(all_dfs, ignore_index=True)
        # 去除所有字符串字段的首尾空白字符
        for col in result.select_dtypes(include='object').columns:
            result[col] = result[col].astype(str).str.strip()
        return result
    
    def get_sz_option_risk(self, start_date=None, end_date=None):
        """
        获取深交所ETF期权风险指标数据（风险指标专用接口）
        :param start_date: 'YYYYMMDD'字符串，默认最近30天
        :param end_date: 'YYYYMMDD'字符串，默认最近30天
        :return: 合并后的DataFrame，含"跟踪ETF"、"ETF代码"、"多空类型"三列
        """
        import time

        # 日期处理
        if end_date is None:
            end = datetime.today()
        else:
            end = datetime.strptime(end_date, "%Y%m%d")
        if start_date is None:
            start = end - timedelta(days=29)
        else:
            start = datetime.strptime(start_date, "%Y%m%d")

        # 生成日期列表
        date_list = []
        cur = start
        while cur <= end:
            date_list.append(cur.strftime("%Y-%m-%d"))
            cur += timedelta(days=1)

        all_dfs = []
        for trade_date in tqdm(date_list, desc="下载深交所ETF期权风险指标"):
            url = f"https://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=option_hyfxzb&TABKEY=tab1&txtSearchDate={trade_date}"
            headers = {
                "Referer": "https://www.szse.cn/",
                "User-Agent": "Mozilla/5.0"
            }
            try:
                resp = requests.get(url, headers=headers, timeout=15)
                if resp.status_code != 200 or len(resp.content) < 100:
                    continue
                df = None
                for enc in ['utf-8', 'gbk', 'gb2312', 'latin1']:
                    try:
                        df = pd.read_excel(pd.io.common.BytesIO(resp.content), dtype=str)
                        # 字段名标准化
                        df.columns = [c.strip().replace('\ufeff', '') for c in df.columns]
                        break
                    except Exception:
                        df = None
                        continue
                if df is None or df.empty:
                    continue
                df['date'] = trade_date
                all_dfs.append(df)
            except Exception as e:
                print(f"{trade_date} 下载或解析失败: {e}")
            time.sleep(0.2)  # 防止被封

        if not all_dfs:
            print("未获取到任何数据")
            return pd.DataFrame()

        result = pd.concat(all_dfs, ignore_index=True)
        # 去除所有字符串字段的首尾空白字符
        for col in result.select_dtypes(include='object').columns:
            result[col] = result[col].astype(str).str.strip()

        # 新增“跟踪ETF”列：为'合约简称'中"购"或"沽"字符前面的部分（不含购、沽）
        def extract_etf_name(x):
            if pd.isna(x):
                return ""
            idx = x.find("购")
            if idx == -1:
                idx = x.find("沽")
            if idx == -1:
                return ""
            return x[:idx]
        if "合约简称" in result.columns:
            result["跟踪ETF"] = result["合约简称"].astype(str).apply(extract_etf_name)
        else:
            result["跟踪ETF"] = ""

        # 新增“ETF代码”列：为'交易代码'前六位
        if "交易代码" in result.columns:
            result["ETF代码"] = result["合约代码"].astype(str).str[:6]
        else:
            result["ETF代码"] = ""

        # 新增“多空类型”列
        def extract_long_short(x):
            if pd.isna(x):
                return ""
            if ("购" in x) or ("多" in x):
                return "购"
            elif ("沽" in x) or ("空" in x):
                return "沽"
            else:
                return ""
        if "合约简称" in result.columns:
            result["多空类型"] = result["合约简称"].astype(str).apply(extract_long_short)
        else:
            result["多空类型"] = ""

        return result
        

# 示例用法
"""
if __name__ == "__main__":
    fetcher = CFFEXDataFetcher()
    # 期货示例
    df_fut = fetcher.get_data('IF', start_date='20250601', end_date='20250625')
    print(df_fut.head())
    # 期权示例
    df_opt = fetcher.get_data('IO', start_date='20250601', end_date='20250625')
    print(df_opt.head())
"""
