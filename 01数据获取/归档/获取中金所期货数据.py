import pandas as pd
import requests
from io import StringIO
from datetime import datetime, timedelta

class CFFEXDataFetcher:
    # 品种映射
    FUTURE_SYMBOLS = ['IF', 'IC', 'IM', 'IH']
    OPTION_SYMBOLS = ['IO', 'MO', 'HO']
    def __init__(self):
        self.base_url = "http://www.cffex.com.cn/sj/ccpm/{ym}/{day}/{symbol}_1.csv"

    def get_cffex_data(self, symbol, start_date=None, end_date=None):
        """
        symbol: 品种名称，如'IF', 'IC', 'IM', 'IH', 'IO', 'MO', 'HO'
        start_date, end_date: 'YYYYMMDD'字符串，默认最近30天
        返回：合并后的DataFrame
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
                col_line = lines[1]
                data_lines = lines[2:]
                columns = [
                    'date', '合约类型', '排名',
                    '成交量-会员简称', '成交量-成交量', '成交量-比上一交易日增减',
                    '买单-会员简称', '买单-持买单量', '买单-比上一交易日增减',
                    '卖单-会员简称', '卖单-持卖单量', '卖单-比上一交易日增减'
                ]
                csv_text = ','.join(columns) + '\n' + '\n'.join(data_lines)
                df = pd.read_csv(StringIO(csv_text))
                df['date'] = date_str
                df['合约类型'] = symbol
                all_dfs.append(df)
            except Exception as e:
                print(f"{date_str} {symbol} 下载或解析失败: {e}")

        if all_dfs:
            result = pd.concat(all_dfs, ignore_index=True)
            result = result.set_index(['date', '合约类型'])
            return result
        else:
            print("未获取到任何数据")
            return pd.DataFrame(columns=[
                'date', '合约类型', '排名',
                '成交量-会员简称', '成交量-成交量', '成交量-比上一交易日增减',
                '买单-会员简称', '买单-持买单量', '买单-比上一交易日增减',
                '卖单-会员简称', '卖单-持卖单量', '卖单-比上一交易日增减'
            ]).set_index(['date', '合约类型'])

# 示例用法
if __name__ == "__main__":
    fetcher = CFFEXDataFetcher()
    df = fetcher.get_cffex_data('IF', start_date='20250101', end_date='20250625')
    df.to_excel('/Users/dremind/Nutstore Files/.symlinks/坚果云/mfkQuant/cffex_if_data.xlsx', index=True)