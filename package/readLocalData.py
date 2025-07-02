import os
import pandas as pd
from typing import List, Optional
#readParquet2yzDF.py
class LocalMarketData:
    def __init__(self, base_dir: str):
        self.base_dir = base_dir

    def read_hfq_data(
        self,
        start_year: int,
        end_year: int,
        columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        读取本地后复权行情数据

        :param start_year: 开始年份（含）
        :param end_year: 结束年份（含）
        :param columns: 需要读取的字段，默认为['date', 'code', 'open', 'high', 'low', 'close', 'volume', 'turnover']
        :return: 合并后的DataFrame
        """
        default_cols = ['date', 'code', 'open', 'high', 'low', 'close', 'volume', 'turnover']
        if columns is None:
            columns = default_cols
        else:
            # 确保'date','code'一定包含
            for col in ['date', 'code']:
                if col not in columns:
                    columns.insert(0, col)

        years = [str(y) for y in range(start_year, end_year + 1)]
        dfs = []
        for year in years:
            year_dir = os.path.join(self.base_dir, f'year={year}')
            if os.path.exists(year_dir):
                df_year = pd.read_parquet(year_dir, columns=columns, engine='pyarrow')
                dfs.append(df_year)
        if dfs:
            df = pd.concat(dfs, ignore_index=True)
            return df
        else:
            print("没有找到指定年份的数据。")
            return pd.DataFrame(columns=columns)

# 示例用法
if __name__ == "__main__":
    base_dir = '/Users/dremind/Nutstore Files/.symlinks/坚果云/mfkQuant/parquet数据/parquet_by_year'
    market_data = LocalMarketData(base_dir)
    df = market_data.read_hfq_data(2022, 2025)
    print(df.head())