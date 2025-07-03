import akshare as ak
import pandas as pd
import os

save_dir = '/Users/dremind/Nutstore Files/.symlinks/坚果云/mfkQuant/01数据获取/指数ETF日线'
os.makedirs(save_dir, exist_ok=True)

# ETF代码与名称（Sina接口格式）
etf_dict = {
    'sh510050': 'ETF510050',
    'sh510300': 'ETF510300',
    'sh510500': 'ETF510500',
}

def add_hfq_price(df, div_df):
    """
    直接将现价+累计分红额，得到ETF基金的后复权价
    """
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    div_df = div_df.copy()
    div_df['date'] = pd.to_datetime(div_df['日期'])
    div_df = div_df[['date', '累计分红']].dropna()
    div_df = div_df.sort_values('date')

    # 生成每个交易日的累计分红（向前填充）
    df = df.merge(div_df, on='date', how='left')
    df['累计分红'] = df['累计分红'].fillna(method='ffill').fillna(0)

    # 直接加累计分红得到复权价
    for col in ['open', 'high', 'low', 'close']:
        df[col + '_hfq'] = df[col] + df['累计分红']
    return df

for code, name in etf_dict.items():
    # 获取原始行情（除权）
    df = ak.fund_etf_hist_sina(symbol=code)
    df = df.rename(columns={'日期': 'date', '开盘价': 'open', '最高价': 'high', '最低价': 'low', '收盘价': 'close'})
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    df.to_csv(os.path.join(save_dir, f"{name}_{code}_日K.csv"), index=False, encoding='utf-8-sig')

    # 获取分红数据（累计分红格式）
    div_df = ak.fund_etf_dividend_sina(symbol=code)
    if not div_df.empty and '累计分红' in div_df.columns and '日期' in div_df.columns:
        div_df.to_csv(os.path.join(save_dir, f"{name}_{code}_分红.csv"), index=False, encoding='utf-8-sig')
        # 直接加累计分红得到复权价
        df_hfq = add_hfq_price(df, div_df)
        df_hfq.to_csv(os.path.join(save_dir, f"{name}_{code}_日K_后复权.csv"), index=False, encoding='utf-8-sig')
        print(f"{name}({code}) 后复权行情已保存")
    else:
        print(f"{name}({code}) 无分红数据或分红数据格式不符，未生成后复权行情")