import os
import pandas as pd
from glob import glob
import re
from datetime import datetime, timedelta
import pickle

op=pd.read_pickle('/Users/dremind/Nutstore Files/.symlinks/坚果云/mfkQuant/01数据获取/ETF期权历史行情/pkl文件/sh_option_risk_50ETF_510050_20250702.pkl')
etf=pd.read_csv('/Users/dremind/Nutstore Files/.symlinks/坚果云/mfkQuant/01数据获取/规模指数ETF日线/ETF510050_sh510050_日K_后复权.csv')

df = pd.merge(op, etf, left_on='日期', right_on='date', how='inner')
df = df.drop(columns=['date_x', 'date_y'])


# 提取'合约简称'中'月'字后面的数字作为'行权价'
def extract_strike_price(s):
    # 匹配“月”字后面的数字（包括小数）
    match = re.search(r'月(\d+(\.\d+)?)', str(s))
    if match:
        return float(match.group(1))
    return None

df['行权价'] = df['合约简称'].apply(extract_strike_price)



def extract_delivery_year_month(code):
    """
    从期权代码中提取交割年和月（如510050C1503M02250 -> 2015, 3）
    """
    match = re.search(r'[CP](\d{4})M', code)
    if match:
        ym = match.group(1)
        year = 2000 + int(ym[:2])
        month = int(ym[2:])
        return year, month
    return None, None

def get_fourth_wednesday(year, month):
    """
    获取指定年月的第四个星期三日期
    """
    first_day = datetime(year, month, 1)
    wednesdays = []
    for i in range(31):
        day = first_day + timedelta(days=i)
        if day.month != month:
            break
        if day.weekday() == 2:  # 0=Monday, 2=Wednesday
            wednesdays.append(day)
    if len(wednesdays) >= 4:
        return wednesdays[3]
    else:
        return None

def extract_delivery_date(code):
    year, month = extract_delivery_year_month(code)
    if year and month:
        dt = get_fourth_wednesday(year, month)
        if dt:
            return dt.strftime('%Y-%m-%d')
    return None

# 提取交割日期
df['交割日期'] = df['交易代码'].apply(extract_delivery_date)

# 计算剩余到期日（交割日期-当前日期）
df['日期_dt'] = pd.to_datetime(df['日期'])
df['交割日期_dt'] = pd.to_datetime(df['交割日期'])
df['剩余到期日'] = (df['交割日期_dt'] - df['日期_dt']).dt.days

# 可选：删除临时字段
df = df.drop(columns=['日期_dt', '交割日期_dt'])


# 计算是否主力合约
def mark_major_contract(group):
    # 只考虑剩余到期日>=3的合约
    valid = group[group['剩余到期日'] >= 3].copy()
    if valid.empty:
        group['是否主力合约'] = False
        return group
    # 计算绝对价差
    valid['价差'] = (valid['close'] * 1000 - valid['行权价']).abs()
    # 找到剩余到期日最小的合约
    min_days = valid['剩余到期日'].min()
    valid_min = valid[valid['剩余到期日'] == min_days]
    # 在这些合约中，找价差最小的
    idx = valid_min['价差'].idxmin()
    group['是否主力合约'] = False
    if idx in group.index:
        group.loc[idx, '是否主力合约'] = True
    return group

df = df.groupby(['日期', '多空类型'], group_keys=False).apply(mark_major_contract)

df.to_pickle('/Users/dremind/Nutstore Files/.symlinks/坚果云/mfkQuant/02数据整理/上证50ETF历史行情与期权风险指标.pkl')
df.to_csv('/Users/dremind/Nutstore Files/.symlinks/坚果云/mfkQuant/02数据整理/上证50ETF历史行情与期权风险指标.csv', index=False)