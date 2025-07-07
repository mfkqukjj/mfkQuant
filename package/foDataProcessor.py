import pandas as pd
import datetime
import calendar

class FoDataProcessor:
    def __init__(self):
        # 不再在初始化时接收df
        pass
        
    def _clean_data(self, df):
        """Clean and preprocess the raw data"""
        df = df.copy()
        # Convert numeric columns
        float_cols = [
            '成交量-成交量', '成交量-比上一交易日增减',
            '买单-持买单量', '买单-比上一交易日增减',
            '卖单-持卖单量', '卖单-比上一交易日增减'
        ]
        for col in float_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        return df

    def _clean_company_name(self, name):
        """Clean company name by removing whitespace"""
        if pd.isna(name):
            return ''
        return ''.join(str(name).split())

    def is_main_contract(self, date_str, contract_code):
        """Determine if it's a main contract"""
        try:
            date = datetime.datetime.strptime(str(date_str), '%Y%m%d')
            year = date.year
            month = date.month

            code_year = int('20' + contract_code[-4:-2])
            code_month = int(contract_code[-2:])

            c = calendar.Calendar()
            month_days = [d for d in c.itermonthdates(year, month) if d.month == month]
            thursdays = [d for d in month_days if d.weekday() == 3]
            fridays = [d for d in month_days if d.weekday() == 4]
            third_thursday = thursdays[2]
            third_friday = fridays[2]

            if code_year == year and code_month == month:
                return date.date() <= third_thursday
            elif code_year == year and code_month == (month % 12 + 1):
                return date.date() >= third_friday
            return False
        except Exception:
            return False

    def process_by_company(self, df):
        """Process data by company and return structured DataFrame"""
        df = self._clean_data(df)  # 清理数据
        
        company_set = set(
            self._clean_company_name(x) for x in 
            df['成交量-会员简称'].tolist() +
            df['买单-会员简称'].tolist() +
            df['卖单-会员简称'].tolist()
            if pd.notna(x)
        )

        result_dfs = []
        for company in company_set:
            company_data = []
            for _, row in df.iterrows():
                c1 = self._clean_company_name(row['成交量-会员简称'])
                c2 = self._clean_company_name(row['买单-会员简称'])
                c3 = self._clean_company_name(row['卖单-会员简称'])
                
                if company in (c1, c2, c3):
                    base_data = {
                        'date': row['date'],
                        '合约代码': row['合约代码'],
                        '公司简称': company,
                        '合约类型': row['合约类型'],
                        '是否主连合约': self.is_main_contract(row['date'], row['合约代码'])
                    }
                    
                    # Process each dimension separately
                    if c1 == company:
                        vol_data = base_data.copy()
                        vol_data.update({
                            '统计维度': '成交量',
                            '排名': row['排名'],
                            '数值': row['成交量-成交量'],
                            '比上一交易日增减': row['成交量-比上一交易日增减'],
                            '比上一交易日增减比例': self._calc_change_rate(
                                row['成交量-成交量'], 
                                row['成交量-比上一交易日增减']
                            )
                        })
                        company_data.append(vol_data)
                        
                    if c2 == company:
                        buy_data = base_data.copy()
                        buy_data.update({
                            '统计维度': '持买单量',
                            '排名': row['排名'],
                            '数值': row['买单-持买单量'],
                            '比上一交易日增减': row['买单-比上一交易日增减'],
                            '比上一交易日增减比例': self._calc_change_rate(
                                row['买单-持买单量'], 
                                row['买单-比上一交易日增减']
                            )
                        })
                        company_data.append(buy_data)
                        
                    if c3 == company:
                        sell_data = base_data.copy()
                        sell_data.update({
                            '统计维度': '持卖单量',
                            '排名': row['排名'],
                            '数值': row['卖单-持卖单量'],
                            '比上一交易日增减': row['卖单-比上一交易日增减'],
                            '比上一交易日增减比例': self._calc_change_rate(
                                row['卖单-持卖单量'], 
                                row['卖单-比上一交易日增减']
                            )
                        })
                        company_data.append(sell_data)
            
            if company_data:
                result_dfs.append(pd.DataFrame(company_data))
        
        return pd.concat(result_dfs, ignore_index=True) if result_dfs else pd.DataFrame()

    def process_original_format(self, df):
        """Process data in original format with all dimensions combined"""
        df = self._clean_data(df)  # 清理数据
        result_rows = []
        grouped = df.groupby(['date', '合约代码', '合约类型'])
        
        for (date, contract, contract_type), group in grouped:
            # Get all unique companies in this group
            companies = set(
                self._clean_company_name(x) for x in 
                group['成交量-会员简称'].tolist() +
                group['买单-会员简称'].tolist() +
                group['卖单-会员简称'].tolist()
                if pd.notna(x)
            )
            
            for company in companies:
                row_data = {
                    'date': date,
                    '合约代码': contract,
                    '公司简称': company,
                    '合约类型': contract_type,
                    '是否主连合约': self.is_main_contract(date, contract),
                    '成交量排名': '',
                    '买单排名': '',
                    '卖单排名': '',
                    '成交量': '',
                    '成交量-比上一交易日增减': '',
                    '成交量-比上一交易日增减比例': '',
                    '买单-持买单量': '',
                    '买单-比上一交易日增减': '',
                    '买单-比上一交易日增减比例': '',
                    '卖单-持卖单量': '',
                    '卖单-比上一交易日增减': '',
                    '卖单-比上一交易日增减比例': ''
                }
                
                # Fill data for each dimension
                for _, record in group.iterrows():
                    if self._clean_company_name(record['成交量-会员简称']) == company:
                        row_data.update({
                            '成交量排名': record['排名'],
                            '成交量': record['成交量-成交量'],
                            '成交量-比上一交易日增减': record['成交量-比上一交易日增减'],
                            '成交量-比上一交易日增减比例': self._calc_change_rate(
                                record['成交量-成交量'],
                                record['成交量-比上一交易日增减']
                            )
                        })
                        
                    if self._clean_company_name(record['买单-会员简称']) == company:
                        row_data.update({
                            '买单排名': record['排名'],
                            '买单-持买单量': record['买单-持买单量'],
                            '买单-比上一交易日增减': record['买单-比上一交易日增减'],
                            '买单-比上一交易日增减比例': self._calc_change_rate(
                                record['买单-持买单量'],
                                record['买单-比上一交易日增减']
                            )
                        })
                        
                    if self._clean_company_name(record['卖单-会员简称']) == company:
                        row_data.update({
                            '卖单排名': record['排名'],
                            '卖单-持卖单量': record['卖单-持卖单量'],
                            '卖单-比上一交易日增减': record['卖单-比上一交易日增减'],
                            '卖单-比上一交易日增减比例': self._calc_change_rate(
                                record['卖单-持卖单量'],
                                record['卖单-比上一交易日增减']
                            )
                        })
                
                result_rows.append(row_data)
        
        return pd.DataFrame(result_rows)

    def _calc_change_rate(self, current, change):
        """Calculate change rate"""
        try:
            prev = float(current) - float(change)
            return float(change) / prev if prev != 0 else None
        except (ValueError, TypeError):
            return None

# 使用示例
# processor = FoDataProcessor()
# processed_by_company = processor.process_by_company(df)
# processed_original = processor.process_original_format(df)