"""
数据源管理器
实现akshare和tushare的自动切换机制
"""

import os
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()


class DataSourceManager:
    """数据源管理器 - 实现akshare与tushare自动切换"""
    
    def __init__(self):
        self.tushare_token = os.getenv('TUSHARE_TOKEN', '')
        self.tushare_available = False
        self.tushare_api = None
        
        # 初始化tushare
        if self.tushare_token:
            try:
                import tushare as ts
                ts.set_token(self.tushare_token)
                self.tushare_api = ts.pro_api()
                self.tushare_available = True
                print("✅ Tushare数据源初始化成功")
            except Exception as e:
                print(f"⚠️ Tushare数据源初始化失败: {e}")
                self.tushare_available = False
        else:
            print("ℹ️ 未配置Tushare Token，将仅使用Akshare数据源")
    
    def _add_market_prefix(self, symbol):
        """为股票代码添加市场前缀（sh/sz/bj）"""
        if symbol.startswith('6'):
            return f"sh{symbol}"
        elif symbol.startswith('0') or symbol.startswith('3'):
            return f"sz{symbol}"
        elif symbol.startswith('8') or symbol.startswith('4'):
            return f"bj{symbol}"
        return symbol

    def get_stock_hist_data(self, symbol, start_date=None, end_date=None, adjust='qfq'):
        """
        获取股票历史数据（优先akshare，失败时使用tushare）
        """
        # 标准化日期格式
        if start_date:
            start_date = start_date.replace('-', '')
        if end_date:
            end_date = end_date.replace('-', '')
        else:
            end_date = datetime.now().strftime('%Y%m%d')
        
        # 优先使用akshare
        try:
            import akshare as ak
            print(f"[Akshare] 正在获取 {symbol} 的历史数据...")
            
            # 尝试不同的接口，优先级：Tencent -> Sina -> EastMoney
            
            # 1. 尝试 stock_zh_a_daily (新浪财经)
            try:
                sina_symbol = self._add_market_prefix(symbol)
                # 新浪接口不需要结束日期，只支持symbol
                # 注意：新浪接口可能需要 adjusting
                df = ak.stock_zh_a_daily(symbol=sina_symbol, start_date=start_date, end_date=end_date, adjust=adjust)
                if df is not None and not df.empty:
                     df = df.rename(columns={
                        'date': 'Date',
                        'open': 'Open',
                        'close': 'Close',
                        'high': 'High',
                        'low': 'Low',
                        'volume': 'Volume',
                        'amount': 'Amount'
                    })
                     df['Date'] = pd.to_datetime(df['Date'])
                     if 'Date' in df.columns:
                        df.set_index('Date', inplace=True)
                     
                     # 检查是否有Volume列
                     if 'Volume' in df.columns:
                         print(f"[Akshare] ✅ 成功通过 stock_zh_a_daily (新浪) 获取 {len(df)} 条数据")
                         return df
                     else:
                         print(f"[Akshare] stock_zh_a_daily (新浪) 返回数据缺少 Volume 列")
            except Exception as e:
                 print(f"[Akshare] stock_zh_a_daily (新浪) 接口失败: {e}")

            # 2. 尝试 stock_zh_a_hist_tx (腾讯财经)
            try:
                tx_symbol = self._add_market_prefix(symbol)
                # Tencent接口通常支持 YYYYMMDD 格式
                df = ak.stock_zh_a_hist_tx(
                    symbol=tx_symbol,
                    start_date=start_date if start_date else "19900101",
                    end_date=end_date,
                    adjust=adjust
                )
                if df is not None and not df.empty:
                    # 打印一下实际列名以便调试
                    print(f"[Akshare] 腾讯接口返回列名: {df.columns.tolist()}")
                    
                    # 腾讯接口通常返回: date, open, close, high, low, amount
                    # 注意：腾讯接口的 amount 其实是成交量(手)，需要乘以100转换为股
                    # 腾讯接口没有成交额数据
                    
                    # 重命名列
                    rename_dict = {
                        'date': 'Date',
                        'open': 'Open',
                        'close': 'Close',
                        'high': 'High',
                        'low': 'Low',
                    }
                    
                    # 特殊处理 amount -> Volume
                    if 'amount' in df.columns:
                        df['Volume'] = df['amount'] * 100
                        # 移除原始 amount 列，因为它不是成交额
                        # df = df.drop(columns=['amount'])
                        # 或者保留但重命名为 Volume_Hand? 不，直接作为 Volume
                    
                    df = df.rename(columns=rename_dict)
                    
                    # 如果列名是中文，进行转换
                    if '日期' in df.columns:
                        df = df.rename(columns={
                            '日期': 'Date',
                            '开盘': 'Open',
                            '收盘': 'Close',
                            '最高': 'High',
                            '最低': 'Low',
                            '成交量': 'Volume',
                            '成交额': 'Amount'
                        })
                    
                    # 处理可能的小写列名
                    if 'Volume' not in df.columns:
                        if 'volume' in df.columns:
                            df = df.rename(columns={'volume': 'Volume'})
                        elif 'vol' in df.columns:
                            df = df.rename(columns={'vol': 'Volume'})
                            
                    # 处理其他可能的小写列名
                    for col in ['Open', 'Close', 'High', 'Low', 'Amount']:
                        lower_col = col.lower()
                        if col not in df.columns and lower_col in df.columns:
                            df = df.rename(columns={lower_col: col})
                    
                    df['Date'] = pd.to_datetime(df['Date'])
                    # 确保索引是日期
                    if 'Date' in df.columns:
                        df.set_index('Date', inplace=True)
                        
                    if 'Volume' in df.columns:
                        print(f"[Akshare] ✅ 成功通过 stock_zh_a_hist_tx (腾讯) 获取 {len(df)} 条数据")
                        return df
                    else:
                        print(f"[Akshare] stock_zh_a_hist_tx (腾讯) 转换后仍缺少 Volume 列")
            except Exception as e:
                print(f"[Akshare] stock_zh_a_hist_tx (腾讯) 接口失败: {e}")

            # 3. 尝试 stock_zh_a_hist (东方财富)
            try:
                df = ak.stock_zh_a_hist(
                    symbol=symbol,
                    period="daily",
                    start_date=start_date,
                    end_date=end_date,
                    adjust=adjust
                )
                if df is not None and not df.empty:
                    # 标准化列名
                    df = df.rename(columns={
                        '日期': 'Date',
                        '开盘': 'Open',
                        '收盘': 'Close',
                        '最高': 'High',
                        '最低': 'Low',
                        '成交量': 'Volume',
                        '成交额': 'Amount',
                        '振幅': 'amplitude',
                        '涨跌幅': 'pct_change',
                        '涨跌额': 'change',
                        '换手率': 'turnover'
                    })
                    df['Date'] = pd.to_datetime(df['Date'])
                    if 'Date' in df.columns:
                        df.set_index('Date', inplace=True)
                    print(f"[Akshare] ✅ 成功通过 stock_zh_a_hist (东财) 获取 {len(df)} 条数据")
                    return df
            except Exception as e:
                print(f"[Akshare] stock_zh_a_hist (东财) 接口失败: {e}")

        except Exception as e:
            print(f"[Akshare] ❌ 获取失败: {e}")
        
        # akshare失败，尝试tushare
        if self.tushare_available:
            try:
                print(f"[Tushare] 正在获取 {symbol} 的历史数据（备用数据源）...")
                
                # 转换股票代码格式（添加市场后缀）
                ts_code = self._convert_to_ts_code(symbol)
                
                # 转换复权类型
                adj_dict = {'qfq': 'qfq', 'hfq': 'hfq', '': None}
                adj = adj_dict.get(adjust, 'qfq')
                
                # 格式化日期
                start = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}" if start_date else None
                end = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}" if end_date else None
                
                # 获取数据
                df = self.tushare_api.daily(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date,
                    adj=adj
                )
                
                if df is not None and not df.empty:
                    # 标准化列名和数据格式
                    df = df.rename(columns={
                        'trade_date': 'date',
                        'vol': 'volume',
                        'amount': 'amount'
                    })
                    df['date'] = pd.to_datetime(df['date'])
                    df = df.sort_values('date')
                    
                    # 转换成交量单位（tushare单位是手，转换为股）
                    df['volume'] = df['volume'] * 100
                    # 转换成交额单位（tushare单位是千元，转换为元）
                    df['amount'] = df['amount'] * 1000
                    
                    print(f"[Tushare] ✅ 成功获取 {len(df)} 条数据")
                    return df
            except Exception as e:
                print(f"[Tushare] ❌ 获取失败: {e}")
        
        # 两个数据源都失败
        print("❌ 所有数据源均获取失败")
        return None
    
    def get_stock_basic_info(self, symbol):
        """
        获取股票基本信息（优先akshare，失败时使用tushare）
        
        Args:
            symbol: 股票代码
            
        Returns:
            dict: 股票基本信息
        """
        info = {
            "symbol": symbol,
            "name": "未知",
            "industry": "未知",
            "market": "未知"
        }
        
        # 优先使用akshare
        try:
            import akshare as ak
            print(f"[Akshare] 正在获取 {symbol} 的基本信息...")
            
            stock_info = ak.stock_individual_info_em(symbol=symbol)
            if stock_info is not None and not stock_info.empty:
                for _, row in stock_info.iterrows():
                    key = row['item']
                    value = row['value']
                    
                    if key == '股票简称':
                        info['name'] = value
                    elif key == '所处行业':
                        info['industry'] = value
                    elif key == '上市时间':
                        info['list_date'] = value
                    elif key == '总市值':
                        info['market_cap'] = value
                    elif key == '流通市值':
                        info['circulating_market_cap'] = value
                
                print(f"[Akshare] ✅ 成功获取基本信息")
                return info
        except Exception as e:
            print(f"[Akshare] ❌ 获取失败: {e}")
        
        # akshare失败，尝试tushare
        if self.tushare_available:
            try:
                print(f"[Tushare] 正在获取 {symbol} 的基本信息（备用数据源）...")
                
                ts_code = self._convert_to_ts_code(symbol)
                df = self.tushare_api.stock_basic(
                    ts_code=ts_code,
                    fields='ts_code,name,area,industry,market,list_date'
                )
                
                if df is not None and not df.empty:
                    info['name'] = df.iloc[0]['name']
                    info['industry'] = df.iloc[0]['industry']
                    info['market'] = df.iloc[0]['market']
                    info['list_date'] = df.iloc[0]['list_date']
                    
                    print(f"[Tushare] ✅ 成功获取基本信息")
                    return info
            except Exception as e:
                print(f"[Tushare] ❌ 获取失败: {e}")
        
        return info
    
    def get_realtime_quotes(self, symbol):
        """
        获取实时行情数据（优先akshare，失败时使用tushare）
        
        Args:
            symbol: 股票代码
            
        Returns:
            dict: 实时行情数据
        """
        quotes = {}
        
        # 优先使用akshare
        try:
            import akshare as ak
            print(f"[Akshare] 正在获取 {symbol} 的实时行情...")
            
            df = ak.stock_zh_a_spot_em()
            stock_df = df[df['代码'] == symbol]
            
            if not stock_df.empty:
                row = stock_df.iloc[0]
                quotes = {
                    'symbol': symbol,
                    'name': row['名称'],
                    'price': row['最新价'],
                    'change_percent': row['涨跌幅'],
                    'change': row['涨跌额'],
                    'volume': row['成交量'],
                    'amount': row['成交额'],
                    'high': row['最高'],
                    'low': row['最低'],
                    'open': row['今开'],
                    'pre_close': row['昨收']
                }
                print(f"[Akshare] ✅ 成功获取实时行情")
                return quotes
        except Exception as e:
            print(f"[Akshare] ❌ 获取失败: {e}")
        
        # akshare失败，尝试tushare
        if self.tushare_available:
            try:
                print(f"[Tushare] 正在获取 {symbol} 的实时行情（备用数据源）...")
                
                ts_code = self._convert_to_ts_code(symbol)
                df = self.tushare_api.daily(
                    ts_code=ts_code,
                    start_date=datetime.now().strftime('%Y%m%d'),
                    end_date=datetime.now().strftime('%Y%m%d')
                )
                
                if df is not None and not df.empty:
                    row = df.iloc[0]
                    quotes = {
                        'symbol': symbol,
                        'price': row['close'],
                        'change_percent': row['pct_chg'],
                        'volume': row['vol'] * 100,
                        'amount': row['amount'] * 1000,
                        'high': row['high'],
                        'low': row['low'],
                        'open': row['open'],
                        'pre_close': row['pre_close']
                    }
                    print(f"[Tushare] ✅ 成功获取实时行情")
                    return quotes
            except Exception as e:
                print(f"[Tushare] ❌ 获取失败: {e}")
        
        return quotes
    
    def get_financial_data(self, symbol, report_type='income'):
        """
        获取财务数据（优先akshare，失败时使用tushare）
        
        Args:
            symbol: 股票代码
            report_type: 报表类型（'income'利润表, 'balance'资产负债表, 'cashflow'现金流量表）
            
        Returns:
            DataFrame: 财务数据
        """
        # 优先使用akshare
        try:
            import akshare as ak
            print(f"[Akshare] 正在获取 {symbol} 的财务数据...")
            
            if report_type == 'income':
                df = ak.stock_financial_report_sina(stock=symbol, symbol="利润表")
            elif report_type == 'balance':
                df = ak.stock_financial_report_sina(stock=symbol, symbol="资产负债表")
            elif report_type == 'cashflow':
                df = ak.stock_financial_report_sina(stock=symbol, symbol="现金流量表")
            else:
                df = None
            
            if df is not None and not df.empty:
                print(f"[Akshare] ✅ 成功获取财务数据")
                return df
        except Exception as e:
            print(f"[Akshare] ❌ 获取失败: {e}")
        
        # akshare失败，尝试tushare
        if self.tushare_available:
            try:
                print(f"[Tushare] 正在获取 {symbol} 的财务数据（备用数据源）...")
                
                ts_code = self._convert_to_ts_code(symbol)
                
                if report_type == 'income':
                    df = self.tushare_api.income(ts_code=ts_code)
                elif report_type == 'balance':
                    df = self.tushare_api.balancesheet(ts_code=ts_code)
                elif report_type == 'cashflow':
                    df = self.tushare_api.cashflow(ts_code=ts_code)
                else:
                    df = None
                
                if df is not None and not df.empty:
                    print(f"[Tushare] ✅ 成功获取财务数据")
                    return df
            except Exception as e:
                print(f"[Tushare] ❌ 获取失败: {e}")
        
        return None
    
    def _convert_to_ts_code(self, symbol):
        """
        将6位股票代码转换为tushare格式（带市场后缀）
        
        Args:
            symbol: 6位股票代码
            
        Returns:
            str: tushare格式代码（如：000001.SZ）
        """
        if not symbol or len(symbol) != 6:
            return symbol
        
        # 根据代码判断市场
        if symbol.startswith('6'):
            # 上海主板
            return f"{symbol}.SH"
        elif symbol.startswith('0') or symbol.startswith('3'):
            # 深圳主板和创业板
            return f"{symbol}.SZ"
        elif symbol.startswith('8') or symbol.startswith('4'):
            # 北交所
            return f"{symbol}.BJ"
        else:
            # 默认深圳
            return f"{symbol}.SZ"
    
    def _convert_from_ts_code(self, ts_code):
        """
        将tushare格式代码转换为6位代码
        
        Args:
            ts_code: tushare格式代码（如：000001.SZ）
            
        Returns:
            str: 6位股票代码
        """
        if '.' in ts_code:
            return ts_code.split('.')[0]
        return ts_code


# 全局数据源管理器实例
data_source_manager = DataSourceManager()

