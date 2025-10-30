#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
热门题材趋势票埋伏策略

策略逻辑：
1. 最近3个月涨停或涨幅>=10%的次数至少2次
2. 近2~3日换手率5%以上
3. 基本面较好，PE值在130以内
4. 近5日涨幅不超过30%，不低于10%
5. 个股所属题材，近10个交易日，在TOP10中出现过3次及以上
6. 近5个交易日股价不较大的偏离5日线
"""

import backtrader as bt
import pandas as pd
import numpy as np
import datetime
import logging
import sys
import os
from typing import Dict, List, Tuple, Optional, Any
import pdb

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from backtest.data.theme import ThemeDataLoader
from backtest.data.loader import Loader


class HotThemeTrendStockStrategy(bt.Strategy):
    """
    热门题材趋势票埋伏策略
    
    策略逻辑：
    1. 最近3个月涨停或涨幅>=10%的次数至少2次
    2. 近2~3日换手率5%以上
    3. 基本面较好，PE值在130以内
    4. 近5日涨幅不超过30%，不低于10%
    5. 个股所属题材，近10个交易日，在TOP10中出现过3次及以上
    6. 近5个交易日股价不较大的偏离5日线
    """
    
    # 策略参数
    params = (
        ('min_zt_count', 2),             # 最近3个月涨停或涨幅>=10%的最小次数
        ('min_turnover_rate', 5.0),      # 最小换手率
        ('max_pe', 130.0),               # 最大PE值
        ('min_5d_return', 10.0),         # 最小5日涨幅
        ('max_5d_return', 30.0),         # 最大5日涨幅
        ('min_theme_top10_count', 3),    # 题材近10日在TOP10中出现的最小次数
        ('max_ma5_deviation', 3.0),      # 股价偏离5日线的最大百分比
        ('stop_loss_pct', 5.0),          # 止损百分比
        ('take_profit_pct', 10.0),       # 止盈百分比
        ('max_holding_days', 5),         # 最大持仓天数
        ('verbose', False),              # 是否输出详细日志
    )
    
    def __init__(self):
        """
        初始化策略
        """
        # 初始化数据加载器
        self.theme_loader = ThemeDataLoader()
        self.loader = Loader()
        
        # 初始化日志
        self.setup_logging()
        
        # 初始化持仓信息
        self.positions_info = {}  # 记录持仓信息：{code: {'entry_price': price, 'entry_date': date, 'days_held': 0}}
        
        # 初始化结果记录
        self.result = {
            'params': dict(self.p._getkwargs()),
            'trades': [],
            'final_value': 0.0,
            'final_cash': 0.0,
            'pnl': 0.0
        }
        
        # 初始化交易日历和主题数据缓存
        self.calendar = self.load_trade_calendar()
        self.theme_top10_cache = {}  # 缓存题材TOP10数据: {date: [theme_codes]}
        self.theme_stock_map = None  # 题材股票映射: {theme_code: [stock_codes]}
        self.stock_theme_map = {}    # 股票题材映射: {stock_code: [theme_codes]}
        
        # 初始化股票历史数据缓存
        self.stock_history_cache = {}  # 缓存股票历史数据: {code: pd.DataFrame}
        
        # 预加载题材股票映射关系
        self.preload_theme_stock_relation()
    
    def setup_logging(self):
        """
        设置日志
        """
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO if self.p.verbose else logging.WARNING)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    def log(self, txt, level=logging.INFO):
        """
        记录日志，同时在终端打印
        """
        dt = self.data.datetime.datetime(0)
        log_message = f'{dt.isoformat()} - {txt}'
        self.logger.log(level, log_message)
        # 同时在终端打印日志
        print(log_message)
    
    def load_trade_calendar(self) -> List[str]:
        """
        加载交易日历
        """
        try:
            if not self.loader._connect():
                self.log("数据库连接失败", logging.ERROR)
                return []
            
            # 获取当前回测的开始和结束日期
            start_date = self.data.datetime.datetime(0).strftime('%Y-%m-%d')
            # 获取3个月前的日期作为查询起点
            three_months_ago = (self.data.datetime.datetime(0) - datetime.timedelta(days=90)).strftime('%Y-%m-%d')
            
            # 查询交易日历
            sql = "SELECT cal_date FROM trade_market_calendar WHERE is_open = 1 AND cal_date >= %s ORDER BY cal_date"
            calendar_df = pd.read_sql(sql, self.loader.connection, params=[three_months_ago])
            
            self.loader._disconnect()
            
            if calendar_df.empty:
                self.log("交易日历数据为空", logging.ERROR)
                return []
            
            # 转换为日期字符串列表
            calendar = calendar_df['cal_date'].apply(str).tolist()
            self.log(f"加载交易日历成功，共{len(calendar)}个交易日")

            return calendar
            
        except Exception as e:
            self.log(f"加载交易日历失败: {e}", logging.ERROR)
            return []
    
    def preload_theme_stock_relation(self):
        """
        预加载题材股票关联关系
        """
        try:
            # 加载题材股票映射
            self.theme_stock_map = self.theme_loader.get_theme_related_stocks()
            if not self.theme_stock_map:
                self.log("题材股票映射加载失败", logging.ERROR)
                return
            
            # 构建股票题材映射
            for theme_code, stock_codes in self.theme_stock_map.items():
                for stock_code in stock_codes:
                    if stock_code not in self.stock_theme_map:
                        self.stock_theme_map[stock_code] = []
                    self.stock_theme_map[stock_code].append(theme_code)
            
            self.log(f"预加载题材股票关联关系成功，共{len(self.theme_stock_map)}个题材，{len(self.stock_theme_map)}只股票")
            
        except Exception as e:
            self.log(f"预加载题材股票关联关系失败: {e}", logging.ERROR)
    
    def next(self):
        """
        策略主逻辑
        """
        # pdb.set_trace()
        # 更新持仓天数
        self.update_positions_info()
        
        # 检查卖出条件
        self.check_sell_conditions()
        
        # 检查买入条件
        self.check_buy_conditions()
    
    def update_positions_info(self):
        """
        更新持仓信息
        """
        # 更新已有持仓的天数
        for code in list(self.positions_info.keys()):
            # 检查是否还持有该股票
            if code not in [d._name for d in self.datas if self.getposition(d).size > 0]:
                del self.positions_info[code]
                continue
            
            # 更新持仓天数
            self.positions_info[code]['days_held'] += 1
    
    def check_buy_conditions(self):
        """
        检查买入条件
        使用前一个交易日的数据，避免使用未来数据
        """
        current_date = self.data.datetime.datetime(0).strftime('%Y-%m-%d')
        
        # 获取当前日期在交易日历中的索引
        try:
            current_idx = self.calendar.index(current_date)
        except ValueError:
            self.log(f"当前日期{current_date}不在交易日历中", logging.ERROR)
            return
        
        # 使用前一个交易日作为基准日期，避免使用未来数据
        if current_idx <= 0:
            self.log(f"没有前一个交易日的数据")
            return
        
        previous_date = self.calendar[current_idx - 1]
        self.log(f"使用前一交易日{previous_date}的数据进行买入决策")
        
        # 获取近10个交易日的TOP10题材出现次数
        theme_appearance_count = self.count_theme_appearances(previous_date, days=10)
        if not theme_appearance_count:
            self.log(f"未能统计近10个交易日的题材出现次数")
            return
        
        # 筛选出现次数达标的题材
        qualified_themes = [theme for theme, count in theme_appearance_count.items() 
                           if count >= self.p.min_theme_top10_count]
        
        if not qualified_themes:
            self.log(f"没有题材在近10个交易日的TOP10中出现{self.p.min_theme_top10_count}次及以上")
            return
        
        self.log(f"符合条件的热门题材: {qualified_themes}")
        
        # 获取这些题材关联的股票
        candidate_stocks = set()
        for theme in qualified_themes:
            if theme in self.theme_stock_map:
                candidate_stocks.update(self.theme_stock_map[theme])
        
        self.log(f"热门题材关联的候选股票数量: {len(candidate_stocks)}")
        
        # 检查每只候选股票是否符合条件
        for data in self.datas:
            code = data._name
            
            # 跳过已持仓的股票
            if self.getposition(data).size > 0:
                continue
            
            # 跳过不在候选列表中的股票
            if code not in candidate_stocks:
                continue
            
            # 检查股票是否符合所有买入条件
            if self.should_buy_stock(data, previous_date):
                self.execute_buy(data)
    
    def get_top_themes(self, date: str, top_n: int = 10) -> List[str]:
        """
        获取指定日期的TOP题材代码列表
        """
        # 检查缓存
        if date in self.theme_top10_cache:
            return self.theme_top10_cache[date]
        
        # 从数据库加载
        top_themes_df = self.theme_loader.get_top_themes_by_rank(date, top_n=top_n)
        if top_themes_df is None or top_themes_df.empty:
            return []
        
        # 提取题材代码列表
        theme_codes = top_themes_df['code'].tolist()
        
        # 更新缓存
        self.theme_top10_cache[date] = theme_codes
        
        return theme_codes
    
    def count_theme_appearances(self, previous_date: str, days: int = 10) -> Dict[str, int]:
        """
        统计近N个交易日题材在TOP10中出现的次数
        使用前一个交易日作为基准，避免使用未来数据
        """
        # 获取前一日期在交易日历中的索引
        try:
            previous_idx = self.calendar.index(previous_date)
        except ValueError:
            self.log(f"前一日期{previous_date}不在交易日历中", logging.ERROR)
            return {}
        
        # 确保有足够的历史数据
        if previous_idx <= 0:
            self.log(f"没有足够的历史数据")
            return {}
        
        # 确保有足够的历史数据
        if previous_idx < days:
            self.log(f"没有足够的历史数据来统计近{days}个交易日的题材出现次数")
            return {}
        
        # 获取近N个交易日的日期列表，使用前一个交易日作为结束日期
        date_list = self.calendar[previous_idx-days:previous_idx]
        
        # 统计每个题材出现的次数
        theme_count = {}
        for date in date_list:
            top_themes = self.get_top_themes(date)
            for theme in top_themes:
                theme_count[theme] = theme_count.get(theme, 0) + 1
        
        return theme_count
    
    def should_buy_stock(self, data, previous_date: str) -> bool:
        """
        检查股票是否符合所有买入条件
        """
        code = data._name
        
        # 1. 检查最近3个月涨停或涨幅>=10%的次数
        zt_count = self.check_zt_count(code, previous_date)
        if zt_count < self.p.min_zt_count:
            self.log(f"{code} 最近3个月涨停或大涨次数不足: {zt_count} < {self.p.min_zt_count}")
            return False
        
        # 2. 检查近3日换手率
        if not self.check_turnover_rate(data, previous_date):
            self.log(f"{code} 近期换手率不达标")
            return False
        
        # 3. 检查PE值
        if not self.check_pe_value(data, previous_date):
            self.log(f"{code} PE值不符合条件")
            return False
        
        # 4. 检查近5日涨幅
        if not self.check_recent_return(data, previous_date):
            self.log(f"{code} 近5日涨幅不符合条件")
            return False
        
        # 6. 检查股价偏离5日线情况
        if not self.check_ma5_deviation(data, previous_date):
            self.log(f"{code} 股价偏离5日线过大")
            return False
        
        self.log(f"{code} 符合所有买入条件")
        return True
    
    def check_zt_count(self, code: str, previous_date: str) -> int:
        """
        检查最近3个月涨停或涨幅>=10%的次数
        使用前一日数据，避免使用未来数据
        """
        # 获取前一日期在交易日历中的索引
        try:
            previous_idx = self.calendar.index(previous_date)
        except ValueError:
            self.log(f"前一日期{previous_date}不在交易日历中", logging.ERROR)
            return 0
        
        # 确保有足够的历史数据
        if previous_idx <= 0:
            self.log(f"没有足够的历史数据")
            return 0
        
        # 确保有足够的历史数据（约60个交易日为3个月）
        if previous_idx < 60:
            self.log(f"没有足够的历史数据来检查最近3个月的涨停情况")
            return 0
        
        # 获取近3个月的日期范围，使用前一个交易日作为结束日期
        start_date = self.calendar[previous_idx-60]
        end_date = previous_date
        
        # 从缓存获取或加载股票历史数据
        if code not in self.stock_history_cache:
            self.load_stock_history(code, start_date, end_date)
        
        if code not in self.stock_history_cache or self.stock_history_cache[code].empty:
            self.log(f"无法获取{code}的历史数据")
            return 0
        
        # 计算涨停或大涨次数
        df = self.stock_history_cache[code]
        # 筛选日期范围
        df = df[(df['datetime'] >= pd.to_datetime(start_date)) & 
                (df['datetime'] <= pd.to_datetime(end_date))]
        
        # 计算涨停或涨幅>=10%的次数
        zt_count = len(df[df['chg_pct'] >= 9.9])  # 涨停通常为9.9%以上
        big_rise_count = len(df[(df['chg_pct'] >= 10.0) & (df['chg_pct'] < 9.9)])  # 涨幅>=10%但不是涨停
        
        total_count = zt_count + big_rise_count
        self.log(f"{code} 最近3个月涨停{zt_count}次，大涨{big_rise_count}次，总计{total_count}次")
        
        return total_count
    
    def load_stock_history(self, code: str, start_date: str, end_date: str):
        """
        加载股票历史数据
        """
        try:
            if not self.loader._connect():
                self.log("数据库连接失败", logging.ERROR)
                return
            
            # 构建SQL查询语句
            sql = """
            SELECT d.*, b.turnover_rate, b.pe, b.pe_ttm 
            FROM trade_market_stock_daily d
            LEFT JOIN trade_market_stock_basic_daily b ON d.code = b.code AND d.trade_date = b.trade_date
            WHERE d.code = %s AND d.trade_date BETWEEN %s AND %s
            ORDER BY d.trade_date
            """
            
            # 执行查询
            df = pd.read_sql(sql, self.loader.connection, params=[code, start_date, end_date])
            
            # 处理数据
            if not df.empty:
                df = self.loader._process_dataframe(df)
                self.stock_history_cache[code] = df
                self.log(f"加载{code}的历史数据成功，共{len(df)}行")
            else:
                self.log(f"未找到{code}的历史数据")
            
            self.loader._disconnect()
            
        except Exception as e:
            self.log(f"加载{code}的历史数据失败: {e}", logging.ERROR)
            self.loader._disconnect()
    
    def check_turnover_rate(self, data, previous_date=None) -> bool:
        """
        检查近3日换手率是否连续达标（5%及以上）
        要求最近3天连续达标
        """
        # 获取当前股票代码
        code = data._name
        
        # 获取前一日日期
        current_date = self.datetime.date()
        if previous_date is None:
            previous_date = self.data.datetime.datetime(-1).date()
        
        # 计算需要查询的日期范围（前3个交易日）
        # 如果previous_date是datetime类型，则转换为字符串格式，否则直接使用
        end_date = previous_date if isinstance(previous_date, str) else previous_date.strftime('%Y-%m-%d')
        # 获取前5个交易日，确保能获取到3天数据
        if isinstance(previous_date, str):
            # 将字符串转换为datetime对象，然后减去天数，再转回字符串
            prev_date_obj = datetime.datetime.strptime(previous_date, '%Y-%m-%d').date()
            start_date = (prev_date_obj - datetime.timedelta(days=10)).strftime('%Y-%m-%d')
        else:
            start_date = (previous_date - datetime.timedelta(days=10)).strftime('%Y-%m-%d')
        
        # 从数据库加载数据
        need_load = False
        if code not in self.stock_history_cache:
            need_load = True
        else:
            # 检查缓存数据的日期范围是否包含所需日期
            hist_data = self.stock_history_cache[code]
            if hist_data.empty or start_date not in hist_data['datetime'].dt.strftime('%Y-%m-%d').values:
                need_load = True
        
        if need_load:
            self.load_stock_history(code, start_date, end_date)
        
        # 检查是否成功加载数据
        if code not in self.stock_history_cache or self.stock_history_cache[code].empty:
            self.log(f"{code} 无法获取历史数据，跳过换手率检查")
            return False
        
        # 获取历史数据
        hist_data = self.stock_history_cache[code]
        
        # 按日期降序排序
        hist_data = hist_data.sort_values('datetime', ascending=False)
        
        # 获取最近3天的换手率
        recent_data = hist_data.head(3)
        if len(recent_data) < 3:
            self.log(f"{code} 历史数据不足3天，无法检查换手率")
            return False
        
        # 检查最近3天是否连续达标
        turnover_rates = recent_data['turnover_rate'].values
        if all(rate >= self.p.min_turnover_rate for rate in turnover_rates):
            self.log(f"{code} 最近3天换手率连续达标")
            return True
        
        self.log(f"{code} 最近3天换手率不达标: {turnover_rates}")
        return False

        return False
    
    def check_pe_value(self, data, previous_date=None) -> bool:
        """
        检查PE值是否在要求范围内（<=130）
        使用前一日数据，避免使用未来数据
        """
        # 获取当前股票代码
        code = data._name
        
        # 获取前一日日期
        current_date = self.datetime.date()
        if previous_date is None:
            previous_date = self.data.datetime.datetime(-1).date()
        
        # 计算需要查询的日期范围
        # 如果previous_date是datetime类型，则转换为字符串格式，否则直接使用
        end_date = previous_date if isinstance(previous_date, str) else previous_date.strftime('%Y-%m-%d')
        # 获取前5天数据，确保能获取到前一日
        if isinstance(previous_date, str):
            # 将字符串转换为datetime对象，然后减去天数，再转回字符串
            prev_date_obj = datetime.datetime.strptime(previous_date, '%Y-%m-%d').date()
            start_date = (prev_date_obj - datetime.timedelta(days=5)).strftime('%Y-%m-%d')
        else:
            start_date = (previous_date - datetime.timedelta(days=5)).strftime('%Y-%m-%d')
        
        # 从数据库加载数据
        need_load = False
        if code not in self.stock_history_cache:
            need_load = True
        else:
            # 检查缓存数据的日期范围是否包含所需日期
            hist_data = self.stock_history_cache[code]
            if hist_data.empty or start_date not in hist_data['datetime'].dt.strftime('%Y-%m-%d').values:
                need_load = True
        
        if need_load:
            self.load_stock_history(code, start_date, end_date)
        
        # 检查是否成功加载数据
        if code not in self.stock_history_cache or self.stock_history_cache[code].empty:
            self.log(f"{code} 无法获取历史数据，跳过PE值检查")
            return False
        
        # 获取历史数据
        hist_data = self.stock_history_cache[code]
        
        # 按日期降序排序
        hist_data = hist_data.sort_values('datetime', ascending=False)
        
        # 获取前一日的PE值
        if len(hist_data) < 1:
            self.log(f"{code} 历史数据不足，无法检查PE值")
            return False
            
        pe = hist_data.iloc[0]['pe']
        
        # 检查PE值是否有效且在范围内
        if pd.isna(pe) or pe <= 0:  # PE为负数表示亏损
            self.log(f"{code} PE值无效或为负")
            return False
        
        self.log(f"{code} PE值: {pe:.2f}")
        
        return pe <= self.p.max_pe
    
    def check_recent_return(self, data, previous_date=None) -> bool:
        """
        检查近5日涨幅是否在要求范围内（10%~30%）
        使用前一日数据，避免使用未来数据
        """
        # 获取当前股票代码
        code = data._name
        
        # 获取前一日日期
        current_date = self.datetime.date()
        if previous_date is None:
            previous_date = self.data.datetime.datetime(-1).date()
        
        # 计算需要查询的日期范围
        # 如果previous_date是datetime类型，则转换为字符串格式，否则直接使用
        end_date = previous_date if isinstance(previous_date, str) else previous_date.strftime('%Y-%m-%d')
        # 获取前10个交易日，确保能获取到5日前的数据
        if isinstance(previous_date, str):
            # 将字符串转换为datetime对象，然后减去天数，再转回字符串
            prev_date_obj = datetime.datetime.strptime(previous_date, '%Y-%m-%d').date()
            start_date = (prev_date_obj - datetime.timedelta(days=15)).strftime('%Y-%m-%d')
        else:
            start_date = (previous_date - datetime.timedelta(days=15)).strftime('%Y-%m-%d')
        
        # 从数据库加载数据
        need_load = False
        if code not in self.stock_history_cache:
            need_load = True
        else:
            # 检查缓存数据的日期范围是否包含所需日期
            hist_data = self.stock_history_cache[code]
            if hist_data.empty or start_date not in hist_data['datetime'].dt.strftime('%Y-%m-%d').values:
                need_load = True
        
        if need_load:
            self.load_stock_history(code, start_date, end_date)
        
        # 检查是否成功加载数据
        if code not in self.stock_history_cache or self.stock_history_cache[code].empty:
            self.log(f"{code} 无法获取历史数据，跳过近5日涨幅检查")
            return False
        
        # 获取历史数据
        hist_data = self.stock_history_cache[code]
        
        # 按日期升序排序
        hist_data = hist_data.sort_values('datetime', ascending=True)
        
        # 检查是否有足够的数据
        if len(hist_data) < 6:  # 需要至少6天数据：前一日和之前的5天
            self.log(f"{code} 历史数据不足，无法计算近5日涨幅")
            return False
        
        # 获取前一日和5日前的收盘价
        last_5_days = hist_data.tail(6)
        close_5d_ago = last_5_days.iloc[0]['close']  # 6天前的收盘价
        previous_close = last_5_days.iloc[-1]['close']  # 前一日的收盘价
        
        if close_5d_ago <= 0:
            self.log(f"{code} 5日前收盘价无效")
            return False
        
        return_5d = (previous_close / close_5d_ago - 1) * 100
        
        self.log(f"{code} 近5日涨幅: {return_5d:.2f}%")
        
        # 检查涨幅是否在范围内
        return self.p.min_5d_return <= return_5d <= self.p.max_5d_return
    
    def check_ma5_deviation(self, data, previous_date=None) -> bool:
        """
        检查股价是否较大偏离5日线
        使用前一日数据，避免使用未来数据
        """
        # 获取当前股票代码
        code = data._name
        
        # 获取前一日日期
        current_date = self.datetime.date()
        if previous_date is None:
            previous_date = self.data.datetime.datetime(-1).date()
        
        # 计算需要查询的日期范围
        # 如果previous_date是datetime类型，则转换为字符串格式，否则直接使用
        end_date = previous_date if isinstance(previous_date, str) else previous_date.strftime('%Y-%m-%d')
        # 获取前10个交易日，确保能计算5日均线
        if isinstance(previous_date, str):
            # 将字符串转换为datetime对象，然后减去天数，再转回字符串
            prev_date_obj = datetime.datetime.strptime(previous_date, '%Y-%m-%d').date()
            start_date = (prev_date_obj - datetime.timedelta(days=15)).strftime('%Y-%m-%d')
        else:
            start_date = (previous_date - datetime.timedelta(days=15)).strftime('%Y-%m-%d')
        
        # 从数据库加载数据
        need_load = False
        if code not in self.stock_history_cache:
            need_load = True
        else:
            # 检查缓存数据的日期范围是否包含所需日期
            hist_data = self.stock_history_cache[code]
            if hist_data.empty or start_date not in hist_data['datetime'].dt.strftime('%Y-%m-%d').values:
                need_load = True
        
        if need_load:
            self.load_stock_history(code, start_date, end_date)
        
        # 检查是否成功加载数据
        if code not in self.stock_history_cache or self.stock_history_cache[code].empty:
            self.log(f"{code} 无法获取历史数据，跳过股价偏离5日线检查")
            return False
        
        # 获取历史数据
        hist_data = self.stock_history_cache[code]
        
        # 按日期升序排序
        hist_data = hist_data.sort_values('datetime', ascending=True)
        
        # 检查是否有足够的数据
        if len(hist_data) < 5:  # 需要至少5天数据计算5日均线
            self.log(f"{code} 历史数据不足，无法计算5日均线")
            return False
        
        # 计算5日均线
        hist_data['ma5'] = hist_data['close'].rolling(window=5).mean()
        
        # 获取前一日的收盘价和5日均线
        last_row = hist_data.iloc[-1]
        previous_price = last_row['close']
        previous_ma5 = last_row['ma5']
        
        if pd.isna(previous_ma5) or previous_ma5 <= 0:
            self.log(f"{code} 5日均线无效")
            return False
        
        # 计算偏离百分比
        deviation_pct = abs(previous_price / previous_ma5 - 1) * 100
        
        self.log(f"{code} 股价偏离5日线: {deviation_pct:.2f}%")
        
        # 检查偏离是否在允许范围内
        return deviation_pct <= self.p.max_ma5_deviation
    
    def execute_buy(self, data):
        """
        执行买入操作
        """
        code = data._name
        cash = self.broker.get_cash()
        value = self.broker.get_value()
        
        # 计算买入金额和数量
        target_value = value * self.p.position_size
        price = data.close[0]
        size = int(target_value / price / 100) * 100  # 买入数量按手（100股）取整
        
        if size <= 0:
            self.log(f"计算的买入数量为0，跳过买入 {code}")
            return
        
        # 创建买入订单
        self.log(f"买入 {code}: 价格={price:.2f}, 数量={size}, 金额={price*size:.2f}")
        self.buy(data=data, size=size)
        
        # 记录持仓信息
        self.positions_info[code] = {
            'entry_price': price,
            'entry_date': self.data.datetime.datetime(0),
            'days_held': 0
        }
    
    def check_sell_conditions(self):
        """
        检查卖出条件
        """
        for data in self.datas:
            code = data._name
            pos = self.getposition(data)
            
            # 跳过未持仓的股票
            if pos.size <= 0 or code not in self.positions_info:
                continue
            
            # 获取持仓信息
            position_info = self.positions_info[code]
            entry_price = position_info['entry_price']
            days_held = position_info['days_held']
            
            # 获取当前价格
            current_price = data.close[0]
            
            # 计算收益率
            profit_pct = (current_price / entry_price - 1) * 100
            
            # 检查止盈条件
            if profit_pct >= self.p.take_profit_pct:
                self.log(f"触发止盈: {code}, 收益率={profit_pct:.2f}%")
                self.sell(data=data, size=pos.size)
                continue
            
            # 检查止损条件
            if profit_pct <= -self.p.stop_loss_pct:
                self.log(f"触发止损: {code}, 收益率={profit_pct:.2f}%")
                self.sell(data=data, size=pos.size)
                continue
            
            # 检查最大持仓天数
            if days_held >= self.p.max_holding_days:
                self.log(f"达到最大持仓天数: {code}, 持仓{days_held}天")
                self.sell(data=data, size=pos.size)
                continue
    
    def notify_order(self, order):
        """
        订单状态通知
        """
        if order.status in [order.Submitted, order.Accepted]:
            return
        
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(f'买入成交: 价格={order.executed.price:.2f}, 数量={order.executed.size}, 金额={order.executed.value:.2f}, 手续费={order.executed.comm:.2f}')
            else:  # Sell
                profit = order.executed.pnl
                self.log(f'卖出成交: 价格={order.executed.price:.2f}, 数量={order.executed.size}, 金额={order.executed.value:.2f}, 手续费={order.executed.comm:.2f}, 盈亏={profit:.2f}')
                
                # 记录交易
                self.result['trades'].append({
                    'code': order.data._name,
                    'buy_date': self.positions_info[order.data._name]['entry_date'].strftime('%Y-%m-%d') if order.data._name in self.positions_info else '',
                    'buy_price': self.positions_info[order.data._name]['entry_price'] if order.data._name in self.positions_info else 0,
                    'sell_date': self.data.datetime.datetime(0).strftime('%Y-%m-%d'),
                    'sell_price': order.executed.price,
                    'size': order.executed.size,
                    'pnl': profit,
                    'return_pct': (order.executed.price / self.positions_info[order.data._name]['entry_price'] - 1) * 100 if order.data._name in self.positions_info else 0,
                    'days_held': self.positions_info[order.data._name]['days_held'] if order.data._name in self.positions_info else 0
                })
        
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log(f'订单取消/拒绝: {order.status}', logging.WARNING)
    
    def notify_trade(self, trade):
        """
        交易完成通知
        """
        if trade.isclosed:
            self.log(f'交易结束: 毛利润={trade.pnl:.2f}, 净利润={trade.pnlcomm:.2f}')
    
    def stop(self):
        """
        策略结束
        """
        # 记录最终结果
        self.result['final_value'] = self.broker.get_value()
        self.result['final_cash'] = self.broker.get_cash()
        self.result['pnl'] = self.broker.get_value() - self.broker.startingcash
        
        self.log(f"策略结束: 参数={self.p._getkwargs()}, 最终总资产={self.result['final_value']:.2f}, 现金={self.result['final_cash']:.2f}, 净收益={self.result['pnl']:.2f}")
        self.log(f"交易记录: {len(self.result['trades'])}笔")