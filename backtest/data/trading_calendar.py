#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
交易日历数据加载器
提供交易日历数据加载和查询功能
"""

import pandas as pd
from datetime import datetime, date
from typing import List, Optional
import sys
import os

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from backtest.data.loader import Loader


class Calendar:
    """
    交易日历类
    提供交易日历数据加载和查询功能
    """
    
    def __init__(self):
        """
        初始化交易日历
        """
        self.loader = Loader()
        self.calendar_data = None
        self.table_name = 'trade_market_calendar'
    
    def load_calendar_data(self, fromdate: str, todate: str) -> bool:
        """
        加载指定时间范围的交易日历数据
        
        Args:
            fromdate: 开始日期，格式：'YYYY-MM-DD'
            todate: 结束日期，格式：'YYYY-MM-DD'
            
        Returns:
            bool: 加载是否成功
        """
        try:
            # 使用自定义SQL查询，因为trade_market_calendar表使用cal_date而不是trade_date
            if not self.loader._connect():
                return False
            
            sql = f"""
            SELECT cal_date as datetime, is_open 
            FROM {self.table_name} 
            WHERE cal_date >= %s AND cal_date <= %s
            ORDER BY cal_date
            """
            
            self.calendar_data = pd.read_sql(sql, self.loader.connection, params=[fromdate, todate])
            
            # 确保datetime列是datetime类型
            if 'datetime' in self.calendar_data.columns:
                self.calendar_data['datetime'] = pd.to_datetime(self.calendar_data['datetime'])
            
            # 确保is_open列是布尔类型
            if 'is_open' in self.calendar_data.columns:
                self.calendar_data['is_open'] = self.calendar_data['is_open'].astype(bool)
            
            self.loader._disconnect()
            return True
            
        except Exception as e:
            print(f"交易日历数据加载失败: {e}")
            self.loader._disconnect()
            return False
    
    def get_trading_days(self, fromdate: str, todate: str) -> List[str]:
        """
        获取指定时间范围内的交易日列表
        
        Args:
            fromdate: 开始日期，格式：'YYYY-MM-DD'
            todate: 结束日期，格式：'YYYY-MM-DD'
            
        Returns:
            List[str]: 交易日列表，格式：['YYYY-MM-DD', ...]
        """
        # 如果没有加载数据或者日期范围不匹配，重新加载
        if (self.calendar_data is None or 
            self.calendar_data.empty or
            not self._is_date_range_covered(fromdate, todate)):
            
            if not self.load_calendar_data(fromdate, todate):
                return []
        
        # 筛选交易日
        trading_days = self.calendar_data[
            (self.calendar_data['datetime'] >= fromdate) & 
            (self.calendar_data['datetime'] <= todate) & 
            (self.calendar_data['is_open'] == True)
        ]['datetime']
        
        # 转换为字符串格式
        return [day.strftime('%Y-%m-%d') for day in trading_days]
    
    def is_trading_day(self, check_date: str) -> bool:
        """
        检查指定日期是否为交易日
        
        Args:
            check_date: 要检查的日期，格式：'YYYY-MM-DD'
            
        Returns:
            bool: 是否为交易日
        """
        # 如果没有加载数据，加载包含该日期的数据
        if (self.calendar_data is None or 
            self.calendar_data.empty or
            not self._is_date_range_covered(check_date, check_date)):
            
            if not self.load_calendar_data(check_date, check_date):
                return False
        
        # 查找指定日期
        result = self.calendar_data[
            (self.calendar_data['datetime'] == check_date) & 
            (self.calendar_data['is_open'] == True)
        ]
        
        return len(result) > 0
    
    def get_next_trading_day(self, current_date: str) -> Optional[str]:
        """
        获取指定日期之后的下一个交易日
        
        Args:
            current_date: 当前日期，格式：'YYYY-MM-DD'
            
        Returns:
            Optional[str]: 下一个交易日，格式：'YYYY-MM-DD'，如果没有则返回None
        """
        # 扩展查询范围以确保能找到下一个交易日
        from datetime import datetime, timedelta
        start_date = current_date
        end_date = (datetime.strptime(current_date, '%Y-%m-%d') + timedelta(days=30)).strftime('%Y-%m-%d')
        
        if not self.load_calendar_data(start_date, end_date):
            return None
        
        # 查找下一个交易日
        next_days = self.calendar_data[
            (self.calendar_data['datetime'] > current_date) & 
            (self.calendar_data['is_open'] == True)
        ]['datetime']
        
        if len(next_days) > 0:
            return next_days.iloc[0].strftime('%Y-%m-%d')
        
        return None
    
    def get_previous_trading_day(self, current_date: str) -> Optional[str]:
        """
        获取指定日期之前的上一个交易日
        
        Args:
            current_date: 当前日期，格式：'YYYY-MM-DD'
            
        Returns:
            Optional[str]: 上一个交易日，格式：'YYYY-MM-DD'，如果没有则返回None
        """
        # 扩展查询范围以确保能找到上一个交易日
        from datetime import datetime, timedelta
        end_date = current_date
        start_date = (datetime.strptime(current_date, '%Y-%m-%d') - timedelta(days=30)).strftime('%Y-%m-%d')
        
        if not self.load_calendar_data(start_date, end_date):
            return None
        
        # 查找上一个交易日
        prev_days = self.calendar_data[
            (self.calendar_data['datetime'] < current_date) & 
            (self.calendar_data['is_open'] == True)
        ]['datetime']
        
        if len(prev_days) > 0:
            return prev_days.iloc[-1].strftime('%Y-%m-%d')
        
        return None
    
    def _is_date_range_covered(self, fromdate: str, todate: str) -> bool:
        """
        检查当前加载的数据是否覆盖指定的日期范围
        
        Args:
            fromdate: 开始日期
            todate: 结束日期
            
        Returns:
            bool: 是否覆盖
        """
        if self.calendar_data is None or self.calendar_data.empty:
            return False
        
        data_min = self.calendar_data['datetime'].min().strftime('%Y-%m-%d')
        data_max = self.calendar_data['datetime'].max().strftime('%Y-%m-%d')
        
        return data_min <= fromdate and data_max >= todate


# 使用示例
if __name__ == "__main__":
    # 创建交易日历实例
    calendar = Calendar()
    
    # 获取交易日列表
    trading_days = calendar.get_trading_days('2025-01-01', '2025-01-31')
    print(f"2025年1月交易日: {trading_days}")
    
    # 检查是否为交易日
    is_trading = calendar.is_trading_day('2025-01-15')
    print(f"2025-01-15是否为交易日: {is_trading}")
    
    # 获取下一个交易日
    next_day = calendar.get_next_trading_day('2025-01-15')
    print(f"2025-01-15的下一个交易日: {next_day}")
    
    # 获取上一个交易日
    prev_day = calendar.get_previous_trading_day('2025-01-15')
    print(f"2025-01-15的上一个交易日: {prev_day}")