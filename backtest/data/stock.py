#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票数据模块
提供股票数据加载和backtrader数据源定义
"""

import pandas as pd
import backtrader as bt
from typing import Optional
import sys
import os

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from backtest.data.loader import Loader


class StockDataLoader:
    """
    股票数据加载器
    提供合并多个股票相关数据表的功能
    """
    
    def __init__(self):
        """
        初始化股票数据加载器
        """
        self.loader = Loader()
    
    def load_merged_stock_data(self, fromdate: str, todate: str) -> Optional[pd.DataFrame]:
        """
        加载并合并股票相关的三个数据表
        
        Args:
            fromdate: 开始日期，格式：'YYYY-MM-DD'
            todate: 结束日期，格式：'YYYY-MM-DD'
            
        Returns:
            pd.DataFrame: 合并后的股票数据，包含日行情、基本指标和因子数据
        """
        try:
            # 加载个股日行情数据
            daily_data = self.loader.load_data(fromdate, todate, 'trade_market_stock_daily')
            if daily_data is None or daily_data.empty:
                print("个股日行情数据加载失败")
                return None
            
            # 加载个股每日指标数据
            basic_data = self.loader.load_data(fromdate, todate, 'trade_market_stock_basic_daily')
            if basic_data is None or basic_data.empty:
                print("个股每日指标数据加载失败")
                return None
            
            # 加载个股因子数据
            factor_data = self.loader.load_data(fromdate, todate, 'trade_factor_stock')
            if factor_data is None or factor_data.empty:
                print("个股因子数据加载失败")
                return None
            
            # 合并数据：首先合并日行情和基本指标
            merged_data = self.loader.merge_dataframes(
                daily_data, basic_data, 
                on=['datetime', 'code'], 
                how='left'
            )
            
            # 再合并因子数据
            final_data = self.loader.merge_dataframes(
                merged_data, factor_data,
                on=['datetime', 'code'],
                how='left'
            )
            
            # 确保数据按日期和代码排序
            final_data = final_data.sort_values(['datetime', 'code']).reset_index(drop=True)
            
            print(f"股票数据合并完成，共{len(final_data)}行数据")
            return final_data
            
        except Exception as e:
            print(f"股票数据加载失败: {e}")
            return None
    
    def get_stock_data_by_code(self, fromdate: str, todate: str) -> Optional[pd.DataFrame]:
        """
        获取指定股票代码的数据
        
        Args:
            fromdate: 开始日期
            todate: 结束日期
            
        Returns:
            pd.DataFrame: 指定股票的数据
        """
        merged_data = self.load_merged_stock_data(fromdate, todate)
        if merged_data is None:
            print(f"未找到股票数据")
            return None
        
        return merged_data.reset_index(drop=True)


class Stock(bt.feeds.PandasData):
    """
    股票数据源类
    继承自backtrader的PandasData，定义股票数据的字段映射
    """
    
    params = (
        # 必需字段
        ('datetime', 'datetime'),
        ('open', 'open'),
        ('high', 'high'),
        ('low', 'low'),
        ('close', 'close'),
        ('volume', 'vol'),
        ('openinterest', -1),
        
        # 基本信息字段
        ('code', 'code'),
        ('name', 'name'),
        
        # 价格相关字段
        ('pre_close', 'pre_close'),
        ('chg_val', 'chg_val'),
        ('chg_pct', 'chg_pct'),
        ('amount', 'amount'),
        
        # 基本指标字段
        ('turnover_rate', 'turnover_rate'),
        ('turnover_rate_f', 'turnover_rate_f'),
        ('volume_ratio', 'volume_ratio'),
        ('pe', 'pe'),
        ('pe_ttm', 'pe_ttm'),
        ('pb', 'pb'),
        ('total_share', 'total_share'),
        ('float_share', 'float_share'),
        ('free_share', 'free_share'),
        ('total_mv', 'total_mv'),
        ('circ_mv', 'circ_mv'),
        
        # 因子字段 - 换手率因子
        ('turnover_rate_today', 'turnover_rate_today'),
        ('turnover_rate_5d_avg', 'turnover_rate_5d_avg'),
        ('turnover_rate_10d_avg', 'turnover_rate_10d_avg'),
        ('turnover_rate_20d_avg', 'turnover_rate_20d_avg'),
        
        # 因子字段 - 放量因子
        ('volume_surge_today', 'volume_surge_today'),
        ('volume_surge_5d', 'volume_surge_5d'),
        
        # 因子字段 - 涨幅因子
        ('avg_return_5d', 'avg_return_5d'),
        ('avg_return_10d', 'avg_return_10d'),
        ('avg_return_20d', 'avg_return_20d'),
        
        # 因子字段 - 技术指标因子
        ('pullback_ma5_days', 'pullback_ma5_days'),
        
        # 因子字段 - 分歧因子
        ('divergence_today', 'divergence_today'),
        
        # 因子字段 - 市值因子
        ('market_cap', 'market_cap'),
        
        # 因子字段 - 量价背离因子
        ('volume_price_divergence_60min', 'volume_price_divergence_60min'),
        
        # 因子字段 - 排名因子
        ('rank_today', 'rank_today'),
        ('rank_5d_avg', 'rank_5d_avg'),
        ('rank_10d_avg', 'rank_10d_avg'),
        ('rank_surge_today', 'rank_surge_today'),
        ('rank_surge_5d', 'rank_surge_5d'),
        
        # 因子字段 - 竞价因子
        ('bid_ask_turnover_rate', 'bid_ask_turnover_rate'),
    )


# 使用示例
if __name__ == "__main__":
    # 创建股票数据加载器
    stock_loader = StockDataLoader()
    
    # 加载合并数据
    merged_data = stock_loader.load_merged_stock_data('2025-01-01', '2025-01-31')
    if merged_data is not None:
        print(f"合并数据成功，共{len(merged_data)}行")
        print("数据列名:", list(merged_data.columns))
        print(merged_data.head())
    
    # 获取特定股票数据
    # stock_data = stock_loader.get_stock_data_by_code('2025-01-01', '2025-01-31', '000001')
    # if stock_data is not None:
    #     print(f"股票数据加载成功，共{len(stock_data)}行")
    #     
    #     # 创建backtrader数据源
    #     data_feed = Stock(dataname=stock_data)
    #     print("Backtrader数据源创建成功")