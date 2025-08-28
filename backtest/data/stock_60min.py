#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票60分钟数据模块
提供股票60分钟数据加载和backtrader数据源定义
"""

import pandas as pd
import backtrader as bt
from typing import Optional
import sys
import os

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from backtest.data.loader import Loader


class Stock60minDataLoader:
    """
    股票60分钟数据加载器
    提供合并60分钟行情数据与日级别基本指标、因子数据的功能
    """
    
    def __init__(self):
        """
        初始化股票60分钟数据加载器
        """
        self.loader = Loader()
    
    def load_merged_stock_60min_data(self, fromdate: str, todate: str) -> Optional[pd.DataFrame]:
        """
        加载并合并股票60分钟行情数据与日级别数据
        
        Args:
            fromdate: 开始日期，格式：'YYYY-MM-DD'
            todate: 结束日期，格式：'YYYY-MM-DD'
            
        Returns:
            pd.DataFrame: 合并后的股票60分钟数据，包含60分钟行情、基本指标和因子数据
        """
        try:
            # 加载个股60分钟行情数据
            min60_data = self.loader.load_data(fromdate, todate, 'trade_market_stock_60min')
            if min60_data is None or min60_data.empty:
                print("个股60分钟行情数据加载失败")
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
            
            # 加载个股竞价数据
            auction_data = self.loader.load_data(fromdate, todate, 'trade_market_stock_auction_daily')
            if auction_data is None or auction_data.empty:
                print("个股竞价数据加载失败")
                return None
            
            # 处理60分钟数据的datetime字段
            # 将trade_date和trade_time正确组合为完整的datetime
            if 'trade_time' in min60_data.columns:
                # datetime字段目前只包含日期，trade_time是时间差
                # 将日期和时间正确组合
                min60_data['datetime'] = pd.to_datetime(min60_data['datetime'].dt.date.astype(str)) + min60_data['trade_time']
            
            # 从60分钟数据的datetime中提取日期
            min60_data['date'] = pd.to_datetime(min60_data['datetime']).dt.date
            basic_data['date'] = pd.to_datetime(basic_data['datetime']).dt.date
            factor_data['date'] = pd.to_datetime(factor_data['datetime']).dt.date
            auction_data['date'] = pd.to_datetime(auction_data['datetime']).dt.date
            
            
            # 特殊处理merge：将日级别数据复制到对应的60分钟数据上
            # 首先合并基本指标数据
            merged_data = self._merge_daily_to_60min(min60_data, basic_data, ['date', 'code'])
            
            # 再合并因子数据
            merged_data = self._merge_daily_to_60min(merged_data, factor_data, ['date', 'code'])
            
            
            # 处理竞价数据字段重命名（避免与其他表字段冲突）
            auction_data = auction_data.rename(columns={
                'vol': 'auction_vol',
                'price': 'auction_price', 
                'amount': 'auction_amount',
                'pre_close': 'auction_pre_close',
                'turnover_rate': 'auction_turnover_rate',
                'volume_ratio': 'auction_volume_ratio',
                'float_share': 'auction_float_share'
            })
            
            # 最后合并竞价数据
            final_data = self._merge_daily_to_60min(merged_data, auction_data, ['date', 'code'])
            
            # 删除临时的date列
            final_data = final_data.drop('date', axis=1)
            
            # 确保数据按日期时间和代码排序
            final_data = final_data.sort_values(['datetime', 'code']).reset_index(drop=True)
            
            print(f"股票60分钟数据合并完成，共{len(final_data)}行数据")
            return final_data
            
        except Exception as e:
            print(f"股票60分钟数据加载失败: {e}")
            return None
    
    def _merge_daily_to_60min(self, min60_data: pd.DataFrame, daily_data: pd.DataFrame, on: list) -> pd.DataFrame:
        """
        将日级别数据合并到60分钟数据上
        
        Args:
            min60_data: 60分钟数据
            daily_data: 日级别数据
            on: 合并的键
            
        Returns:
            pd.DataFrame: 合并后的数据
        """
        # 使用left join，将日级别数据复制到对应的60分钟数据上
        merged = pd.merge(min60_data, daily_data, on=on, how='left', suffixes=('', '_daily'))
        
        # 处理重复列名（如果有的话）
        duplicate_cols = [col for col in merged.columns if col.endswith('_daily')]
        for col in duplicate_cols:
            original_col = col.replace('_daily', '')
            if original_col in merged.columns:
                # 如果60分钟数据中没有该字段的值，使用日级别数据填充
                merged[original_col] = merged[original_col].fillna(merged[col])
            else:
                # 如果60分钟数据中没有该字段，直接使用日级别数据
                merged[original_col] = merged[col]
            # 删除临时列
            merged = merged.drop(col, axis=1)
        
        return merged
    
    def get_stock_60min_data_by_code(self, fromdate: str, todate: str, code: str) -> Optional[pd.DataFrame]:
        """
        获取指定股票代码的60分钟数据
        
        Args:
            fromdate: 开始日期
            todate: 结束日期
            code: 股票代码
            
        Returns:
            pd.DataFrame: 指定股票的60分钟数据
        """
        merged_data = self.load_merged_stock_60min_data(fromdate, todate)
        if merged_data is None:
            print(f"未找到股票60分钟数据")
            return None
        
        # 筛选指定股票代码的数据
        stock_data = merged_data[merged_data['code'] == code].copy()
        if stock_data.empty:
            print(f"未找到股票代码 {code} 的60分钟数据")
            return None
        
        return stock_data.reset_index(drop=True)


class Stock60min(bt.feeds.PandasData):
    """
    股票60分钟数据源类
    继承自backtrader的PandasData，定义股票60分钟数据的字段映射
    """
    
    params = (
        # 必需字段
        ('datetime', None),
        ('open', 'open'),
        ('high', 'high'),
        ('low', 'low'),
        ('close', 'close'),
        ('volume', 'vol'),
        ('openinterest', -1),
        
        # 基本信息字段（来自60分钟行情表）
        ('code', 'code'),
        ('name', 'name'),
        
        # 价格相关字段（来自60分钟行情表）
        ('amount', 'amount'),
        
        # 基本指标字段（来自日级别数据）
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
        
        # 因子字段 - 换手率因子（来自日级别数据）
        ('turnover_rate_today', 'turnover_rate_today'),
        ('turnover_rate_5d_avg', 'turnover_rate_5d_avg'),
        ('turnover_rate_10d_avg', 'turnover_rate_10d_avg'),
        ('turnover_rate_20d_avg', 'turnover_rate_20d_avg'),
        
        # 因子字段 - 放量因子（来自日级别数据）
        ('volume_surge_today', 'volume_surge_today'),
        ('volume_surge_5d', 'volume_surge_5d'),
        
        # 因子字段 - 涨幅因子（来自日级别数据）
        ('avg_return_5d', 'avg_return_5d'),
        ('avg_return_10d', 'avg_return_10d'),
        ('avg_return_20d', 'avg_return_20d'),
        
        # 因子字段 - 技术指标因子（来自日级别数据）
        ('pullback_ma5_days', 'pullback_ma5_days'),
        
        # 因子字段 - 分歧因子（来自日级别数据）
        ('divergence_today', 'divergence_today'),
        
        # 因子字段 - 市值因子（来自日级别数据）
        ('market_cap', 'market_cap'),
        
        # 因子字段 - 量价背离因子
        ('volume_price_divergence_60min', 'volume_price_divergence_60min'),
        
        # 因子字段 - 排名因子（来自日级别数据）
        ('rank_today', 'rank_today'),
        ('rank_5d_avg', 'rank_5d_avg'),
        ('rank_10d_avg', 'rank_10d_avg'),
        ('rank_surge_today', 'rank_surge_today'),
        ('rank_surge_5d', 'rank_surge_5d'),
        
        # 因子字段 - 竞价因子（来自日级别数据）
        # 买卖盘换手率
        ('bid_ask_turnover_rate', 'bid_ask_turnover_rate'),
        
        # 竞价数据字段（来自竞价表）
        ('auction_vol', 'auction_vol'),
        ('auction_price', 'auction_price'),
        ('auction_amount', 'auction_amount'),
        ('auction_pre_close', 'auction_pre_close'),
        ('auction_turnover_rate', 'auction_turnover_rate'),
        ('auction_volume_ratio', 'auction_volume_ratio'),
        ('auction_float_share', 'auction_float_share'),
    )


# 使用示例
if __name__ == "__main__":
    # 创建股票60分钟数据加载器
    stock_60min_loader = Stock60minDataLoader()
    
    # 加载合并数据
    merged_data = stock_60min_loader.load_merged_stock_60min_data('2025-01-01', '2025-01-31')
    if merged_data is not None:
        print(f"合并数据成功，共{len(merged_data)}行")
        print("数据列名:", list(merged_data.columns))
        print(merged_data.head())
    
    # 获取特定股票60分钟数据
    # stock_data = stock_60min_loader.get_stock_60min_data_by_code('2025-01-01', '2025-01-31', '000001.SZ')
    # if stock_data is not None:
    #     print(f"股票60分钟数据加载成功，共{len(stock_data)}行")
    #     
    #     # 创建backtrader数据源
    #     data_feed = Stock60min(dataname=stock_data)
    #     print("Backtrader 60分钟数据源创建成功")