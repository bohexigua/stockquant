#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
题材数据模块
提供题材数据加载和关联股票查询功能
"""

import pandas as pd
from typing import Optional, List, Dict
import sys
import os

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from backtest.data.loader import Loader


class ThemeDataLoader:
    """
    题材数据加载器
    提供题材相关数据加载和股票关联查询功能
    """
    
    def __init__(self):
        """
        初始化题材数据加载器
        """
        self.loader = Loader()
    
    def load_merged_theme_data(self, fromdate: str, todate: str) -> Optional[pd.DataFrame]:
        """
        加载并合并题材相关的两个数据表
        
        Args:
            fromdate: 开始日期，格式：'YYYY-MM-DD'
            todate: 结束日期，格式：'YYYY-MM-DD'
            
        Returns:
            pd.DataFrame: 合并后的题材数据，包含因子数据和市场数据
        """
        try:
            # 加载题材因子数据
            factor_data = self.loader.load_data(fromdate, todate, 'trade_factor_theme')
            if factor_data is None or factor_data.empty:
                print("题材因子数据加载失败")
                return None
            
            # 加载题材市场数据
            market_data = self.loader.load_data(fromdate, todate, 'trade_market_theme')
            if market_data is None or market_data.empty:
                print("题材市场数据加载失败")
                return None
            
            # 合并数据：基于日期和题材代码
            merged_data = self.loader.merge_dataframes(
                factor_data, market_data,
                on=['datetime', 'code'],
                how='left'
            )
            
            # 确保数据按日期和代码排序
            merged_data = merged_data.sort_values(['datetime', 'code']).reset_index(drop=True)
            
            print(f"题材数据合并完成，共{len(merged_data)}行数据")
            return merged_data
            
        except Exception as e:
            print(f"题材数据加载失败: {e}")
            return None
    
    def get_theme_data_by_code(self, fromdate: str, todate: str, theme_code: str) -> Optional[pd.DataFrame]:
        """
        获取指定题材代码的数据
        
        Args:
            fromdate: 开始日期
            todate: 结束日期
            theme_code: 题材代码
            
        Returns:
            pd.DataFrame: 指定题材的数据
        """
        merged_data = self.load_merged_theme_data(fromdate, todate)
        if merged_data is None:
            return None
        
        theme_data = merged_data[merged_data['code'] == theme_code].copy()
        if theme_data.empty:
            print(f"未找到题材代码 {theme_code} 的数据")
            return None
        
        return theme_data.reset_index(drop=True)
    
    
    def get_theme_related_stocks(self, theme_codes: List[str] = None) -> Optional[Dict[str, List[str]]]:
        """
        获取题材板块关联的个股代码
        
        Args:
            theme_codes: 题材代码列表，如果为None则获取所有题材的关联股票
            
        Returns:
            Dict[str, List[str]]: 题材代码到股票代码列表的映射
        """
        try:
            # 直接查询题材股票关联表（该表没有trade_date字段）
            if not self.loader._connect():
                print("数据库连接失败")
                return None
            
            # 构建SQL查询语句
            if theme_codes:
                placeholders = ','.join(['%s'] * len(theme_codes))
                sql = f"""
                SELECT theme_sector_code, stock_code 
                FROM trade_stock_theme_relation 
                WHERE theme_sector_code IN ({placeholders})
                """
                relation_data = pd.read_sql(sql, self.loader.connection, params=theme_codes)
            else:
                sql = "SELECT theme_sector_code, stock_code FROM trade_stock_theme_relation"
                relation_data = pd.read_sql(sql, self.loader.connection)
            
            self.loader._disconnect()
            
            if relation_data is None or relation_data.empty:
                print("题材股票关联数据加载失败")
                return None
            
            # 构建题材到股票的映射
            theme_stock_map = {}
            for theme_code in relation_data['theme_sector_code'].unique():
                stocks = relation_data[relation_data['theme_sector_code'] == theme_code]['stock_code'].tolist()
                theme_stock_map[theme_code] = stocks
            
            print(f"获取到{len(theme_stock_map)}个题材的股票关联关系")
            return theme_stock_map
            
        except Exception as e:
            print(f"获取题材关联股票失败: {e}")
            return None

    def get_top_themes_by_rank(self, date: str, top_n: int = 10) -> Optional[pd.DataFrame]:
        """
        根据排名获取指定交易日的TOP题材
        
        Args:
            date: 交易日期，格式：'YYYY-MM-DD'
            top_n: 返回前N个题材
            
        Returns:
            pd.DataFrame: TOP题材数据
        """
        try:
            # 加载指定日期的数据
            merged_data = self.load_merged_theme_data(date, date)
            if merged_data is None:
                return None
            
            # 过滤指定日期的数据
            filtered_data = merged_data[merged_data['datetime'] == pd.to_datetime(date)]
            
            if filtered_data.empty:
                print(f"日期 {date} 没有数据")
                return None
            
            # 按排名排序，取前N个
            if 'rank_value' in filtered_data.columns:
                top_themes = filtered_data.sort_values('rank_value').head(top_n)
            else:
                print("数据中缺少rank_value字段")
                return None
            
            print(f"获取到 {date} 日期TOP {len(top_themes)}个题材")
            return top_themes.reset_index(drop=True)
            
        except Exception as e:
            print(f"获取TOP题材失败: {e}")
            return None
    


# 使用示例
if __name__ == "__main__":
    # 创建题材数据加载器
    theme_loader = ThemeDataLoader()
    
    # 加载合并数据
    merged_data = theme_loader.load_merged_theme_data('2025-01-01', '2025-01-31')
    if merged_data is not None:
        print(f"合并数据成功，共{len(merged_data)}行")
        print("数据列名:", list(merged_data.columns))
        print(merged_data.head())
    
    # 获取TOP题材
    top_themes = theme_loader.get_top_themes_by_rank('2025-01-15', top_n=5)
    if top_themes is not None:
        print(f"\nTOP 5题材:")
        print(top_themes[['code', 'name', 'rank_value', 'z_t_num']].head())