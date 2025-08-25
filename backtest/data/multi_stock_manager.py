#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
多股票数据管理器
用于动态加载和管理多只股票的数据
"""

import pandas as pd
import backtrader as bt
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from loguru import logger
from .database_loader import DatabaseLoader


class MultiStockDataManager:
    """
    多股票数据管理器
    负责动态加载股票数据并创建Backtrader数据源
    """
    
    def __init__(self, db_config: Dict[str, Any]):
        """
        初始化数据管理器
        
        Args:
            db_config: 数据库配置
        """
        self.db_config = db_config
        self.db_loader = DatabaseLoader(db_config)
        self.stock_data_cache = {}  # 股票数据缓存
        self.data_feeds = {}        # Backtrader数据源
        
    def get_stock_universe(self, start_date: str, end_date: str, min_days: int = 100) -> List[str]:
        """
        获取股票池（有足够交易数据的股票）
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            min_days: 最少交易天数
            
        Returns:
            股票代码列表
        """
        try:
            with self.db_loader:
                # 获取在指定期间有足够交易数据的股票
                sql = """
                SELECT code, COUNT(*) as trading_days
                FROM trade_market_stock_daily 
                WHERE trade_date BETWEEN %s AND %s
                AND close > 0 AND vol > 0
                GROUP BY code
                HAVING trading_days >= %s
                ORDER BY trading_days DESC
                """
                
                df = pd.read_sql(sql, self.db_loader.connection, 
                               params=[start_date, end_date, min_days])
                
                stock_codes = df['code'].tolist()
                logger.info(f"找到 {len(stock_codes)} 只股票，交易天数 >= {min_days}")
                
                return stock_codes
                
        except Exception as e:
            logger.error(f"获取股票池失败: {e}")
            return []
    
    def load_stock_data(self, stock_codes: List[str], start_date: str, end_date: str) -> Dict[str, pd.DataFrame]:
        """
        加载股票数据
        
        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            股票数据字典
        """
        try:
            with self.db_loader:
                stock_data = self.db_loader.get_multiple_stocks_data(
                    stock_codes, start_date, end_date
                )
                
                # 数据质量检查和清洗
                cleaned_data = {}
                for code, data in stock_data.items():
                    if not data.empty and len(data) > 50:  # 至少50个交易日
                        # 填充缺失值
                        data = data.fillna(method='ffill').fillna(method='bfill')
                        
                        # 过滤异常数据
                        data = data[
                            (data['close'] > 0) & 
                            (data['vol'] > 0) &
                            (data['high'] >= data['low']) &
                            (data['high'] >= data['close']) &
                            (data['low'] <= data['close'])
                        ]
                        
                        if len(data) > 30:  # 清洗后仍有足够数据
                            cleaned_data[code] = data
                
                logger.info(f"成功加载 {len(cleaned_data)} 只股票数据")
                self.stock_data_cache.update(cleaned_data)
                
                return cleaned_data
                
        except Exception as e:
            logger.error(f"加载股票数据失败: {e}")
            return {}
    
    def create_data_feeds(self, stock_data: Dict[str, pd.DataFrame]) -> Dict[str, bt.feeds.PandasData]:
        """
        创建Backtrader数据源
        
        Args:
            stock_data: 股票数据字典
            
        Returns:
            Backtrader数据源字典
        """
        feeds = {}
        
        for code, data in stock_data.items():
            try:
                # 确保数据格式正确
                if data.empty:
                    continue
                    
                # 创建数据源
                feed = bt.feeds.PandasData(
                    dataname=data,
                    datetime=None,  # 使用索引作为日期
                    open='open',
                    high='high',
                    low='low',
                    close='close',
                    volume='vol',
                    openinterest=None,
                    name=code  # 设置数据源名称
                )
                
                feeds[code] = feed
                
            except Exception as e:
                logger.warning(f"创建数据源失败 {code}: {e}")
                continue
        
        logger.info(f"创建 {len(feeds)} 个数据源")
        self.data_feeds.update(feeds)
        
        return feeds
    
    def get_trading_calendar(self, start_date: str, end_date: str) -> List[str]:
        """
        获取交易日历
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            交易日期列表
        """
        try:
            with self.db_loader:
                return self.db_loader.get_trading_dates(start_date, end_date)
        except Exception as e:
            logger.error(f"获取交易日历失败: {e}")
            return []
    
    def prepare_backtest_data(self, start_date: str, end_date: str, 
                            stock_limit: int = 500, min_days: int = 50) -> Dict[str, bt.feeds.PandasData]:
        """
        准备回测数据
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            stock_limit: 股票数量限制
            min_days: 最少交易天数
            
        Returns:
            Backtrader数据源字典
        """
        logger.info(f"准备回测数据: {start_date} 到 {end_date}")
        
        # 1. 获取股票池
        stock_universe = self.get_stock_universe(start_date, end_date, min_days)
        
        # 2. 限制股票数量（选择交易最活跃的股票）
        if len(stock_universe) > stock_limit:
            stock_universe = stock_universe[:stock_limit]
            logger.info(f"限制股票数量为 {stock_limit} 只")
        
        # 3. 加载股票数据
        stock_data = self.load_stock_data(stock_universe, start_date, end_date)
        
        # 4. 创建数据源
        data_feeds = self.create_data_feeds(stock_data)
        
        logger.info(f"回测数据准备完成，共 {len(data_feeds)} 只股票")
        
        return data_feeds
    
    def get_concept_data_for_date(self, trade_date: str) -> Dict[str, Any]:
        """
        获取指定日期的概念板块数据
        
        Args:
            trade_date: 交易日期
            
        Returns:
            概念板块数据
        """
        try:
            with self.db_loader:
                # 获取概念板块排名
                concepts = self.db_loader.get_concept_ranking(trade_date, 10)
                
                # 获取概念板块成分股
                if concepts:
                    concept_codes = [c['code'] for c in concepts]
                    concept_stocks = self.db_loader.get_concept_stocks(concept_codes)
                    
                    return {
                        'concepts': concepts,
                        'concept_stocks': concept_stocks
                    }
                    
        except Exception as e:
            logger.error(f"获取概念数据失败 {trade_date}: {e}")
            
        return {'concepts': [], 'concept_stocks': {}}
    
    def cleanup(self):
        """
        清理资源
        """
        self.stock_data_cache.clear()
        self.data_feeds.clear()
        logger.info("数据管理器资源已清理")