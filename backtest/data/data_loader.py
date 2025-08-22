#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据加载器模块
支持多种数据源的获取和处理
"""

import pandas as pd
import yfinance as yf
import backtrader as bt
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Union
from loguru import logger
import os
import pickle


class DataLoader:
    """
    数据加载器类
    支持Yahoo Finance、CSV文件等多种数据源
    """
    
    def __init__(self, data_path: str = './data/'):
        """
        初始化数据加载器
        
        Args:
            data_path: 数据存储路径
        """
        self.data_path = data_path
        os.makedirs(data_path, exist_ok=True)
        logger.info(f"数据加载器初始化，数据路径: {data_path}")
    
    def download_yahoo_data(
        self, 
        symbol: str, 
        start_date: str, 
        end_date: str,
        interval: str = '1d',
        save_to_file: bool = True
    ) -> pd.DataFrame:
        """
        从Yahoo Finance下载数据
        
        Args:
            symbol: 股票代码
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            interval: 数据间隔 (1d, 1h, 5m, 1m等)
            save_to_file: 是否保存到文件
            
        Returns:
            股票数据DataFrame
        """
        try:
            logger.info(f"下载{symbol}数据: {start_date} 到 {end_date}")
            
            # 下载数据
            ticker = yf.Ticker(symbol)
            data = ticker.history(
                start=start_date,
                end=end_date,
                interval=interval,
                auto_adjust=True,
                prepost=True
            )
            
            if data.empty:
                logger.warning(f"未获取到{symbol}的数据")
                return pd.DataFrame()
            
            # 重命名列名以符合backtrader要求
            data.columns = [col.lower() for col in data.columns]
            data = data.rename(columns={
                'adj close': 'adjclose'
            })
            
            # 确保索引为日期时间格式
            if not isinstance(data.index, pd.DatetimeIndex):
                data.index = pd.to_datetime(data.index)
            
            logger.info(f"成功获取{symbol}数据，共{len(data)}条记录")
            
            # 保存到文件
            if save_to_file:
                filename = f"{symbol}_{start_date}_{end_date}_{interval}.csv"
                filepath = os.path.join(self.data_path, filename)
                data.to_csv(filepath)
                logger.info(f"数据已保存到: {filepath}")
            
            return data
            
        except Exception as e:
            logger.error(f"下载{symbol}数据失败: {e}")
            return pd.DataFrame()
    
    def load_csv_data(self, filepath: str) -> pd.DataFrame:
        """
        从CSV文件加载数据
        
        Args:
            filepath: CSV文件路径
            
        Returns:
            股票数据DataFrame
        """
        try:
            logger.info(f"加载CSV数据: {filepath}")
            
            data = pd.read_csv(filepath, index_col=0, parse_dates=True)
            
            # 标准化列名
            data.columns = [col.lower() for col in data.columns]
            
            # 检查必要的列
            required_columns = ['open', 'high', 'low', 'close', 'volume']
            missing_columns = [col for col in required_columns if col not in data.columns]
            
            if missing_columns:
                logger.warning(f"缺少必要列: {missing_columns}")
            
            logger.info(f"成功加载CSV数据，共{len(data)}条记录")
            return data
            
        except Exception as e:
            logger.error(f"加载CSV数据失败: {e}")
            return pd.DataFrame()
    
    def create_backtrader_feed(
        self, 
        data: pd.DataFrame, 
        name: str = None
    ) -> bt.feeds.PandasData:
        """
        创建Backtrader数据源
        
        Args:
            data: 股票数据DataFrame
            name: 数据源名称
            
        Returns:
            Backtrader数据源对象
        """
        try:
            if data.empty:
                logger.error("数据为空，无法创建Backtrader数据源")
                return None
            
            # 确保数据按日期排序
            data = data.sort_index()
            
            # 创建Backtrader数据源
            data_feed = bt.feeds.PandasData(
                dataname=data,
                name=name,
                datetime=None,  # 使用索引作为日期
                open='open',
                high='high',
                low='low',
                close='close',
                volume='volume',
                openinterest=-1  # 不使用持仓量
            )
            
            logger.info(f"成功创建Backtrader数据源: {name}")
            return data_feed
            
        except Exception as e:
            logger.error(f"创建Backtrader数据源失败: {e}")
            return None
    
    def get_multiple_symbols(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str,
        interval: str = '1d'
    ) -> Dict[str, pd.DataFrame]:
        """
        获取多个股票的数据
        
        Args:
            symbols: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            interval: 数据间隔
            
        Returns:
            股票数据字典
        """
        data_dict = {}
        
        for symbol in symbols:
            logger.info(f"正在获取{symbol}数据...")
            data = self.download_yahoo_data(
                symbol, start_date, end_date, interval
            )
            
            if not data.empty:
                data_dict[symbol] = data
            else:
                logger.warning(f"跳过{symbol}，数据为空")
        
        logger.info(f"成功获取{len(data_dict)}个股票的数据")
        return data_dict
    
    def cache_data(self, data: pd.DataFrame, cache_key: str):
        """
        缓存数据到本地
        
        Args:
            data: 要缓存的数据
            cache_key: 缓存键名
        """
        try:
            cache_file = os.path.join(self.data_path, f"{cache_key}.pkl")
            with open(cache_file, 'wb') as f:
                pickle.dump(data, f)
            logger.info(f"数据已缓存到: {cache_file}")
        except Exception as e:
            logger.error(f"缓存数据失败: {e}")
    
    def load_cached_data(self, cache_key: str) -> Optional[pd.DataFrame]:
        """
        从本地加载缓存数据
        
        Args:
            cache_key: 缓存键名
            
        Returns:
            缓存的数据或None
        """
        try:
            cache_file = os.path.join(self.data_path, f"{cache_key}.pkl")
            if os.path.exists(cache_file):
                with open(cache_file, 'rb') as f:
                    data = pickle.load(f)
                logger.info(f"成功加载缓存数据: {cache_file}")
                return data
            else:
                logger.info(f"缓存文件不存在: {cache_file}")
                return None
        except Exception as e:
            logger.error(f"加载缓存数据失败: {e}")
            return None
    
    def validate_data(self, data: pd.DataFrame) -> bool:
        """
        验证数据质量
        
        Args:
            data: 要验证的数据
            
        Returns:
            数据是否有效
        """
        if data.empty:
            logger.warning("数据为空")
            return False
        
        # 检查必要列
        required_columns = ['open', 'high', 'low', 'close']
        missing_columns = [col for col in required_columns if col not in data.columns]
        
        if missing_columns:
            logger.warning(f"缺少必要列: {missing_columns}")
            return False
        
        # 检查数据完整性
        null_counts = data[required_columns].isnull().sum()
        if null_counts.any():
            logger.warning(f"存在空值: {null_counts[null_counts > 0].to_dict()}")
        
        # 检查价格逻辑
        invalid_prices = (
            (data['high'] < data['low']) |
            (data['high'] < data['open']) |
            (data['high'] < data['close']) |
            (data['low'] > data['open']) |
            (data['low'] > data['close'])
        ).sum()
        
        if invalid_prices > 0:
            logger.warning(f"存在{invalid_prices}条价格逻辑错误的记录")
        
        logger.info("数据验证完成")
        return True
    
    def resample_data(
        self, 
        data: pd.DataFrame, 
        timeframe: str
    ) -> pd.DataFrame:
        """
        重采样数据到指定时间框架
        
        Args:
            data: 原始数据
            timeframe: 目标时间框架 (1H, 4H, 1D, 1W等)
            
        Returns:
            重采样后的数据
        """
        try:
            logger.info(f"重采样数据到{timeframe}")
            
            # 定义重采样规则
            agg_dict = {
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            }
            
            # 只对存在的列进行重采样
            available_agg = {k: v for k, v in agg_dict.items() if k in data.columns}
            
            resampled_data = data.resample(timeframe).agg(available_agg)
            
            # 删除空行
            resampled_data = resampled_data.dropna()
            
            logger.info(f"重采样完成，从{len(data)}条记录变为{len(resampled_data)}条")
            return resampled_data
            
        except Exception as e:
            logger.error(f"重采样失败: {e}")
            return data


class DataManager:
    """
    数据管理器
    统一管理多个数据源和数据处理流程
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        初始化数据管理器
        
        Args:
            config: 配置字典
        """
        self.config = config
        self.data_config = config.get('data', {})
        self.loader = DataLoader(self.data_config.get('data_path', './data/'))
        self.data_feeds = {}
        
    def prepare_data(self) -> Dict[str, bt.feeds.PandasData]:
        """
        准备所有数据源
        
        Returns:
            数据源字典
        """
        symbols = self.data_config.get('symbols', ['AAPL'])
        start_date = self.config.get('backtest', {}).get('start_date', '2020-01-01')
        end_date = self.config.get('backtest', {}).get('end_date', '2023-12-31')
        timeframe = self.data_config.get('timeframe', '1d')
        
        logger.info(f"准备数据: {symbols}, {start_date} 到 {end_date}")
        
        for symbol in symbols:
            # 尝试加载缓存数据
            cache_key = f"{symbol}_{start_date}_{end_date}_{timeframe}"
            data = self.loader.load_cached_data(cache_key)
            
            if data is None or data.empty:
                # 下载新数据
                data = self.loader.download_yahoo_data(
                    symbol, start_date, end_date, timeframe
                )
                
                if not data.empty:
                    self.loader.cache_data(data, cache_key)
            
            # 验证数据
            if self.loader.validate_data(data):
                # 创建Backtrader数据源
                data_feed = self.loader.create_backtrader_feed(data, symbol)
                if data_feed:
                    self.data_feeds[symbol] = data_feed
        
        logger.info(f"数据准备完成，共{len(self.data_feeds)}个数据源")
        return self.data_feeds