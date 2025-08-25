#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库数据加载器
用于从MySQL数据库加载股票和概念板块数据
"""

import pandas as pd
import pymysql
from typing import Dict, List, Optional, Tuple
from datetime import datetime, date
import backtrader as bt
from loguru import logger


class DatabaseLoader:
    """
    数据库数据加载器
    
    用于连接MySQL数据库并获取股票、概念板块等相关数据
    """
    
    def __init__(self, db_config: dict):
        """
        初始化数据库连接
        
        Args:
            db_config: 数据库配置字典
        """
        self.db_config = db_config
        self.connection = None
        
    def __enter__(self):
        """上下文管理器入口"""
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.close()
        
    def connect(self):
        """
        建立数据库连接
        """
        try:
            self.connection = pymysql.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                database=self.db_config['database'],
                charset='utf8mb4',
                autocommit=True
            )
            logger.info("数据库连接成功")
        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
            raise
            
    def close(self):
        """
        关闭数据库连接
        """
        if self.connection:
            self.connection.close()
            logger.info("数据库连接已关闭")
            
    def execute_query(self, sql: str, params: tuple = None) -> pd.DataFrame:
        """
        执行SQL查询
        
        Args:
            sql: SQL语句
            params: 查询参数
            
        Returns:
            查询结果DataFrame
        """
        try:
            if not self.connection:
                self.connect()
                
            df = pd.read_sql(sql, self.connection, params=params)
            return df
        except Exception as e:
            logger.error(f"SQL查询失败: {e}")
            logger.error(f"SQL: {sql}")
            raise
            
    def get_trading_calendar(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取交易日历
        
        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            
        Returns:
            交易日历DataFrame
        """
        sql = """
        SELECT trade_date, is_open
        FROM trade_market_calendar
        WHERE trade_date BETWEEN %s AND %s
        AND is_open = 1
        ORDER BY trade_date
        """
        
        return self.execute_query(sql, (start_date, end_date))
        
    def get_concept_ranking(self, trade_date: str, limit: int = 10) -> pd.DataFrame:
        """
        获取概念板块排名数据
        
        Args:
            trade_date: 交易日期 (YYYY-MM-DD)
            limit: 返回前N个概念板块
            
        Returns:
            概念板块排名DataFrame
        """
        sql = """
        SELECT code as concept_code, name as concept_name, rank_value as hot_rank
        FROM trade_market_dc_concept
        WHERE trade_date = %s
        ORDER BY rank_value ASC
        LIMIT %s
        """
        
        return self.execute_query(sql, (trade_date, limit))
        
    def get_concept_constituents(self, concept_code: str, trade_date: str) -> pd.DataFrame:
        """
        获取概念板块成分股
        
        Args:
            concept_code: 概念板块代码
            trade_date: 交易日期
            
        Returns:
            成分股DataFrame
        """
        sql = """
        SELECT stock_code as ts_code, '' as stock_name, 1.0 as weight
        FROM trade_stock_concept_relation
        WHERE concept_sector_code = %s
        ORDER BY stock_code
        """
        
        return self.execute_query(sql, (concept_code,))
        
    def get_stock_hot_ranking(self, trade_date: str, stock_codes: List[str] = None) -> pd.DataFrame:
        """
        获取个股人气排名
        
        Args:
            trade_date: 交易日期
            stock_codes: 股票代码列表，如果为None则获取所有股票
            
        Returns:
            个股人气排名DataFrame
        """
        if stock_codes:
            placeholders = ','.join(['%s'] * len(stock_codes))
            sql = f"""
            SELECT code as ts_code, hot_rank, 0 as hot_score, pct_change as change_pct
            FROM trade_market_dc_stock_hot
            WHERE trade_date = %s
            AND code IN ({placeholders})
            ORDER BY hot_rank
            """
            params = [trade_date] + stock_codes
        else:
            sql = """
            SELECT code as ts_code, hot_rank, 0 as hot_score, pct_change as change_pct
            FROM trade_market_dc_stock_hot
            WHERE trade_date = %s
            ORDER BY hot_rank
            LIMIT 100
            """
            params = [trade_date]
            
        return self.execute_query(sql, tuple(params))
        
    def get_stock_daily_data(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取股票日线数据
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            股票日线数据DataFrame
        """
        sql = """
        SELECT trade_date, code as ts_code, `open`, `high`, `low`, `close`, 
               pre_close, chg_val as `change`, chg_pct as pct_chg, vol, amount
        FROM trade_market_stock_daily
        WHERE code = %s
        AND trade_date BETWEEN %s AND %s
        ORDER BY trade_date
        """
        
        df = self.execute_query(sql, (ts_code, start_date, end_date))
        if not df.empty:
            # 转换日期格式并设置为索引
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            df.set_index('trade_date', inplace=True)
        return df
        
    def get_multiple_stocks_data(self, stock_codes: List[str], start_date: str, end_date: str) -> Dict[str, pd.DataFrame]:
        """
        批量获取多只股票的日线数据
        
        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            股票代码为key，日线数据DataFrame为value的字典
        """
        result = {}
        
        # 批量查询优化
        if len(stock_codes) > 50:  # 如果股票数量较多，分批查询
            batch_size = 50
            for i in range(0, len(stock_codes), batch_size):
                batch_codes = stock_codes[i:i + batch_size]
                batch_result = self._get_batch_stocks_data(batch_codes, start_date, end_date)
                result.update(batch_result)
        else:
            result = self._get_batch_stocks_data(stock_codes, start_date, end_date)
            
        return result
        
    def _get_batch_stocks_data(self, stock_codes: List[str], start_date: str, end_date: str) -> Dict[str, pd.DataFrame]:
        """
        批量获取股票数据的内部方法
        
        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            股票数据字典
        """
        if not stock_codes:
            return {}
            
        placeholders = ','.join(['%s'] * len(stock_codes))
        sql = f"""
        SELECT trade_date, code as ts_code, `open`, `high`, `low`, `close`, 
               pre_close, chg_val as `change`, chg_pct as pct_chg, vol, amount
        FROM trade_market_stock_daily
        WHERE code IN ({placeholders})
        AND trade_date BETWEEN %s AND %s
        ORDER BY code, trade_date
        """
        
        params = stock_codes + [start_date, end_date]
        df = self.execute_query(sql, tuple(params))
        
        # 按股票代码分组
        result = {}
        for ts_code in stock_codes:
            stock_data = df[df['ts_code'] == ts_code].copy()
            if not stock_data.empty:
                # 转换日期格式并设置为索引
                stock_data['trade_date'] = pd.to_datetime(stock_data['trade_date'])
                stock_data.set_index('trade_date', inplace=True)
                result[ts_code] = stock_data
                
        return result
        
    def pandas_to_backtrader_feed(self, df: pd.DataFrame, name: str = None) -> bt.feeds.PandasData:
        """
        将Pandas DataFrame转换为Backtrader数据源
        
        Args:
            df: 包含OHLCV数据的DataFrame
            name: 数据源名称
            
        Returns:
            Backtrader数据源
        """
        # 确保数据格式正确
        df = df.copy()
        
        # 转换日期格式
        if 'trade_date' in df.columns:
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            df.set_index('trade_date', inplace=True)
        
        # 确保列名正确
        column_mapping = {
            'open': 'open',
            'high': 'high', 
            'low': 'low',
            'close': 'close',
            'vol': 'volume',
            'amount': 'openinterest'  # 使用amount作为openinterest
        }
        
        # 重命名列
        df = df.rename(columns=column_mapping)
        
        # 确保必要的列存在
        required_columns = ['open', 'high', 'low', 'close', 'volume']
        for col in required_columns:
            if col not in df.columns:
                logger.warning(f"缺少列 {col}，使用close价格填充")
                if col in ['open', 'high', 'low']:
                    df[col] = df['close']
                elif col == 'volume':
                    df[col] = 0
        
        # 创建Backtrader数据源
        data_feed = bt.feeds.PandasData(
            dataname=df,
            name=name or 'stock_data',
            datetime=None,  # 使用index作为datetime
            open='open',
            high='high',
            low='low',
            close='close',
            volume='volume',
            openinterest='openinterest' if 'openinterest' in df.columns else None
        )
        
        return data_feed