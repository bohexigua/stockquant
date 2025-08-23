#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
个股量价因子计算模块
从trade_market_stock_daily、trade_market_stock_60min、trade_market_stock_basic_daily表读取数据，
计算量价相关因子并将结果写入trade_factor_stock表
"""

import pandas as pd
import pymysql
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import logging
import os
import sys
import numpy as np

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)
from config import config, DatabaseConfig

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StockPriceVolumeFactorCalculator:
    """
    个股量价因子计算器
    
    主要功能:
    1. 从trade_market_stock_daily表读取个股日行情数据
    2. 从trade_market_stock_60min表读取个股60分钟行情数据
    3. 从trade_market_stock_basic_daily表读取个股基本面数据
    4. 计算量价相关因子
    5. 将计算结果写入trade_factor_stock表
    """
    
    def __init__(self):
        """
        初始化个股量价因子计算器
        """
        self.db_config = config.database
        self.connection = None
        self._init_database()
    
    def _init_database(self):
        """初始化数据库连接"""
        try:
            self.connection = pymysql.connect(
                host=self.db_config.host,
                port=self.db_config.port,
                user=self.db_config.user,
                password=self.db_config.password,
                database=self.db_config.database,
                charset=self.db_config.charset,
                autocommit=True
            )
            logger.info("数据库连接初始化成功")
        except Exception as e:
            logger.error(f"数据库连接初始化失败: {e}")
            raise
    
    def _close_database(self):
        """关闭数据库连接"""
        if self.connection:
            try:
                self.connection.close()
                logger.info("数据库连接已关闭")
            except Exception as e:
                # 忽略连接已关闭的错误
                if "Already closed" not in str(e):
                    logger.warning(f"关闭数据库连接时出现异常: {e}")
            finally:
                self.connection = None
    
    def get_daily_data(self, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        获取个股日行情数据
        
        Args:
            start_date: 开始日期，可选
            end_date: 结束日期，可选
            
        Returns:
            个股日行情数据DataFrame
        """
        try:
            with self.connection.cursor() as cursor:
                # 构建SQL查询
                base_sql = """
                SELECT 
                    code,
                    trade_date,
                    name,
                    open,
                    high,
                    low,
                    close,
                    pre_close,
                    chg_val,
                    chg_pct,
                    vol,
                    amount
                FROM trade_market_stock_daily
                """
                
                params = []
                if start_date and end_date:
                    base_sql += " WHERE trade_date BETWEEN %s AND %s"
                    params.extend([start_date, end_date])
                elif start_date:
                    base_sql += " WHERE trade_date >= %s"
                    params.append(start_date)
                elif end_date:
                    base_sql += " WHERE trade_date <= %s"
                    params.append(end_date)
                
                base_sql += " ORDER BY code, trade_date"
                
                cursor.execute(base_sql, params)
                results = cursor.fetchall()
                
                if not results:
                    logger.warning("未获取到个股日行情数据")
                    return pd.DataFrame()
                
                # 转换为DataFrame
                columns = [
                    'code', 'trade_date', 'name', 'open', 'high', 'low', 'close',
                    'pre_close', 'chg_val', 'chg_pct', 'vol', 'amount'
                ]
                
                df = pd.DataFrame(results, columns=columns)
                df['trade_date'] = pd.to_datetime(df['trade_date'])
                
                logger.info(f"获取到 {len(df)} 条个股日行情数据")
                return df
                
        except Exception as e:
            logger.error(f"获取个股日行情数据失败: {e}")
            return pd.DataFrame()
    
    def get_60min_data(self, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        获取个股60分钟行情数据
        
        Args:
            start_date: 开始日期，可选
            end_date: 结束日期，可选
            
        Returns:
            个股60分钟行情数据DataFrame
        """
        try:
            with self.connection.cursor() as cursor:
                # 构建SQL查询
                base_sql = """
                SELECT 
                    code,
                    trade_date,
                    name,
                    trade_time,
                    open,
                    close,
                    high,
                    low,
                    vol,
                    amount
                FROM trade_market_stock_60min
                """
                
                params = []
                if start_date and end_date:
                    base_sql += " WHERE trade_date BETWEEN %s AND %s"
                    params.extend([start_date, end_date])
                elif start_date:
                    base_sql += " WHERE trade_date >= %s"
                    params.append(start_date)
                elif end_date:
                    base_sql += " WHERE trade_date <= %s"
                    params.append(end_date)
                
                base_sql += " ORDER BY code, trade_date, trade_time"
                
                cursor.execute(base_sql, params)
                results = cursor.fetchall()
                
                if not results:
                    logger.warning("未获取到个股60分钟行情数据")
                    return pd.DataFrame()
                
                # 转换为DataFrame
                columns = [
                    'code', 'trade_date', 'name', 'trade_time', 'open', 'close',
                    'high', 'low', 'vol', 'amount'
                ]
                
                df = pd.DataFrame(results, columns=columns)
                df['trade_date'] = pd.to_datetime(df['trade_date'])
                
                logger.info(f"获取到 {len(df)} 条个股60分钟行情数据")
                return df
                
        except Exception as e:
            logger.error(f"获取个股60分钟行情数据失败: {e}")
            return pd.DataFrame()
    
    def get_basic_data(self, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        获取个股基本面数据
        
        Args:
            start_date: 开始日期，可选
            end_date: 结束日期，可选
            
        Returns:
            个股基本面数据DataFrame
        """
        try:
            with self.connection.cursor() as cursor:
                # 构建SQL查询
                base_sql = """
                SELECT 
                    code,
                    trade_date,
                    name,
                    turnover_rate,
                    turnover_rate_f,
                    volume_ratio,
                    pe,
                    pe_ttm,
                    pb,
                    total_share,
                    float_share,
                    free_share,
                    total_mv,
                    circ_mv
                FROM trade_market_stock_basic_daily
                """
                
                params = []
                if start_date and end_date:
                    base_sql += " WHERE trade_date BETWEEN %s AND %s"
                    params.extend([start_date, end_date])
                elif start_date:
                    base_sql += " WHERE trade_date >= %s"
                    params.append(start_date)
                elif end_date:
                    base_sql += " WHERE trade_date <= %s"
                    params.append(end_date)
                
                base_sql += " ORDER BY code, trade_date"
                
                cursor.execute(base_sql, params)
                results = cursor.fetchall()
                
                if not results:
                    logger.warning("未获取到个股基本面数据")
                    return pd.DataFrame()
                
                # 转换为DataFrame
                columns = [
                    'code', 'trade_date', 'name', 'turnover_rate', 'turnover_rate_f', 'volume_ratio',
                    'pe', 'pe_ttm', 'pb', 'total_share', 'float_share', 'free_share', 'total_mv', 'circ_mv'
                ]
                
                df = pd.DataFrame(results, columns=columns)
                df['trade_date'] = pd.to_datetime(df['trade_date'])
                
                logger.info(f"获取到 {len(df)} 条个股基本面数据")
                return df
                
        except Exception as e:
            logger.error(f"获取个股基本面数据失败: {e}")
            return pd.DataFrame()
    
    def get_latest_factor_date(self, code: str) -> str:
        """
        获取指定个股的最新因子数据日期
        
        Args:
            code: 股票代码
            
        Returns:
            最新因子数据日期，如果没有数据则返回None
        """
        try:
            with self.connection.cursor() as cursor:
                sql = """
                SELECT MAX(trade_date) 
                FROM trade_factor_stock 
                WHERE code = %s AND (
                    turnover_rate_today IS NOT NULL OR
                    turnover_rate_5d_avg IS NOT NULL OR
                    turnover_rate_10d_avg IS NOT NULL OR
                    turnover_rate_20d_avg IS NOT NULL OR
                    volume_surge_today IS NOT NULL OR
                    volume_surge_5d IS NOT NULL OR
                    avg_return_5d IS NOT NULL OR
                    avg_return_10d IS NOT NULL OR
                    avg_return_20d IS NOT NULL OR
                    pullback_ma5_days IS NOT NULL OR
                    divergence_today IS NOT NULL OR
                    market_cap IS NOT NULL OR
                    volume_price_divergence_60min IS NOT NULL
                )
                """
                cursor.execute(sql, (code,))
                result = cursor.fetchone()
                if result and result[0]:
                    return result[0].strftime('%Y-%m-%d')
                return None
        except Exception as e:
            logger.error(f"获取最新因子数据日期失败: {e}")
            return None
    
    def calculate_turnover_rate(self, daily_df: pd.DataFrame, basic_df: pd.DataFrame) -> pd.DataFrame:
        """
        计算换手率因子
        
        Args:
            daily_df: 日行情数据
            basic_df: 基本面数据（包含turnover_rate字段）
            
        Returns:
            包含换手率因子的DataFrame
        """
        # 合并日行情和基本面数据，直接使用数据库中的换手率字段
        merged_df = pd.merge(
            daily_df[['code', 'trade_date', 'name', 'vol', 'amount']],
            basic_df[['code', 'trade_date', 'turnover_rate', 'turnover_rate_f', 'volume_ratio']],
            on=['code', 'trade_date'],
            how='left'
        )
        
        # 使用数据库中的换手率字段，填充空值为0
        merged_df['turnover_rate_today'] = merged_df['turnover_rate'].fillna(0)
        
        # 按股票分组计算移动平均换手率
        result_list = []
        for code in merged_df['code'].unique():
            stock_data = merged_df[merged_df['code'] == code].copy()
            stock_data = stock_data.sort_values('trade_date')
            
            # 计算移动平均换手率
            stock_data['turnover_rate_5d_avg'] = stock_data['turnover_rate_today'].rolling(window=5, min_periods=1).mean()
            stock_data['turnover_rate_10d_avg'] = stock_data['turnover_rate_today'].rolling(window=10, min_periods=1).mean()
            stock_data['turnover_rate_20d_avg'] = stock_data['turnover_rate_today'].rolling(window=20, min_periods=1).mean()
            
            result_list.append(stock_data)
        
        if result_list:
            return pd.concat(result_list, ignore_index=True)
        else:
            return pd.DataFrame()
    
    def calculate_volume_surge(self, daily_df: pd.DataFrame) -> pd.DataFrame:
        """
        计算放量因子
        
        Args:
            daily_df: 日行情数据
            
        Returns:
            包含放量因子的DataFrame
        """
        result_list = []
        for code in daily_df['code'].unique():
            stock_data = daily_df[daily_df['code'] == code].copy()
            stock_data = stock_data.sort_values('trade_date')
            
            # 计算成交量的移动平均
            stock_data['vol_ma20'] = stock_data['vol'].rolling(window=20, min_periods=1).mean()
            
            # 当日放量：当日成交量 > 20日平均成交量的2倍
            stock_data['volume_surge_today'] = (
                stock_data['vol'] > stock_data['vol_ma20'] * 2
            ).astype(int)
            
            # 近5日放量：近5日内有任何一天放量
            stock_data['volume_surge_5d'] = (
                stock_data['volume_surge_today'].rolling(window=5, min_periods=1).sum() > 0
            ).astype(int)
            
            result_list.append(stock_data[['code', 'trade_date', 'name', 'volume_surge_today', 'volume_surge_5d']])
        
        if result_list:
            return pd.concat(result_list, ignore_index=True)
        else:
            return pd.DataFrame()
    
    def calculate_return_factors(self, daily_df: pd.DataFrame) -> pd.DataFrame:
        """
        计算涨幅因子
        
        Args:
            daily_df: 日行情数据
            
        Returns:
            包含涨幅因子的DataFrame
        """
        result_list = []
        for code in daily_df['code'].unique():
            stock_data = daily_df[daily_df['code'] == code].copy()
            stock_data = stock_data.sort_values('trade_date')
            
            # 计算移动平均涨幅
            stock_data['avg_return_5d'] = stock_data['chg_pct'].rolling(window=5, min_periods=1).mean()
            stock_data['avg_return_10d'] = stock_data['chg_pct'].rolling(window=10, min_periods=1).mean()
            stock_data['avg_return_20d'] = stock_data['chg_pct'].rolling(window=20, min_periods=1).mean()
            
            result_list.append(stock_data[['code', 'trade_date', 'name', 'avg_return_5d', 'avg_return_10d', 'avg_return_20d']])
        
        if result_list:
            return pd.concat(result_list, ignore_index=True)
        else:
            return pd.DataFrame()
    
    def calculate_technical_factors(self, daily_df: pd.DataFrame) -> pd.DataFrame:
        """
        计算技术指标因子
        
        Args:
            daily_df: 日行情数据
            
        Returns:
            包含技术指标因子的DataFrame
        """
        result_list = []
        for code in daily_df['code'].unique():
            stock_data = daily_df[daily_df['code'] == code].copy()
            stock_data = stock_data.sort_values('trade_date')
            
            # 计算5日移动平均线
            stock_data['ma5'] = stock_data['close'].rolling(window=5, min_periods=1).mean()
            
            # 计算回踩五日线：收盘价低于5日均线
            stock_data['below_ma5'] = (stock_data['close'] < stock_data['ma5']).astype(int)
            
            # 计算近5日回踩五日线天数
            stock_data['pullback_ma5_days'] = stock_data['below_ma5'].rolling(window=5, min_periods=1).sum()
            
            # 计算分歧：当日涨幅与成交量变化方向不一致
            stock_data['vol_change'] = stock_data['vol'].pct_change()
            stock_data['divergence_today'] = (
                ((stock_data['chg_pct'] > 0) & (stock_data['vol_change'] < 0)) |
                ((stock_data['chg_pct'] < 0) & (stock_data['vol_change'] > 0))
            ).astype(int)
            
            result_list.append(stock_data[['code', 'trade_date', 'name', 'pullback_ma5_days', 'divergence_today']])
        
        if result_list:
            return pd.concat(result_list, ignore_index=True)
        else:
            return pd.DataFrame()
    
    def calculate_market_cap(self, basic_df: pd.DataFrame) -> pd.DataFrame:
        """
        计算市值因子
        
        Args:
            basic_df: 基本面数据
            
        Returns:
            包含市值因子的DataFrame
        """
        # 市值直接使用total_mv字段
        return basic_df[['code', 'trade_date', 'name', 'total_mv']].rename(columns={'total_mv': 'market_cap'})
    
    def calculate_volume_price_divergence_60min(self, min60_df: pd.DataFrame) -> pd.DataFrame:
        """
        计算60分钟量价背离因子
        
        Args:
            min60_df: 60分钟行情数据
            
        Returns:
            包含量价背离因子的DataFrame
        """
        result_list = []
        
        # 按股票和交易日分组
        for (code, trade_date), group in min60_df.groupby(['code', 'trade_date']):
            group = group.sort_values('trade_time')
            
            if len(group) < 2:
                # 数据不足，无法判断背离
                divergence = 0
            else:
                # 计算价格和成交量的变化趋势
                price_trend = (group['close'].iloc[-1] - group['close'].iloc[0]) / group['close'].iloc[0]
                vol_trend = (group['vol'].iloc[-1] - group['vol'].iloc[0]) / max(group['vol'].iloc[0], 1)
                
                # 判断量价背离：价格上涨但成交量下降，或价格下跌但成交量上升
                divergence = int(
                    (price_trend > 0.02 and vol_trend < -0.1) or
                    (price_trend < -0.02 and vol_trend > 0.1)
                )
            
            result_list.append({
                'code': code,
                'trade_date': trade_date,
                'name': group['name'].iloc[0],
                'volume_price_divergence_60min': divergence
            })
        
        if result_list:
            return pd.DataFrame(result_list)
        else:
            return pd.DataFrame()
    
    def calculate_price_volume_factors(self, daily_df: pd.DataFrame = None, min60_df: pd.DataFrame = None, basic_df: pd.DataFrame = None) -> pd.DataFrame:
        """
        计算个股量价因子
        
        Args:
            daily_df: 日行情数据DataFrame，可选，为None时获取全部数据
            min60_df: 60分钟行情数据DataFrame，可选
            basic_df: 基本面数据DataFrame，可选
            
        Returns:
            计算后的因子数据DataFrame
        """
        if daily_df is None:
            daily_df = self.get_daily_data()
        if min60_df is None:
            min60_df = self.get_60min_data()
        if basic_df is None:
            basic_df = self.get_basic_data()
        
        if daily_df.empty:
            logger.warning("没有日行情数据可供计算因子")
            return pd.DataFrame()

        try:
            # 按股票代码分组计算因子
            all_factor_list = []
            unique_codes = daily_df['code'].unique()
            total_stocks = len(unique_codes)
            
            logger.info(f"开始计算 {total_stocks} 只个股的量价因子数据")
            
            for idx, code in enumerate(unique_codes, 1):
                # 获取该股票的所有数据
                stock_daily = daily_df[daily_df['code'] == code].copy()
                stock_basic = basic_df[basic_df['code'] == code].copy() if not basic_df.empty else pd.DataFrame()
                stock_60min = min60_df[min60_df['code'] == code].copy() if not min60_df.empty else pd.DataFrame()
                
                stock_daily = stock_daily.sort_values('trade_date')
                stock_name = stock_daily['name'].iloc[-1] if not stock_daily.empty else ''
                total_dates = len(stock_daily)
                
                # 检查该股票是否已有最新数据
                latest_date = self.get_latest_factor_date(code)
                if latest_date:
                    latest_daily_date = stock_daily['trade_date'].max().strftime('%Y-%m-%d')
                    if latest_date >= latest_daily_date:
                        logger.info(f"跳过个股 [{idx}/{total_stocks}] {code} ({stock_name})，已有最新数据 (最新日期: {latest_date})")
                        continue
                else:
                    logger.info(f"处理个股 [{idx}/{total_stocks}] {code} ({stock_name})，全量计算 {total_dates} 个交易日")
                
                if stock_daily.empty:
                    continue
                
                # 计算各类因子
                turnover_factors = self.calculate_turnover_rate(stock_daily, stock_basic)
                volume_factors = self.calculate_volume_surge(stock_daily)
                return_factors = self.calculate_return_factors(stock_daily)
                technical_factors = self.calculate_technical_factors(stock_daily)
                market_cap_factors = self.calculate_market_cap(stock_basic) if not stock_basic.empty else pd.DataFrame()
                divergence_60min_factors = self.calculate_volume_price_divergence_60min(stock_60min) if not stock_60min.empty else pd.DataFrame()
                
                # 合并所有因子数据
                factor_df = stock_daily[['code', 'trade_date', 'name']].copy()
                
                # 逐个合并因子
                if not turnover_factors.empty:
                    factor_df = pd.merge(factor_df, turnover_factors[[
                        'code', 'trade_date', 'turnover_rate_today', 'turnover_rate_5d_avg',
                        'turnover_rate_10d_avg', 'turnover_rate_20d_avg'
                    ]], on=['code', 'trade_date'], how='left')
                
                if not volume_factors.empty:
                    factor_df = pd.merge(factor_df, volume_factors[[
                        'code', 'trade_date', 'volume_surge_today', 'volume_surge_5d'
                    ]], on=['code', 'trade_date'], how='left')
                
                if not return_factors.empty:
                    factor_df = pd.merge(factor_df, return_factors[[
                        'code', 'trade_date', 'avg_return_5d', 'avg_return_10d', 'avg_return_20d'
                    ]], on=['code', 'trade_date'], how='left')
                
                if not technical_factors.empty:
                    factor_df = pd.merge(factor_df, technical_factors[[
                        'code', 'trade_date', 'pullback_ma5_days', 'divergence_today'
                    ]], on=['code', 'trade_date'], how='left')
                
                if not market_cap_factors.empty:
                    factor_df = pd.merge(factor_df, market_cap_factors[[
                        'code', 'trade_date', 'market_cap'
                    ]], on=['code', 'trade_date'], how='left')
                
                if not divergence_60min_factors.empty:
                    factor_df = pd.merge(factor_df, divergence_60min_factors[[
                        'code', 'trade_date', 'volume_price_divergence_60min'
                    ]], on=['code', 'trade_date'], how='left')
                
                # 数据类型转换和清洗
                factor_df['trade_date'] = pd.to_datetime(factor_df['trade_date'])
                
                # 数值列填充NaN
                numeric_columns = [
                    'turnover_rate_today', 'turnover_rate_5d_avg', 'turnover_rate_10d_avg', 'turnover_rate_20d_avg',
                    'volume_surge_today', 'volume_surge_5d', 'avg_return_5d', 'avg_return_10d', 'avg_return_20d',
                    'pullback_ma5_days', 'divergence_today', 'market_cap', 'volume_price_divergence_60min'
                ]
                
                for col in numeric_columns:
                    if col in factor_df.columns:
                        if col in ['volume_surge_today', 'volume_surge_5d', 'divergence_today', 'volume_price_divergence_60min', 'pullback_ma5_days']:
                            factor_df[col] = pd.to_numeric(factor_df[col], errors='coerce').fillna(0).astype(int)
                        else:
                            factor_df[col] = pd.to_numeric(factor_df[col], errors='coerce').fillna(0)
                    else:
                        # 如果列不存在，创建并填充默认值
                        if col in ['volume_surge_today', 'volume_surge_5d', 'divergence_today', 'volume_price_divergence_60min', 'pullback_ma5_days']:
                            factor_df[col] = 0
                        else:
                            factor_df[col] = 0.0
                
                # 字符串列填充
                factor_df['name'] = factor_df['name'].fillna('')
                
                # 每个股票处理完成后立即插入数据库
                if not factor_df.empty:
                    success = self.insert_factor_data(factor_df)
                    if success:
                        logger.info(f"个股 {code} 处理完成，已插入 {len(factor_df)} 条因子数据")
                        all_factor_list.extend(factor_df.to_dict('records'))
                    else:
                        logger.error(f"个股 {code} 数据插入失败")
                else:
                    logger.info(f"个股 {code} 无新数据需要处理")
                
                # 每处理50个交易日打印一次进度
                if total_dates > 50 and idx % 10 == 0:
                    logger.info(f"  - 已处理 {idx}/{total_stocks} 只个股")
            
            # 返回所有处理的因子数据
            if all_factor_list:
                final_factor_df = pd.DataFrame(all_factor_list)
                final_factor_df['trade_date'] = pd.to_datetime(final_factor_df['trade_date'])
                logger.info(f"所有个股处理完成，共计算 {len(final_factor_df)} 条个股量价因子数据")
                return final_factor_df
            else:
                logger.info("所有个股均已是最新数据，无需处理")
                return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"计算个股量价因子失败: {e}")
            return pd.DataFrame()
    
    def insert_factor_data(self, df: pd.DataFrame) -> bool:
        """
        插入因子数据到数据库
        
        Args:
            df: 因子数据DataFrame
            
        Returns:
            True表示成功，False表示失败
        """
        if df.empty:
            logger.warning("没有数据需要插入")
            return True
        
        try:
            with self.connection.cursor() as cursor:
                # 构建插入SQL - 使用ON DUPLICATE KEY UPDATE更新量价因子字段
                sql = """
                INSERT INTO trade_factor_stock (
                    trade_date, code, name, turnover_rate_today, turnover_rate_5d_avg,
                    turnover_rate_10d_avg, turnover_rate_20d_avg, volume_surge_today,
                    volume_surge_5d, avg_return_5d, avg_return_10d, avg_return_20d,
                    pullback_ma5_days, divergence_today, market_cap, volume_price_divergence_60min
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                ) ON DUPLICATE KEY UPDATE
                    name = VALUES(name),
                    turnover_rate_today = VALUES(turnover_rate_today),
                    turnover_rate_5d_avg = VALUES(turnover_rate_5d_avg),
                    turnover_rate_10d_avg = VALUES(turnover_rate_10d_avg),
                    turnover_rate_20d_avg = VALUES(turnover_rate_20d_avg),
                    volume_surge_today = VALUES(volume_surge_today),
                    volume_surge_5d = VALUES(volume_surge_5d),
                    avg_return_5d = VALUES(avg_return_5d),
                    avg_return_10d = VALUES(avg_return_10d),
                    avg_return_20d = VALUES(avg_return_20d),
                    pullback_ma5_days = VALUES(pullback_ma5_days),
                    divergence_today = VALUES(divergence_today),
                    market_cap = VALUES(market_cap),
                    volume_price_divergence_60min = VALUES(volume_price_divergence_60min),
                    updated_time = CURRENT_TIMESTAMP
                """
                
                # 准备数据
                data_list = []
                for _, row in df.iterrows():
                    data_tuple = (
                        row['trade_date'].strftime('%Y-%m-%d'),
                        row['code'],
                        row['name'],
                        float(row.get('turnover_rate_today', 0)),
                        float(row.get('turnover_rate_5d_avg', 0)),
                        float(row.get('turnover_rate_10d_avg', 0)),
                        float(row.get('turnover_rate_20d_avg', 0)),
                        int(row.get('volume_surge_today', 0)),
                        int(row.get('volume_surge_5d', 0)),
                        float(row.get('avg_return_5d', 0)),
                        float(row.get('avg_return_10d', 0)),
                        float(row.get('avg_return_20d', 0)),
                        int(row.get('pullback_ma5_days', 0)),
                        int(row.get('divergence_today', 0)),
                        float(row.get('market_cap', 0)),
                        int(row.get('volume_price_divergence_60min', 0))
                    )
                    data_list.append(data_tuple)
                
                # 批量插入
                cursor.executemany(sql, data_list)
                
                logger.info(f"成功插入 {len(data_list)} 条个股量价因子数据")
                return True
                
        except Exception as e:
            logger.error(f"插入因子数据失败: {e}")
            return False
    
    def update_factor_data(self, start_date: str = None, end_date: str = None) -> bool:
        """
        更新因子数据
        
        Args:
            start_date: 开始日期，可选
            end_date: 结束日期，可选
            
        Returns:
            True表示成功，False表示失败
        """
        try:
            logger.info("开始更新个股量价因子数据")
            
            # 获取各类数据
            daily_df = self.get_daily_data(start_date, end_date)
            min60_df = self.get_60min_data(start_date, end_date)
            basic_df = self.get_basic_data(start_date, end_date)
            
            if daily_df.empty:
                logger.warning("没有日行情数据可供更新")
                return True
            
            # 计算因子
            factor_df = self.calculate_price_volume_factors(daily_df, min60_df, basic_df)
            
            if not factor_df.empty:
                logger.info(f"成功更新 {len(factor_df)} 条个股量价因子数据")
            else:
                logger.info("所有数据均已是最新，无需更新")
            
            return True
            
        except Exception as e:
            logger.error(f"更新因子数据失败: {e}")
            return False
    
    def __del__(self):
        """析构函数，确保数据库连接被关闭"""
        self._close_database()


def main():
    """
    主函数
    """
    calculator = StockPriceVolumeFactorCalculator()
    try:
        # 更新所有个股量价因子数据
        success = calculator.update_factor_data()
        if success:
            logger.info("个股量价因子计算完成")
        else:
            logger.error("个股量价因子计算失败")
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
    finally:
        calculator._close_database()


if __name__ == "__main__":
    main()