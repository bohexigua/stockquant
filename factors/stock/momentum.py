#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
动量因子计算模块
计算个股5日窗口内的量价背离度（VWAP与成交量的皮尔逊相关系数）
"""

import os
import sys
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
import pymysql
from dataclasses import dataclass

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config import config, DatabaseConfig


class MomentumFactorCalculator:
    """动量因子计算器"""
    
    def __init__(self):
        """
        初始化动量因子计算器
        """
        self.db_config = config.database
        self.logger = self._setup_logger()
        self.window_size = 7  # 7日窗口
    
    def _setup_logger(self) -> logging.Logger:
        """设置日志"""
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _get_db_connection(self) -> pymysql.Connection:
        """获取数据库连接"""
        try:
            connection = pymysql.connect(
                host=self.db_config.host,
                port=self.db_config.port,
                user=self.db_config.user,
                password=self.db_config.password,
                database=self.db_config.database,
                charset=self.db_config.charset,
                autocommit=False
            )
            return connection
        except Exception as e:
            self.logger.error(f"数据库连接失败: {e}")
            raise
    
    def get_latest_trade_date(self) -> Optional[str]:
        """
        获取最新的交易日期
        
        Returns:
            最新交易日期字符串，格式为YYYY-MM-DD
        """
        connection = None
        try:
            connection = self._get_db_connection()
            with connection.cursor() as cursor:
                sql = """
                SELECT MAX(trade_date) as latest_date 
                FROM trade_market_stock_daily
                """
                cursor.execute(sql)
                result = cursor.fetchone()
                
                if result and result[0]:
                    return result[0].strftime('%Y-%m-%d') if hasattr(result[0], 'strftime') else str(result[0])
                return None
                
        except Exception as e:
            self.logger.error(f"获取最新交易日期失败: {e}")
            return None
        finally:
            if connection:
                connection.close()
    
    def get_stock_data_for_window(self, trade_date: str, window_size: int = 7) -> pd.DataFrame:
        """
        获取指定日期前N个交易日的股票数据（用于计算窗口内的量价背离度）
        
        Args:
            trade_date: 目标交易日期，格式为YYYY-MM-DD
            window_size: 窗口大小，默认7天
            
        Returns:
            包含股票数据的DataFrame
        """
        connection = None
        try:
            connection = self._get_db_connection()
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:
                # 使用子查询获取每只股票最近window_size天的数据
                sql = """
                SELECT 
                    trade_date,
                    code,
                    name,
                    open,
                    high,
                    low,
                    close,
                    vol,
                    amount
                FROM (
                    SELECT 
                        trade_date,
                        code,
                        name,
                        open,
                        high,
                        low,
                        close,
                        vol,
                        amount,
                        ROW_NUMBER() OVER (PARTITION BY code ORDER BY trade_date DESC) as rn
                    FROM trade_market_stock_daily 
                    WHERE trade_date <= %s 
                        AND vol > 0 
                        AND amount > 0
                        AND close > 0
                ) ranked_data
                WHERE rn <= %s
                ORDER BY code, trade_date DESC
                """
                cursor.execute(sql, (trade_date, window_size))
                results = cursor.fetchall()
                
                if not results:
                    self.logger.warning(f"未获取到 {trade_date} 及之前 {window_size} 天的股票数据")
                    return pd.DataFrame()
                
                df = pd.DataFrame(results)
                self.logger.info(f"获取到 {len(df)} 条股票数据（窗口大小: {window_size}天），用于计算 {trade_date} 的量价背离度")
                return df
                
        except Exception as e:
            self.logger.error(f"获取股票数据失败: {e}")
            return pd.DataFrame()
        finally:
            if connection:
                connection.close()
    
    def calculate_vwap(self, prices: np.ndarray, volumes: np.ndarray) -> float:
        """
        计算成交量加权平均价格 (VWAP)
        
        Args:
            prices: 价格数组
            volumes: 成交量数组
            
        Returns:
            VWAP值
        """
        if len(prices) == 0 or len(volumes) == 0 or np.sum(volumes) == 0:
            return np.nan
        
        return np.sum(prices * volumes) / np.sum(volumes)
    
    def calculate_volume_price_divergence(self, stock_data: pd.DataFrame, window_size: int = 7) -> float:
        """
        计算量价背离度（7日窗口内VWAP与成交量的皮尔逊相关系数）
        
        Args:
            stock_data: 单只股票的历史数据，按日期降序排列
            window_size: 窗口大小，默认7天
            
        Returns:
            量价背离度（皮尔逊相关系数）
        """
        try:
            if len(stock_data) < window_size:
                return np.nan
            
            # 取最近window_size天的数据
            recent_data = stock_data.head(window_size).copy()
            recent_data = recent_data.sort_values('trade_date')  # 按日期升序排列
            
            # 确保数值列为float类型
            numeric_columns = ['open', 'high', 'low', 'close', 'vol', 'amount']
            for col in numeric_columns:
                if col in recent_data.columns:
                    recent_data[col] = pd.to_numeric(recent_data[col], errors='coerce')
            
            # 计算每日的VWAP（这里使用典型价格作为代理）
            # 典型价格 = (high + low + close) / 3
            recent_data['typical_price'] = (recent_data['high'] + recent_data['low'] + recent_data['close']) / 3
            
            # 提取VWAP和成交量数据
            vwap_values = recent_data['typical_price'].values.astype(float)
            volume_values = recent_data['vol'].values.astype(float)
            
            # 检查数据有效性
            if len(vwap_values) < 2 or len(volume_values) < 2:
                return np.nan
            
            # 检查是否有NaN值
            if np.any(np.isnan(vwap_values)) or np.any(np.isnan(volume_values)):
                return np.nan
            
            # 检查是否有零方差
            if np.std(vwap_values) == 0 or np.std(volume_values) == 0:
                return np.nan
            
            # 计算皮尔逊相关系数
            correlation = np.corrcoef(vwap_values, volume_values)[0, 1]
            
            return float(correlation) if not np.isnan(correlation) else np.nan
            
        except Exception as e:
            self.logger.error(f"计算量价背离度失败: {e}")
            return np.nan
    
    def calculate_momentum_factors_for_date(self, trade_date: str) -> List[Dict[str, Any]]:
        """
        计算指定日期所有股票的动量因子
        
        Args:
            trade_date: 交易日期，格式为YYYY-MM-DD
            
        Returns:
            动量因子数据列表
        """
        try:
            # 获取股票数据
            stock_data = self.get_stock_data_for_window(trade_date, self.window_size)
            if stock_data.empty:
                self.logger.warning(f"{trade_date} 没有可用的股票数据")
                return []
            
            # 按股票代码分组计算
            momentum_factors = []
            stock_groups = stock_data.groupby('code')
            
            total_stocks = len(stock_groups)
            processed_count = 0
            
            for code, group_data in stock_groups:
                try:
                    # 计算量价背离度
                    divergence = self.calculate_volume_price_divergence(group_data, self.window_size)
                    
                    if not np.isnan(divergence):
                        stock_name = group_data.iloc[0]['name']
                        
                        momentum_factors.append({
                            'trade_date': trade_date,
                            'code': code,
                            'name': stock_name,
                            'volume_price_divergence_5d': round(divergence, 4)
                        })
                    
                    processed_count += 1
                    if processed_count % 500 == 0:
                        self.logger.info(f"已处理 {processed_count}/{total_stocks} 只股票")
                        
                except Exception as e:
                    self.logger.warning(f"计算股票 {code} 的动量因子失败: {e}")
                    continue
            
            self.logger.info(f"完成 {trade_date} 的动量因子计算，共 {len(momentum_factors)} 只股票")
            return momentum_factors
            
        except Exception as e:
            self.logger.error(f"计算 {trade_date} 动量因子失败: {e}")
            return []
    
    def clear_existing_data(self, trade_date: str = None, start_date: str = None, end_date: str = None) -> bool:
        """
        清除现有动量因子数据
        
        Args:
            trade_date: 单个交易日期，可选
            start_date: 开始日期，可选
            end_date: 结束日期，可选
            
        Returns:
            是否成功
        """
        connection = None
        try:
            connection = self._get_db_connection()
            with connection.cursor() as cursor:
                if trade_date:
                    # 删除单个日期的数据
                    sql = "DELETE FROM trade_factor_stock_momentum WHERE trade_date = %s"
                    cursor.execute(sql, (trade_date,))
                    deleted_count = cursor.rowcount
                    self.logger.info(f"清除了 {deleted_count} 条 {trade_date} 的现有动量因子数据")
                elif start_date and end_date:
                    # 删除指定日期范围的数据
                    sql = "DELETE FROM trade_factor_stock_momentum WHERE trade_date BETWEEN %s AND %s"
                    cursor.execute(sql, (start_date, end_date))
                    deleted_count = cursor.rowcount
                    self.logger.info(f"清除了 {deleted_count} 条 {start_date} 到 {end_date} 的现有动量因子数据")
                else:
                    # 删除全部数据
                    sql = "DELETE FROM trade_factor_stock_momentum"
                    cursor.execute(sql)
                    deleted_count = cursor.rowcount
                    self.logger.info(f"清除了 {deleted_count} 条全部现有动量因子数据")
                
                connection.commit()
                return True
                
        except Exception as e:
            self.logger.error(f"清除现有数据失败: {e}")
            if connection:
                connection.rollback()
            return False
        finally:
            if connection:
                connection.close()
    
    def insert_momentum_factors(self, momentum_data: List[Dict[str, Any]]) -> bool:
        """
        批量插入动量因子数据
        
        Args:
            momentum_data: 动量因子数据列表
            
        Returns:
            是否成功
        """
        if not momentum_data:
            self.logger.warning("没有数据需要插入")
            return True
            
        connection = None
        try:
            connection = self._get_db_connection()
            with connection.cursor() as cursor:
                sql = """
                INSERT INTO trade_factor_stock_momentum 
                (trade_date, code, name, volume_price_divergence_5d, created_time, updated_time)
                VALUES (%s, %s, %s, %s, %s, %s)
                """
                
                current_time = datetime.now()
                data_to_insert = []
                
                for item in momentum_data:
                    data_to_insert.append((
                        item['trade_date'],
                        item['code'],
                        item['name'],
                        item['volume_price_divergence_5d'],
                        current_time,
                        current_time
                    ))
                
                cursor.executemany(sql, data_to_insert)
                connection.commit()
                
                self.logger.info(f"成功插入 {len(data_to_insert)} 条动量因子数据")
                return True
                
        except Exception as e:
            self.logger.error(f"插入动量因子数据失败: {e}")
            if connection:
                connection.rollback()
            return False
        finally:
            if connection:
                connection.close()
    
    def calculate_momentum_factor(self, trade_date: str = None) -> bool:
        """
        计算动量因子
        
        Args:
            trade_date: 交易日期，默认为最新交易日
            
        Returns:
            是否成功
        """
        try:
            # 如果没有指定日期，使用最新交易日
            if not trade_date:
                trade_date = self.get_latest_trade_date()
                if not trade_date:
                    self.logger.error("无法获取最新交易日期")
                    return False
            
            self.logger.info(f"开始计算 {trade_date} 的动量因子")
            
            # 计算动量因子
            momentum_data = self.calculate_momentum_factors_for_date(trade_date)
            if not momentum_data:
                self.logger.warning(f"{trade_date} 没有可用的动量因子数据")
                return False
            
            # 清除现有数据
            if not self.clear_existing_data(trade_date):
                return False
            
            # 插入新数据
            if not self.insert_momentum_factors(momentum_data):
                return False
            
            self.logger.info(f"动量因子计算完成: {trade_date}")
            return True
            
        except Exception as e:
            self.logger.error(f"计算动量因子失败: {e}")
            return False
    
    def calculate_recent_days(self, days: int = 5) -> bool:
        """
        计算最近N天的动量因子
        
        Args:
            days: 天数
            
        Returns:
            是否成功
        """
        try:
            latest_date = self.get_latest_trade_date()
            if not latest_date:
                self.logger.error("无法获取最新交易日期")
                return False
            
            # 获取最近的交易日期列表
            connection = self._get_db_connection()
            try:
                with connection.cursor() as cursor:
                    sql = """
                    SELECT DISTINCT trade_date 
                    FROM trade_market_stock_daily 
                    WHERE trade_date <= %s 
                    ORDER BY trade_date DESC 
                    LIMIT %s
                    """
                    cursor.execute(sql, (latest_date, days))
                    trade_dates = [row[0].strftime('%Y-%m-%d') if hasattr(row[0], 'strftime') else str(row[0]) 
                                 for row in cursor.fetchall()]
            finally:
                connection.close()
            
            success_count = 0
            total_count = len(trade_dates)
            
            for i, trade_date in enumerate(trade_dates, 1):
                self.logger.info(f"处理第 {i}/{total_count} 个交易日: {trade_date}")
                
                if self.calculate_momentum_factor(trade_date):
                    success_count += 1
                else:
                    self.logger.warning(f"计算 {trade_date} 动量因子失败")
            
            self.logger.info(f"完成最近 {days} 天的动量因子计算，成功 {success_count}/{total_count} 天")
            return success_count > 0
            
        except Exception as e:
            self.logger.error(f"计算最近几天动量因子失败: {e}")
            return False
    
    def update_factor_data(self, start_date: str = None, end_date: str = None) -> bool:
        """
        更新动量因子数据
        
        Args:
            start_date: 开始日期 (YYYY-MM-DD)，可选，为None时更新全部数据
            end_date: 结束日期 (YYYY-MM-DD)，可选，为None时更新全部数据
            
        Returns:
            更新是否成功
        """
        try:
            if start_date and end_date:
                self.logger.info(f"开始更新动量因子数据: {start_date} 到 {end_date}")
                # 获取指定日期范围内的交易日期
                trade_dates = self._get_trade_dates_in_range(start_date, end_date)
            else:
                self.logger.info("开始更新全部动量因子数据")
                # 获取全部交易日期
                trade_dates = self._get_all_trade_dates()
            
            if not trade_dates:
                self.logger.warning("未获取到交易日期数据")
                return False
            
            # 先删除已有的因子数据
            if start_date and end_date:
                if not self.clear_existing_data(start_date=start_date, end_date=end_date):
                    return False
            else:
                if not self.clear_existing_data():
                    return False
            
            # 批量计算和插入动量因子数据
            all_momentum_data = []
            success_count = 0
            total_count = len(trade_dates)
            
            for i, trade_date in enumerate(trade_dates, 1):
                self.logger.info(f"处理第 {i}/{total_count} 个交易日: {trade_date}")
                
                momentum_data = self.calculate_momentum_factors_for_date(trade_date)
                if momentum_data:
                    all_momentum_data.extend(momentum_data)
                    success_count += 1
                else:
                    self.logger.warning(f"计算 {trade_date} 动量因子失败")
            
            # 批量插入所有数据
            if all_momentum_data:
                if not self.insert_momentum_factors(all_momentum_data):
                    return False
            
            self.logger.info(f"动量因子数据更新完成: 成功 {success_count}/{total_count} 天，共插入 {len(all_momentum_data)} 条数据")
            return success_count > 0
            
        except Exception as e:
            self.logger.error(f"更新动量因子数据失败: {e}")
            return False
    
    def _get_trade_dates_in_range(self, start_date: str, end_date: str) -> List[str]:
        """
        获取指定日期范围内的交易日期
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            交易日期列表
        """
        try:
            connection = self._get_db_connection()
            try:
                with connection.cursor() as cursor:
                    sql = """
                    SELECT DISTINCT trade_date 
                    FROM trade_market_stock_daily 
                    WHERE trade_date >= %s AND trade_date <= %s
                    ORDER BY trade_date ASC
                    """
                    cursor.execute(sql, (start_date, end_date))
                    trade_dates = [row[0].strftime('%Y-%m-%d') if hasattr(row[0], 'strftime') else str(row[0]) 
                                 for row in cursor.fetchall()]
                    return trade_dates
            finally:
                connection.close()
        except Exception as e:
            self.logger.error(f"获取指定日期范围交易日期失败: {e}")
            return []
    
    def _get_all_trade_dates(self) -> List[str]:
        """
        获取全部交易日期
        
        Returns:
            交易日期列表
        """
        try:
            connection = self._get_db_connection()
            try:
                with connection.cursor() as cursor:
                    sql = """
                    SELECT DISTINCT trade_date 
                    FROM trade_market_stock_daily 
                    ORDER BY trade_date ASC
                    """
                    cursor.execute(sql)
                    trade_dates = [row[0].strftime('%Y-%m-%d') if hasattr(row[0], 'strftime') else str(row[0]) 
                                 for row in cursor.fetchall()]
                    return trade_dates
            finally:
                connection.close()
        except Exception as e:
            self.logger.error(f"获取全部交易日期失败: {e}")
            return []


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='动量因子计算')
    parser.add_argument('--date', '-d', help='交易日期 (YYYY-MM-DD)，单独计算某一天')
    parser.add_argument('--start-date', help='开始日期 (YYYY-MM-DD)，与--end-date配合使用')
    parser.add_argument('--end-date', help='结束日期 (YYYY-MM-DD)，与--start-date配合使用')
    parser.add_argument('--days', type=int, help='计算最近N天')
    parser.add_argument('--all', action='store_true', help='更新全部数据')
    
    args = parser.parse_args()
    
    try:
        calculator = MomentumFactorCalculator()
        
        if args.all:
            # 更新全部数据
            success = calculator.update_factor_data()
        elif args.start_date and args.end_date:
            # 更新指定日期范围的数据
            success = calculator.update_factor_data(args.start_date, args.end_date)
        elif args.days:
            # 计算最近N天
            success = calculator.calculate_recent_days(args.days)
        elif args.date:
            # 计算单独某一天
            success = calculator.calculate_momentum_factor(args.date)
        else:
            # 默认计算最新交易日
            success = calculator.calculate_momentum_factor()
        
        if success:
            print("动量因子计算完成")
        else:
            print("动量因子计算失败")
            sys.exit(1)
            
    except Exception as e:
        print(f"程序执行失败: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()