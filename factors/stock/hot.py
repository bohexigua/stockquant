#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
个股人气排名因子计算模块
从trade_market_dc_stock_hot表读取个股人气数据，
计算排名相关因子并更新到trade_factor_stock表的rank_today、rank_5d_avg、rank_10d_avg、rank_surge_today、rank_surge_5d字段
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

# 创建logs目录
logs_dir = os.path.join(project_root, 'logs')
os.makedirs(logs_dir, exist_ok=True)

# 配置日志 - 输出到文件和控制台
log_filename = os.path.join(logs_dir, f'stock_hot_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class StockHotFactorCalculator:
    """
    个股人气排名因子计算器
    
    主要功能:
    1. 从trade_market_dc_stock_hot表读取个股人气数据
    2. 计算排名相关因子
    3. 将计算结果更新到trade_factor_stock表的排名相关字段
    """
    
    def __init__(self):
        """
        初始化个股人气排名因子计算器
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
    
    def get_hot_data(self, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        获取个股人气数据
        
        Args:
            start_date: 开始日期，可选
            end_date: 结束日期，可选
            
        Returns:
            个股人气数据DataFrame
        """
        try:
            with self.connection.cursor() as cursor:
                # 构建SQL查询
                base_sql = """
                SELECT 
                    code,
                    trade_date,
                    name,
                    hot_rank,
                    pct_change,
                    current_price
                FROM trade_market_dc_stock_hot
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
                
                base_sql += " ORDER BY trade_date, hot_rank"
                
                cursor.execute(base_sql, params)
                results = cursor.fetchall()
                
                if not results:
                    logger.warning("未获取到个股人气数据")
                    return pd.DataFrame()
                
                # 转换为DataFrame
                columns = [
                    'code', 'trade_date', 'name', 'hot_rank', 'pct_change', 'current_price'
                ]
                
                df = pd.DataFrame(results, columns=columns)
                df['trade_date'] = pd.to_datetime(df['trade_date'])
                
                logger.info(f"获取到 {len(df)} 条个股人气数据")
                return df
                
        except Exception as e:
            logger.error(f"获取个股人气数据失败: {e}")
            return pd.DataFrame()
    
    def get_latest_factor_date(self, code: str) -> str:
        """
        获取指定个股的最新因子数据日期
        
        Args:
            code: 股票代码
            
        Returns:
            最新因子数据日期，如果没有数据返回None
        """
        try:
            with self.connection.cursor() as cursor:
                sql = """
                SELECT MAX(trade_date) as latest_date
                FROM trade_factor_stock
                WHERE code = %s AND rank_today IS NOT NULL
                """
                cursor.execute(sql, (code,))
                result = cursor.fetchone()
                
                if result and result[0]:
                    return result[0].strftime('%Y-%m-%d')
                else:
                    return None
                    
        except Exception as e:
            logger.error(f"获取最新因子数据日期失败: {e}")
            return None
    
    def calculate_rank_factors(self, stock_data: pd.DataFrame) -> Dict[str, Any]:
        """
        计算个股排名因子
        
        Args:
            stock_data: 个股人气数据DataFrame
            
        Returns:
            排名因子字典
        """
        try:
            if stock_data.empty:
                return {}
            
            # 按日期排序
            stock_data = stock_data.sort_values('trade_date')
            
            factors = {}
            
            for idx, row in stock_data.iterrows():
                trade_date = row['trade_date'].strftime('%Y-%m-%d')
                rank_today = row['hot_rank'] if pd.notna(row['hot_rank']) else None
                
                # 获取当前日期之前的数据用于计算历史平均
                current_date = row['trade_date']
                historical_data = stock_data[stock_data['trade_date'] < current_date]
                
                # 计算近5日平均排名
                rank_5d_avg = None
                if len(historical_data) >= 1:
                    recent_5d = historical_data.tail(5)
                    valid_ranks = recent_5d['hot_rank'].dropna()
                    if len(valid_ranks) > 0:
                        rank_5d_avg = float(valid_ranks.mean())
                
                # 计算近10日平均排名
                rank_10d_avg = None
                if len(historical_data) >= 1:
                    recent_10d = historical_data.tail(10)
                    valid_ranks = recent_10d['hot_rank'].dropna()
                    if len(valid_ranks) > 0:
                        rank_10d_avg = float(valid_ranks.mean())
                
                # 计算当日排名是否有大幅上升（排名数字变小表示上升）
                rank_surge_today = 0
                if len(historical_data) >= 1 and rank_today is not None:
                    yesterday_rank = historical_data['hot_rank'].iloc[-1] if not historical_data.empty else None
                    if yesterday_rank is not None and pd.notna(yesterday_rank):
                        # 排名上升超过20位认为是大幅上升
                        if yesterday_rank - rank_today >= 20:
                            rank_surge_today = 1
                
                # 计算近5日排名是否有大幅上升
                rank_surge_5d = 0
                if len(historical_data) >= 5 and rank_today is not None:
                    five_days_ago_data = historical_data.tail(5)
                    if not five_days_ago_data.empty:
                        five_days_ago_rank = five_days_ago_data['hot_rank'].iloc[0] if len(five_days_ago_data) > 0 else None
                        if five_days_ago_rank is not None and pd.notna(five_days_ago_rank):
                            # 5日内排名上升超过40位认为是大幅上升
                            if five_days_ago_rank - rank_today >= 40:
                                rank_surge_5d = 1
                
                factors[trade_date] = {
                    'rank_today': rank_today,
                    'rank_5d_avg': rank_5d_avg,
                    'rank_10d_avg': rank_10d_avg,
                    'rank_surge_today': rank_surge_today,
                    'rank_surge_5d': rank_surge_5d
                }
            
            return factors
            
        except Exception as e:
            logger.warning(f"计算排名因子失败: {e}")
            return {}
    
    def update_factor_data(self, code: str, trade_date: str, factors: Dict[str, Any], name: str = None) -> bool:
        """
        更新个股因子数据
        
        Args:
            code: 股票代码
            trade_date: 交易日期
            factors: 因子数据字典
            name: 股票名称
            
        Returns:
            更新是否成功
        """
        try:
            with self.connection.cursor() as cursor:
                # 使用INSERT ... ON DUPLICATE KEY UPDATE语句
                sql = """
                INSERT INTO trade_factor_stock (
                    code, trade_date, name, rank_today, rank_5d_avg, rank_10d_avg, 
                    rank_surge_today, rank_surge_5d
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                rank_today = VALUES(rank_today),
                rank_5d_avg = VALUES(rank_5d_avg),
                rank_10d_avg = VALUES(rank_10d_avg),
                rank_surge_today = VALUES(rank_surge_today),
                rank_surge_5d = VALUES(rank_surge_5d),
                updated_time = CURRENT_TIMESTAMP
                """
                
                cursor.execute(sql, (
                    code, trade_date, name,
                    factors.get('rank_today'),
                    factors.get('rank_5d_avg'),
                    factors.get('rank_10d_avg'),
                    factors.get('rank_surge_today'),
                    factors.get('rank_surge_5d')
                ))
                return True
                
        except Exception as e:
            logger.error(f"更新因子数据失败: {e}")
            return False
    
    def calculate_hot_factors(self, df: pd.DataFrame = None) -> bool:
        """
        计算个股人气排名因子
        
        Args:
            df: 个股人气数据DataFrame，可选，为None时获取全部数据
            
        Returns:
            计算是否成功
        """
        if df is None:
            df = self.get_hot_data()
        
        if df.empty:
            logger.warning("没有数据可供计算因子")
            return False

        try:
            # 按股票代码分组计算因子
            unique_codes = df['code'].unique()
            total_stocks = len(unique_codes)
            
            logger.info(f"开始计算 {total_stocks} 只个股的人气排名因子数据")
            
            processed_count = 0
            skipped_count = 0
            
            for idx, code in enumerate(unique_codes, 1):
                stock_data = df[df['code'] == code].copy()
                stock_data = stock_data.sort_values('trade_date')
                
                # 获取股票名称
                stock_name = stock_data['name'].iloc[-1] if not stock_data.empty else ''
                
                # 检查该股票是否已有最新数据
                latest_date = self.get_latest_factor_date(code)
                if latest_date:
                    latest_market_date = stock_data['trade_date'].max().strftime('%Y-%m-%d')
                    if latest_date >= latest_market_date:
                        logger.info(f"跳过个股 [{idx}/{total_stocks}] {code} ({stock_name})，已有最新数据 (最新日期: {latest_date})")
                        skipped_count += 1
                        continue
                
                if stock_data.empty:
                    continue
                
                logger.info(f"处理个股 [{idx}/{total_stocks}] {code} ({stock_name})，计算 {len(stock_data)} 个交易日")
                
                # 计算排名因子
                factors_dict = self.calculate_rank_factors(stock_data)
                
                if not factors_dict:
                    logger.warning(f"个股 {code} 因子计算失败")
                    continue
                
                # 更新每个交易日的因子数据
                success_count = 0
                for trade_date, factors in factors_dict.items():
                    if self.update_factor_data(code, trade_date, factors, stock_name):
                        success_count += 1
                
                if success_count > 0:
                    logger.info(f"个股 {code} 处理完成，成功更新 {success_count} 条因子数据")
                    processed_count += 1
                else:
                    logger.warning(f"个股 {code} 处理失败")
            
            logger.info(f"人气排名因子计算完成，共处理 {processed_count} 只个股，跳过 {skipped_count} 只个股")
            return True
            
        except Exception as e:
            logger.error(f"计算个股人气排名因子失败: {e}")
            return False
    
    def update_all_factors(self) -> bool:
        """
        更新全部个股人气排名因子数据
        
        Returns:
            更新是否成功
        """
        try:
            logger.info("开始更新个股人气排名因子数据")
            
            # 计算因子
            success = self.calculate_hot_factors()
            
            if success:
                logger.info("个股人气排名因子数据更新完成")
            else:
                logger.error("个股人气排名因子数据更新失败")
            
            return success
            
        except Exception as e:
            logger.error(f"更新个股人气排名因子数据失败: {e}")
            return False
    
    def __del__(self):
        """析构函数，确保数据库连接关闭"""
        try:
            self._close_database()
        except Exception:
            # 忽略析构函数中的所有异常
            pass


def main():
    """
    主函数，执行个股人气排名因子数据更新
    """
    calculator = StockHotFactorCalculator()
    
    try:
        # 更新全部数据
        success = calculator.update_all_factors()
        if success:
            print("个股人气排名因子数据更新成功")
        else:
            print("个股人气排名因子数据更新失败")
    except Exception as e:
        logger.error(f"主函数执行失败: {e}")
        print(f"个股人气排名因子数据更新失败: {e}")
    finally:
        calculator._close_database()


if __name__ == "__main__":
    main()