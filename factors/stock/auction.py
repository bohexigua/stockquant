#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
个股竞价因子计算模块
从trade_market_stock_auction_daily表读取个股竞价数据，
计算竞价相关因子并将结果写入trade_factor_stock表的bid_ask_turnover_rate字段
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


class StockAuctionFactorCalculator:
    """
    个股竞价因子计算器
    
    主要功能:
    1. 从trade_market_stock_auction_daily表读取个股竞价数据
    2. 计算竞价换手率因子
    3. 将计算结果写入trade_factor_stock表的bid_ask_turnover_rate字段
    """
    
    def __init__(self):
        """
        初始化个股竞价因子计算器
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
    
    def get_auction_data(self, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        获取个股竞价数据
        
        Args:
            start_date: 开始日期，可选
            end_date: 结束日期，可选
            
        Returns:
            个股竞价数据DataFrame
        """
        try:
            with self.connection.cursor() as cursor:
                # 构建SQL查询
                base_sql = """
                SELECT 
                    code,
                    trade_date,
                    name,
                    vol,
                    price,
                    amount,
                    pre_close,
                    turnover_rate,
                    volume_ratio,
                    float_share
                FROM trade_market_stock_auction_daily
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
                    logger.warning("未获取到个股竞价数据")
                    return pd.DataFrame()
                
                # 转换为DataFrame
                columns = [
                    'code', 'trade_date', 'name', 'vol', 'price', 'amount',
                    'pre_close', 'turnover_rate', 'volume_ratio', 'float_share'
                ]
                
                df = pd.DataFrame(results, columns=columns)
                df['trade_date'] = pd.to_datetime(df['trade_date'])
                
                logger.info(f"获取到 {len(df)} 条个股竞价数据")
                return df
                
        except Exception as e:
            logger.error(f"获取个股竞价数据失败: {e}")
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
                WHERE code = %s AND bid_ask_turnover_rate IS NOT NULL
                """
                cursor.execute(sql, (code,))
                result = cursor.fetchone()
                if result and result[0]:
                    return result[0].strftime('%Y-%m-%d')
                return None
        except Exception as e:
            logger.error(f"获取最新因子数据日期失败: {e}")
            return None
    
    def calculate_auction_factors(self, df: pd.DataFrame = None) -> pd.DataFrame:
        """
        计算个股竞价因子
        
        Args:
            df: 个股竞价数据DataFrame，可选，为None时获取全部数据
            
        Returns:
            计算后的因子数据DataFrame
        """
        if df is None:
            df = self.get_auction_data()
        
        if df.empty:
            logger.warning("没有数据可供计算因子")
            return pd.DataFrame()

        try:
            # 按股票代码分组计算因子
            all_factor_list = []
            unique_codes = df['code'].unique()
            total_stocks = len(unique_codes)
            
            logger.info(f"开始计算 {total_stocks} 只个股的竞价因子数据")
            
            for idx, code in enumerate(unique_codes, 1):
                stock_data = df[df['code'] == code].copy()
                stock_data = stock_data.sort_values('trade_date')
                
                # 获取股票名称
                stock_name = stock_data['name'].iloc[-1] if not stock_data.empty else ''
                total_dates = len(stock_data)
                
                # 检查该股票是否已有最新数据
                latest_date = self.get_latest_factor_date(code)
                if latest_date:
                    latest_auction_date = stock_data['trade_date'].max().strftime('%Y-%m-%d')
                    if latest_date >= latest_auction_date:
                        logger.info(f"跳过个股 [{idx}/{total_stocks}] {code} ({stock_name})，已有最新数据 (最新日期: {latest_date})")
                        continue
                else:
                    logger.info(f"处理个股 [{idx}/{total_stocks}] {code} ({stock_name})，全量计算 {total_dates} 个交易日")
                
                if stock_data.empty:
                    continue
                
                factor_list = []
                for date_idx, (i, row) in enumerate(stock_data.iterrows(), 1):
                    trade_date = row['trade_date']
                    
                    # 计算竞价换手率因子
                    # 竞价换手率直接使用竞价数据中的turnover_rate字段
                    bid_ask_turnover_rate = float(row['turnover_rate']) if pd.notna(row['turnover_rate']) else 0.0
                    
                    # 构建因子数据
                    factors = {
                        'trade_date': trade_date,
                        'code': code,
                        'name': stock_name,
                        'bid_ask_turnover_rate': bid_ask_turnover_rate
                    }
                    
                    factor_list.append(factors)
                    
                    # 每处理50个交易日打印一次进度
                    if date_idx % 50 == 0 or date_idx == total_dates:
                        logger.info(f"  - 已处理 {date_idx}/{total_dates} 个交易日")
                
                # 每个股票处理完成后立即插入数据库
                if factor_list:
                    # 转换为DataFrame
                    stock_factor_df = pd.DataFrame(factor_list)
                    
                    # 数据类型转换和清洗
                    stock_factor_df['trade_date'] = pd.to_datetime(stock_factor_df['trade_date'])
                    
                    # 数值列填充NaN为0
                    stock_factor_df['bid_ask_turnover_rate'] = pd.to_numeric(
                        stock_factor_df['bid_ask_turnover_rate'], errors='coerce'
                    ).fillna(0)
                    
                    # 字符串列填充
                    stock_factor_df['name'] = stock_factor_df['name'].fillna('')
                    
                    # 立即插入数据库
                    success = self.insert_factor_data(stock_factor_df)
                    if success:
                        logger.info(f"个股 {code} 处理完成，已插入 {len(stock_factor_df)} 条因子数据")
                        all_factor_list.extend(factor_list)
                    else:
                        logger.error(f"个股 {code} 数据插入失败")
                else:
                    logger.info(f"个股 {code} 无新数据需要处理")
            
            # 返回所有处理的因子数据
            if all_factor_list:
                final_factor_df = pd.DataFrame(all_factor_list)
                final_factor_df['trade_date'] = pd.to_datetime(final_factor_df['trade_date'])
                logger.info(f"所有个股处理完成，共计算 {len(final_factor_df)} 条个股竞价因子数据")
                return final_factor_df
            else:
                logger.info("所有个股均已是最新数据，无需处理")
                return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"计算个股竞价因子失败: {e}")
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
                # 构建插入SQL - 使用ON DUPLICATE KEY UPDATE更新bid_ask_turnover_rate字段
                sql = """
                INSERT INTO trade_factor_stock (
                    trade_date, code, name, bid_ask_turnover_rate
                ) VALUES (
                    %s, %s, %s, %s
                ) ON DUPLICATE KEY UPDATE
                    name = VALUES(name),
                    bid_ask_turnover_rate = VALUES(bid_ask_turnover_rate),
                    updated_time = CURRENT_TIMESTAMP
                """
                
                # 准备数据
                data_list = []
                for _, row in df.iterrows():
                    data_tuple = (
                        row['trade_date'].strftime('%Y-%m-%d'),
                        row['code'],
                        row['name'],
                        row['bid_ask_turnover_rate']
                    )
                    data_list.append(data_tuple)
                
                # 批量插入
                cursor.executemany(sql, data_list)
                
                logger.info(f"成功插入 {len(data_list)} 条个股竞价因子数据")
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
            logger.info("开始更新个股竞价因子数据")
            
            # 获取竞价数据
            df = self.get_auction_data(start_date, end_date)
            if df.empty:
                logger.warning("没有竞价数据可供更新")
                return True
            
            # 计算因子
            factor_df = self.calculate_auction_factors(df)
            
            if not factor_df.empty:
                logger.info(f"成功更新 {len(factor_df)} 条个股竞价因子数据")
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
    calculator = StockAuctionFactorCalculator()
    try:
        # 更新所有个股竞价因子数据
        success = calculator.update_factor_data()
        if success:
            logger.info("个股竞价因子计算完成")
        else:
            logger.error("个股竞价因子计算失败")
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
    finally:
        calculator._close_database()


if __name__ == "__main__":
    main()