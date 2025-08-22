#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票题材关联数据清洗模块
从Tushare获取题材成分股数据并写入数据库
"""

import tushare as ts
import pandas as pd
import pymysql
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import logging
import os
from pathlib import Path
import sys

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


class StockThemeRelationCleaner:
    """
    股票题材关联数据清洗器
    
    主要功能：
    1. 从Tushare获取题材成分股数据
    2. 清洗和标准化数据
    3. 写入数据库
    """
    
    def __init__(self, db_config: DatabaseConfig = None):
        """
        初始化清洗器
        
        Args:
            db_config: 数据库配置，如果为None则使用全局配置
        """
        self.db_config = db_config or config.database
        self.connection = None
        self.cursor = None
        
        # 初始化Tushare
        self._init_tushare()
    
    def _init_tushare(self):
        """
        初始化Tushare API
        """
        try:
            ts.set_token(config.tushare.token)
            self.pro = ts.pro_api()
            logger.info("Tushare API初始化成功")
        except Exception as e:
            logger.error(f"Tushare API初始化失败: {e}")
            raise
    
    def _get_db_connection(self):
        """
        获取数据库连接
        """
        try:
            self.connection = pymysql.connect(
                host=self.db_config.host,
                port=self.db_config.port,
                user=self.db_config.user,
                password=self.db_config.password,
                database=self.db_config.database,
                charset=self.db_config.charset,
                autocommit=False
            )
            self.cursor = self.connection.cursor()
            logger.info("数据库连接成功")
        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
            raise
    
    def _close_db_connection(self):
        """
        关闭数据库连接
        """
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("数据库连接已关闭")
    
    def get_trading_date_range(self) -> tuple:
        """
        从交易日历表获取最早和最晚的交易日期
        
        Returns:
            (start_date, end_date) 格式为 YYYYMMDD
        """
        try:
            self._get_db_connection()
            
            # 查询最早和最晚的交易日期
            query = """
            SELECT 
                MIN(cal_date) as min_date,
                MAX(cal_date) as max_date
            FROM trade_market_calendar 
            WHERE is_open = 1
            """
            
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            
            if result and result[0] and result[1]:
                min_date = result[0].strftime('%Y%m%d')
                max_date = result[1].strftime('%Y%m%d')
                logger.info(f"获取交易日期范围: {min_date} - {max_date}")
                return min_date, max_date
            else:
                logger.warning("未找到交易日期数据，使用默认范围")
                # 使用默认范围
                end_date = datetime.now().strftime('%Y%m%d')
                start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
                return start_date, end_date
                
        except Exception as e:
            logger.error(f"获取交易日期范围失败: {e}")
            raise
        finally:
            self._close_db_connection()
    
    def get_latest_trading_date(self) -> str:
        """
        获取距离今日向前最近的交易日期
        
        Returns:
            最近的交易日期 (YYYYMMDD格式)
        """
        try:
            # 获取交易日历数据
            connection = self._get_db_connection()
            cursor = connection.cursor()
            
            # 查询最近的交易日期
            query = """
            SELECT cal_date 
            FROM trade_market_calendar 
            WHERE is_open = 1 AND cal_date <= CURDATE()
            ORDER BY cal_date DESC 
            LIMIT 1
            """
            
            cursor.execute(query)
            result = cursor.fetchone()
            
            if result:
                latest_date = result[0].strftime('%Y%m%d')
                logger.info(f"获取到最近交易日期: {latest_date}")
                return latest_date
            else:
                # 如果没有找到交易日期，使用当前日期的前一天
                yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
                logger.warning(f"未找到交易日期，使用昨日日期: {yesterday}")
                return yesterday
                
        except Exception as e:
            logger.error(f"获取最近交易日期失败: {e}")
            # 返回昨日日期作为备选
            return (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
        finally:
            if 'connection' in locals():
                self._close_db_connection()
    
    def fetch_theme_concept_data(self) -> pd.DataFrame:
        """
        从Tushare获取题材成分股数据
        
        Returns:
            包含题材成分股数据的DataFrame
        """
        try:
            trade_date = self.get_latest_trading_date()
            
            logger.info(f"开始获取题材成分股数据: {trade_date}")
            
            # 调用Tushare接口获取题材成分股数据
            df = self.pro.kpl_concept_cons(trade_date=trade_date)
            
            if df.empty:
                logger.warning(f"日期 {trade_date} 未获取到题材成分股数据")
                return pd.DataFrame()
            
            logger.info(f"成功获取 {len(df)} 条题材成分股记录")
            return df
            
        except Exception as e:
            logger.error(f"获取题材成分股数据失败: {e}")
            return pd.DataFrame()
    
    def clean_theme_relation_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        清洗题材关联数据
        
        Args:
            df: 原始数据DataFrame
            
        Returns:
            清洗后的DataFrame
        """
        if df.empty:
            return df
            
        try:
            # 重命名列名以匹配数据库表结构
            df_cleaned = df.copy()
            
            # 根据API返回的字段进行重命名，匹配数据库表字段
            column_mapping = {
                'ts_code': 'theme_sector_code',  # 题材代码
                'con_code': 'stock_code',             # 股票代码
            }
            
            # 重命名列
            df_cleaned = df_cleaned.rename(columns=column_mapping)
            
            # 只保留需要的列（匹配数据库表结构）
            required_columns = ['theme_sector_code', 'stock_code']
            df_cleaned = df_cleaned[required_columns]
            
            # 数据清洗
            # 去除空值
            df_cleaned = df_cleaned.dropna()
            
            # 去除重复记录
            df_cleaned = df_cleaned.drop_duplicates()
            
            # 数据类型转换
            df_cleaned['theme_sector_code'] = df_cleaned['theme_sector_code'].astype(str)
            df_cleaned['stock_code'] = df_cleaned['stock_code'].astype(str)
            
            logger.info(f"数据清洗完成，共 {len(df_cleaned)} 条记录")
            
            return df_cleaned
            
        except Exception as e:
            logger.error(f"数据清洗失败: {e}")
            raise
    
    def insert_theme_relation_data(self, df: pd.DataFrame, batch_size: int = 1000):
        """
        批量插入题材关联数据到数据库
        
        Args:
            df: 要插入的数据DataFrame
            batch_size: 批量插入的大小
        """
        if df.empty:
            logger.warning("没有数据需要插入")
            return
        
        try:
            self._get_db_connection()
            
            # 清空现有数据（可选，根据业务需求决定）
            # self.cursor.execute("DELETE FROM trade_stock_theme_relation")
            # logger.info("已清空现有题材关联数据")
            
            # 准备插入SQL
            insert_sql = """
            INSERT IGNORE INTO trade_stock_theme_relation 
            (theme_sector_code, stock_code) 
            VALUES (%s, %s)
            """
            
            # 转换数据为元组列表
            data_tuples = [
                (row['theme_sector_code'], row['stock_code'])
                for _, row in df.iterrows()
            ]
            
            logger.info(f"开始插入 {len(data_tuples)} 条题材关联数据")
            
            # 批量插入
            total_inserted = 0
            for i in range(0, len(data_tuples), batch_size):
                batch_data = data_tuples[i:i + batch_size]
                
                self.cursor.executemany(insert_sql, batch_data)
                self.connection.commit()
                
                total_inserted += len(batch_data)
                logger.info(f"已插入 {total_inserted} 条记录")
            
            logger.info(f"题材关联数据插入完成，共 {total_inserted} 条记录")
            
        except Exception as e:
            if self.connection:
                self.connection.rollback()
            logger.error(f"插入题材关联数据失败: {e}")
            raise
        finally:
            self._close_db_connection()
    
    def update_theme_relation_data(self):
        """
        更新题材关联数据的主方法
        使用距离今日向前最近的交易日期获取数据
        """
        try:
            logger.info(f"开始更新题材关联数据")
            
            # 获取题材成分股数据（使用最近的交易日期）
            df = self.fetch_theme_concept_data()
            
            if df.empty:
                logger.warning("未获取到题材成分股数据")
                return
            
            # 清洗数据
            cleaned_df = self.clean_theme_relation_data(df)
            
            # 插入数据库
            self.insert_theme_relation_data(cleaned_df)
            
            logger.info("题材关联数据更新完成")
            
        except Exception as e:
            logger.error(f"更新题材关联数据失败: {e}")
            raise
    
    def get_theme_stocks(self, theme_code: str) -> List[str]:
        """
        获取指定题材的成分股列表
        
        Args:
            theme_code: 题材代码
            
        Returns:
            股票代码列表
        """
        try:
            self._get_db_connection()
            
            query = """
            SELECT stock_code 
            FROM trade_stock_theme_relation 
            WHERE theme_sector_code = %s
            ORDER BY stock_code
            """
            
            self.cursor.execute(query, (theme_code,))
            results = self.cursor.fetchall()
            
            stock_codes = [row[0] for row in results]
            logger.info(f"题材 {theme_code} 包含 {len(stock_codes)} 只股票")
            
            return stock_codes
            
        except Exception as e:
            logger.error(f"获取题材成分股失败: {e}")
            return []
        finally:
            self._close_db_connection()


def main():
    """
    主函数 - 示例用法
    """
    try:
        # 创建题材关联数据清洗器
        cleaner = StockThemeRelationCleaner()
        
        # 更新题材关联数据
        cleaner.update_theme_relation_data()
        
        # 示例：获取指定题材的成分股
        # theme_stocks = cleaner.get_theme_stocks('000111.KP')
        # logger.info(f"示例题材成分股: {theme_stocks[:10]}")
        
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        raise


if __name__ == '__main__':
    main()