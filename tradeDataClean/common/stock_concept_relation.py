#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票概念关联数据清洗模块
从Tushare获取概念板块成分数据并写入数据库
"""

import tushare as ts
import pandas as pd
import pymysql
from datetime import datetime
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


class StockConceptRelationCleaner:
    """
    股票概念关联数据清洗器
    
    负责从Tushare获取概念板块成分数据，清洗后写入数据库
    """
    
    def __init__(self, db_config: DatabaseConfig = None):
        """
        初始化清洗器
        
        Args:
            db_config: 数据库配置，如果为None则使用默认配置
        """
        self.db_config = db_config or config.database
        self.pro = None
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
        
        Returns:
            数据库连接对象
        """
        try:
            connection = pymysql.connect(
                host=self.db_config.host,
                port=self.db_config.port,
                user=self.db_config.user,
                password=self.db_config.password,
                database=self.db_config.database,
                charset='utf8mb4',
                autocommit=False
            )
            logger.info("数据库连接成功")
            return connection
        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
            raise
    
    def _close_db_connection(self):
        """
        关闭数据库连接
        """
        if hasattr(self, 'connection') and self.connection:
            self.connection.close()
            logger.info("数据库连接已关闭")
    
    def get_latest_trading_date(self) -> str:
        """
        从交易日历表获取距今日最近的交易日期
        
        Returns:
            最近交易日期，格式为YYYYMMDD
        """
        connection = None
        try:
            connection = self._get_db_connection()
            cursor = connection.cursor()
            
            # 查询距今日最近的交易日期
            query = """
            SELECT cal_date 
            FROM trade_market_calendar 
            WHERE is_open = 1 AND cal_date <= CURDATE()
            ORDER BY cal_date DESC 
            LIMIT 1
            """
            
            cursor.execute(query)
            result = cursor.fetchone()
            
            if result and result[0]:
                latest_date = result[0].strftime('%Y%m%d')
                logger.info(f"获取到最近交易日期: {latest_date}")
                return latest_date
            else:
                # 如果没有找到，使用当前日期
                latest_date = datetime.now().strftime('%Y%m%d')
                logger.warning(f"未找到交易日期数据，使用当前日期: {latest_date}")
                return latest_date
                
        except Exception as e:
            logger.error(f"获取最近交易日期失败: {e}")
            # 返回当前日期作为备选
            return datetime.now().strftime('%Y%m%d')
        finally:
            if connection:
                connection.close()
    
    def get_all_concept_codes(self) -> List[str]:
        """
        获取所有概念板块代码
        
        Returns:
            概念板块代码列表
        """
        try:
            # 获取概念板块基础信息
            df = self.pro.ths_index(exchange='A', type='N')
            if df.empty:
                logger.warning("未获取到概念板块信息")
                return []
            
            concept_codes = df['ts_code'].tolist()
            logger.info(f"获取到 {len(concept_codes)} 个概念板块代码")
            return concept_codes
            
        except Exception as e:
            logger.error(f"获取概念板块代码失败: {e}")
            return []
    
    def fetch_concept_member_data(self) -> pd.DataFrame:
        """
        从Tushare获取概念板块成分数据
            
        Returns:
            包含概念板块成分数据的DataFrame
        """
        try:
            # 获取最近的交易日期
            trade_date = self.get_latest_trading_date()
            
            df = self.pro.dc_member(trade_date=trade_date)
            
            if df.empty:
                logger.warning(f"未获取到概念板块成分数据")
                return pd.DataFrame()
            
            logger.info(f"成功获取 {len(df)} 条概念板块成分记录")
            return df
            
        except Exception as e:
            logger.error(f"获取概念板块成分数据失败: {e}")
            return pd.DataFrame()
    
    def clean_concept_relation_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        清洗概念关联数据
        
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
                'ts_code': 'concept_sector_code',  # 概念板块代码
                'con_code': 'stock_code',          # 股票代码
            }
            
            # 重命名列
            df_cleaned = df_cleaned.rename(columns=column_mapping)
            
            # 只保留需要的列（匹配数据库表结构）
            required_columns = ['concept_sector_code', 'stock_code']
            df_cleaned = df_cleaned[required_columns]
            
            # 数据清洗
            # 去除空值
            df_cleaned = df_cleaned.dropna()
            
            # 去除重复记录
            df_cleaned = df_cleaned.drop_duplicates()
            
            # 数据类型转换
            df_cleaned['concept_sector_code'] = df_cleaned['concept_sector_code'].astype(str)
            df_cleaned['stock_code'] = df_cleaned['stock_code'].astype(str)
            
            logger.info(f"数据清洗完成，共 {len(df_cleaned)} 条记录")
            
            return df_cleaned
            
        except Exception as e:
            logger.error(f"数据清洗失败: {e}")
            raise
    
    def insert_concept_relation_data(self, df: pd.DataFrame, batch_size: int = 1000):
        """
        将概念关联数据批量插入数据库
        
        Args:
            df: 清洗后的数据DataFrame
            batch_size: 批量插入的大小
        """
        if df.empty:
            logger.warning("没有数据需要插入")
            return
        
        connection = None
        try:
            connection = self._get_db_connection()
            cursor = connection.cursor()
            
            # 清空现有数据（可选，根据业务需求决定）
            # cursor.execute("DELETE FROM trade_stock_concept_relation")
            
            # 准备插入SQL
            insert_sql = """
            INSERT IGNORE INTO trade_stock_concept_relation 
            (concept_sector_code, stock_code) 
            VALUES (%s, %s)
            """
            
            # 批量插入数据
            total_rows = len(df)
            inserted_count = 0
            
            for i in range(0, total_rows, batch_size):
                batch_df = df.iloc[i:i + batch_size]
                batch_data = [
                    (row['concept_sector_code'], row['stock_code'])
                    for _, row in batch_df.iterrows()
                ]
                
                cursor.executemany(insert_sql, batch_data)
                connection.commit()
                
                inserted_count += len(batch_data)
                logger.info(f"已插入 {inserted_count}/{total_rows} 条记录")
            
            logger.info(f"概念关联数据插入完成，共插入 {inserted_count} 条记录")
            
        except Exception as e:
            if connection:
                connection.rollback()
            logger.error(f"插入概念关联数据失败: {e}")
            raise
        finally:
            if connection:
                connection.close()
    
    def update_concept_relation_data(self):
        """
        更新概念关联数据的主方法
        """
        try:
            logger.info(f"开始更新概念关联数据")
            
            # 直接获取概念板块成分数据
            df = self.fetch_concept_member_data()
            
            if df.empty:
                return
            
            # 清洗数据
            cleaned_df = self.clean_concept_relation_data(df)
            
            # 插入数据库
            self.insert_concept_relation_data(cleaned_df)
            
            logger.info(f"概念关联数据更新完成，共插入 {len(cleaned_df)} 条记录")
            
        except Exception as e:
            logger.error(f"更新概念关联数据失败: {e}")
            raise
    
    def get_concept_stocks(self, concept_code: str) -> List[str]:
        """
        查询指定概念板块的成分股
        
        Args:
            concept_code: 概念板块代码
            
        Returns:
            股票代码列表
        """
        connection = None
        try:
            connection = self._get_db_connection()
            cursor = connection.cursor()
            
            query = """
            SELECT stock_code 
            FROM trade_stock_concept_relation 
            WHERE concept_sector_code = %s
            """
            
            cursor.execute(query, (concept_code,))
            results = cursor.fetchall()
            
            stock_codes = [row[0] for row in results]
            logger.info(f"概念板块 {concept_code} 包含 {len(stock_codes)} 只股票")
            
            return stock_codes
            
        except Exception as e:
            logger.error(f"查询概念板块成分股失败: {e}")
            return []
        finally:
            if connection:
                connection.close()


def main():
    """
    主函数
    """
    import sys
    
    try:
        # 创建清洗器实例
        cleaner = StockConceptRelationCleaner()
        
        logger.info("开始获取所有概念板块数据")
        cleaner.update_concept_relation_data()
        
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        raise


if __name__ == '__main__':
    main()