#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
交易日历数据清洗模块
基于Tushare接口获取交易日历数据并写入数据库
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

# 添加项目根目录到路径以便导入配置
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)
from config import config, DatabaseConfig

# 创建logs目录
logs_dir = os.path.join(project_root, 'logs/tradeDataClean')
os.makedirs(logs_dir, exist_ok=True)

# 配置日志 - 输出到文件和控制台
log_filename = os.path.join(logs_dir, f'trading_calendar_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class TradingCalendarCleaner:
    """
    交易日历数据清洗器
    
    功能：
    1. 从Tushare获取交易日历数据
    2. 数据清洗和格式化
    3. 写入MySQL数据库
    """
    
    def __init__(self, db_config: DatabaseConfig = None):
        """
        初始化交易日历清洗器
        
        Args:
            db_config: 数据库配置，如果不提供则使用全局配置
        """
        self.tushare_token = config.tushare.token
        self.db_config = db_config or config.database
        self.pro = None
        self.connection = None
        
        # 初始化Tushare
        self._init_tushare()
    
    def _init_tushare(self):
        """初始化Tushare连接"""
        try:
            ts.set_token(self.tushare_token)
            self.pro = ts.pro_api()
            logger.info("Tushare API初始化成功")
        except Exception as e:
            logger.error(f"Tushare API初始化失败: {e}")
            raise
    
    def _get_db_connection(self):
        """获取数据库连接"""
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
            logger.info("数据库连接成功")
            return self.connection
        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
            raise
    
    def _close_db_connection(self):
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()
            logger.info("数据库连接已关闭")
    
    def fetch_trading_calendar(
        self, 
        exchange: str = 'SSE',
        start_date: str = None,
        end_date: str = None,
        is_open: str = None
    ) -> pd.DataFrame:
        """
        从Tushare获取交易日历数据
        
        Args:
            exchange: 交易所代码 (SSE上交所, SZSE深交所, CFFEX中金所等)
            start_date: 开始日期 (格式: YYYYMMDD)
            end_date: 结束日期 (格式: YYYYMMDD)
            is_open: 是否交易 ('0'休市, '1'交易)
            
        Returns:
            交易日历数据DataFrame
        """
        try:
            logger.info(f"开始获取交易日历数据: {exchange}, {start_date} - {end_date}")
            
            # 调用Tushare接口
            df = self.pro.trade_cal(
                exchange=exchange,
                start_date=start_date,
                end_date=end_date,
                is_open=is_open
            )
            
            logger.info(f"成功获取 {len(df)} 条交易日历记录")
            return df
            
        except Exception as e:
            logger.error(f"获取交易日历数据失败: {e}")
            raise
    
    def clean_calendar_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        清洗交易日历数据
        
        Args:
            df: 原始交易日历数据
            
        Returns:
            清洗后的数据
        """
        try:
            logger.info("开始清洗交易日历数据")
            
            # 创建副本避免修改原数据
            cleaned_df = df.copy()
            
            # 1. 日期格式转换
            cleaned_df['cal_date'] = pd.to_datetime(cleaned_df['cal_date'], format='%Y%m%d')
            
            # 2. 数据类型转换
            cleaned_df['is_open'] = cleaned_df['is_open'].astype(int)
            
            # 3. 去重
            cleaned_df = cleaned_df.drop_duplicates(subset=['cal_date'], keep='last')
            
            # 4. 排序
            cleaned_df = cleaned_df.sort_values('cal_date')
            
            # 5. 重置索引
            cleaned_df = cleaned_df.reset_index(drop=True)
            
            # 6. 选择需要的列
            cleaned_df = cleaned_df[['cal_date', 'is_open']]
            
            logger.info(f"数据清洗完成，共 {len(cleaned_df)} 条记录")
            return cleaned_df
            
        except Exception as e:
            logger.error(f"数据清洗失败: {e}")
            raise
    
    def insert_calendar_data(self, df: pd.DataFrame, batch_size: int = 1000):
        """
        批量插入交易日历数据到数据库
        
        Args:
            df: 清洗后的交易日历数据
            batch_size: 批量插入大小
        """
        try:
            logger.info(f"开始插入 {len(df)} 条交易日历数据")
            
            # 使用ON DUPLICATE KEY UPDATE处理重复数据
            insert_sql = """
            INSERT INTO trade_market_calendar (cal_date, is_open)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE
                is_open = VALUES(is_open),
                updated_time = CURRENT_TIMESTAMP
            """
            
            # 准备数据
            data_list = []
            for _, row in df.iterrows():
                data_list.append((
                    row['cal_date'].strftime('%Y-%m-%d'),
                    int(row['is_open'])
                ))
            
            # 批量插入
            with self.connection.cursor() as cursor:
                total_inserted = 0
                
                for i in range(0, len(data_list), batch_size):
                    batch_data = data_list[i:i + batch_size]
                    cursor.executemany(insert_sql, batch_data)
                    total_inserted += len(batch_data)
                    
                    if i % (batch_size * 10) == 0:  # 每10个批次提交一次
                        self.connection.commit()
                        logger.info(f"已插入 {total_inserted} 条记录")
                
                # 最终提交
                self.connection.commit()
                logger.info(f"交易日历数据插入完成，共 {total_inserted} 条记录")
                
        except Exception as e:
            logger.error(f"插入数据失败: {e}")
            if self.connection:
                self.connection.rollback()
            raise
    
    def get_date_range_for_update(self, years_back: int = 5) -> tuple:
        """
        获取需要更新的日期范围
        
        Args:
            years_back: 向前追溯的年数
            
        Returns:
            (start_date, end_date) 格式为 YYYYMMDD
        """
        # 固定结束时间为2027-01-01
        end_date = datetime(2027, 1, 1)
        # 固定开始时间为2025-01-01
        start_date = datetime(2025, 4, 1)
        
        return (
            start_date.strftime('%Y%m%d'),
            end_date.strftime('%Y%m%d')
        )
    
    def update_trading_calendar(
        self,
        exchange: str = 'SSE',
        start_date: str = None,
        end_date: str = None,
        years_back: int = 5
    ):
        """
        更新交易日历数据的主方法
        
        Args:
            exchange: 交易所代码
            start_date: 开始日期
            end_date: 结束日期
            years_back: 如果未指定日期范围，向前追溯的年数
        """
        try:
            # 获取数据库连接
            self._get_db_connection()
            
            # 确定日期范围
            if not start_date or not end_date:
                start_date, end_date = self.get_date_range_for_update(years_back)
            
            # 获取交易日历数据
            df = self.fetch_trading_calendar(
                exchange=exchange,
                start_date=start_date,
                end_date=end_date
            )
            
            if df.empty:
                logger.warning("未获取到交易日历数据")
                return
            
            # 清洗数据
            cleaned_df = self.clean_calendar_data(df)
            
            # 插入数据库
            self.insert_calendar_data(cleaned_df)
            
            logger.info("交易日历数据更新完成")
            
        except Exception as e:
            logger.error(f"更新交易日历数据失败: {e}")
            raise
        finally:
            # 关闭数据库连接
            self._close_db_connection()
    
    def get_trading_days(
        self,
        start_date: str,
        end_date: str,
        exchange: str = 'SSE'
    ) -> List[str]:
        """
        获取指定日期范围内的交易日
        
        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            exchange: 交易所代码
            
        Returns:
            交易日列表
        """
        try:
            self._get_db_connection()
            
            query_sql = """
            SELECT cal_date FROM trade_market_calendar
            WHERE cal_date BETWEEN %s AND %s
            AND is_open = 1
            ORDER BY cal_date
            """
            
            with self.connection.cursor() as cursor:
                cursor.execute(query_sql, (start_date, end_date))
                results = cursor.fetchall()
                
            trading_days = [row[0].strftime('%Y-%m-%d') for row in results]
            logger.info(f"获取到 {len(trading_days)} 个交易日")
            
            return trading_days
            
        except Exception as e:
            logger.error(f"获取交易日失败: {e}")
            raise
        finally:
            self._close_db_connection()


def main():
    """
    主函数 - 示例用法
    """
    try:
        # 创建交易日历清洗器（使用全局配置）
        cleaner = TradingCalendarCleaner()
        
        # 更新交易日历数据（最近5年）
        cleaner.update_trading_calendar(
            exchange='SSE',  # 上交所
            years_back=5
        )
        
        # 示例：获取最近30天的交易日
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        trading_days = cleaner.get_trading_days(start_date, end_date)
        logger.info(f"最近30天的交易日: {trading_days}")
        
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        raise


if __name__ == '__main__':
    main()