#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
指数日线行情数据清洗模块
从Tushare获取指数日线行情数据并写入数据库
"""

import sys
import os
import logging
import argparse
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

import pandas as pd
import pymysql
import tushare as ts
from config import config

# 创建logs目录
logs_dir = os.path.join(project_root, 'logs/tradeDataClean')
os.makedirs(logs_dir, exist_ok=True)

# 配置日志 - 输出到文件和控制台
log_filename = os.path.join(logs_dir, f'index_daily_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class IndexDailyCleaner:
    """指数日线行情数据清洗器"""
    
    def __init__(self, days: int = 120):
        self.db_config = config.database
        self.tushare_token = config.tushare.token
        self.connection = None
        self.tushare_api = None
        self.days = days
        
        # 初始化Tushare API
        self._init_tushare()
        
        # 初始化数据库连接
        self._init_database()
    
    def _init_tushare(self):
        """初始化Tushare API"""
        try:
            ts.set_token(self.tushare_token)
            self.tushare_api = ts.pro_api()
            logger.info("Tushare API初始化成功")
        except Exception as e:
            logger.error(f"Tushare API初始化失败: {e}")
            raise
    
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
    
    def get_latest_trading_date(self) -> str:
        """从trade_market_calendar表获取最近的交易日期"""
        try:
            with self.connection.cursor() as cursor:
                sql = """
                SELECT cal_date 
                FROM trade_market_calendar 
                WHERE is_open = 1 AND cal_date <= CURDATE()
                ORDER BY cal_date DESC 
                LIMIT 1
                """
                cursor.execute(sql)
                result = cursor.fetchone()
                if result:
                    # 将日期转换为YYYYMMDD格式
                    return result[0].strftime('%Y%m%d')
                else:
                    logger.warning("未找到交易日期，使用当前日期")
                    return datetime.now().strftime('%Y%m%d')
        except Exception as e:
            logger.error(f"获取最近交易日期失败: {e}")
            return datetime.now().strftime('%Y%m%d')
    
    def get_trading_date_range(self) -> tuple:
        """获取交易日期范围
        
        Returns:
            tuple: (start_date, end_date) 格式为YYYYMMDD的字符串
        """
        try:
            end_date = self.get_latest_trading_date()
            
            # 获取指定天数前的交易日
            with self.connection.cursor() as cursor:
                sql = """
                SELECT cal_date 
                FROM trade_market_calendar 
                WHERE is_open = 1 AND cal_date <= STR_TO_DATE(%s, '%%Y%%m%%d')
                ORDER BY cal_date DESC
                LIMIT %s, 1
                """
                cursor.execute(sql, (end_date, self.days))
                result = cursor.fetchone()
                
                if result:
                    start_date = result[0].strftime('%Y%m%d')
                else:
                    # 如果没有找到，使用默认日期范围
                    start_date = (datetime.strptime(end_date, '%Y%m%d') - timedelta(days=self.days)).strftime('%Y%m%d')
                
                logger.info(f"获取交易日期范围: {start_date} - {end_date}")
                return start_date, end_date
                
        except Exception as e:
            logger.error(f"获取交易日期范围失败: {e}")
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=self.days)).strftime('%Y%m%d')
            return start_date, end_date
    
    def fetch_index_codes(self) -> List[str]:
        """从数据库获取需要抓取的指数代码"""
        try:
            with self.connection.cursor() as cursor:
                sql = """
                SELECT ts_code 
                FROM trade_market_index_basic
                """
                cursor.execute(sql)
                results = cursor.fetchall()
                
                if results:
                    index_codes = [result[0] for result in results]
                    logger.info(f"成功获取{len(index_codes)}个指数代码")
                    return index_codes
                else:
                    logger.warning("未获取到指数代码")
                    return []
                    
        except Exception as e:
            logger.error(f"获取指数代码失败: {e}")
            return []
    
    def fetch_index_daily_data(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取单个指数的日线行情数据
        
        Args:
            ts_code: 指数代码
            start_date: 开始日期，格式YYYYMMDD
            end_date: 结束日期，格式YYYYMMDD
            
        Returns:
            pd.DataFrame: 指数日线行情数据
        """
        try:
            logger.info(f"开始获取指数 {ts_code} 的日线行情数据，日期范围: {start_date} - {end_date}")
            
            # 调用Tushare的index_daily接口
            df = self.tushare_api.index_daily(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
                fields='ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount'
            )
            
            if df.empty:
                logger.warning(f"未获取到指数 {ts_code} 的日线行情数据")
                return pd.DataFrame()
            
            logger.info(f"成功获取指数 {ts_code} 的{len(df)}条日线行情数据")
            return df
            
        except Exception as e:
            logger.error(f"获取指数 {ts_code} 的日线行情数据失败: {e}")
            return pd.DataFrame()
    
    def clean_index_daily_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """清洗指数日线行情数据
        
        Args:
            df: 原始数据
            
        Returns:
            pd.DataFrame: 清洗后的数据
        """
        if df.empty:
            return df
        
        try:
            # 重命名列以匹配数据库表结构
            df_cleaned = df.rename(columns={
                'ts_code': 'code',
                'trade_date': 'trade_date',
                'open': 'open',
                'high': 'high',
                'low': 'low',
                'close': 'close',
                'pre_close': 'pre_close',
                'change': 'chg',
                'pct_chg': 'pct_chg',
                'vol': 'vol',
                'amount': 'amount'
            })
            
            # 转换日期格式
            df_cleaned['trade_date'] = pd.to_datetime(df_cleaned['trade_date'], format='%Y%m%d').dt.date
            
            # 处理空值和数据类型
            numeric_columns = ['open', 'high', 'low', 'close', 'pre_close', 'chg', 'pct_chg', 'amount']
            for col in numeric_columns:
                df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors='coerce')
            
            # 处理成交量（转换为整数）
            df_cleaned['vol'] = pd.to_numeric(df_cleaned['vol'], errors='coerce').fillna(0).astype('int64')
            
            # 去除重复数据
            df_cleaned = df_cleaned.drop_duplicates(subset=['trade_date', 'code'])
            
            logger.info(f"数据清洗完成，清洗后数据量: {len(df_cleaned)}")
            return df_cleaned
            
        except Exception as e:
            logger.error(f"数据清洗失败: {e}")
            return pd.DataFrame()
    
    def insert_index_daily_data(self, df: pd.DataFrame) -> bool:
        """将指数日线行情数据插入数据库
        
        Args:
            df: 清洗后的数据
            
        Returns:
            bool: 是否成功
        """
        if df.empty:
            logger.warning("没有数据需要插入")
            return False
        
        try:
            with self.connection.cursor() as cursor:
                # 构建插入SQL
                sql = """
                INSERT INTO trade_market_index_daily 
                (trade_date, code, open, high, low, close, pre_close, chg, pct_chg, vol, amount)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                open = VALUES(open),
                high = VALUES(high),
                low = VALUES(low),
                close = VALUES(close),
                pre_close = VALUES(pre_close),
                chg = VALUES(chg),
                pct_chg = VALUES(pct_chg),
                vol = VALUES(vol),
                amount = VALUES(amount),
                updated_time = CURRENT_TIMESTAMP
                """
                
                # 准备数据
                data_list = []
                for _, row in df.iterrows():
                    data_list.append((
                        row['trade_date'],
                        row['code'],
                        row['open'],
                        row['high'],
                        row['low'],
                        row['close'],
                        row['pre_close'],
                        row['chg'],
                        row['pct_chg'],
                        row['vol'],
                        row['amount']
                    ))
                
                # 批量插入
                cursor.executemany(sql, data_list)
                
                logger.info(f"成功插入{len(data_list)}条指数日线行情数据到数据库")
                return True
                
        except Exception as e:
            logger.error(f"插入指数日线行情数据失败: {e}")
            return False
    
    def update_index_daily_data(self) -> bool:
        """更新指数日线行情数据
        
        Returns:
            bool: 是否成功
        """
        try:
            logger.info("开始更新指数日线行情数据")
            
            # 获取交易日期范围
            start_date, end_date = self.get_trading_date_range()
            
            # 获取需要抓取的指数代码
            index_codes = self.fetch_index_codes()
            
            if not index_codes:
                logger.error("未获取到指数代码，无法更新数据")
                return False
            
            total_count = 0
            success_count = 0
            
            # 循环获取每个指数的日线行情数据
            for ts_code in index_codes:
                try:
                    # 获取单个指数的日线行情数据
                    df = self.fetch_index_daily_data(ts_code, start_date, end_date)
                    
                    if not df.empty:
                        # 清洗数据
                        df_cleaned = self.clean_index_daily_data(df)
                        
                        if not df_cleaned.empty:
                            # 插入数据库
                            success = self.insert_index_daily_data(df_cleaned)
                            
                            if success:
                                total_count += len(df_cleaned)
                                success_count += 1
                                logger.info(f"指数 {ts_code} 的日线行情数据已成功入库，共{len(df_cleaned)}条")
                            else:
                                logger.error(f"指数 {ts_code} 的日线行情数据入库失败")
                        else:
                            logger.warning(f"指数 {ts_code} 的日线行情数据清洗后为空")
                    else:
                        logger.warning(f"未获取到指数 {ts_code} 的日线行情数据")
                        
                except Exception as e:
                    logger.error(f"处理指数 {ts_code} 的日线行情数据失败: {e}")
                    continue
            
            logger.info(f"指数日线行情数据更新完成，共处理{len(index_codes)}个指数，成功{success_count}个，总共入库{total_count}条数据")
            return success_count > 0
                
        except Exception as e:
            logger.error(f"更新指数日线行情数据失败: {e}")
            return False
    
    def close(self):
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()
            logger.info("数据库连接已关闭")


def main():
    """主函数"""
    
    cleaner = None
    try:
        # 创建清洗器实例
        cleaner = IndexDailyCleaner(days=120)
        
        success = cleaner.update_index_daily_data()
        
        if success:
            logger.info("指数日线行情数据处理完成")
            sys.exit(0)
        else:
            logger.error("指数日线行情数据处理失败")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        sys.exit(1)
    finally:
        if cleaner:
            cleaner.close()


if __name__ == '__main__':
    main()