#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票60分钟行情数据清洗模块
从Tushare获取股票60分钟行情数据并写入数据库
"""

import sys
import os
import logging
import argparse
import time
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import pandas as pd
import pymysql
import tushare as ts
from config import config

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class Stock60MinCleaner:
    """股票60分钟行情数据清洗器"""
    
    def __init__(self):
        """初始化"""
        self.db_config = config.database
        self.tushare_token = config.tushare.token
        self.connection = None
        self.tushare_api = None
        self.stock_basic_cache = None  # 缓存股票基础信息
        
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
        """获取交易日期范围，end_date为距离今日最近的交易时间，start_date为今日-30天"""
        try:
            with self.connection.cursor() as cursor:
                # 获取距离今日最近的交易时间作为end_date
                today = datetime.now().strftime('%Y%m%d')
                sql_end = """
                SELECT MAX(cal_date) as end_date
                FROM trade_market_calendar 
                WHERE is_open = 1 AND cal_date <= %s
                """
                cursor.execute(sql_end, (today,))
                end_result = cursor.fetchone()
                
                if end_result and end_result[0]:
                    end_date = end_result[0].strftime('%Y%m%d')
                else:
                    # 如果没有找到距离今日最近的交易日，使用今日
                    end_date = today
                
                # 计算今日-30天作为start_date
                start_date_obj = datetime.now() - timedelta(days=30)
                start_date = start_date_obj.strftime('%Y%m%d')
                
                return start_date, end_date
        except Exception as e:
            logger.error(f"获取交易日期范围失败: {e}")
            today = datetime.now().strftime('%Y%m%d')
            start_date_obj = datetime.now() - timedelta(days=30)
            start_date = start_date_obj.strftime('%Y%m%d')
            return start_date, today
    
    def fetch_stock_basic(self) -> pd.DataFrame:
        """获取股票基础信息（主要用于获取股票名称）"""
        if self.stock_basic_cache is not None:
            return self.stock_basic_cache
        
        try:
            logger.info("开始获取股票基础信息")
            
            # 调用Tushare的stock_basic接口
            df = self.tushare_api.stock_basic(
                list_status='L',
                fields='ts_code,name'
            )
            
            if df.empty:
                logger.warning("未获取到股票基础信息")
                return pd.DataFrame()
            
            # 缓存结果
            self.stock_basic_cache = df
            logger.info(f"成功获取到{len(df)}只股票的基础信息")
            return df
            
        except Exception as e:
            logger.error(f"获取股票基础信息失败: {e}")
            return pd.DataFrame()
    
    def fetch_60min_data_range(self, start_date: str, end_date: str) -> pd.DataFrame:
        """获取指定日期范围的股票60分钟行情数据（按股票代码循环获取并立即入库）"""
        try:
            logger.info(f"开始获取股票60分钟行情数据，日期范围: {start_date} - {end_date}")
            
            # 获取股票基础信息
            df_basic = self.fetch_stock_basic()
            if df_basic.empty:
                logger.error("无法获取股票基础信息")
                return pd.DataFrame()
            
            total_count = 0
            request_count = 0  # 请求计数器
            start_time = time.time()  # 开始时间
            
            # 按股票代码循环获取数据并立即入库
            for _, stock_row in df_basic.iterrows():
                ts_code = stock_row['ts_code']
                stock_name = stock_row['name']
                
                try:
                    # 检查当前股票的最新结束时间是否已经是最新的
                    if self._check_stock_latest_time(ts_code, end_date):
                        logger.info(f"{ts_code}数据已是最新，跳过获取")
                        continue
                    
                    # 请求频率控制：每分钟不超过500次请求
                    current_time = time.time()
                    elapsed_time = current_time - start_time
                    
                    # 如果在1分钟内请求次数达到480次（留一些余量），则等待
                    if elapsed_time < 60 and request_count >= 480:
                        wait_time = 60 - elapsed_time + 1  # 等待到下一分钟
                        logger.info(f"请求频率限制，等待{wait_time:.1f}秒")
                        time.sleep(wait_time)
                        # 重置计数器和时间
                        request_count = 0
                        start_time = time.time()
                    elif elapsed_time >= 60:
                        # 超过1分钟，重置计数器和时间
                        request_count = 0
                        start_time = time.time()
                    
                    logger.info(f"正在获取{ts_code}({stock_name})的60分钟行情数据")
                    
                    # 构建开始和结束时间
                    start_datetime = f"{start_date} 09:00:00"
                    end_datetime = f"{end_date} 15:00:00"
                    
                    # 调用Tushare的stk_mins接口获取60分钟数据
                    df_60min = self.tushare_api.stk_mins(
                        ts_code=ts_code,
                        freq='60min',
                        start_date=start_datetime,
                        end_date=end_datetime
                    )
                    request_count += 1  # 增加请求计数
                    
                    # 每次请求后稍微延迟，避免请求过于频繁
                    time.sleep(0.1)
                    
                    if not df_60min.empty:
                        # 按日期分组处理数据
                        df_60min['trade_date_str'] = pd.to_datetime(df_60min['trade_time']).dt.strftime('%Y%m%d')
                        
                        for trade_date_str, group_df in df_60min.groupby('trade_date_str'):
                            # 清洗数据
                            df_cleaned = self.clean_60min_data(group_df, stock_name, trade_date_str)
                            
                            if not df_cleaned.empty:
                                # 立即入库
                                success = self.insert_60min_data(df_cleaned)
                                if success:
                                    total_count += len(df_cleaned)
                                else:
                                    logger.error(f"{ts_code}在{trade_date_str}的60分钟数据入库失败")
                    else:
                        logger.info(f"{ts_code}在指定时间范围内无60分钟数据")
                        
                except Exception as e:
                    logger.error(f"处理{ts_code}的60分钟数据失败: {e}")
                    continue
            
            logger.info(f"日期范围{start_date}-{end_date}处理完成，总共入库{total_count}条数据")
            # 返回一个包含总数的DataFrame，用于兼容原有接口
            return pd.DataFrame({'total_count': [total_count]})
            
        except Exception as e:
            logger.error(f"获取股票60分钟行情数据失败: {e}")
            return pd.DataFrame()
    
    def _get_trading_dates_in_range(self, start_date: str, end_date: str) -> List[str]:
        """获取指定日期范围内的所有交易日"""
        try:
            with self.connection.cursor() as cursor:
                sql = """
                SELECT cal_date 
                FROM trade_market_calendar 
                WHERE is_open = 1 
                AND cal_date >= STR_TO_DATE(%s, '%%Y%%m%%d')
                AND cal_date <= STR_TO_DATE(%s, '%%Y%%m%%d')
                ORDER BY cal_date
                """
                cursor.execute(sql, (start_date, end_date))
                results = cursor.fetchall()
                
                if results:
                    trading_dates = [result[0].strftime('%Y%m%d') for result in results]
                    logger.info(f"在{start_date}-{end_date}范围内找到{len(trading_dates)}个交易日")
                    return trading_dates
                else:
                    logger.warning(f"在{start_date}-{end_date}范围内未找到交易日")
                    return []
                    
        except Exception as e:
            logger.error(f"获取交易日期失败: {e}")
            return []
    
    def _check_date_exists(self, trade_date: str) -> bool:
        """检查指定交易日期是否已存在于数据库中"""
        try:
            with self.connection.cursor() as cursor:
                sql = """
                SELECT COUNT(*) 
                FROM trade_market_stock_60min 
                WHERE trade_date = STR_TO_DATE(%s, '%%Y%%m%%d')
                """
                cursor.execute(sql, (trade_date,))
                result = cursor.fetchone()
                
                if result and result[0] > 0:
                    return True
                else:
                    return False
                    
        except Exception as e:
            logger.error(f"检查日期{trade_date}是否存在失败: {e}")
            return False
    
    def _check_stock_latest_time(self, ts_code: str, end_date: str) -> bool:
        """检查当前股票的最新结束时间是否已经是最新的结束时间"""
        try:
            with self.connection.cursor() as cursor:
                sql = """
                SELECT MAX(trade_date) as latest_date
                FROM trade_market_stock_60min 
                WHERE code = %s
                """
                cursor.execute(sql, (ts_code,))
                result = cursor.fetchone()
                
                if result and result[0]:
                    latest_date_str = result[0].strftime('%Y%m%d')
                    # 如果数据库中的最新日期等于或大于结束日期，则跳过
                    if latest_date_str >= end_date:
                        return True
                    else:
                        return False
                else:
                    # 如果没有找到该股票的数据，则不跳过
                    return False
                    
        except Exception as e:
            logger.error(f"检查股票{ts_code}最新时间失败: {e}")
            return False
    
    def clean_60min_data(self, df_60min: pd.DataFrame, stock_name: str, trade_date: str) -> pd.DataFrame:
        """清洗股票60分钟行情数据"""
        if df_60min.empty:
            return df_60min
        
        try:
            # 复制数据
            df_cleaned = df_60min.copy()
            
            # 添加股票名称和交易日期
            df_cleaned['name'] = stock_name
            df_cleaned['trade_date'] = pd.to_datetime(trade_date, format='%Y%m%d').date()
            
            # 重命名列以匹配数据库表结构
            df_cleaned = df_cleaned.rename(columns={
                'ts_code': 'code',
                'trade_time': 'trade_time_str',
                'open': 'open',
                'high': 'high',
                'low': 'low',
                'close': 'close',
                'vol': 'vol',
                'amount': 'amount'
            })
            
            # 处理交易时间，提取时间部分
            df_cleaned['trade_time'] = pd.to_datetime(df_cleaned['trade_time_str']).dt.time
            
            # 处理空值和数据类型
            numeric_columns = ['open', 'high', 'low', 'close', 'amount']
            for col in numeric_columns:
                df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors='coerce')
            
            # 处理成交量（转换为整数）
            df_cleaned['vol'] = pd.to_numeric(df_cleaned['vol'], errors='coerce').fillna(0).astype('int64')
            
            # 选择需要的列
            df_cleaned = df_cleaned[[
                'trade_date', 'code', 'name', 'trade_time', 
                'open', 'close', 'high', 'low', 'vol', 'amount'
            ]]
            
            # 去除重复数据
            df_cleaned = df_cleaned.drop_duplicates(subset=['trade_date', 'code', 'trade_time'])
            
            logger.debug(f"60分钟数据清洗完成，清洗后数据量: {len(df_cleaned)}")
            return df_cleaned
            
        except Exception as e:
            logger.error(f"60分钟数据清洗失败: {e}")
            return pd.DataFrame()
    
    def insert_60min_data(self, df: pd.DataFrame) -> bool:
        """将股票60分钟行情数据插入数据库"""
        if df.empty:
            logger.warning("没有60分钟数据需要插入")
            return False
        
        try:
            with self.connection.cursor() as cursor:
                # 构建插入SQL
                sql = """
                INSERT INTO trade_market_stock_60min 
                (trade_date, code, name, trade_time, open, close, high, low, vol, amount)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                open = VALUES(open),
                close = VALUES(close),
                high = VALUES(high),
                low = VALUES(low),
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
                        row['name'],
                        row['trade_time'],
                        row['open'],
                        row['close'],
                        row['high'],
                        row['low'],
                        row['vol'],
                        row['amount']
                    ))
                
                # 批量插入
                cursor.executemany(sql, data_list)
                
                logger.debug(f"成功插入{len(data_list)}条股票60分钟行情数据到数据库")
                return True
                
        except Exception as e:
            logger.error(f"插入股票60分钟行情数据失败: {e}")
            return False
    
    def update_60min_data(self) -> bool:
        """更新股票60分钟行情数据"""
        try:
            start_date, end_date = self.get_trading_date_range()
            logger.info(f"开始更新股票60分钟行情数据，日期范围: {start_date} - {end_date}")
            
            # fetch_60min_data_range 方法已经包含了数据获取、清洗和入库的完整流程
            result_df = self.fetch_60min_data_range(start_date, end_date)
            
            if not result_df.empty and 'total_count' in result_df.columns:
                total_count = result_df['total_count'].iloc[0]
                if total_count > 0:
                    logger.info(f"股票60分钟行情数据更新完成，共处理{total_count}条数据")
                    return True
                else:
                    logger.info("所有数据已存在或无新数据需要处理")
                    return True
            else:
                logger.warning("股票60分钟行情数据更新失败")
                return False
            
        except Exception as e:
            logger.error(f"更新股票60分钟行情数据失败: {e}")
            return False
    
    def get_60min_data_by_date(self, trade_date: str, limit: int = 10) -> pd.DataFrame:
        """查询指定日期的股票60分钟行情数据"""
        try:
            with self.connection.cursor() as cursor:
                sql = """
                SELECT trade_date, code, name, trade_time, open, close, high, low, vol, amount
                FROM trade_market_stock_60min
                WHERE trade_date = %s
                ORDER BY amount DESC
                LIMIT %s
                """
                cursor.execute(sql, (trade_date, limit))
                results = cursor.fetchall()
                
                if results:
                    df = pd.DataFrame(results, columns=[
                        'trade_date', 'code', 'name', 'trade_time', 
                        'open', 'close', 'high', 'low', 'vol', 'amount'
                    ])
                    logger.info(f"查询到{len(df)}条股票60分钟行情数据")
                    return df
                else:
                    logger.info(f"未查询到{trade_date}的股票60分钟行情数据")
                    return pd.DataFrame()
                    
        except Exception as e:
            logger.error(f"查询股票60分钟行情数据失败: {e}")
            return pd.DataFrame()
    
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
        cleaner = Stock60MinCleaner()
        
        success = cleaner.update_60min_data()
        
        if success:
            logger.info("股票60分钟行情数据处理完成")
        else:
            logger.error("股票60分钟行情数据处理失败")
            
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
    finally:
        if cleaner:
            cleaner.close()


if __name__ == '__main__':
    main()