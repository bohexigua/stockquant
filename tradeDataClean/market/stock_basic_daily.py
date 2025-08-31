#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票每日基本面指标数据清洗模块
从Tushare获取股票每日基本面指标数据并写入数据库
"""

import pdb
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
log_filename = os.path.join(logs_dir, f'stock_basic_daily_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class StockBasicDailyCleaner:
    """股票每日基本面指标数据清洗器"""
    
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
        """获取最早和最晚的交易日期"""
        try:
            with self.connection.cursor() as cursor:
                sql = """
                SELECT MIN(cal_date) as start_date, MAX(cal_date) as end_date
                FROM trade_market_calendar 
                WHERE is_open = 1
                """
                cursor.execute(sql)
                result = cursor.fetchone()
                if result and result[0] and result[1]:
                    start_date = result[0].strftime('%Y%m%d')
                    end_date = result[1].strftime('%Y%m%d')
                    return start_date, end_date
                else:
                    # 如果没有找到，使用默认日期范围
                    end_date = datetime.now().strftime('%Y%m%d')
                    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')
                    return start_date, end_date
        except Exception as e:
            logger.error(f"获取交易日期范围失败: {e}")
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')
            return start_date, end_date
    
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
    
    def fetch_daily_basic_data_range(self, start_date: str, end_date: str) -> pd.DataFrame:
        """获取指定日期范围的股票每日基本面指标数据（单日循环获取并立即入库）"""
        try:
            logger.info(f"开始获取股票每日基本面指标数据，日期范围: {start_date} - {end_date}")
            
            # 获取日期范围内的所有交易日
            trading_dates = self._get_trading_dates_in_range(start_date, end_date)
            
            if not trading_dates:
                logger.warning(f"在{start_date}-{end_date}范围内未找到交易日")
                return pd.DataFrame()
            
            total_count = 0
            
            # 按单日循环获取数据并立即入库
            for trade_date in trading_dates:
                try:
                    # 检查当前交易日期是否已存在于数据库中
                    if self._check_date_exists(trade_date):
                        logger.info(f"{trade_date}数据已存在，跳过获取")
                        continue
                    
                    logger.info(f"正在获取{trade_date}的股票每日基本面指标数据")
                    
                    # 获取单日数据
                    df_daily_basic = self.tushare_api.daily_basic(
                        trade_date=trade_date,
                        fields='ts_code,trade_date,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,total_share,float_share,free_share,total_mv,circ_mv'
                    )
                    
                    if not df_daily_basic.empty:
                        logger.info(f"成功获取{trade_date}的{len(df_daily_basic)}条数据")
                        
                        # 获取股票基础信息
                        df_basic = self.fetch_stock_basic()
                        if df_basic.empty:
                            logger.error(f"无法获取股票基础信息，跳过{trade_date}")
                            continue
                        
                        # 清洗数据
                        df_cleaned = self.clean_daily_basic_data(df_daily_basic, df_basic)
                        
                        if not df_cleaned.empty:
                            # 立即入库
                            success = self.insert_daily_basic_data(df_cleaned)
                            if success:
                                total_count += len(df_cleaned)
                                logger.info(f"{trade_date}数据已成功入库，共{len(df_cleaned)}条")
                            else:
                                logger.error(f"{trade_date}数据入库失败")
                        else:
                            logger.warning(f"{trade_date}数据清洗后为空")
                    else:
                        logger.warning(f"未获取到{trade_date}的数据")
                        
                except Exception as e:
                    logger.error(f"处理{trade_date}数据失败: {e}")
                    continue
            
            logger.info(f"日期范围{start_date}-{end_date}处理完成，总共入库{total_count}条数据")
            # 返回一个包含总数的DataFrame，用于兼容原有接口
            return pd.DataFrame({'total_count': [total_count]})
            
        except Exception as e:
            logger.error(f"获取股票每日基本面指标数据失败: {e}")
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
                FROM trade_market_stock_basic_daily 
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
    
    def clean_daily_basic_data(self, df_daily_basic: pd.DataFrame, df_basic: pd.DataFrame) -> pd.DataFrame:
        """清洗股票每日基本面指标数据"""
        if df_daily_basic.empty:
            return df_daily_basic
        
        try:
            # 合并股票基础信息，获取股票名称
            df_merged = df_daily_basic.merge(
                df_basic[['ts_code', 'name']], 
                on='ts_code', 
                how='left'
            )
            
            # 重命名列以匹配数据库表结构
            df_cleaned = df_merged.rename(columns={
                'trade_date': 'trade_date',
                'ts_code': 'code',
                'name': 'name',
                'turnover_rate': 'turnover_rate',
                'turnover_rate_f': 'turnover_rate_f',
                'volume_ratio': 'volume_ratio',
                'pe': 'pe',
                'pe_ttm': 'pe_ttm',
                'pb': 'pb',
                'total_share': 'total_share',
                'float_share': 'float_share',
                'free_share': 'free_share',
                'total_mv': 'total_mv',
                'circ_mv': 'circ_mv'
            })
            
            # 转换日期格式
            df_cleaned['trade_date'] = pd.to_datetime(df_cleaned['trade_date'], format='%Y%m%d').dt.date
            
            # 处理空值和数据类型
            numeric_columns = ['turnover_rate', 'turnover_rate_f', 'volume_ratio', 'pe', 'pe_ttm', 'pb', 'total_share', 'float_share', 'free_share', 'total_mv', 'circ_mv']
            for col in numeric_columns:
                df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors='coerce')
            
            # 根据数据库表字段类型填充NaN值
            # 对于FLOAT类型字段，保持NaN为NULL（数据库可接受）
            # 但对于股本和市值相关字段，如果为NaN则填充为0
            share_mv_columns = ['total_share', 'float_share', 'free_share', 'total_mv', 'circ_mv']
            for col in share_mv_columns:
                df_cleaned[col] = df_cleaned[col].fillna(0)
            
            # 对于估值指标（PE、PB），保持NaN为NULL，因为亏损股票的PE本身就应该为空
            
            # 去除没有股票名称的记录
            df_cleaned = df_cleaned.dropna(subset=['name'])
            
            # 去除重复数据
            df_cleaned = df_cleaned.drop_duplicates(subset=['trade_date', 'code'])
            
            logger.info(f"数据清洗完成，清洗后数据量: {len(df_cleaned)}")
            return df_cleaned
            
        except Exception as e:
            logger.error(f"数据清洗失败: {e}")
            return pd.DataFrame()
    
    def insert_daily_basic_data(self, df: pd.DataFrame) -> bool:
        """将股票每日基本面指标数据插入数据库"""
        if df.empty:
            logger.warning("没有数据需要插入")
            return False
        
        try:
            with self.connection.cursor() as cursor:
                # 构建插入SQL
                sql = """
                INSERT INTO trade_market_stock_basic_daily 
                (trade_date, code, name, turnover_rate, turnover_rate_f, volume_ratio, 
                 pe, pe_ttm, pb, total_share, float_share, free_share, total_mv, circ_mv)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                turnover_rate = VALUES(turnover_rate),
                turnover_rate_f = VALUES(turnover_rate_f),
                volume_ratio = VALUES(volume_ratio),
                pe = VALUES(pe),
                pe_ttm = VALUES(pe_ttm),
                pb = VALUES(pb),
                total_share = VALUES(total_share),
                float_share = VALUES(float_share),
                free_share = VALUES(free_share),
                total_mv = VALUES(total_mv),
                circ_mv = VALUES(circ_mv),
                updated_time = CURRENT_TIMESTAMP
                """
                
                # 准备数据
                data_list = []
                for _, row in df.iterrows():
                    # 将pandas的NaN值转换为None，以便在数据库中正确存储为NULL
                    data_list.append((
                        row['trade_date'],
                        row['code'],
                        row['name'],
                        None if pd.isna(row['turnover_rate']) else row['turnover_rate'],
                        None if pd.isna(row['turnover_rate_f']) else row['turnover_rate_f'],
                        None if pd.isna(row['volume_ratio']) else row['volume_ratio'],
                        None if pd.isna(row['pe']) else row['pe'],
                        None if pd.isna(row['pe_ttm']) else row['pe_ttm'],
                        None if pd.isna(row['pb']) else row['pb'],
                        None if pd.isna(row['total_share']) else row['total_share'],
                        None if pd.isna(row['float_share']) else row['float_share'],
                        None if pd.isna(row['free_share']) else row['free_share'],
                        None if pd.isna(row['total_mv']) else row['total_mv'],
                        None if pd.isna(row['circ_mv']) else row['circ_mv']
                    ))

                # 批量插入
                cursor.executemany(sql, data_list)
                
                logger.info(f"成功插入{len(data_list)}条股票每日基本面指标数据到数据库")
                return True
                
        except Exception as e:
            logger.error(f"插入股票每日基本面指标数据失败: {e}")
            return False
    
    def update_daily_basic_data(self) -> bool:
        """更新股票每日基本面指标数据"""
        try:
            start_date, end_date = self.get_trading_date_range()
            logger.info(f"开始更新股票每日基本面指标数据，日期范围: {start_date} - {end_date}")
            
            # fetch_daily_basic_data_range 方法已经包含了数据获取、清洗和入库的完整流程
            result_df = self.fetch_daily_basic_data_range(start_date, end_date)
            
            if not result_df.empty and 'total_count' in result_df.columns:
                total_count = result_df['total_count'].iloc[0]
                if total_count > 0:
                    logger.info(f"股票每日基本面指标数据更新完成，共处理{total_count}条数据")
                    return True
                else:
                    logger.info("所有数据已存在或无新数据需要处理")
                    return True
            else:
                logger.warning("股票每日基本面指标数据更新失败")
                return False
            
        except Exception as e:
            logger.error(f"更新股票每日基本面指标数据失败: {e}")
            return False
    
    def get_daily_basic_data_by_date(self, trade_date: str, limit: int = 10) -> pd.DataFrame:
        """查询指定日期的股票每日基本面指标数据"""
        try:
            with self.connection.cursor() as cursor:
                sql = """
                SELECT trade_date, code, name, turnover_rate, turnover_rate_f, volume_ratio,
                       pe, pe_ttm, pb, total_share, float_share, free_share, total_mv, circ_mv
                FROM trade_market_stock_basic_daily
                WHERE trade_date = %s
                ORDER BY total_mv DESC
                LIMIT %s
                """
                cursor.execute(sql, (trade_date, limit))
                results = cursor.fetchall()
                
                if results:
                    df = pd.DataFrame(results, columns=[
                        'trade_date', 'code', 'name', 'turnover_rate', 'turnover_rate_f', 'volume_ratio',
                        'pe', 'pe_ttm', 'pb', 'total_share', 'float_share', 'free_share', 'total_mv', 'circ_mv'
                    ])
                    logger.info(f"查询到{len(df)}条股票每日基本面指标数据")
                    return df
                else:
                    logger.info(f"未查询到{trade_date}的股票每日基本面指标数据")
                    return pd.DataFrame()
                    
        except Exception as e:
            logger.error(f"查询股票每日基本面指标数据失败: {e}")
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
        cleaner = StockBasicDailyCleaner()
        
        success = cleaner.update_daily_basic_data()
        
        if success:
            logger.info("股票每日基本面指标数据处理完成")
        else:
            logger.error("股票每日基本面指标数据处理失败")
            
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
    finally:
        if cleaner:
            cleaner.close()


if __name__ == '__main__':
    main()