#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
个股资金流向数据清洗模块
从Tushare获取个股资金流向数据并写入数据库
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
log_filename = os.path.join(logs_dir, f'stock_fund_flow_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class StockFundFlowCleaner:
    """个股资金流向数据清洗器"""
    
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
    
    def get_all_stock_codes(self) -> List[str]:
        """从数据库获取所有股票代码"""
        try:
            with self.connection.cursor() as cursor:
                sql = """
                SELECT DISTINCT code 
                FROM trade_market_stock_daily
                """
                cursor.execute(sql)
                results = cursor.fetchall()
                
                if results:
                    stock_codes = [result[0] for result in results]
                    logger.info(f"从数据库获取到{len(stock_codes)}只股票代码")
                    return stock_codes
                else:
                    logger.warning("未从数据库获取到股票代码")
                    return []
                    
        except Exception as e:
            logger.error(f"获取股票代码失败: {e}")
            return []
    
    def fetch_fund_flow_data(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取指定股票代码和日期范围的资金流向数据"""
        try:
            logger.info(f"开始获取股票{ts_code}的资金流向数据，日期范围: {start_date} - {end_date}")
            
            # 调用Tushare的moneyflow_dc接口
            df = self.tushare_api.moneyflow_dc(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date
            )
            
            if df.empty:
                logger.warning(f"未获取到股票{ts_code}的资金流向数据")
                return pd.DataFrame()
            
            logger.info(f"成功获取到股票{ts_code}的{len(df)}条资金流向数据")
            return df
            
        except Exception as e:
            logger.error(f"获取股票{ts_code}的资金流向数据失败: {e}")
            return pd.DataFrame()
    
    def clean_fund_flow_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """清洗个股资金流向数据"""
        if df.empty:
            return df
        
        try:
            # 重命名列以匹配数据库表结构
            df_cleaned = df.rename(columns={
                'trade_date': 'trade_date',
                'ts_code': 'code',
                'name': 'name',
                'pct_change': 'pct_change',
                'close': 'close',
                'net_amount': 'net_amount',
                'net_amount_rate': 'net_amount_rate',
                'buy_elg_amount': 'buy_elg_amount',
                'buy_elg_amount_rate': 'buy_elg_amount_rate',
                'buy_lg_amount': 'buy_lg_amount',
                'buy_lg_amount_rate': 'buy_lg_amount_rate',
                'buy_md_amount': 'buy_md_amount',
                'buy_md_amount_rate': 'buy_md_amount_rate',
                'buy_sm_amount': 'buy_sm_amount',
                'buy_sm_amount_rate': 'buy_sm_amount_rate'
            })
            
            # 转换日期格式
            df_cleaned['trade_date'] = pd.to_datetime(df_cleaned['trade_date'], format='%Y%m%d').dt.date
            
            # 处理空值和数据类型
            numeric_columns = ['pct_change', 'close', 'net_amount', 'net_amount_rate',
                              'buy_elg_amount', 'buy_elg_amount_rate', 'buy_lg_amount', 'buy_lg_amount_rate',
                              'buy_md_amount', 'buy_md_amount_rate', 'buy_sm_amount', 'buy_sm_amount_rate']
            for col in numeric_columns:
                df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors='coerce')
            
            # 去除没有股票名称的记录
            df_cleaned = df_cleaned.dropna(subset=['name'])
            
            # 去除重复数据
            df_cleaned = df_cleaned.drop_duplicates(subset=['trade_date', 'code'])
            
            logger.info(f"数据清洗完成，清洗后数据量: {len(df_cleaned)}")
            return df_cleaned
            
        except Exception as e:
            logger.error(f"数据清洗失败: {e}")
            return pd.DataFrame()
    
    def insert_fund_flow_data(self, df: pd.DataFrame) -> bool:
        """将个股资金流向数据插入数据库"""
        if df.empty:
            logger.warning("没有数据需要插入")
            return False
        
        try:
            with self.connection.cursor() as cursor:
                # 构建插入SQL
                sql = """
                INSERT INTO trade_market_stock_fund_flow 
                (trade_date, code, name, pct_change, close, 
                 net_amount, net_amount_rate, 
                 buy_elg_amount, buy_elg_amount_rate, 
                 buy_lg_amount, buy_lg_amount_rate, 
                 buy_md_amount, buy_md_amount_rate, 
                 buy_sm_amount, buy_sm_amount_rate)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                pct_change = VALUES(pct_change),
                close = VALUES(close),
                net_amount = VALUES(net_amount),
                net_amount_rate = VALUES(net_amount_rate),
                buy_elg_amount = VALUES(buy_elg_amount),
                buy_elg_amount_rate = VALUES(buy_elg_amount_rate),
                buy_lg_amount = VALUES(buy_lg_amount),
                buy_lg_amount_rate = VALUES(buy_lg_amount_rate),
                buy_md_amount = VALUES(buy_md_amount),
                buy_md_amount_rate = VALUES(buy_md_amount_rate),
                buy_sm_amount = VALUES(buy_sm_amount),
                buy_sm_amount_rate = VALUES(buy_sm_amount_rate),
                updated_time = CURRENT_TIMESTAMP
                """
                
                # 准备数据
                data_list = []
                for _, row in df.iterrows():
                    data_list.append((
                        row['trade_date'],
                        row['code'],
                        row['name'],
                        row['pct_change'],
                        row['close'],
                        row['net_amount'],
                        row['net_amount_rate'],
                        row['buy_elg_amount'],
                        row['buy_elg_amount_rate'],
                        row['buy_lg_amount'],
                        row['buy_lg_amount_rate'],
                        row['buy_md_amount'],
                        row['buy_md_amount_rate'],
                        row['buy_sm_amount'],
                        row['buy_sm_amount_rate']
                    ))
                
                # 批量插入
                cursor.executemany(sql, data_list)
                
                logger.info(f"成功插入{len(data_list)}条个股资金流向数据到数据库")
                return True
                
        except Exception as e:
            logger.error(f"插入个股资金流向数据失败: {e}")
            return False
    
    def _check_date_exists(self, trade_date: str, ts_code: str) -> bool:
        """检查指定交易日期和股票代码是否已存在于数据库中"""
        try:
            with self.connection.cursor() as cursor:
                sql = """
                SELECT COUNT(*) 
                FROM trade_market_stock_fund_flow 
                WHERE trade_date = STR_TO_DATE(%s, '%%Y%%m%%d')
                AND code = %s
                """
                cursor.execute(sql, (trade_date, ts_code))
                result = cursor.fetchone()
                
                if result and result[0] > 0:
                    return True
                else:
                    return False
                    
        except Exception as e:
            logger.error(f"检查日期{trade_date}和股票{ts_code}是否存在失败: {e}")
            return False
    
    def update_fund_flow_data(self, days: int = 30) -> bool:
        """更新个股资金流向数据
        
        Args:
            days: 更新最近多少天的数据，默认30天
        """
        try:
            # 获取最近的交易日期
            end_date = self.get_latest_trading_date()
            # 计算开始日期（默认30天前）
            start_date = (datetime.strptime(end_date, '%Y%m%d') - timedelta(days=days)).strftime('%Y%m%d')
            
            logger.info(f"开始更新个股资金流向数据，日期范围: {start_date} - {end_date}")
            
            # 获取所有股票代码
            stock_codes = self.get_all_stock_codes()
            if not stock_codes:
                logger.error("未获取到股票代码，无法更新资金流向数据")
                return False
            
            total_count = 0
            success_count = 0
            skipped_count = 0
            
            # 按股票代码循环获取数据
            for i, ts_code in enumerate(stock_codes):
                try:
                    logger.info(f"正在处理第{i+1}/{len(stock_codes)}只股票: {ts_code}")
                    
                    # 检查最新交易日期的数据是否已存在
                    if self._check_date_exists(end_date, ts_code):
                        logger.info(f"股票{ts_code}在{end_date}的资金流向数据已存在，跳过获取")
                        skipped_count += 1
                        continue
                    
                    # 获取单只股票的资金流向数据
                    df = self.fetch_fund_flow_data(ts_code, start_date, end_date)
                    
                    if not df.empty:
                        # 清洗数据
                        df_cleaned = self.clean_fund_flow_data(df)
                        
                        if not df_cleaned.empty:
                            # 入库
                            success = self.insert_fund_flow_data(df_cleaned)
                            if success:
                                total_count += len(df_cleaned)
                                success_count += 1
                                logger.info(f"股票{ts_code}的资金流向数据已成功入库，共{len(df_cleaned)}条")
                            else:
                                logger.error(f"股票{ts_code}的资金流向数据入库失败")
                        else:
                            logger.warning(f"股票{ts_code}的资金流向数据清洗后为空")
                    else:
                        logger.warning(f"未获取到股票{ts_code}的资金流向数据")
                        
                except Exception as e:
                    logger.error(f"处理股票{ts_code}的资金流向数据失败: {e}")
                    continue
            
            logger.info(f"个股资金流向数据更新完成，共处理{len(stock_codes)}只股票，成功{success_count}只，跳过{skipped_count}只，总共入库{total_count}条数据")
            return success_count > 0 or skipped_count > 0
            
        except Exception as e:
            logger.error(f"更新个股资金流向数据失败: {e}")
            return False
    
    def close(self):
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()
            logger.info("数据库连接已关闭")


def main():
    """主函数"""
    
    parser = argparse.ArgumentParser(description='个股资金流向数据清洗工具')
    parser.add_argument('--days', type=int, default=30, help='更新最近多少天的数据，默认30天')
    args = parser.parse_args()
    
    cleaner = None
    try:
        # 创建清洗器实例
        cleaner = StockFundFlowCleaner()
        
        success = cleaner.update_fund_flow_data(days=args.days)
        
        if success:
            logger.info("个股资金流向数据处理完成")
        else:
            logger.error("个股资金流向数据处理失败")
            
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
    finally:
        if cleaner:
            cleaner.close()


if __name__ == '__main__':
    main()