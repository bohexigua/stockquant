#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
题材市场数据清洗模块
从Tushare获取开盘啦概念题材数据并写入数据库
"""

import sys
import os
import logging
import argparse
from datetime import datetime
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


class ThemeDataCleaner:
    """题材数据清洗器"""
    
    def __init__(self):
        """初始化"""
        self.db_config = config.database
        self.tushare_token = config.tushare.token
        self.connection = None
        self.tushare_api = None
        
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
    
    def fetch_theme_data(self, trade_date: str) -> pd.DataFrame:
        """获取题材数据"""
        try:
            logger.info(f"开始获取题材数据，交易日期: {trade_date}")
            
            # 调用Tushare的kpl_concept接口
            df = self.tushare_api.kpl_concept(trade_date=trade_date)
            
            if df.empty:
                logger.warning(f"未获取到{trade_date}的题材数据")
                return pd.DataFrame()
            
            logger.info(f"成功获取到{len(df)}条题材数据")
            return df
            
        except Exception as e:
            logger.error(f"获取题材数据失败: {e}")
            return pd.DataFrame()
    
    def clean_theme_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """清洗题材数据"""
        if df.empty:
            return df
        
        try:
            # 重命名列以匹配数据库表结构
            df_cleaned = df.rename(columns={
                'trade_date': 'trade_date',
                'ts_code': 'code',
                'name': 'name',
                'z_t_num': 'z_t_num',
                'up_num': 'up_num'
            })
            
            # 添加排名字段（使用DataFrame中的顺序值）
            df_cleaned['rank_value'] = range(1, len(df_cleaned) + 1)
            
            # 转换日期格式
            df_cleaned['trade_date'] = pd.to_datetime(df_cleaned['trade_date'], format='%Y%m%d').dt.date
            
            # 处理空值
            df_cleaned['z_t_num'] = df_cleaned['z_t_num'].fillna(0).astype(int)
            df_cleaned['up_num'] = df_cleaned['up_num'].fillna(0).astype(int)
            
            # 去除重复数据
            df_cleaned = df_cleaned.drop_duplicates(subset=['trade_date', 'code'])
            
            logger.info(f"数据清洗完成，清洗后数据量: {len(df_cleaned)}")
            return df_cleaned
            
        except Exception as e:
            logger.error(f"数据清洗失败: {e}")
            return pd.DataFrame()
    
    def insert_theme_data(self, df: pd.DataFrame) -> bool:
        """将题材数据插入数据库"""
        if df.empty:
            logger.warning("没有数据需要插入")
            return False
        
        try:
            with self.connection.cursor() as cursor:
                # 构建插入SQL
                sql = """
                INSERT INTO trade_market_theme 
                (trade_date, code, name, z_t_num, up_num, rank_value)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                z_t_num = VALUES(z_t_num),
                up_num = VALUES(up_num),
                rank_value = VALUES(rank_value),
                updated_time = CURRENT_TIMESTAMP
                """
                
                # 准备数据
                data_list = []
                for _, row in df.iterrows():
                    data_list.append((
                        row['trade_date'],
                        row['code'],
                        row['name'],
                        row['z_t_num'],
                        row['up_num'],
                        row['rank_value']
                    ))
                
                # 批量插入
                cursor.executemany(sql, data_list)
                
                logger.info(f"成功插入{len(data_list)}条题材数据到数据库")
                return True
                
        except Exception as e:
            logger.error(f"插入题材数据失败: {e}")
            return False
    
    def update_theme_data(self, trade_date: Optional[str] = None) -> bool:
        """更新题材数据"""
        try:
            # 如果没有指定交易日期，获取最近的交易日期
            if not trade_date:
                trade_date = self.get_latest_trading_date()
            
            logger.info(f"开始更新题材数据，交易日期: {trade_date}")
            
            # 获取题材数据
            df = self.fetch_theme_data(trade_date)
            
            if df.empty:
                logger.warning("未获取到题材数据")
                return False
            
            # 清洗数据
            df_cleaned = self.clean_theme_data(df)
            
            if df_cleaned.empty:
                logger.warning("数据清洗后为空")
                return False
            
            # 插入数据库
            success = self.insert_theme_data(df_cleaned)
            
            if success:
                logger.info("题材数据更新完成")
            else:
                logger.error("题材数据更新失败")
            
            return success
            
        except Exception as e:
            logger.error(f"更新题材数据失败: {e}")
            return False
    
    def get_theme_data_by_date(self, trade_date: str) -> pd.DataFrame:
        """查询指定日期的题材数据"""
        try:
            with self.connection.cursor() as cursor:
                sql = """
                SELECT trade_date, code, name, z_t_num, up_num, rank_value
                FROM trade_market_theme
                WHERE trade_date = %s
                ORDER BY z_t_num DESC, up_num DESC
                """
                cursor.execute(sql, (trade_date,))
                results = cursor.fetchall()
                
                if results:
                    df = pd.DataFrame(results, columns=[
                        'trade_date', 'code', 'name', 'z_t_num', 'up_num', 'rank_value'
                    ])
                    logger.info(f"查询到{len(df)}条题材数据")
                    return df
                else:
                    logger.info(f"未查询到{trade_date}的题材数据")
                    return pd.DataFrame()
                    
        except Exception as e:
            logger.error(f"查询题材数据失败: {e}")
            return pd.DataFrame()
    
    def close(self):
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()
            logger.info("数据库连接已关闭")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='题材数据清洗工具')
    parser.add_argument('--test', action='store_true', help='测试模式，只处理最近一个交易日的数据')
    parser.add_argument('--date', type=str, help='指定交易日期（YYYYMMDD格式）')
    args = parser.parse_args()
    
    cleaner = None
    try:
        # 创建清洗器实例
        cleaner = ThemeDataCleaner()
        
        if args.test:
            logger.info("=== 测试模式 ===")
            # 测试模式：更新最近交易日的数据
            success = cleaner.update_theme_data()
            if success:
                # 查询并显示数据
                trade_date = cleaner.get_latest_trading_date()
                df = cleaner.get_theme_data_by_date(trade_date)
                if not df.empty:
                    print(f"\n{trade_date}的题材数据（前5条）:")
                    print(df.head().to_string(index=False))
        else:
            # 正常模式：更新指定日期或最近交易日的数据
            success = cleaner.update_theme_data(args.date)
            
        if success:
            logger.info("题材数据处理完成")
        else:
            logger.error("题材数据处理失败")
            
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
    finally:
        if cleaner:
            cleaner.close()


if __name__ == '__main__':
    main()