#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
题材因子计算模块
从trade_market_theme表读取题材市场数据，计算题材排名相关因子
并将结果写入trade_factor_theme表
"""

import pdb
import pandas as pd
import pymysql
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import logging
import os
from pathlib import Path
import sys
import numpy as np

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
from config import config, DatabaseConfig

# 创建logs目录
logs_dir = os.path.join(project_root, 'logs/factors')
os.makedirs(logs_dir, exist_ok=True)

# 配置日志 - 输出到文件和控制台
log_filename = os.path.join(logs_dir, f'theme_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class ThemeFactorCalculator:
    """
    题材因子计算器
    
    主要功能:
    1. 从trade_market_theme表读取题材市场数据
    2. 计算题材排名相关因子
    3. 将计算结果写入trade_factor_theme表
    """
    
    def __init__(self):
        """
        初始化题材因子计算器
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
            self.connection.close()
            logger.info("数据库连接已关闭")
    
    def get_trading_dates(self, start_date: str, end_date: str) -> List[str]:
        """
        获取指定日期范围内的交易日期
        
        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            
        Returns:
            交易日期列表
        """
        try:
            with self.connection.cursor() as cursor:
                sql = """
                SELECT cal_date 
                FROM trade_market_calendar 
                WHERE cal_date BETWEEN %s AND %s 
                AND is_open = 1
                ORDER BY cal_date
                """
                cursor.execute(sql, (start_date, end_date))
                results = cursor.fetchall()
                
                trading_dates = [row[0].strftime('%Y-%m-%d') for row in results]
                logger.info(f"获取到 {len(trading_dates)} 个交易日")
                return trading_dates
                
        except Exception as e:
            logger.error(f"获取交易日期失败: {e}")
            return []
    
    def get_theme_market_data(self, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        获取题材市场数据
        
        Args:
            start_date: 开始日期 (YYYY-MM-DD)，可选，为None时获取全部数据
            end_date: 结束日期 (YYYY-MM-DD)，可选，为None时获取全部数据
            
        Returns:
            题材市场数据DataFrame
        """
        try:
            with self.connection.cursor() as cursor:
                if start_date and end_date:
                    sql = """
                    SELECT trade_date, code, name, z_t_num, up_num, rank_value
                    FROM trade_market_theme 
                    WHERE trade_date BETWEEN %s AND %s
                    ORDER BY trade_date, rank_value
                    """
                    cursor.execute(sql, (start_date, end_date))
                    logger.info(f"获取 {start_date} 到 {end_date} 的题材市场数据")
                else:
                    sql = """
                    SELECT trade_date, code, name, z_t_num, up_num, rank_value
                    FROM trade_market_theme 
                    ORDER BY trade_date, rank_value
                    """
                    cursor.execute(sql)
                    logger.info("获取全部题材市场数据")
                
                results = cursor.fetchall()
                
                if not results:
                    logger.warning("未获取到题材市场数据")
                    return pd.DataFrame()
                
                # 转换为DataFrame
                df = pd.DataFrame(results, columns=[
                    'trade_date', 'code', 'name', 'z_t_num', 'up_num', 'rank_value'
                ])
                
                # 数据类型转换
                df['trade_date'] = pd.to_datetime(df['trade_date'])
                df['z_t_num'] = pd.to_numeric(df['z_t_num'], errors='coerce').fillna(0)
                df['up_num'] = pd.to_numeric(df['up_num'], errors='coerce').fillna(0)
                df['rank_value'] = pd.to_numeric(df['rank_value'], errors='coerce').fillna(0)
                
                logger.info(f"获取到 {len(df)} 条题材市场数据")
                return df
                
        except Exception as e:
            logger.error(f"获取题材市场数据失败: {e}")
            return pd.DataFrame()
    
    def calculate_theme_factors(self, df: pd.DataFrame = None) -> pd.DataFrame:
        """
        计算题材因子
        
        Args:
            df: 题材市场数据DataFrame，可选，为None时获取全部数据
            
        Returns:
            计算后的因子数据DataFrame
        """
        if df is None:
            df = self.get_theme_market_data()
        
        if df.empty:
            logger.warning("没有数据可供计算因子")
            return pd.DataFrame()

        try:
            # 按题材代码分组计算因子
            factor_list = []
            
            for code in df['code'].unique():
                theme_data = df[df['code'] == code].copy()
                theme_data = theme_data.sort_values('trade_date')
                
                # 获取题材名称
                theme_name = theme_data['name'].iloc[-1] if not theme_data.empty else ''
                
                for i, row in theme_data.iterrows():
                    trade_date = row['trade_date']
                    current_rank = row['rank_value']
                    
                    # 获取历史数据用于计算移动平均
                    hist_data = theme_data[theme_data['trade_date'] <= trade_date]
                    
                    # 计算因子
                    factors = {
                        'trade_date': trade_date,
                        'code': code,
                        'name': theme_name,
                        'theme_rank_today': float(current_rank),
                        'theme_rank_5d_avg': self._calculate_avg_rank(hist_data, 5),
                        'theme_rank_10d_avg': self._calculate_avg_rank(hist_data, 10),
                        'theme_rank_20d_avg': self._calculate_avg_rank(hist_data, 20),
                        'theme_rank_5d_surge': self._calculate_rank_surge(hist_data, 5)
                    }
                    
                    factor_list.append(factors)
            
            # 转换为DataFrame
            factor_df = pd.DataFrame(factor_list)
            
            # 数据类型转换和清洗
            factor_df['trade_date'] = pd.to_datetime(factor_df['trade_date'])
            
            # 数值列填充NaN为0，字符串列填充为空字符串
            numeric_columns = ['theme_rank_today', 'theme_rank_5d_avg', 'theme_rank_10d_avg', 'theme_rank_20d_avg']
            for col in numeric_columns:
                factor_df[col] = pd.to_numeric(factor_df[col], errors='coerce').fillna(0)
            
            # 字符串列填充
            string_columns = ['name']
            for col in string_columns:
                factor_df[col] = factor_df[col].fillna('')
            
            # 布尔列处理
            factor_df['theme_rank_5d_surge'] = factor_df['theme_rank_5d_surge'].fillna(0).astype(int)
            
            
            
            logger.info(f"计算完成 {len(factor_df)} 条题材因子数据")
            
            return factor_df
            
        except Exception as e:
            logger.error(f"计算题材因子失败: {e}")
            return pd.DataFrame()
    
    def _calculate_avg_rank(self, hist_data: pd.DataFrame, days: int) -> Optional[float]:
        """
        计算指定天数的平均排名
        
        Args:
            hist_data: 历史数据
            days: 天数
            
        Returns:
            平均排名
        """
        if len(hist_data) < days:
            return None
        
        recent_data = hist_data.tail(days)
        avg_rank = recent_data['rank_value'].mean()
        return float(avg_rank) if not pd.isna(avg_rank) else None
    
    def _calculate_rank_surge(self, hist_data: pd.DataFrame, days: int) -> int:
        """
        计算排名是否有大幅上升
        
        Args:
            hist_data: 历史数据
            days: 天数
            
        Returns:
            1表示有大幅上升，0表示没有
        """
        if len(hist_data) < days + 1:
            return 0
        
        try:
            # 获取最近days天的数据
            recent_data = hist_data.tail(days + 1)
            
            # 计算排名变化（排名越小越好，所以上升是数值减小）
            first_rank = recent_data['rank_value'].iloc[0]
            last_rank = recent_data['rank_value'].iloc[-1]
            
            # 如果排名提升超过20位，认为是大幅上升
            rank_improvement = first_rank - last_rank
            
            return 1 if rank_improvement >= 20 else 0
            
        except Exception as e:
            logger.error(f"计算排名上升失败: {e}")
            return 0
    
    def check_factor_data_exists(self, trade_date: str, code: str) -> bool:
        """
        检查指定日期和题材代码的因子数据是否已存在
        
        Args:
            trade_date: 交易日期
            code: 题材代码
            
        Returns:
            True表示存在，False表示不存在
        """
        try:
            with self.connection.cursor() as cursor:
                sql = """
                SELECT COUNT(*) FROM trade_factor_theme 
                WHERE trade_date = %s AND code = %s
                """
                cursor.execute(sql, (trade_date, code))
                count = cursor.fetchone()[0]
                return count > 0
                
        except Exception as e:
            logger.error(f"检查因子数据是否存在失败: {e}")
            return False
    
    def insert_factor_data(self, df: pd.DataFrame) -> bool:
        """
        将题材因子数据插入数据库
        
        Args:
            df: 因子数据DataFrame
            
        Returns:
            插入是否成功
        """
        if df.empty:
            logger.warning("没有因子数据需要插入")
            return False
        
        try:
            with self.connection.cursor() as cursor:
                # 构建插入SQL
                sql = """
                INSERT INTO trade_factor_theme 
                (trade_date, code, name, theme_rank_today, theme_rank_5d_avg, 
                 theme_rank_10d_avg, theme_rank_20d_avg, theme_rank_5d_surge)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                theme_rank_today = VALUES(theme_rank_today),
                theme_rank_5d_avg = VALUES(theme_rank_5d_avg),
                theme_rank_10d_avg = VALUES(theme_rank_10d_avg),
                theme_rank_20d_avg = VALUES(theme_rank_20d_avg),
                theme_rank_5d_surge = VALUES(theme_rank_5d_surge),
                updated_time = CURRENT_TIMESTAMP
                """
                
                # 准备数据
                data_list = []
                for _, row in df.iterrows():
                    data_list.append((
                        row['trade_date'].strftime('%Y-%m-%d'),
                        row['code'],
                        row['name'],
                        row['theme_rank_today'],
                        row['theme_rank_5d_avg'],
                        row['theme_rank_10d_avg'],
                        row['theme_rank_20d_avg'],
                        row['theme_rank_5d_surge']
                    ))
                
                # 批量插入
                cursor.executemany(sql, data_list)
                
                logger.info(f"成功插入 {len(data_list)} 条题材因子数据到数据库")
                return True
                
        except Exception as e:
            logger.error(f"插入题材因子数据失败: {e}")
            return False
    
    def update_factor_data(self, start_date: str = None, end_date: str = None) -> bool:
        """
        更新题材因子数据
        
        Args:
            start_date: 开始日期 (YYYY-MM-DD)，可选，为None时更新全部数据
            end_date: 结束日期 (YYYY-MM-DD)，可选，为None时更新全部数据
            
        Returns:
            更新是否成功
        """
        try:
            if start_date and end_date:
                logger.info(f"开始更新题材因子数据: {start_date} 到 {end_date}")
                # 获取题材市场数据
                market_df = self.get_theme_market_data(start_date, end_date)
            else:
                logger.info("开始更新全部题材因子数据")
                # 获取全部题材市场数据
                market_df = self.get_theme_market_data()
            
            if market_df.empty:
                logger.warning("未获取到题材市场数据")
                return False
            
            # 计算因子
            factor_df = self.calculate_theme_factors(market_df)
            
            if factor_df.empty:
                logger.warning("未计算出题材因子数据")
                return False
            
            # 如果指定了日期范围，只保留目标日期范围的数据
            if start_date and end_date:
                factor_df = factor_df[
                    (factor_df['trade_date'] >= start_date) & 
                    (factor_df['trade_date'] <= end_date)
                ]
                
                if factor_df.empty:
                    logger.warning(f"目标日期范围 {start_date} 到 {end_date} 没有因子数据")
                    return False
            
            # 删除已有的因子数据
            try:
                with self.connection.cursor() as cursor:
                    if start_date and end_date:
                        # 删除指定日期范围的数据
                        delete_sql = """
                        DELETE FROM trade_factor_theme 
                        WHERE trade_date BETWEEN %s AND %s
                        """
                        cursor.execute(delete_sql, (start_date, end_date))
                        logger.info(f"已删除 {start_date} 到 {end_date} 的题材因子数据")
                    else:
                        # 删除全部数据
                        delete_sql = "DELETE FROM trade_factor_theme"
                        cursor.execute(delete_sql)
                        logger.info("已删除全部题材因子数据")
            except Exception as e:
                logger.error(f"删除已有因子数据失败: {e}")
                return False
            
            # 插入数据库
            success = self.insert_factor_data(factor_df)
            
            if success:
                if start_date and end_date:
                    logger.info(f"题材因子数据更新完成: {start_date} 到 {end_date}")
                else:
                    logger.info("全部题材因子数据更新完成")
            
            return success
            
        except Exception as e:
            logger.error(f"更新题材因子数据失败: {e}")
            return False
    
    
    def get_factor_data(self, start_date: str, end_date: str, code: str = None) -> pd.DataFrame:
        """
        获取题材因子数据
        
        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            code: 题材代码，可选
            
        Returns:
            因子数据DataFrame
        """
        try:
            with self.connection.cursor() as cursor:
                if code:
                    sql = """
                    SELECT trade_date, code, name, theme_rank_today, theme_rank_5d_avg,
                           theme_rank_10d_avg, theme_rank_20d_avg, theme_rank_5d_surge
                    FROM trade_factor_theme 
                    WHERE trade_date BETWEEN %s AND %s AND code = %s
                    ORDER BY trade_date
                    """
                    cursor.execute(sql, (start_date, end_date, code))
                else:
                    sql = """
                    SELECT trade_date, code, name, theme_rank_today, theme_rank_5d_avg,
                           theme_rank_10d_avg, theme_rank_20d_avg, theme_rank_5d_surge
                    FROM trade_factor_theme 
                    WHERE trade_date BETWEEN %s AND %s
                    ORDER BY trade_date, code
                    """
                    cursor.execute(sql, (start_date, end_date))
                
                results = cursor.fetchall()
                
                if not results:
                    logger.warning(f"未获取到因子数据: {start_date} 到 {end_date}")
                    return pd.DataFrame()
                
                # 转换为DataFrame
                df = pd.DataFrame(results, columns=[
                    'trade_date', 'code', 'name', 'theme_rank_today', 'theme_rank_5d_avg',
                    'theme_rank_10d_avg', 'theme_rank_20d_avg', 'theme_rank_5d_surge'
                ])
                
                df['trade_date'] = pd.to_datetime(df['trade_date'])
                
                logger.info(f"获取到 {len(df)} 条因子数据")
                return df
                
        except Exception as e:
            logger.error(f"获取因子数据失败: {e}")
            return pd.DataFrame()
    
    def __del__(self):
        """析构函数，确保数据库连接关闭"""
        self._close_database()


def main():
    """
    主函数 - 更新题材因子数据
    """
    try:
        calculator = ThemeFactorCalculator()
        
        # 更新全部数据
        logger.info("开始更新全部题材因子数据")
        
        success = calculator.update_factor_data()
        
        if success:
            logger.info("题材因子数据更新成功")
        else:
            logger.error("题材因子数据更新失败")
            
    except Exception as e:
        logger.error(f"主函数执行失败: {e}")


if __name__ == "__main__":
    main()