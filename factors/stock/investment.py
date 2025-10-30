#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
投资因子计算模块
从资金流向数据中获取净流入前500的股票数据，并存入投资因子表
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import pymysql
from dataclasses import dataclass

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config import config, DatabaseConfig


class InvestmentFactorCalculator:
    """投资因子计算器"""
    
    def __init__(self):
        """
        初始化投资因子计算器
        """
        self.db_config = config.database
        self.logger = self._setup_logger()
    
    def _setup_logger(self) -> logging.Logger:
        """设置日志"""
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _get_db_connection(self) -> pymysql.Connection:
        """获取数据库连接"""
        try:
            connection = pymysql.connect(
                host=self.db_config.host,
                port=self.db_config.port,
                user=self.db_config.user,
                password=self.db_config.password,
                database=self.db_config.database,
                charset=self.db_config.charset,
                autocommit=False
            )
            return connection
        except Exception as e:
            self.logger.error(f"数据库连接失败: {e}")
            raise
    
    def get_latest_trade_date(self) -> Optional[str]:
        """
        获取最新的交易日期
        
        Returns:
            最新交易日期字符串，格式为YYYY-MM-DD
        """
        connection = None
        try:
            connection = self._get_db_connection()
            with connection.cursor() as cursor:
                sql = """
                SELECT MAX(trade_date) as latest_date 
                FROM trade_market_stock_fund_flow
                """
                cursor.execute(sql)
                result = cursor.fetchone()
                
                if result and result[0]:
                    return result[0].strftime('%Y-%m-%d') if hasattr(result[0], 'strftime') else str(result[0])
                return None
                
        except Exception as e:
            self.logger.error(f"获取最新交易日期失败: {e}")
            return None
        finally:
            if connection:
                connection.close()
    
    def get_top_fund_inflow_stocks(self, trade_date: str, top_n: int = 500) -> List[Dict[str, Any]]:
        """
        获取指定日期净流入前N的股票数据
        
        Args:
            trade_date: 交易日期，格式为YYYY-MM-DD
            top_n: 获取前N名，默认500
            
        Returns:
            股票数据列表
        """
        connection = None
        try:
            connection = self._get_db_connection()
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:
                sql = """
                SELECT 
                    trade_date,
                    code,
                    name,
                    net_amount,
                    pct_change,
                    close,
                    ROW_NUMBER() OVER (ORDER BY net_amount DESC) as fund_inflow_rank
                FROM trade_market_stock_fund_flow 
                WHERE trade_date = %s 
                    AND net_amount IS NOT NULL
                ORDER BY net_amount DESC 
                LIMIT %s
                """
                cursor.execute(sql, (trade_date, top_n))
                results = cursor.fetchall()
                
                self.logger.info(f"获取到 {len(results)} 条 {trade_date} 的资金流向数据")
                return results
                
        except Exception as e:
            self.logger.error(f"获取资金流向数据失败: {e}")
            return []
        finally:
            if connection:
                connection.close()
    
    def clear_existing_data(self, trade_date: str) -> bool:
        """
        清除指定日期的现有投资因子数据
        
        Args:
            trade_date: 交易日期
            
        Returns:
            是否成功
        """
        connection = None
        try:
            connection = self._get_db_connection()
            with connection.cursor() as cursor:
                sql = "DELETE FROM trade_factor_stock_investment WHERE trade_date = %s"
                cursor.execute(sql, (trade_date,))
                deleted_count = cursor.rowcount
                connection.commit()
                
                self.logger.info(f"清除了 {deleted_count} 条 {trade_date} 的现有投资因子数据")
                return True
                
        except Exception as e:
            self.logger.error(f"清除现有数据失败: {e}")
            if connection:
                connection.rollback()
            return False
        finally:
            if connection:
                connection.close()
    
    def insert_investment_factors(self, fund_flow_data: List[Dict[str, Any]]) -> bool:
        """
        批量插入投资因子数据
        
        Args:
            fund_flow_data: 资金流向数据列表
            
        Returns:
            是否成功
        """
        if not fund_flow_data:
            self.logger.warning("没有数据需要插入")
            return True
            
        connection = None
        try:
            connection = self._get_db_connection()
            with connection.cursor() as cursor:
                sql = """
                INSERT INTO trade_factor_stock_investment 
                (trade_date, code, name, top_fund_inflow_rank, created_time, updated_time)
                VALUES (%s, %s, %s, %s, %s, %s)
                """
                
                current_time = datetime.now()
                data_to_insert = []
                
                for item in fund_flow_data:
                    data_to_insert.append((
                        item['trade_date'],
                        item['code'],
                        item['name'],
                        item['fund_inflow_rank'],
                        current_time,
                        current_time
                    ))
                
                cursor.executemany(sql, data_to_insert)
                connection.commit()
                
                self.logger.info(f"成功插入 {len(data_to_insert)} 条投资因子数据")
                return True
                
        except Exception as e:
            self.logger.error(f"插入投资因子数据失败: {e}")
            if connection:
                connection.rollback()
            return False
        finally:
            if connection:
                connection.close()
    
    def calculate_investment_factor(self, trade_date: str = None, top_n: int = 500) -> bool:
        """
        计算投资因子
        
        Args:
            trade_date: 交易日期，默认为最新交易日
            top_n: 获取前N名，默认500
            
        Returns:
            是否成功
        """
        try:
            # 如果没有指定日期，使用最新交易日
            if not trade_date:
                trade_date = self.get_latest_trade_date()
                if not trade_date:
                    self.logger.error("无法获取最新交易日期")
                    return False
            
            self.logger.info(f"开始计算 {trade_date} 的投资因子，获取前 {top_n} 名")
            
            # 获取资金流向数据
            fund_flow_data = self.get_top_fund_inflow_stocks(trade_date, top_n)
            if not fund_flow_data:
                self.logger.warning(f"{trade_date} 没有可用的资金流向数据")
                return False
            
            # 清除现有数据
            if not self.clear_existing_data(trade_date):
                return False
            
            # 插入新数据
            if not self.insert_investment_factors(fund_flow_data):
                return False
            
            self.logger.info(f"投资因子计算完成: {trade_date}")
            return True
            
        except Exception as e:
            self.logger.error(f"计算投资因子失败: {e}")
            return False
    
    def calculate_recent_days(self, days: int = 5, top_n: int = 500) -> bool:
        """
        计算最近N天的投资因子
        
        Args:
            days: 天数
            top_n: 获取前N名股票，默认500
            
        Returns:
            是否成功
        """
        try:
            latest_date = self.get_latest_trade_date()
            if not latest_date:
                self.logger.error("无法获取最新交易日期")
                return False
            
            # 获取最近的交易日期列表
            connection = self._get_db_connection()
            try:
                with connection.cursor() as cursor:
                    sql = """
                    SELECT DISTINCT trade_date 
                    FROM trade_market_stock_fund_flow 
                    WHERE trade_date <= %s 
                    ORDER BY trade_date DESC 
                    LIMIT %s
                    """
                    cursor.execute(sql, (latest_date, days))
                    trade_dates = [row[0].strftime('%Y-%m-%d') if hasattr(row[0], 'strftime') else str(row[0]) 
                                 for row in cursor.fetchall()]
            finally:
                connection.close()
            
            success_count = 0
            total_count = len(trade_dates)
            
            for i, trade_date in enumerate(trade_dates, 1):
                self.logger.info(f"处理第 {i}/{total_count} 个交易日: {trade_date}")
                
                if self.calculate_investment_factor(trade_date, top_n):
                    success_count += 1
                else:
                    self.logger.warning(f"计算 {trade_date} 投资因子失败")
            
            self.logger.info(f"完成最近 {days} 天的投资因子计算，成功 {success_count}/{total_count} 天")
            return success_count > 0
            
        except Exception as e:
            self.logger.error(f"计算最近几天投资因子失败: {e}")
            return False
    
    def update_factor_data(self, start_date: str = None, end_date: str = None, top_n: int = 500) -> bool:
        """
        更新投资因子数据
        
        Args:
            start_date: 开始日期 (YYYY-MM-DD)，可选，为None时更新全部数据
            end_date: 结束日期 (YYYY-MM-DD)，可选，为None时更新全部数据
            top_n: 获取前N名股票，默认500
            
        Returns:
            更新是否成功
        """
        try:
            if start_date and end_date:
                self.logger.info(f"开始更新投资因子数据: {start_date} 到 {end_date}")
                # 获取指定日期范围内的交易日期
                trade_dates = self._get_trade_dates_in_range(start_date, end_date)
            else:
                self.logger.info("开始更新全部投资因子数据")
                # 获取全部交易日期
                trade_dates = self._get_all_trade_dates()
            
            if not trade_dates:
                self.logger.warning("未获取到交易日期数据")
                return False
            
            success_count = 0
            total_count = len(trade_dates)
            
            for i, trade_date in enumerate(trade_dates, 1):
                self.logger.info(f"处理第 {i}/{total_count} 个交易日: {trade_date}")
                
                if self.calculate_investment_factor(trade_date, top_n):
                    success_count += 1
                else:
                    self.logger.warning(f"计算 {trade_date} 投资因子失败")
            
            self.logger.info(f"投资因子数据更新完成: 成功 {success_count}/{total_count} 天")
            return success_count > 0
            
        except Exception as e:
            self.logger.error(f"更新投资因子数据失败: {e}")
            return False
    
    def _get_trade_dates_in_range(self, start_date: str, end_date: str) -> List[str]:
        """
        获取指定日期范围内的交易日期
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            交易日期列表
        """
        try:
            connection = self._get_db_connection()
            try:
                with connection.cursor() as cursor:
                    sql = """
                    SELECT DISTINCT trade_date 
                    FROM trade_market_stock_fund_flow 
                    WHERE trade_date >= %s AND trade_date <= %s
                    ORDER BY trade_date ASC
                    """
                    cursor.execute(sql, (start_date, end_date))
                    trade_dates = [row[0].strftime('%Y-%m-%d') if hasattr(row[0], 'strftime') else str(row[0]) 
                                 for row in cursor.fetchall()]
                    return trade_dates
            finally:
                connection.close()
        except Exception as e:
            self.logger.error(f"获取指定日期范围交易日期失败: {e}")
            return []
    
    def _get_all_trade_dates(self) -> List[str]:
        """
        获取全部交易日期
        
        Returns:
            交易日期列表
        """
        try:
            connection = self._get_db_connection()
            try:
                with connection.cursor() as cursor:
                    sql = """
                    SELECT DISTINCT trade_date 
                    FROM trade_market_stock_fund_flow 
                    ORDER BY trade_date ASC
                    """
                    cursor.execute(sql)
                    trade_dates = [row[0].strftime('%Y-%m-%d') if hasattr(row[0], 'strftime') else str(row[0]) 
                                 for row in cursor.fetchall()]
                    return trade_dates
            finally:
                connection.close()
        except Exception as e:
            self.logger.error(f"获取全部交易日期失败: {e}")
            return []


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='投资因子计算')
    parser.add_argument('--date', '-d', help='交易日期 (YYYY-MM-DD)，单独计算某一天')
    parser.add_argument('--start-date', help='开始日期 (YYYY-MM-DD)，与--end-date配合使用')
    parser.add_argument('--end-date', help='结束日期 (YYYY-MM-DD)，与--start-date配合使用')
    parser.add_argument('--top', '-t', type=int, default=500, help='获取前N名 (默认500)')
    parser.add_argument('--days', type=int, help='计算最近N天')
    parser.add_argument('--all', action='store_true', help='更新全部数据')
    
    args = parser.parse_args()
    
    try:
        calculator = InvestmentFactorCalculator()
        
        if args.all:
            # 更新全部数据
            success = calculator.update_factor_data(top_n=args.top)
        elif args.start_date and args.end_date:
            # 更新指定日期范围的数据
            success = calculator.update_factor_data(args.start_date, args.end_date, args.top)
        elif args.days:
            # 计算最近N天（保持向后兼容）
            success = calculator.calculate_recent_days(args.days)
        elif args.date:
            # 计算单独某一天（保持向后兼容）
            success = calculator.calculate_investment_factor(args.date, args.top)
        else:
            # 默认更新全部数据
            success = calculator.update_factor_data(top_n=args.top)
        
        if success:
            print("投资因子计算完成")
        else:
            print("投资因子计算失败")
            sys.exit(1)
            
    except Exception as e:
        print(f"程序执行失败: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()