#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据加载器
提供数据库数据加载和DataFrame操作功能
"""

import pandas as pd
import pymysql
from datetime import datetime
from typing import Optional, List
import sys
import os

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config import config


class Loader:
    """
    数据加载器类
    提供数据库数据加载和DataFrame操作功能
    """
    
    def __init__(self):
        """
        初始化数据加载器
        """
        self.db_config = config.database
        self.connection = None
    
    def _connect(self):
        """
        建立数据库连接
        """
        try:
            self.connection = pymysql.connect(
                host=self.db_config.host,
                port=self.db_config.port,
                user=self.db_config.user,
                password=self.db_config.password,
                database=self.db_config.database,
                charset=self.db_config.charset
            )
            return True
        except Exception as e:
            print(f"数据库连接失败: {e}")
            return False
    
    def _disconnect(self):
        """
        关闭数据库连接
        """
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def load_data(self, fromdate: str, todate: str, table_name: str) -> Optional[pd.DataFrame]:
        """
        从数据库加载指定时间范围的数据
        
        Args:
            fromdate: 开始日期，格式：'YYYY-MM-DD'
            todate: 结束日期，格式：'YYYY-MM-DD'
            table_name: 表名
            
        Returns:
            pandas.DataFrame: 处理后的数据，如果失败返回None
        """
        if not self._connect():
            return None
        
        try:
            # 构建SQL查询语句
            sql = f"""
            SELECT * FROM {table_name} 
            WHERE trade_date >= %s AND trade_date <= %s
            ORDER BY trade_date
            """
            
            # 执行查询
            df = pd.read_sql(sql, self.connection, params=[fromdate, todate])
            
            # 数据处理
            df = self._process_dataframe(df)
            
            return df
            
        except Exception as e:
            print(f"数据加载失败: {e}")
            return None
        finally:
            self._disconnect()
    
    def _process_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        处理DataFrame：移除不需要的列，重命名列
        
        Args:
            df: 原始DataFrame
            
        Returns:
            pd.DataFrame: 处理后的DataFrame
        """
        # 移除不需要的列
        columns_to_remove = ['id', 'created_time', 'updated_time']
        for col in columns_to_remove:
            if col in df.columns:
                df = df.drop(columns=[col])
        
        # 重命名trade_date列为datetime
        if 'trade_date' in df.columns:
            df = df.rename(columns={'trade_date': 'datetime'})
        
        # 确保datetime列是datetime类型
        if 'datetime' in df.columns:
            df['datetime'] = pd.to_datetime(df['datetime'])
        
        return df
    
    def merge_dataframes(self, df1: pd.DataFrame, df2: pd.DataFrame, 
                        on: Optional[List[str]] = None, 
                        how: str = 'inner') -> pd.DataFrame:
        """
        合并两个DataFrame并去除重复列
        
        Args:
            df1: 第一个DataFrame
            df2: 第二个DataFrame
            on: 合并的键列，如果为None则自动检测相同列名
            how: 合并方式，默认为'inner'
            
        Returns:
            pd.DataFrame: 合并后的DataFrame
        """
        if df1.empty or df2.empty:
            return df1 if not df1.empty else df2
        
        # 如果没有指定合并键，自动检测相同的列名
        if on is None:
            common_columns = list(set(df1.columns) & set(df2.columns))
            if common_columns:
                on = common_columns
            else:
                # 如果没有相同列名，尝试使用datetime列
                if 'datetime' in df1.columns and 'datetime' in df2.columns:
                    on = ['datetime']
                else:
                    raise ValueError("无法找到合并键，请指定on参数")
        
        # 识别重复列（除了合并键之外的相同列名）
        merge_keys = on if isinstance(on, list) else [on]
        duplicate_columns = []
        
        for col in df2.columns:
            if col in df1.columns and col not in merge_keys:
                duplicate_columns.append(col)
        
        # 为重复列添加后缀
        df2_renamed = df2.copy()
        if duplicate_columns:
            rename_dict = {col: f"{col}_y" for col in duplicate_columns}
            df2_renamed = df2_renamed.rename(columns=rename_dict)
        
        # 执行合并
        try:
            merged_df = pd.merge(df1, df2_renamed, on=on, how=how)
            
            # 去除重复列（保留左侧DataFrame的列）
            for col in duplicate_columns:
                col_y = f"{col}_y"
                if col_y in merged_df.columns:
                    merged_df = merged_df.drop(columns=[col_y])
            
            return merged_df
            
        except Exception as e:
            print(f"DataFrame合并失败: {e}")
            return df1
    
    def __enter__(self):
        """
        上下文管理器入口
        """
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        上下文管理器退出
        """
        self._disconnect()


# 使用示例
if __name__ == "__main__":
    # 创建加载器实例
    loader = Loader()
    
    # 加载数据示例
    df = loader.load_data('2025-01-01', '2025-01-31', 'trade_market_stock_daily')
    if df is not None:
        print(f"加载数据成功，共{len(df)}行")
        print(df.head())
    
    # DataFrame合并示例
    # df1 = loader.load_data('2025-01-01', '2025-01-31', 'table1')
    # df2 = loader.load_data('2025-01-01', '2025-01-31', 'table2')
    # merged_df = loader.merge_dataframes(df1, df2)