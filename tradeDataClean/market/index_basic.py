#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
指数基本信息数据清洗模块
从Tushare获取指数基本信息数据并写入数据库
"""

import sys
import os
import logging
import argparse
from datetime import datetime
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
log_filename = os.path.join(logs_dir, f'index_basic_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class IndexBasicCleaner:
    """指数基本信息数据清洗器"""
    
    def __init__(self):
        """初始化"""
        self.db_config = config.database
        self.tushare_token = config.tushare.token
        self.connection = None
        self.tushare_api = None
        
        # 需要抓取的指数列表
        self.target_indices = [
            "上证指数", "深证指数", "上证50", "沪深300", 
            "中证500", "中证800", "中证1000", "中证2000"
        ]
        
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
    
    def fetch_index_basic(self) -> pd.DataFrame:
        """获取指数基本信息"""
        try:
            logger.info("开始获取指数基本信息")
            
            # 获取所有指数基本信息
            all_indices = []
            
            # 从不同市场获取指数信息
            markets = ['SSE', 'SZSE', 'CSI']  # 上交所、深交所、中证指数
            
            for market in markets:
                try:
                    logger.info(f"获取{market}市场的指数基本信息")
                    df = self.tushare_api.index_basic(
                        market=market,
                        fields='ts_code,name,fullname,market,publisher,category,base_date,base_point,list_date'
                    )
                    
                    if not df.empty:
                        all_indices.append(df)
                        logger.info(f"成功获取{market}市场的{len(df)}条指数信息")
                    else:
                        logger.warning(f"未获取到{market}市场的指数信息")
                        
                except Exception as e:
                    logger.error(f"获取{market}市场指数信息失败: {e}")
                    continue
            
            # 合并所有市场的指数信息
            if all_indices:
                df_all = pd.concat(all_indices, ignore_index=True)
                logger.info(f"共获取到{len(df_all)}条指数基本信息")
                
                # 筛选目标指数
                df_filtered = self._filter_target_indices(df_all)
                
                if not df_filtered.empty:
                    logger.info(f"筛选后剩余{len(df_filtered)}条目标指数信息")
                    return df_filtered
                else:
                    logger.warning("筛选后没有符合条件的指数信息")
                    return pd.DataFrame()
            else:
                logger.warning("未获取到任何指数信息")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"获取指数基本信息失败: {e}")
            return pd.DataFrame()
    
    def _filter_target_indices(self, df: pd.DataFrame) -> pd.DataFrame:
        """筛选目标指数"""
        if df.empty:
            return df
        
        try:
            # 使用精确匹配筛选目标指数
            mask = df['name'].apply(lambda x: x in self.target_indices)
            df_filtered = df[mask].copy()
            
            # 处理上交所和深交所有相同指数的情况，优先保留上交所的数据
            if not df_filtered.empty and len(df_filtered) > 1:
                logger.info("处理重复指数，优先保留上交所数据")
                
                # 按指数名称分组
                grouped = df_filtered.groupby('name')
                
                # 存储处理后的结果
                result_indices = []
                
                for name, group in grouped:
                    if len(group) > 1:
                        # 如果同名指数有多个，优先选择上交所的
                        sse_indices = group[group['market'] == 'SSE']
                        if not sse_indices.empty:
                            result_indices.append(sse_indices.iloc[0])
                            logger.info(f"指数 {name} 存在多个来源，选择上交所数据")
                        else:
                            # 如果没有上交所的，选择第一个
                            result_indices.append(group.iloc[0])
                            logger.info(f"指数 {name} 存在多个来源，但无上交所数据，选择第一个")
                    else:
                        # 只有一个的直接添加
                        result_indices.append(group.iloc[0])
                
                # 将结果转换回DataFrame
                df_filtered = pd.DataFrame(result_indices)
                logger.info(f"处理重复指数后，剩余{len(df_filtered)}条指数信息")
            
            return df_filtered
            
        except Exception as e:
            logger.error(f"筛选目标指数失败: {e}")
            return pd.DataFrame()
    
    def clean_index_basic_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """清洗指数基本信息数据"""
        if df.empty:
            return df
        
        try:
            # 重命名列以匹配数据库表结构
            df_cleaned = df.rename(columns={
                'ts_code': 'code',  # 将ts_code映射为code以匹配数据库字段
                'name': 'name',
                'fullname': 'fullname',
                'market': 'market',
                'publisher': 'publisher',
                'category': 'category',
                'base_date': 'base_date',
                'base_point': 'base_point',
                'list_date': 'list_date'
            })
            
            # 处理空值
            df_cleaned['base_point'] = pd.to_numeric(df_cleaned['base_point'], errors='coerce')
            
            # 去除重复数据
            df_cleaned = df_cleaned.drop_duplicates(subset=['code'])
            
            logger.info(f"数据清洗完成，清洗后数据量: {len(df_cleaned)}")
            return df_cleaned
            
        except Exception as e:
            logger.error(f"数据清洗失败: {e}")
            return pd.DataFrame()
    
    def insert_index_basic_data(self, df: pd.DataFrame) -> bool:
        """将指数基本信息数据插入数据库"""
        if df.empty:
            logger.warning("没有数据需要插入")
            return False
        
        try:
            with self.connection.cursor() as cursor:
                # 构建插入SQL
                sql = """
                INSERT INTO trade_market_index_basic 
                (code, name, market, publisher, category, base_date, base_point, list_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                market = VALUES(market),
                publisher = VALUES(publisher),
                category = VALUES(category),
                base_date = VALUES(base_date),
                base_point = VALUES(base_point),
                list_date = VALUES(list_date),
                updated_time = CURRENT_TIMESTAMP
                """
                
                # 准备数据
                data_list = []
                for _, row in df.iterrows():
                    data_list.append((
                        row['code'],  # 使用重命名后的code字段
                        row['name'],
                        row.get('market', None),
                        row.get('publisher', None),
                        row.get('category', None),
                        row.get('base_date', None),
                        row.get('base_point', None),
                        row.get('list_date', None)
                    ))
                
                # 批量插入
                cursor.executemany(sql, data_list)
                
                logger.info(f"成功插入{len(data_list)}条指数基本信息数据到数据库")
                return True
                
        except Exception as e:
            logger.error(f"插入指数基本信息数据失败: {e}")
            return False
    
    def update_index_basic_data(self) -> bool:
        """更新指数基本信息数据"""
        try:
            logger.info("开始更新指数基本信息数据")
            
            # 获取指数基本信息
            df = self.fetch_index_basic()
            
            if df.empty:
                logger.warning("未获取到指数基本信息数据")
                return False
            
            # 清洗数据
            df_cleaned = self.clean_index_basic_data(df)
            
            if df_cleaned.empty:
                logger.warning("指数基本信息数据清洗后为空")
                return False
            
            # 插入数据库
            success = self.insert_index_basic_data(df_cleaned)
            
            if success:
                logger.info("指数基本信息数据更新完成")
                return True
            else:
                logger.warning("指数基本信息数据更新失败")
                return False
                
        except Exception as e:
            logger.error(f"更新指数基本信息数据失败: {e}")
            return False


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='指数基本信息数据清洗')
    args = parser.parse_args()
    
    try:
        cleaner = IndexBasicCleaner()
        success = cleaner.update_index_basic_data()
        
        if success:
            logger.info("指数基本信息数据清洗完成")
            sys.exit(0)
        else:
            logger.error("指数基本信息数据清洗失败")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()