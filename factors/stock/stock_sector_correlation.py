#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
个股板块相似度计算模块
基于K线特征计算个股与板块的相似度，并存入相关性因子表
"""

import os
import sys
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
import pymysql
from dataclasses import dataclass

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config import config


@dataclass
class KLineFeatures:
    """K线特征数据类"""
    direction: int  # 涨跌方向：1=涨，-1=跌，0=平
    body_ratio: float  # 实体长度占比：|C-O|/(H-L)
    upper_shadow_ratio: float  # 上影线长度占比：(H-max(O,C))/(H-L)
    lower_shadow_ratio: float  # 下影线长度占比：(min(O,C)-L)/(H-L)


class StockSectorCorrelationCalculator:
    """个股板块相似度计算器"""
    
    def __init__(self):
        """
        初始化相似度计算器
        """
        self.db_config = config.database
        self.logger = self._setup_logger()
        self.window_size = 5  # 5日窗口
    
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
                autocommit=True
            )
            return connection
        except Exception as e:
            self.logger.error(f"数据库连接失败: {e}")
            raise
    
    def get_latest_trade_date(self) -> Optional[str]:
        """获取最新交易日期"""
        try:
            connection = self._get_db_connection()
            with connection.cursor() as cursor:
                sql = """
                SELECT MAX(trade_date) as latest_date 
                FROM trade_market_stock_daily 
                WHERE trade_date <= CURDATE()
                """
                cursor.execute(sql)
                result = cursor.fetchone()
                
                if result and result[0]:
                    return result[0].strftime('%Y-%m-%d')
                return None
                
        except Exception as e:
            self.logger.error(f"获取最新交易日期失败: {e}")
            return None
        finally:
            if 'connection' in locals():
                connection.close()
    
    def get_stock_theme_relations(self) -> Dict[str, List[str]]:
        """获取股票和题材板块的关联关系"""
        try:
            connection = self._get_db_connection()
            
            sql = """
            SELECT stock_code, theme_sector_code
            FROM trade_stock_theme_relation
            ORDER BY stock_code, theme_sector_code
            """
            
            df = pd.read_sql(sql, connection)
            
            if df.empty:
                self.logger.warning("未找到股票板块关联数据")
                return {}
            
            # 构建股票到板块列表的映射
            stock_theme_map = {}
            for _, row in df.iterrows():
                stock_code = row['stock_code']
                theme_code = row['theme_sector_code']
                
                if stock_code not in stock_theme_map:
                    stock_theme_map[stock_code] = []
                stock_theme_map[stock_code].append(theme_code)
            
            self.logger.info(f"获取到 {len(stock_theme_map)} 只股票的板块关联关系")
            return stock_theme_map
            
        except Exception as e:
            self.logger.error(f"获取股票板块关联关系失败: {e}")
            return {}
        finally:
            if 'connection' in locals():
                connection.close()
    
    def get_stock_data_for_window(self, trade_date: str, window_size: int = 5) -> pd.DataFrame:
        """获取指定窗口期内的个股数据"""
        try:
            connection = self._get_db_connection()
            
            sql = """
            SELECT trade_date, code, name, open, high, low, close, pre_close, chg_pct, vol, amount
            FROM trade_market_stock_daily 
            WHERE trade_date <= %s 
            ORDER BY trade_date DESC, code 
            LIMIT %s
            """
            
            # 获取足够的数据以确保每只股票都有window_size天的数据
            limit = window_size * 50000  # 假设最多50000只股票
            
            df = pd.read_sql(sql, connection, params=[trade_date, limit])
            
            if df.empty:
                self.logger.warning(f"未找到交易日期 {trade_date} 的个股数据")
                return pd.DataFrame()
            
            # 按股票代码分组，确保每只股票都有足够的历史数据
            df_filtered = []
            for code, group in df.groupby('code'):
                if len(group) >= window_size:
                    df_filtered.append(group.head(window_size))
            
            if df_filtered:
                result_df = pd.concat(df_filtered, ignore_index=True)
                self.logger.info(f"获取到 {len(result_df)} 条个股数据，涉及 {result_df['code'].nunique()} 只股票")
                return result_df
            else:
                self.logger.warning("没有股票有足够的历史数据")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"获取个股数据失败: {e}")
            return pd.DataFrame()
        finally:
            if 'connection' in locals():
                connection.close()
    
    def get_sector_data_for_window(self, trade_date: str, window_size: int = 5) -> pd.DataFrame:
        """获取指定窗口期内的板块数据"""
        try:
            connection = self._get_db_connection()
            
            sql = """
            SELECT trade_date, code, name, open, high, low, close, pct_change, turnover_rate
            FROM trade_market_theme 
            WHERE trade_date <= %s 
            ORDER BY trade_date DESC, code 
            LIMIT %s
            """
            
            # 获取足够的数据以确保每个板块都有window_size天的数据
            limit = window_size * 1000  # 假设最多1000个板块
            
            df = pd.read_sql(sql, connection, params=[trade_date, limit])
            
            if df.empty:
                self.logger.warning(f"未找到交易日期 {trade_date} 的板块数据")
                return pd.DataFrame()
            
            # 按板块代码分组，确保每个板块都有足够的历史数据
            df_filtered = []
            for code, group in df.groupby('code'):
                if len(group) >= window_size:
                    df_filtered.append(group.head(window_size))
            
            if df_filtered:
                result_df = pd.concat(df_filtered, ignore_index=True)
                self.logger.info(f"获取到 {len(result_df)} 条板块数据，涉及 {result_df['code'].nunique()} 个板块")
                return result_df
            else:
                self.logger.warning("没有板块有足够的历史数据")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"获取板块数据失败: {e}")
            return pd.DataFrame()
        finally:
            if 'connection' in locals():
                connection.close()
    
    def extract_kline_features(self, open_price: float, high_price: float, 
                              low_price: float, close_price: float) -> KLineFeatures:
        """
        提取K线特征
        
        Args:
            open_price: 开盘价
            high_price: 最高价
            low_price: 最低价
            close_price: 收盘价
            
        Returns:
            KLineFeatures: K线特征对象
        """
        try:
            # 处理异常数据
            if pd.isna([open_price, high_price, low_price, close_price]).any():
                return KLineFeatures(0, 0.0, 0.0, 0.0)
            
            if high_price <= low_price or high_price <= 0 or low_price <= 0:
                return KLineFeatures(0, 0.0, 0.0, 0.0)
            
            # 计算涨跌方向
            if close_price > open_price:
                direction = 1  # 涨
            elif close_price < open_price:
                direction = -1  # 跌
            else:
                direction = 0  # 平
            
            # 计算价格范围
            price_range = high_price - low_price
            if price_range == 0:
                return KLineFeatures(direction, 0.0, 0.0, 0.0)
            
            # 计算实体长度占比
            body_length = abs(close_price - open_price)
            body_ratio = body_length / price_range
            
            # 计算上影线长度占比
            upper_shadow_length = high_price - max(open_price, close_price)
            upper_shadow_ratio = upper_shadow_length / price_range
            
            # 计算下影线长度占比
            lower_shadow_length = min(open_price, close_price) - low_price
            lower_shadow_ratio = lower_shadow_length / price_range
            
            return KLineFeatures(
                direction=direction,
                body_ratio=body_ratio,
                upper_shadow_ratio=upper_shadow_ratio,
                lower_shadow_ratio=lower_shadow_ratio
            )
            
        except Exception as e:
            self.logger.error(f"提取K线特征失败: {e}")
            return KLineFeatures(0, 0.0, 0.0, 0.0)
    
    def calculate_cosine_similarity(self, features1: List[KLineFeatures], 
                                   features2: List[KLineFeatures]) -> float:
        """
        计算两个特征序列的余弦相似度
        
        Args:
            features1: 第一个特征序列
            features2: 第二个特征序列
            
        Returns:
            float: 余弦相似度值，范围[-1, 1]
        """
        try:
            if len(features1) != len(features2) or len(features1) == 0:
                return 0.0
            
            # 将特征转换为向量
            vector1 = []
            vector2 = []
            
            for f1, f2 in zip(features1, features2):
                vector1.extend([f1.direction, f1.body_ratio, f1.upper_shadow_ratio, f1.lower_shadow_ratio])
                vector2.extend([f2.direction, f2.body_ratio, f2.upper_shadow_ratio, f2.lower_shadow_ratio])
            
            vector1 = np.array(vector1)
            vector2 = np.array(vector2)
            
            # 计算余弦相似度
            dot_product = np.dot(vector1, vector2)
            norm1 = np.linalg.norm(vector1)
            norm2 = np.linalg.norm(vector2)
            
            if norm1 == 0 or norm2 == 0:
                return 0.0
            
            similarity = dot_product / (norm1 * norm2)
            
            # 确保结果在[-1, 1]范围内
            similarity = max(-1.0, min(1.0, similarity))
            
            return similarity
            
        except Exception as e:
            self.logger.error(f"计算余弦相似度失败: {e}")
            return 0.0
    
    def calculate_correlation_for_date(self, trade_date: str) -> List[Dict[str, Any]]:
        """
        计算指定日期的个股板块相关性
        
        Args:
            trade_date: 交易日期
            
        Returns:
            List[Dict]: 相关性数据列表
        """
        try:
            self.logger.info(f"开始计算 {trade_date} 的个股板块相关性")
            
            # 获取股票-板块关联关系
            stock_theme_relations = self.get_stock_theme_relations()
            if not stock_theme_relations:
                self.logger.warning("未获取到股票板块关联关系，无法计算相关性")
                return []
            
            # 获取个股和板块数据
            stock_df = self.get_stock_data_for_window(trade_date, self.window_size)
            sector_df = self.get_sector_data_for_window(trade_date, self.window_size)
            
            if stock_df.empty or sector_df.empty:
                self.logger.warning(f"缺少 {trade_date} 的数据")
                return []
            
            correlation_data = []
            
            # 按股票分组计算特征
            stock_features = {}
            for stock_code, stock_group in stock_df.groupby('code'):
                if len(stock_group) < self.window_size:
                    continue
                
                # 按日期排序
                stock_group = stock_group.sort_values('trade_date')
                
                # 提取K线特征
                features = []
                for _, row in stock_group.iterrows():
                    feature = self.extract_kline_features(
                        row['open'], row['high'], row['low'], row['close']
                    )
                    features.append(feature)
                
                stock_features[stock_code] = {
                    'name': stock_group.iloc[-1]['name'],
                    'features': features,  # 保存所有日期的特征用于计算每日相似度
                    'today_feature': features[-1],  # 当日特征
                }
            
            # 按板块分组计算特征
            sector_features = {}
            for sector_code, sector_group in sector_df.groupby('code'):
                if len(sector_group) < self.window_size:
                    continue
                
                # 按日期排序
                sector_group = sector_group.sort_values('trade_date')
                
                # 提取K线特征
                features = []
                for _, row in sector_group.iterrows():
                    feature = self.extract_kline_features(
                        row['open'], row['high'], row['low'], row['close']
                    )
                    features.append(feature)
                
                sector_features[sector_code] = {
                    'name': sector_group.iloc[-1]['name'],
                    'features': features,  # 保存所有日期的特征用于计算每日相似度
                    'today_feature': features[-1],  # 当日特征
                }
            
            # 计算每个股票与其关联板块的相似度
            for stock_code, stock_data in stock_features.items():
                # 获取该股票关联的板块列表
                related_sectors = stock_theme_relations.get(stock_code, [])
                if not related_sectors:
                    self.logger.debug(f"股票 {stock_code} 没有关联的板块")
                    continue
                
                # 只计算与关联板块的相似度
                for sector_code in related_sectors:
                    if sector_code not in sector_features:
                        self.logger.debug(f"板块 {sector_code} 没有数据，跳过")
                        continue
                    
                    sector_data = sector_features[sector_code]
                    
                    # 计算当日相似度
                    similarity_today = self.calculate_cosine_similarity(
                        [stock_data['today_feature']], 
                        [sector_data['today_feature']]
                    )
                    
                    # 计算近3日每日相似度，然后求加权平均
                    stock_features_3d = stock_data['features'][-3:]  # 最近3日特征
                    sector_features_3d = sector_data['features'][-3:]  # 最近3日特征
                    
                    daily_similarities_3d = []
                    for i in range(len(stock_features_3d)):
                        daily_sim = self.calculate_cosine_similarity(
                            [stock_features_3d[i]], 
                            [sector_features_3d[i]]
                        )
                        daily_similarities_3d.append(daily_sim)
                    
                    similarity_3d = self._calculate_weighted_average_similarity(daily_similarities_3d)
                    
                    # 计算近5日每日相似度，然后求加权平均
                    stock_features_5d = stock_data['features']  # 所有5日特征
                    sector_features_5d = sector_data['features']  # 所有5日特征
                    
                    daily_similarities_5d = []
                    for i in range(len(stock_features_5d)):
                        daily_sim = self.calculate_cosine_similarity(
                            [stock_features_5d[i]], 
                            [sector_features_5d[i]]
                        )
                        daily_similarities_5d.append(daily_sim)
                    
                    similarity_5d = self._calculate_weighted_average_similarity(daily_similarities_5d)
                    
                    correlation_data.append({
                        'trade_date': trade_date,
                        'stock_code': stock_code,
                        'stock_name': stock_data['name'],
                        'sector_code': sector_code,
                        'sector_name': sector_data['name'],
                        'cosine_similarity_today': round(similarity_today, 6),
                        'cosine_similarity_3d': round(similarity_3d, 6),
                        'cosine_similarity_5d': round(similarity_5d, 6)
                    })
            
            self.logger.info(f"计算完成，共生成 {len(correlation_data)} 条相关性数据")
            return correlation_data
            
        except Exception as e:
            self.logger.error(f"计算相关性失败: {e}")
            return []
    
    def _calculate_average_features(self, features: List[KLineFeatures]) -> KLineFeatures:
        """计算特征的平均值"""
        if not features:
            return KLineFeatures(0, 0.0, 0.0, 0.0)
        
        avg_direction = sum(f.direction for f in features) / len(features)
        avg_body_ratio = sum(f.body_ratio for f in features) / len(features)
        avg_upper_shadow_ratio = sum(f.upper_shadow_ratio for f in features) / len(features)
        avg_lower_shadow_ratio = sum(f.lower_shadow_ratio for f in features) / len(features)
        
        # 方向取整数
        avg_direction = 1 if avg_direction > 0.33 else (-1 if avg_direction < -0.33 else 0)
        
        return KLineFeatures(
            direction=avg_direction,
            body_ratio=avg_body_ratio,
            upper_shadow_ratio=avg_upper_shadow_ratio,
            lower_shadow_ratio=avg_lower_shadow_ratio
        )
    
    def _calculate_weighted_average_similarity(self, similarities: List[float]) -> float:
        """
        计算加权平均相似度，越靠近当日权重越高
        
        Args:
            similarities: 相似度列表，按时间顺序排列（最新的在最后）
            
        Returns:
            加权平均相似度
        """
        if not similarities:
            return 0.0
        
        n = len(similarities)
        if n == 1:
            return similarities[0]
        
        # 生成权重：越靠近当日权重越高
        # 使用线性递增权重：1, 2, 3, ..., n
        weights = list(range(1, n + 1))
        total_weight = sum(weights)
        
        # 计算加权平均
        weighted_sum = sum(sim * weight for sim, weight in zip(similarities, weights))
        return weighted_sum / total_weight
    
    def clear_old_data(self, current_date: str) -> bool:
        """清除前2日的数据，保留近两日的数据"""
        try:
            connection = self._get_db_connection()
            with connection.cursor() as cursor:
                # 计算前2日的日期
                current_dt = datetime.strptime(current_date, '%Y-%m-%d')
                cutoff_date = current_dt - timedelta(days=2)
                cutoff_date_str = cutoff_date.strftime('%Y-%m-%d')
                
                # 删除前2日的数据（保留近两日）
                sql = "DELETE FROM trade_factor_stock_sector_correlation WHERE trade_date < %s"
                cursor.execute(sql, [cutoff_date_str])
                deleted_count = cursor.rowcount
                
                if deleted_count > 0:
                    self.logger.info(f"清除了 {deleted_count} 条 {cutoff_date_str} 之前的历史数据，保留近两日数据")
                else:
                    self.logger.info("没有需要清除的历史数据")
                
                return True
                
        except Exception as e:
            self.logger.error(f"清除历史数据失败: {e}")
            return False
        finally:
            if 'connection' in locals():
                connection.close()
    
    def insert_correlation_data(self, correlation_data: List[Dict[str, Any]]) -> bool:
        """插入相关性数据到数据库"""
        if not correlation_data:
            self.logger.warning("没有相关性数据需要插入")
            return True
        
        try:
            connection = self._get_db_connection()
            with connection.cursor() as cursor:
                sql = """
                INSERT INTO trade_factor_stock_sector_correlation 
                (trade_date, stock_code, stock_name, sector_code, sector_name, 
                 cosine_similarity_today, cosine_similarity_3d, cosine_similarity_5d)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                stock_name = VALUES(stock_name),
                sector_name = VALUES(sector_name),
                cosine_similarity_today = VALUES(cosine_similarity_today),
                cosine_similarity_3d = VALUES(cosine_similarity_3d),
                cosine_similarity_5d = VALUES(cosine_similarity_5d),
                updated_time = CURRENT_TIMESTAMP
                """
                
                # 准备数据
                data_list = []
                for item in correlation_data:
                    data_list.append([
                        item['trade_date'],
                        item['stock_code'],
                        item['stock_name'],
                        item['sector_code'],
                        item['sector_name'],
                        item['cosine_similarity_today'],
                        item['cosine_similarity_3d'],
                        item['cosine_similarity_5d']
                    ])
                
                # 批量插入
                cursor.executemany(sql, data_list)
                
                self.logger.info(f"成功插入 {len(data_list)} 条相关性数据")
                return True
                
        except Exception as e:
            self.logger.error(f"插入相关性数据失败: {e}")
            return False
        finally:
            if 'connection' in locals():
                connection.close()
    
    def calculate_correlation_factor(self, trade_date: str = None) -> bool:
        """
        计算个股板块相关性因子
        
        Args:
            trade_date: 交易日期，默认为最新交易日
            
        Returns:
            bool: 计算是否成功
        """
        try:
            if trade_date is None:
                # trade_date = '2025-11-03'
                trade_date = self.get_latest_trade_date()
                if trade_date is None:
                    self.logger.error("无法获取最新交易日期")
                    return False
            
            self.logger.info(f"开始计算 {trade_date} 的个股板块相关性因子")
            
            # 清除历史数据，保留近两日数据
            if not self.clear_old_data(trade_date):
                self.logger.error("清除历史数据失败")
                return False
            
            # 计算相关性
            correlation_data = self.calculate_correlation_for_date(trade_date)
            
            if not correlation_data:
                self.logger.warning(f"没有计算出 {trade_date} 的相关性数据")
                return False
            
            # 插入数据
            if not self.insert_correlation_data(correlation_data):
                self.logger.error("插入相关性数据失败")
                return False
            
            self.logger.info(f"成功计算并保存 {trade_date} 的个股板块相关性因子")
            return True
            
        except Exception as e:
            self.logger.error(f"计算相关性因子失败: {e}")
            return False
    
    def calculate_recent_days(self, days: int = 5) -> bool:
        """
        计算最近几天的相关性因子
        
        Args:
            days: 天数
            
        Returns:
            bool: 计算是否成功
        """
        try:
            latest_date = self.get_latest_trade_date()
            if latest_date is None:
                self.logger.error("无法获取最新交易日期")
                return False
            
            # 获取最近的交易日期列表
            trade_dates = self._get_recent_trade_dates(latest_date, days)
            
            success_count = 0
            for trade_date in trade_dates:
                if self.calculate_correlation_factor(trade_date):
                    success_count += 1
                else:
                    self.logger.warning(f"计算 {trade_date} 的相关性因子失败")
            
            self.logger.info(f"成功计算了 {success_count}/{len(trade_dates)} 天的相关性因子")
            return success_count > 0
            
        except Exception as e:
            self.logger.error(f"计算最近几天的相关性因子失败: {e}")
            return False
    
    def _get_recent_trade_dates(self, end_date: str, days: int) -> List[str]:
        """获取最近的交易日期列表"""
        try:
            connection = self._get_db_connection()
            with connection.cursor() as cursor:
                sql = """
                SELECT DISTINCT trade_date 
                FROM trade_market_stock_daily 
                WHERE trade_date <= %s 
                ORDER BY trade_date DESC 
                LIMIT %s
                """
                cursor.execute(sql, [end_date, days])
                results = cursor.fetchall()
                
                return [result[0].strftime('%Y-%m-%d') for result in results]
                
        except Exception as e:
            self.logger.error(f"获取交易日期列表失败: {e}")
            return []
        finally:
            if 'connection' in locals():
                connection.close()


def main():
    """
    主函数
    """
    calculator = StockSectorCorrelationCalculator()
    
    # 计算最新交易日的相关性因子
    success = calculator.calculate_correlation_factor()
    
    if success:
        print("个股板块相关性因子计算完成")
    else:
        print("个股板块相关性因子计算失败")
        sys.exit(1)


if __name__ == '__main__':
    main()