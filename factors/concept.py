#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
概念板块因子计算模块
从trade_market_dc_concept表读取概念板块市场数据，
结合trade_stock_concept_relation和trade_market_dc_stock_hot表计算概念板块因子
并将结果写入trade_factor_dc_concept表
"""

import pandas as pd
import pymysql
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import logging
import os
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
log_filename = os.path.join(logs_dir, f'concept_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class ConceptFactorCalculator:
    """
    概念板块因子计算器
    
    主要功能:
    1. 从trade_market_dc_concept表读取概念板块市场数据
    2. 结合trade_stock_concept_relation和trade_market_dc_stock_hot表计算因子
    3. 将计算结果写入trade_factor_dc_concept表
    """
    
    def __init__(self):
        """
        初始化概念板块因子计算器
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
            try:
                self.connection.close()
                logger.info("数据库连接已关闭")
            except Exception as e:
                # 忽略连接已关闭的错误
                if "Already closed" not in str(e):
                    logger.warning(f"关闭数据库连接时出现异常: {e}")
            finally:
                self.connection = None
    
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
    
    def get_concept_market_data(self, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        获取概念板块市场数据
        
        Args:
            start_date: 开始日期，可选
            end_date: 结束日期，可选
            
        Returns:
            概念板块市场数据DataFrame
        """
        try:
            with self.connection.cursor() as cursor:
                # 构建SQL查询
                base_sql = """
                SELECT 
                    code,
                    trade_date,
                    name,
                    pct_change,
                    total_mv,
                    turnover_rate,
                    volume,
                    amount,
                    up_num,
                    down_num,
                    net_amount,
                    net_amount_rate,
                    buy_elg_amount,
                    buy_elg_amount_rate,
                    buy_lg_amount,
                    buy_lg_amount_rate,
                    buy_md_amount,
                    buy_md_amount_rate,
                    buy_sm_amount,
                    buy_sm_amount_rate,
                    rank_value
                FROM trade_market_dc_concept
                """
                
                params = []
                if start_date and end_date:
                    base_sql += " WHERE trade_date BETWEEN %s AND %s"
                    params.extend([start_date, end_date])
                elif start_date:
                    base_sql += " WHERE trade_date >= %s"
                    params.append(start_date)
                elif end_date:
                    base_sql += " WHERE trade_date <= %s"
                    params.append(end_date)
                
                base_sql += " ORDER BY trade_date, code"
                
                cursor.execute(base_sql, params)
                results = cursor.fetchall()
                
                if not results:
                    logger.warning("未获取到概念板块市场数据")
                    return pd.DataFrame()
                
                # 转换为DataFrame
                columns = [
                    'code', 'trade_date', 'name', 'pct_change', 'total_mv', 'turnover_rate',
                    'volume', 'amount', 'up_num', 'down_num', 'net_amount', 'net_amount_rate',
                    'buy_elg_amount', 'buy_elg_amount_rate', 'buy_lg_amount', 'buy_lg_amount_rate',
                    'buy_md_amount', 'buy_md_amount_rate', 'buy_sm_amount', 'buy_sm_amount_rate',
                    'rank_value'
                ]
                
                df = pd.DataFrame(results, columns=columns)
                df['trade_date'] = pd.to_datetime(df['trade_date'])
                
                logger.info(f"获取到 {len(df)} 条概念板块市场数据")
                return df
                
        except Exception as e:
            logger.error(f"获取概念板块市场数据失败: {e}")
            return pd.DataFrame()
    
    def get_concept_stock_hot_rank(self, concept_code: str, trade_date: str) -> float:
        """
        获取概念板块内人气排名TOP10个股的平均排名值
        
        Args:
            concept_code: 概念板块代码
            trade_date: 交易日期
            
        Returns:
            该板块内TOP10个股的平均排名值
        """
        try:
            with self.connection.cursor() as cursor:
                sql = """
                SELECT 
                    CASE 
                        WHEN COUNT(*) < 10 THEN 
                            (SUM(hot_rank) + (10 - COUNT(*)) * 101) / 10
                        ELSE 
                            AVG(hot_rank)
                    END as avg_rank
                FROM (
                    SELECT h.hot_rank
                    FROM trade_market_dc_stock_hot h
                    INNER JOIN trade_stock_concept_relation r ON h.code = r.stock_code
                    WHERE r.concept_sector_code = %s 
                    AND h.trade_date = %s 
                    AND h.hot_rank IS NOT NULL
                    ORDER BY h.hot_rank ASC
                    LIMIT 10
                ) h
                """
                
                cursor.execute(sql, (concept_code, trade_date))
                result = cursor.fetchone()
                
                if result and result[0] is not None:
                    return float(result[0])
                else:
                    return 0.0
                    
        except Exception as e:
            logger.error(f"获取概念板块个股排名失败: {e}")
            return 0.0
    
    def calculate_concept_factors(self, df: pd.DataFrame = None) -> pd.DataFrame:
        """
        计算概念板块因子
        
        Args:
            df: 概念板块市场数据DataFrame，可选，为None时获取全部数据
            
        Returns:
            计算后的因子数据DataFrame
        """
        if df is None:
            df = self.get_concept_market_data()
        
        if df.empty:
            logger.warning("没有数据可供计算因子")
            return pd.DataFrame()

        try:
            # 按概念板块代码分组计算因子
            all_factor_list = []
            unique_codes = df['code'].unique()
            total_concepts = len(unique_codes)
            
            logger.info(f"开始计算 {total_concepts} 个概念板块的因子数据")
            
            for idx, code in enumerate(unique_codes, 1):
                concept_data = df[df['code'] == code].copy()
                concept_data = concept_data.sort_values('trade_date')
                
                # 获取概念板块名称
                concept_name = concept_data['name'].iloc[-1] if not concept_data.empty else ''
                total_dates = len(concept_data)
                
                # 检查该板块是否已有最新数据
                latest_date = self.get_latest_factor_date(code)
                if latest_date:
                    latest_market_date = concept_data['trade_date'].max().strftime('%Y-%m-%d')
                    if latest_date >= latest_market_date:
                        logger.info(f"跳过概念板块 [{idx}/{total_concepts}] {code} ({concept_name})，已有最新数据 (最新日期: {latest_date})")
                        continue
                else:
                    logger.info(f"处理概念板块 [{idx}/{total_concepts}] {code} ({concept_name})，全量计算 {total_dates} 个交易日")
                
                if concept_data.empty:
                    continue
                
                # 为了计算移动平均，需要获取更多历史数据
                all_concept_data = df[df['code'] == code].copy().sort_values('trade_date')
                
                factor_list = []
                for date_idx, (i, row) in enumerate(concept_data.iterrows(), 1):
                    trade_date = row['trade_date']
                    trade_date_str = trade_date.strftime('%Y-%m-%d')
                    
                    # 获取历史数据用于计算移动平均（包含所有历史数据）
                    hist_data = all_concept_data[all_concept_data['trade_date'] <= trade_date]
                    
                    # 计算换手率因子
                    turnover_rate_today = float(row['turnover_rate']) if pd.notna(row['turnover_rate']) else 0.0
                    turnover_rate_5d_avg = self._calculate_avg_turnover_rate(hist_data, 5)
                    turnover_rate_10d_avg = self._calculate_avg_turnover_rate(hist_data, 10)
                    volume_surge_5d = self._calculate_volume_surge(hist_data, 5)
                    
                    # 计算分歧因子（基于资金净流入趋势变化和涨跌股票数量比例变化）
                    divergence_today = self._calculate_divergence(row, hist_data)
                    
                    # 计算排名因子（基于个股人气排名）
                    top10_rank_today = self.get_concept_stock_hot_rank(code, trade_date_str)
                    top10_rank_5d_avg = self._calculate_avg_stock_rank(code, hist_data, 5)
                    top10_rank_10d_avg = self._calculate_avg_stock_rank(code, hist_data, 10)
                    top10_rank_5d_surge = self._calculate_rank_surge(code, hist_data, 5)
                    
                    # 计算主力资金因子
                    main_fund_net_today = float(row['net_amount']) / 10000 if pd.notna(row['net_amount']) else 0.0  # 转换为万元
                    main_fund_net_5d = self._calculate_sum_main_fund(hist_data, 5)
                    main_fund_net_10d = self._calculate_sum_main_fund(hist_data, 10)
                    
                    # 构建因子数据
                    factors = {
                        'trade_date': trade_date,
                        'code': code,
                        'name': concept_name,
                        'turnover_rate_today': turnover_rate_today,
                        'turnover_rate_5d_avg': turnover_rate_5d_avg,
                        'turnover_rate_10d_avg': turnover_rate_10d_avg,
                        'volume_surge_5d': volume_surge_5d,
                        'divergence_today': divergence_today,
                        'top10_rank_today': top10_rank_today,
                        'top10_rank_5d_avg': top10_rank_5d_avg,
                        'top10_rank_10d_avg': top10_rank_10d_avg,
                        'top10_rank_5d_surge': top10_rank_5d_surge,
                        'main_fund_net_today': main_fund_net_today,
                        'main_fund_net_5d': main_fund_net_5d,
                        'main_fund_net_10d': main_fund_net_10d
                    }
                    
                    factor_list.append(factors)
                    
                    # 每处理10个交易日打印一次进度
                    if date_idx % 10 == 0 or date_idx == total_dates:
                        logger.info(f"  - 已处理 {date_idx}/{total_dates} 个交易日")
                
                # 每个板块处理完成后立即插入数据库
                if factor_list:
                    # 转换为DataFrame
                    concept_factor_df = pd.DataFrame(factor_list)
                    
                    # 数据类型转换和清洗
                    concept_factor_df['trade_date'] = pd.to_datetime(concept_factor_df['trade_date'])
                    
                    # 数值列填充NaN为0
                    numeric_columns = [
                        'turnover_rate_today', 'turnover_rate_5d_avg', 'turnover_rate_10d_avg',
                        'top10_rank_today', 'top10_rank_5d_avg', 'top10_rank_10d_avg',
                        'main_fund_net_today', 'main_fund_net_5d', 'main_fund_net_10d'
                    ]
                    for col in numeric_columns:
                        concept_factor_df[col] = pd.to_numeric(concept_factor_df[col], errors='coerce').fillna(0)
                    
                    # 布尔列处理
                    bool_columns = ['volume_surge_5d', 'divergence_today', 'top10_rank_5d_surge']
                    for col in bool_columns:
                        concept_factor_df[col] = concept_factor_df[col].fillna(0).astype(int)
                    
                    # 字符串列填充
                    concept_factor_df['name'] = concept_factor_df['name'].fillna('')
                    
                    # 立即插入数据库
                    success = self.insert_factor_data(concept_factor_df)
                    if success:
                        logger.info(f"概念板块 {code} 处理完成，已插入 {len(concept_factor_df)} 条因子数据")
                        all_factor_list.extend(factor_list)
                    else:
                        logger.error(f"概念板块 {code} 数据插入失败")
                else:
                    logger.info(f"概念板块 {code} 无新数据需要处理")
            
            # 返回所有处理的因子数据
            if all_factor_list:
                final_factor_df = pd.DataFrame(all_factor_list)
                final_factor_df['trade_date'] = pd.to_datetime(final_factor_df['trade_date'])
                logger.info(f"所有板块处理完成，共计算 {len(final_factor_df)} 条概念板块因子数据")
                return final_factor_df
            else:
                logger.info("所有板块均已是最新数据，无需处理")
                return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"计算概念板块因子失败: {e}")
            return pd.DataFrame()
    
    def _calculate_avg_turnover_rate(self, hist_data: pd.DataFrame, days: int) -> float:
        """
        计算指定天数的平均换手率
        
        Args:
            hist_data: 历史数据
            days: 天数
            
        Returns:
            平均换手率
        """
        if len(hist_data) < days:
            return 0.0
        
        recent_data = hist_data.tail(days)
        avg_turnover = recent_data['turnover_rate'].mean()
        return float(avg_turnover) if pd.notna(avg_turnover) else 0.0
    
    def _calculate_volume_surge(self, hist_data: pd.DataFrame, days: int) -> int:
        """
        计算是否有大幅放量（成交量是否显著增加）
        
        Args:
            hist_data: 历史数据
            days: 天数
            
        Returns:
            1表示有大幅放量，0表示没有
        """
        if len(hist_data) < days + 5:  # 需要更多历史数据作为基准
            return 0
        
        try:
            # 最近days天的平均成交量
            recent_volume = hist_data.tail(days)['volume'].mean()
            # 之前days天的平均成交量
            prev_volume = hist_data.iloc[-(days*2):-days]['volume'].mean()
            
            # 如果最近成交量比之前增加50%以上，认为是大幅放量
            if prev_volume > 0 and recent_volume / prev_volume > 1.5:
                return 1
            else:
                return 0
        except:
            return 0
    
    def _calculate_divergence(self, row: pd.Series, hist_data: pd.DataFrame) -> int:
        """
        计算是否出现分歧（基于资金净流入趋势变化和涨跌股票数量比例变化）
        
        Args:
            row: 当日数据行
            hist_data: 历史数据，用于计算趋势变化
            
        Returns:
            正数表示高位分歧，负数表示低位分歧，0表示没有分歧
        """
        divergence_signals = 0
        
        # 信号1：资金净流入趋势变化
        if len(hist_data) >= 3:
            try:
                # 获取最近3天的净流入数据
                recent_net_amounts = hist_data.tail(3)['net_amount'].fillna(0)
                if len(recent_net_amounts) >= 3:
                    # 前两天是正的，今天变负，表明高位分歧（负面信号）
                    if (recent_net_amounts.iloc[-3] > 0 and 
                        recent_net_amounts.iloc[-2] > 0 and 
                        recent_net_amounts.iloc[-1] < 0):
                        return 1  # 高位分歧
                    # 或者前几天是负的，今天变正，表明低位分歧（正面信号）
                    elif (recent_net_amounts.iloc[-3] < 0 and 
                          recent_net_amounts.iloc[-2] < 0 and 
                          recent_net_amounts.iloc[-1] > 0):
                        return -1  # 低位分歧
            except:
                pass
        
        # 信号2：上涨下跌家数比例变化
        up_num = row['up_num'] if pd.notna(row['up_num']) else 0
        down_num = row['down_num'] if pd.notna(row['down_num']) else 0
        total_num = up_num + down_num
        
        if total_num > 0:
            current_up_ratio = up_num / total_num
            
            # 检查历史上涨比例趋势
            if len(hist_data) >= 3:
                hist_up_ratios = []
                for _, hist_row in hist_data.tail(3).iterrows():
                    hist_up = hist_row['up_num'] if pd.notna(hist_row['up_num']) else 0
                    hist_down = hist_row['down_num'] if pd.notna(hist_row['down_num']) else 0
                    hist_total = hist_up + hist_down
                    if hist_total > 0:
                        hist_up_ratios.append(hist_up / hist_total)
                
                if len(hist_up_ratios) >= 2:
                    # 如果上涨比例连续下降超过20%，认为出现高位分歧
                    if (len(hist_up_ratios) >= 3 and 
                        hist_up_ratios[-1] < hist_up_ratios[-2] - 0.2 and
                        hist_up_ratios[-2] < hist_up_ratios[-3] - 0.1):
                        return 1  # 高位分歧
                    # 如果上涨比例连续上升超过20%，认为出现低位分歧
                    elif (len(hist_up_ratios) >= 3 and 
                          hist_up_ratios[-1] > hist_up_ratios[-2] + 0.2 and
                          hist_up_ratios[-2] > hist_up_ratios[-3] + 0.1):
                        return -1  # 低位分歧
        
        # 如果没有分歧信号，返回0
        return 0
        

    
    def _calculate_avg_stock_rank(self, concept_code: str, hist_data: pd.DataFrame, days: int) -> float:
        """
        计算指定天数内个股排名的平均值
        
        Args:
            concept_code: 概念板块代码
            hist_data: 历史数据
            days: 天数
            
        Returns:
            平均排名值
        """
        if len(hist_data) < days:
            return 0.0
        
        recent_dates = hist_data.tail(days)['trade_date']
        rank_sum = 0.0
        valid_days = 0
        
        for trade_date in recent_dates:
            trade_date_str = trade_date.strftime('%Y-%m-%d')
            rank_value = self.get_concept_stock_hot_rank(concept_code, trade_date_str)
            if rank_value > 0:
                rank_sum += rank_value
                valid_days += 1
        
        return rank_sum / valid_days if valid_days > 0 else 0.0
    
    def _calculate_rank_surge(self, concept_code: str, hist_data: pd.DataFrame, days: int) -> int:
        """
        计算排名是否有大幅上升
        
        Args:
            concept_code: 概念板块代码
            hist_data: 历史数据
            days: 天数
            
        Returns:
            1表示有大幅上升，0表示没有
        """
        if len(hist_data) < days * 2:
            return 0
        
        try:
            # 最近days天的平均排名
            recent_avg = self._calculate_avg_stock_rank(concept_code, hist_data.tail(days), days)
            # 之前days天的平均排名
            prev_avg = self._calculate_avg_stock_rank(concept_code, hist_data.iloc[-(days*2):-days], days)
            
            # 排名数值越小越好，如果最近排名比之前提升30%以上，认为是大幅上升
            if prev_avg > 0 and recent_avg > 0 and prev_avg / recent_avg > 1.3:
                return 1
            else:
                return 0
        except:
            return 0
    
    def _calculate_sum_main_fund(self, hist_data: pd.DataFrame, days: int) -> float:
        """
        计算指定天数的主力资金净流入总和
        
        Args:
            hist_data: 历史数据
            days: 天数
            
        Returns:
            主力资金净流入总和（万元）
        """
        if len(hist_data) < days:
            return 0.0
        
        recent_data = hist_data.tail(days)
        sum_fund = recent_data['net_amount'].sum() / 10000  # 转换为万元
        return float(sum_fund) if pd.notna(sum_fund) else 0.0
    
    def check_factor_data_exists(self, trade_date: str, code: str) -> bool:
        """
        检查因子数据是否已存在
        
        Args:
            trade_date: 交易日期
            code: 概念板块代码
            
        Returns:
            True表示已存在，False表示不存在
        """
        try:
            with self.connection.cursor() as cursor:
                sql = """
                SELECT COUNT(*) 
                FROM trade_factor_dc_concept 
                WHERE trade_date = %s AND code = %s
                """
                cursor.execute(sql, (trade_date, code))
                result = cursor.fetchone()
                return result[0] > 0
        except Exception as e:
            logger.error(f"检查因子数据是否存在失败: {e}")
            return False
    
    def get_latest_factor_date(self, code: str) -> str:
        """
        获取指定概念板块的最新因子数据日期
        
        Args:
            code: 概念板块代码
            
        Returns:
            最新因子数据日期，如果没有数据则返回None
        """
        try:
            with self.connection.cursor() as cursor:
                sql = """
                SELECT MAX(trade_date) 
                FROM trade_factor_dc_concept 
                WHERE code = %s
                """
                cursor.execute(sql, (code,))
                result = cursor.fetchone()
                if result and result[0]:
                    return result[0].strftime('%Y-%m-%d')
                return None
        except Exception as e:
            logger.error(f"获取最新因子数据日期失败: {e}")
            return None
    
    def insert_factor_data(self, df: pd.DataFrame) -> bool:
        """
        插入因子数据到数据库
        
        Args:
            df: 因子数据DataFrame
            
        Returns:
            True表示成功，False表示失败
        """
        if df.empty:
            logger.warning("没有数据需要插入")
            return True
        
        try:
            with self.connection.cursor() as cursor:
                # 构建插入SQL
                sql = """
                INSERT INTO trade_factor_dc_concept (
                    trade_date, code, name,
                    turnover_rate_today, turnover_rate_5d_avg, turnover_rate_10d_avg, volume_surge_5d,
                    divergence_today,
                    top10_rank_today, top10_rank_5d_avg, top10_rank_10d_avg, top10_rank_5d_surge,
                    main_fund_net_today, main_fund_net_5d, main_fund_net_10d
                ) VALUES (
                    %s, %s, %s,
                    %s, %s, %s, %s,
                    %s,
                    %s, %s, %s, %s,
                    %s, %s, %s
                ) ON DUPLICATE KEY UPDATE
                    name = VALUES(name),
                    turnover_rate_today = VALUES(turnover_rate_today),
                    turnover_rate_5d_avg = VALUES(turnover_rate_5d_avg),
                    turnover_rate_10d_avg = VALUES(turnover_rate_10d_avg),
                    volume_surge_5d = VALUES(volume_surge_5d),
                    divergence_today = VALUES(divergence_today),
                    top10_rank_today = VALUES(top10_rank_today),
                    top10_rank_5d_avg = VALUES(top10_rank_5d_avg),
                    top10_rank_10d_avg = VALUES(top10_rank_10d_avg),
                    top10_rank_5d_surge = VALUES(top10_rank_5d_surge),
                    main_fund_net_today = VALUES(main_fund_net_today),
                    main_fund_net_5d = VALUES(main_fund_net_5d),
                    main_fund_net_10d = VALUES(main_fund_net_10d),
                    updated_time = CURRENT_TIMESTAMP
                """
                
                # 准备数据
                data_list = []
                for _, row in df.iterrows():
                    data_tuple = (
                        row['trade_date'].strftime('%Y-%m-%d'),
                        row['code'],
                        row['name'],
                        row['turnover_rate_today'],
                        row['turnover_rate_5d_avg'],
                        row['turnover_rate_10d_avg'],
                        row['volume_surge_5d'],
                        row['divergence_today'],
                        row['top10_rank_today'],
                        row['top10_rank_5d_avg'],
                        row['top10_rank_10d_avg'],
                        row['top10_rank_5d_surge'],
                        row['main_fund_net_today'],
                        row['main_fund_net_5d'],
                        row['main_fund_net_10d']
                    )
                    data_list.append(data_tuple)
                
                # 批量插入
                cursor.executemany(sql, data_list)
                
                logger.info(f"成功插入 {len(data_list)} 条概念板块因子数据")
                return True
                
        except Exception as e:
            logger.error(f"插入因子数据失败: {e}")
            return False
    
    def delete_factor_data(self, start_date: str = None, end_date: str = None) -> bool:
        """
        删除指定日期范围的因子数据
        
        Args:
            start_date: 开始日期，可选
            end_date: 结束日期，可选
            
        Returns:
            True表示成功，False表示失败
        """
        try:
            with self.connection.cursor() as cursor:
                if start_date and end_date:
                    sql = "DELETE FROM trade_factor_dc_concept WHERE trade_date BETWEEN %s AND %s"
                    cursor.execute(sql, (start_date, end_date))
                    logger.info(f"删除了 {start_date} 到 {end_date} 的因子数据")
                elif start_date:
                    sql = "DELETE FROM trade_factor_dc_concept WHERE trade_date >= %s"
                    cursor.execute(sql, (start_date,))
                    logger.info(f"删除了 {start_date} 之后的因子数据")
                elif end_date:
                    sql = "DELETE FROM trade_factor_dc_concept WHERE trade_date <= %s"
                    cursor.execute(sql, (end_date,))
                    logger.info(f"删除了 {end_date} 之前的因子数据")
                else:
                    sql = "DELETE FROM trade_factor_dc_concept"
                    cursor.execute(sql)
                    logger.info("删除了所有因子数据")
                
                return True
                
        except Exception as e:
            logger.error(f"删除因子数据失败: {e}")
            return False
    
    def update_factor_data(self, start_date: str = None, end_date: str = None) -> bool:
        """
        更新因子数据
        
        Args:
            start_date: 开始日期，可选
            end_date: 结束日期，可选
            
        Returns:
            True表示成功，False表示失败
        """
        try:
            logger.info("开始更新概念板块因子数据")
            
            # 获取概念板块市场数据
            market_data = self.get_concept_market_data(start_date, end_date)
            if market_data.empty:
                logger.warning("没有获取到市场数据")
                return False
            
            # 计算因子（现在会自动检查已有数据并逐个板块插入）
            factor_data = self.calculate_concept_factors(market_data)
            
            # 无论是否有新数据计算，都认为更新成功
            # 因为calculate_concept_factors已经处理了数据插入
            logger.info("概念板块因子数据更新完成")
            return True
            
        except Exception as e:
            logger.error(f"更新因子数据失败: {e}")
            return False
    
    def get_factor_data(self, start_date: str, end_date: str, code: str = None) -> pd.DataFrame:
        """
        获取因子数据
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            code: 概念板块代码，可选
            
        Returns:
            因子数据DataFrame
        """
        try:
            with self.connection.cursor() as cursor:
                base_sql = """
                SELECT 
                    trade_date, code, name,
                    turnover_rate_today, turnover_rate_5d_avg, turnover_rate_10d_avg, volume_surge_5d,
                    divergence_today,
                    top10_rank_today, top10_rank_5d_avg, top10_rank_10d_avg, top10_rank_5d_surge,
                    main_fund_net_today, main_fund_net_5d, main_fund_net_10d
                FROM trade_factor_dc_concept
                WHERE trade_date BETWEEN %s AND %s
                """
                
                params = [start_date, end_date]
                if code:
                    base_sql += " AND code = %s"
                    params.append(code)
                
                base_sql += " ORDER BY trade_date, code"
                
                cursor.execute(base_sql, params)
                results = cursor.fetchall()
                
                if not results:
                    logger.info("未获取到因子数据")
                    return pd.DataFrame()
                
                columns = [
                    'trade_date', 'code', 'name',
                    'turnover_rate_today', 'turnover_rate_5d_avg', 'turnover_rate_10d_avg', 'volume_surge_5d',
                    'divergence_today',
                    'top10_rank_today', 'top10_rank_5d_avg', 'top10_rank_10d_avg', 'top10_rank_5d_surge',
                    'main_fund_net_today', 'main_fund_net_5d', 'main_fund_net_10d'
                ]
                
                df = pd.DataFrame(results, columns=columns)
                df['trade_date'] = pd.to_datetime(df['trade_date'])
                
                logger.info(f"获取到 {len(df)} 条因子数据")
                return df
                
        except Exception as e:
            logger.error(f"获取因子数据失败: {e}")
            return pd.DataFrame()
    
    def __del__(self):
        """析构函数，确保数据库连接关闭"""
        try:
            self._close_database()
        except Exception:
            # 忽略析构函数中的所有异常
            pass


def main():
    """
    主函数，执行概念板块因子数据更新
    """
    calculator = ConceptFactorCalculator()
    
    try:
        # 更新全部数据
        success = calculator.update_factor_data()
        if success:
            print("概念板块因子数据更新成功")
        else:
            print("概念板块因子数据更新失败")
    except Exception as e:
        print(f"执行失败: {e}")
    finally:
        calculator._close_database()


if __name__ == "__main__":
    main()