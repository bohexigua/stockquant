#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
东财概念板块数据清洗模块
从Tushare获取东财概念板块数据和资金流向数据并写入数据库
"""

import sys
import os
import logging
import signal
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


class DCConceptCleaner:
    """东财概念板块数据清洗器"""
    
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
                FROM trade_market_dc_concept 
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
    
    def timeout_handler(self, signum, frame):
        """超时处理函数"""
        raise TimeoutError("API调用超时")
    
    def fetch_concept_data_range(self, start_date: str, end_date: str) -> pd.DataFrame:
        """获取指定日期范围的东财概念板块数据（单日循环获取并立即入库）"""
        try:
            logger.info(f"开始获取东财概念板块数据，日期范围: {start_date} - {end_date}")
            
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
                    
                    logger.info(f"正在获取{trade_date}的东财概念板块数据")
                    
                    # 设置超时处理
                    signal.signal(signal.SIGALRM, self.timeout_handler)
                    
                    try:
                        signal.alarm(20)  # 设置20秒超时
                        
                        # 获取概念板块数据
                        df_concept = self.tushare_api.dc_index(
                            trade_date=trade_date,
                            fields='ts_code,trade_date,name,leading,leading_code,pct_change,leading_pct,total_mv,turnover_rate,up_num,down_num'
                        )
                        
                        # 过滤掉包含NaN值的数据行
                        if not df_concept.empty:
                            df_concept = df_concept.dropna()
                        
                        signal.alarm(0)  # 取消超时
                    except TimeoutError:
                        logger.warning(f"获取{trade_date}概念板块数据超时，跳过")
                        continue
                    except Exception as e:
                        signal.alarm(0)  # 确保取消超时
                        logger.error(f"获取{trade_date}概念板块数据失败: {e}")
                        continue
                    
                    # 获取资金流向数据
                    try:
                        signal.alarm(20)  # 设置20秒超时
                        
                        df_moneyflow = self.tushare_api.moneyflow_ind_dc(
                            trade_date=trade_date,
                            content_type='概念',
                            fields='ts_code,trade_date,name,pct_change,close,net_amount,net_amount_rate,buy_elg_amount,buy_elg_amount_rate,buy_lg_amount,buy_lg_amount_rate,buy_md_amount,buy_md_amount_rate,buy_sm_amount,buy_sm_amount_rate,buy_sm_amount_stock,rank'
                        )
                        
                        # 过滤掉包含NaN值的数据行
                        if not df_moneyflow.empty:
                            df_moneyflow = df_moneyflow.dropna()
                        
                        signal.alarm(0)  # 取消超时
                    except TimeoutError:
                        logger.warning(f"获取{trade_date}资金流向数据超时，跳过")
                        continue
                    except Exception as e:
                        signal.alarm(0)  # 确保取消超时
                        logger.error(f"获取{trade_date}资金流向数据失败: {e}")
                        continue
                    
                    if not df_concept.empty or not df_moneyflow.empty:
                        logger.info(f"成功获取{trade_date}的概念板块数据{len(df_concept)}条，资金流向数据{len(df_moneyflow)}条")
                        
                        # 清洗数据
                        df_cleaned = self.clean_concept_data(df_concept, df_moneyflow)
                        
                        if not df_cleaned.empty:
                            # 立即入库
                            success = self.insert_concept_data(df_cleaned)
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
            logger.error(f"获取东财概念板块数据失败: {e}")
            return pd.DataFrame()
    
    def clean_concept_data(self, df_concept: pd.DataFrame, df_moneyflow: pd.DataFrame) -> pd.DataFrame:
        """清洗东财概念板块数据"""
        try:
            # 如果两个数据框都为空，返回空DataFrame
            if df_concept.empty and df_moneyflow.empty:
                return pd.DataFrame()
            
            # 如果概念板块数据为空，只处理资金流向数据
            if df_concept.empty:
                df_merged = df_moneyflow.copy()
                # 添加缺失的列
                missing_cols = ['leading', 'leading_code', 'leading_pct', 'total_mv', 'turnover_rate', 'up_num', 'down_num']
                for col in missing_cols:
                    df_merged[col] = None
            # 如果资金流向数据为空，只处理概念板块数据
            elif df_moneyflow.empty:
                df_merged = df_concept.copy()
                # 添加缺失的列
                missing_cols = ['net_amount', 'net_amount_rate', 'buy_elg_amount', 'buy_elg_amount_rate', 
                               'buy_lg_amount', 'buy_lg_amount_rate', 'buy_md_amount', 'buy_md_amount_rate',
                               'buy_sm_amount', 'buy_sm_amount_rate', 'buy_sm_amount_stock', 'rank']
                for col in missing_cols:
                    df_merged[col] = None
            else:
                # 合并两个数据框
                df_merged = df_concept.merge(
                    df_moneyflow[['ts_code', 'net_amount', 'net_amount_rate', 'buy_elg_amount', 'buy_elg_amount_rate',
                                 'buy_lg_amount', 'buy_lg_amount_rate', 'buy_md_amount', 'buy_md_amount_rate',
                                 'buy_sm_amount', 'buy_sm_amount_rate', 'buy_sm_amount_stock', 'rank']], 
                    on='ts_code', 
                    how='left'
                )
            
            # 重命名列以匹配数据库表结构
            df_cleaned = df_merged.rename(columns={
                'ts_code': 'code',
                'trade_date': 'trade_date',
                'name': 'name',
                'leading': 'leading_name',
                'leading_code': 'leading_code',
                'pct_change': 'pct_change',
                'leading_pct': 'leading_pct',
                'total_mv': 'total_mv',
                'turnover_rate': 'turnover_rate',
                'up_num': 'up_num',
                'down_num': 'down_num',
                'net_amount': 'net_amount',
                'net_amount_rate': 'net_amount_rate',
                'buy_elg_amount': 'buy_elg_amount',
                'buy_elg_amount_rate': 'buy_elg_amount_rate',
                'buy_lg_amount': 'buy_lg_amount',
                'buy_lg_amount_rate': 'buy_lg_amount_rate',
                'buy_md_amount': 'buy_md_amount',
                'buy_md_amount_rate': 'buy_md_amount_rate',
                'buy_sm_amount': 'buy_sm_amount',
                'buy_sm_amount_rate': 'buy_sm_amount_rate',
                'buy_sm_amount_stock': 'buy_sm_amount_stock',
                'rank': 'rank_value'
            })
            
            # 转换日期格式
            df_cleaned['trade_date'] = pd.to_datetime(df_cleaned['trade_date'], format='%Y%m%d').dt.date
            
            # 处理空值和数据类型
            numeric_columns = ['pct_change', 'leading_pct', 'total_mv', 'turnover_rate', 
                             'net_amount', 'net_amount_rate', 'buy_elg_amount', 'buy_elg_amount_rate',
                             'buy_lg_amount', 'buy_lg_amount_rate', 'buy_md_amount', 'buy_md_amount_rate',
                             'buy_sm_amount', 'buy_sm_amount_rate']
            for col in numeric_columns:
                if col in df_cleaned.columns:
                    df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors='coerce')
            
            # 处理整数列
            int_columns = ['up_num', 'down_num', 'rank_value']
            for col in int_columns:
                if col in df_cleaned.columns:
                    df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors='coerce').fillna(0).astype('int64')
            
            # 添加默认值列
            df_cleaned['volume'] = 0
            df_cleaned['amount'] = 0.0
            
            # 去除没有概念名称的记录
            df_cleaned = df_cleaned.dropna(subset=['name'])
            
            # 分别处理不同类型字段的NaN值
            # 数值字段填充为0
            numeric_fill_columns = ['pct_change', 'leading_pct', 'total_mv', 'turnover_rate', 
                                  'net_amount', 'net_amount_rate', 'buy_elg_amount', 'buy_elg_amount_rate',
                                  'buy_lg_amount', 'buy_lg_amount_rate', 'buy_md_amount', 'buy_md_amount_rate',
                                  'buy_sm_amount', 'buy_sm_amount_rate', 'buy_sm_amount_stock']
            for col in numeric_fill_columns:
                if col in df_cleaned.columns:
                    df_cleaned[col] = df_cleaned[col].fillna(0)
            
            # 字符串字段填充为空字符串
            string_fill_columns = ['leading_name', 'leading_code']
            for col in string_fill_columns:
                if col in df_cleaned.columns:
                    df_cleaned[col] = df_cleaned[col].fillna('')
            
            # 去除重复数据
            df_cleaned = df_cleaned.drop_duplicates(subset=['trade_date', 'code'])
            
            logger.info(f"数据清洗完成，清洗后数据量: {len(df_cleaned)}")
            return df_cleaned
            
        except Exception as e:
            logger.error(f"数据清洗失败: {e}")
            return pd.DataFrame()
    
    def insert_concept_data(self, df: pd.DataFrame) -> bool:
        """将东财概念板块数据插入数据库"""
        if df.empty:
            logger.warning("没有数据需要插入")
            return False
        
        try:
            with self.connection.cursor() as cursor:
                # 构建插入SQL
                sql = """
                INSERT INTO trade_market_dc_concept 
                (code, trade_date, name, leading_name, leading_code, pct_change, leading_pct, 
                 total_mv, turnover_rate, volume, amount, up_num, down_num, net_amount, 
                 net_amount_rate, buy_elg_amount, buy_elg_amount_rate, buy_lg_amount, 
                 buy_lg_amount_rate, buy_md_amount, buy_md_amount_rate, buy_sm_amount, 
                 buy_sm_amount_rate, buy_sm_amount_stock, rank_value)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                leading_name = VALUES(leading_name),
                leading_code = VALUES(leading_code),
                pct_change = VALUES(pct_change),
                leading_pct = VALUES(leading_pct),
                total_mv = VALUES(total_mv),
                turnover_rate = VALUES(turnover_rate),
                volume = VALUES(volume),
                amount = VALUES(amount),
                up_num = VALUES(up_num),
                down_num = VALUES(down_num),
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
                buy_sm_amount_stock = VALUES(buy_sm_amount_stock),
                rank_value = VALUES(rank_value),
                updated_time = CURRENT_TIMESTAMP
                """
                
                # 准备数据
                data_list = []
                for _, row in df.iterrows():
                    data_list.append((
                        row['code'],
                        row['trade_date'],
                        row['name'],
                        row.get('leading_name'),
                        row.get('leading_code'),
                        row.get('pct_change'),
                        row.get('leading_pct'),
                        row.get('total_mv'),
                        row.get('turnover_rate'),
                        row.get('volume', 0),
                        row.get('amount', 0.0),
                        row.get('up_num', 0),
                        row.get('down_num', 0),
                        row.get('net_amount'),
                        row.get('net_amount_rate'),
                        row.get('buy_elg_amount'),
                        row.get('buy_elg_amount_rate'),
                        row.get('buy_lg_amount'),
                        row.get('buy_lg_amount_rate'),
                        row.get('buy_md_amount'),
                        row.get('buy_md_amount_rate'),
                        row.get('buy_sm_amount'),
                        row.get('buy_sm_amount_rate'),
                        row.get('buy_sm_amount_stock'),
                        row.get('rank_value', 0)
                    ))
                
                # 批量插入
                cursor.executemany(sql, data_list)
                
                logger.info(f"成功插入{len(data_list)}条东财概念板块数据到数据库")
                return True
                
        except Exception as e:
            logger.error(f"插入东财概念板块数据失败: {e}")
            return False
    
    def update_concept_data(self) -> bool:
        """更新东财概念板块数据"""
        try:
            start_date, end_date = self.get_trading_date_range()
            logger.info(f"开始更新东财概念板块数据，日期范围: {start_date} - {end_date}")
            
            # fetch_concept_data_range 方法已经包含了数据获取、清洗和入库的完整流程
            result_df = self.fetch_concept_data_range(start_date, end_date)
            
            if not result_df.empty and 'total_count' in result_df.columns:
                total_count = result_df['total_count'].iloc[0]
                if total_count > 0:
                    logger.info(f"东财概念板块数据更新完成，共处理{total_count}条数据")
                    return True
                else:
                    logger.info("所有数据已存在或无新数据需要处理")
                    return True
            else:
                logger.warning("东财概念板块数据更新失败")
                return False
            
        except Exception as e:
            logger.error(f"更新东财概念板块数据失败: {e}")
            return False
    
    def get_concept_data_by_date(self, trade_date: str, limit: int = 10) -> pd.DataFrame:
        """查询指定日期的东财概念板块数据"""
        try:
            with self.connection.cursor() as cursor:
                sql = """
                SELECT trade_date, code, name, leading_name, leading_code, pct_change, 
                       leading_pct, total_mv, turnover_rate, up_num, down_num, 
                       net_amount, net_amount_rate, rank_value
                FROM trade_market_dc_concept
                WHERE trade_date = %s
                ORDER BY rank_value ASC
                LIMIT %s
                """
                cursor.execute(sql, (trade_date, limit))
                results = cursor.fetchall()
                
                if results:
                    df = pd.DataFrame(results, columns=[
                        'trade_date', 'code', 'name', 'leading_name', 'leading_code', 'pct_change',
                        'leading_pct', 'total_mv', 'turnover_rate', 'up_num', 'down_num',
                        'net_amount', 'net_amount_rate', 'rank_value'
                    ])
                    logger.info(f"查询到{len(df)}条东财概念板块数据")
                    return df
                else:
                    logger.info(f"未查询到{trade_date}的东财概念板块数据")
                    return pd.DataFrame()
                    
        except Exception as e:
            logger.error(f"查询东财概念板块数据失败: {e}")
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
        cleaner = DCConceptCleaner()
        
        success = cleaner.update_concept_data()
        
        if success:
            logger.info("东财概念板块数据处理完成")
        else:
            logger.error("东财概念板块数据处理失败")
            
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
    finally:
        if cleaner:
            cleaner.close()


if __name__ == '__main__':
    main()