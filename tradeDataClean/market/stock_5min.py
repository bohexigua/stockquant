#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票5分钟行情数据清洗模块
从Tushare获取股票5分钟行情数据并写入数据库，保留最近30个交易日
"""

import sys
import os
import logging
import time
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
log_filename = os.path.join(logs_dir, f'stock_5min_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class Stock5MinCleaner:
    """股票5分钟行情数据清洗器"""

    def __init__(self):
        self.db_config = config.database
        self.tushare_token = config.tushare.token
        self.connection = None
        self.tushare_api = None
        self.stock_basic_cache = None
        self._init_tushare()
        self._init_database()

    def _init_tushare(self):
        try:
            ts.set_token(self.tushare_token)
            self.tushare_api = ts.pro_api()
            logger.info('Tushare API初始化成功')
        except Exception as e:
            logger.error(f'Tushare API初始化失败: {e}')
            raise

    def _init_database(self):
        try:
            self.connection = pymysql.connect(
                host=self.db_config.host,
                port=self.db_config.port,
                user=self.db_config.user,
                password=self.db_config.password,
                database=self.db_config.database,
                charset=self.db_config.charset,
                autocommit=True,
            )
            logger.info('数据库连接初始化成功')
        except Exception as e:
            logger.error(f'数据库连接初始化失败: {e}')
            raise

    def _get_latest_trading_date(self) -> str:
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    "SELECT cal_date FROM trade_market_calendar WHERE is_open=1 AND cal_date<=CURDATE() ORDER BY cal_date DESC LIMIT 1"
                )
                r = cursor.fetchone()
                return r[0].strftime('%Y%m%d') if r and r[0] else datetime.now().strftime('%Y%m%d')
        except Exception:
            return datetime.now().strftime('%Y%m%d')

    def _get_trading_dates_last_n(self, n: int) -> List[str]:
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    "SELECT cal_date FROM trade_market_calendar WHERE is_open=1 AND cal_date<=CURDATE() ORDER BY cal_date DESC LIMIT %s",
                    (n,),
                )
                rows = cursor.fetchall()
                return [row[0].strftime('%Y-%m-%d') for row in rows]
        except Exception as e:
            logger.error(f'获取最近{n}个交易日失败: {e}')
            return []

    def cleanup_older_than_last_n_days(self, n: int = 30) -> None:
        last_n = self._get_trading_dates_last_n(n)
        if not last_n:
            return
        cutoff = min(last_n)
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM trade_market_stock_5min WHERE trade_date < %s",
                    (cutoff,),
                )
                logger.info(f'已清理{cutoff}之前的5分钟数据')
        except Exception as e:
            logger.error(f'清理5分钟数据失败: {e}')

    def fetch_stock_basic(self) -> pd.DataFrame:
        if self.stock_basic_cache is not None:
            return self.stock_basic_cache
        try:
            df = self.tushare_api.stock_basic(list_status='L', fields='ts_code,name')
            if df.empty:
                return pd.DataFrame()
            self.stock_basic_cache = df
            return df
        except Exception as e:
            logger.error(f'获取股票基础信息失败: {e}')
            return pd.DataFrame()

    def fetch_5min_data_range(self, start_date: str, end_date: str) -> pd.DataFrame:
        try:
            logger.info(f'开始获取股票5分钟行情数据，日期范围: {start_date} - {end_date}')
            df_basic = self.fetch_stock_basic()
            if df_basic.empty:
                logger.error('无法获取股票基础信息')
                return pd.DataFrame()
            total_count = 0
            request_count = 0
            start_time_ts = time.time()
            for _, stock_row in df_basic.iterrows():
                ts_code = stock_row['ts_code']
                stock_name = stock_row['name']
                try:
                    current_time = time.time()
                    elapsed = current_time - start_time_ts
                    if elapsed < 60 and request_count >= 480:
                        wait_time = 60 - elapsed + 1
                        logger.info(f'请求频率限制，等待{wait_time:.1f}秒')
                        time.sleep(wait_time)
                        request_count = 0
                        start_time_ts = time.time()
                    elif elapsed >= 60:
                        request_count = 0
                        start_time_ts = time.time()
                    logger.info(f'正在获取{ts_code}({stock_name})的5分钟行情数据')
                    start_datetime = f"{start_date} 09:00:00"
                    end_datetime = f"{end_date} 15:00:00"
                    df_5min = self.tushare_api.stk_mins(
                        ts_code=ts_code,
                        freq='5min',
                        start_date=start_datetime,
                        end_date=end_datetime,
                    )
                    request_count += 1
                    time.sleep(0.1)
                    if not df_5min.empty:
                        df_5min['trade_date_str'] = pd.to_datetime(df_5min['trade_time']).dt.strftime('%Y%m%d')
                        for trade_date_str, group_df in df_5min.groupby('trade_date_str'):
                            df_cleaned = self.clean_5min_data(group_df, stock_name, trade_date_str)
                            if not df_cleaned.empty:
                                ok = self.insert_5min_data(df_cleaned)
                                if ok:
                                    total_count += len(df_cleaned)
                    else:
                        logger.info(f'{ts_code}在指定时间范围内无5分钟数据')
                except Exception as e:
                    logger.error(f'处理{ts_code}的5分钟数据失败: {e}')
                    continue
            logger.info(f'日期范围{start_date}-{end_date}处理完成，总共入库{total_count}条数据')
            return pd.DataFrame({'total_count': [total_count]})
        except Exception as e:
            logger.error(f'获取股票5分钟行情数据失败: {e}')
            return pd.DataFrame()

    def clean_5min_data(self, df_5min: pd.DataFrame, stock_name: str, trade_date: str) -> pd.DataFrame:
        if df_5min.empty:
            return df_5min
        try:
            df_cleaned = df_5min.copy()
            df_cleaned['name'] = stock_name
            df_cleaned['trade_date'] = pd.to_datetime(trade_date, format='%Y%m%d').date()
            df_cleaned = df_cleaned.rename(columns={
                'ts_code': 'code',
                'trade_time': 'trade_time_str',
                'open': 'open',
                'high': 'high',
                'low': 'low',
                'close': 'close',
                'vol': 'vol',
                'amount': 'amount',
            })
            df_cleaned['trade_time'] = pd.to_datetime(df_cleaned['trade_time_str']).dt.time
            numeric_columns = ['open', 'high', 'low', 'close', 'amount']
            for col in numeric_columns:
                df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors='coerce')
            df_cleaned['vol'] = pd.to_numeric(df_cleaned['vol'], errors='coerce').fillna(0).astype('int64')
            df_cleaned = df_cleaned[[
                'trade_date', 'code', 'name', 'trade_time',
                'open', 'close', 'high', 'low', 'vol', 'amount',
            ]]
            df_cleaned = df_cleaned.drop_duplicates(subset=['trade_date', 'code', 'trade_time'])
            return df_cleaned
        except Exception as e:
            logger.error(f'5分钟数据清洗失败: {e}')
            return pd.DataFrame()

    def insert_5min_data(self, df: pd.DataFrame) -> bool:
        if df.empty:
            logger.warning('没有5分钟数据需要插入')
            return False
        try:
            with self.connection.cursor() as cursor:
                sql = (
                    "INSERT INTO trade_market_stock_5min "
                    "(trade_date, code, name, trade_time, open, close, high, low, vol, amount) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON DUPLICATE KEY UPDATE "
                    "name=VALUES(name), open=VALUES(open), close=VALUES(close), high=VALUES(high), low=VALUES(low), vol=VALUES(vol), amount=VALUES(amount), updated_time=CURRENT_TIMESTAMP"
                )
                data_list = []
                for _, row in df.iterrows():
                    data_list.append((
                        row['trade_date'], row['code'], row['name'], row['trade_time'],
                        row['open'], row['close'], row['high'], row['low'], row['vol'], row['amount'],
                    ))
                cursor.executemany(sql, data_list)
                return True
        except Exception as e:
            logger.error(f'插入5分钟行情数据失败: {e}')
            return False

    def update_5min_data(self) -> bool:
        try:
            # 先清理保留最近30个交易日
            self.cleanup_older_than_last_n_days(30)
            # 获取日期范围：最近交易日为end_date，start_date取最近30个交易日中的最早一个
            latest = self._get_latest_trading_date()
            last_30 = self._get_trading_dates_last_n(30)
            if not last_30:
                start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')
                end_date = latest
            else:
                start_date = datetime.strptime(min(last_30), '%Y-%m-%d').strftime('%Y%m%d')
                end_date = latest
            logger.info(f'开始更新股票5分钟行情数据，日期范围: {start_date} - {end_date}')
            result_df = self.fetch_5min_data_range(start_date, end_date)
            if not result_df.empty and 'total_count' in result_df.columns:
                total_count = result_df['total_count'].iloc[0]
                logger.info(f'股票5分钟行情数据更新完成，共处理{total_count}条数据')
                return True
            logger.warning('股票5分钟行情数据更新失败或无数据')
            return False
        except Exception as e:
            logger.error(f'更新股票5分钟行情数据失败: {e}')
            return False

    def close(self):
        if self.connection:
            self.connection.close()
            logger.info('数据库连接已关闭')


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', type=str, help='起始日期，格式YYYYMMDD')
    parser.add_argument('--end', type=str, help='结束日期，格式YYYYMMDD')
    parser.add_argument('--codes', type=str, default='', help='指定代码，逗号分隔，默认全市场')
    parser.add_argument('--skip-clean', action='store_true', help='是否跳过30日清理')
    args = parser.parse_args()

    cleaner = None
    try:
        cleaner = Stock5MinCleaner()
        if not args.skip_clean:
            cleaner.cleanup_older_than_last_n_days(30)
        if args.start and args.end:
            start_date = args.start
            end_date = args.end
            logger.info(f'按参数更新5分钟数据，日期范围: {start_date} - {end_date}')
            df = cleaner.fetch_5min_data_range(start_date, end_date)
            if not df.empty and 'total_count' in df.columns:
                logger.info(f'处理完成，共入库{df["total_count"].iloc[0]}条')
            else:
                logger.warning('处理完成，无数据或入库失败')
        else:
            ok = cleaner.update_5min_data()
            if ok:
                logger.info('股票5分钟行情数据处理完成')
            else:
                logger.error('股票5分钟行情数据处理失败')
    except Exception as e:
        logger.error(f'程序执行失败: {e}')
    finally:
        if cleaner:
            cleaner.close()


if __name__ == '__main__':
    main()
