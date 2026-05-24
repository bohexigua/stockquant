#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
东方财富题材库和题材成分入库。

数据源：
- Tushare dc_concept：东方财富题材库
- Tushare dc_concept_cons：东方财富题材成分
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from typing import List

import pandas as pd
import pymysql
import tushare as ts

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

from config import config

logs_dir = os.path.join(project_root, 'logs/tradeDataClean')
os.makedirs(logs_dir, exist_ok=True)

log_filename = os.path.join(logs_dir, f'dc_theme_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


def db_value(value):
    if pd.isna(value):
        return None
    return value


class DCThemeCleaner:
    def __init__(self):
        self.db_config = config.database
        ts.set_token(config.tushare.token)
        self.pro = ts.pro_api()
        self.connection = pymysql.connect(
            host=self.db_config.host,
            port=self.db_config.port,
            user=self.db_config.user,
            password=self.db_config.password,
            database=self.db_config.database,
            charset=self.db_config.charset,
            autocommit=True,
        )

    def ensure_tables(self):
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS trade_market_dc_theme (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
                    theme_code VARCHAR(60) NOT NULL COMMENT '题材代码',
                    trade_date DATE NOT NULL COMMENT '交易日期',
                    name VARCHAR(100) NOT NULL COMMENT '题材名称',
                    pct_change DECIMAL(10,4) DEFAULT NULL COMMENT '题材涨跌幅',
                    hot DECIMAL(20,4) DEFAULT NULL COMMENT '热度',
                    sort_value INT DEFAULT NULL COMMENT '排名',
                    strength DECIMAL(20,4) DEFAULT NULL COMMENT '强度',
                    z_t_num INT DEFAULT NULL COMMENT '涨停数量',
                    main_change DECIMAL(20,2) DEFAULT NULL COMMENT '主力净流入（元）',
                    lead_stock VARCHAR(100) DEFAULT NULL COMMENT '领涨股票',
                    lead_stock_code VARCHAR(60) DEFAULT NULL COMMENT '领涨股票代码',
                    lead_stock_pct_change DECIMAL(10,4) DEFAULT NULL COMMENT '领涨股票涨跌幅',
                    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                    UNIQUE KEY uk_theme_date (theme_code, trade_date),
                    INDEX idx_trade_date (trade_date),
                    INDEX idx_name_date (name, trade_date),
                    INDEX idx_sort_value (sort_value)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='东方财富题材库'
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS trade_market_dc_theme_stock (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
                    ts_code VARCHAR(60) NOT NULL COMMENT '股票代码',
                    trade_date DATE NOT NULL COMMENT '交易日期',
                    name VARCHAR(100) DEFAULT NULL COMMENT '股票名称',
                    theme_code VARCHAR(60) NOT NULL COMMENT '题材代码',
                    industry_code VARCHAR(60) DEFAULT NULL COMMENT '所属行业代码',
                    industry VARCHAR(100) DEFAULT NULL COMMENT '所属行业',
                    reason TEXT COMMENT '入选原因',
                    hot_num INT DEFAULT NULL COMMENT '热点排行',
                    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                    UNIQUE KEY uk_stock_theme_date (ts_code, theme_code, trade_date),
                    INDEX idx_trade_date (trade_date),
                    INDEX idx_theme_date (theme_code, trade_date),
                    INDEX idx_ts_code (ts_code)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='东方财富题材成分'
                """
            )

    def get_latest_trading_date(self) -> str:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT cal_date
                FROM trade_market_calendar
                WHERE is_open = 1 AND cal_date <= CURDATE()
                ORDER BY cal_date DESC
                LIMIT 1
                """
            )
            row = cursor.fetchone()
            return row[0].strftime('%Y%m%d') if row else datetime.now().strftime('%Y%m%d')

    def get_trading_dates(self, start_date: str, end_date: str) -> List[str]:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT cal_date
                FROM trade_market_calendar
                WHERE is_open = 1
                  AND cal_date >= STR_TO_DATE(%s, '%%Y%%m%%d')
                  AND cal_date <= STR_TO_DATE(%s, '%%Y%%m%%d')
                ORDER BY cal_date
                """,
                (start_date, end_date),
            )
            rows = cursor.fetchall()
            return [row[0].strftime('%Y%m%d') for row in rows]

    def fetch_theme(self, trade_date: str) -> pd.DataFrame:
        fields = [
            'theme_code',
            'trade_date',
            'name',
            'pct_change',
            'hot',
            'sort',
            'strength',
            'z_t_num',
            'main_change',
            'lead_stock',
            'lead_stock_code',
            'lead_stock_pct_change',
        ]
        return self.pro.dc_concept(trade_date=trade_date, fields=','.join(fields))

    def fetch_theme_stock(self, trade_date: str, theme_code: str = '') -> pd.DataFrame:
        fields = [
            'ts_code',
            'trade_date',
            'name',
            'theme_code',
            'industry_code',
            'industry',
            'reason',
            'hot_num',
        ]
        return self.pro.dc_concept_cons(
            trade_date=trade_date,
            theme_code=theme_code,
            fields=','.join(fields),
        )

    def insert_theme(self, df: pd.DataFrame):
        if df.empty:
            logger.warning("题材数据为空，跳过入库")
            return

        df = df.rename(columns={'sort': 'sort_value'}).copy()
        df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d').dt.date
        for col in ['pct_change', 'hot', 'strength', 'main_change', 'lead_stock_pct_change']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        for col in ['sort_value', 'z_t_num']:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
        df = df.where(pd.notnull(df), None)

        sql = """
            INSERT INTO trade_market_dc_theme
            (theme_code, trade_date, name, pct_change, hot, sort_value, strength, z_t_num,
             main_change, lead_stock, lead_stock_code, lead_stock_pct_change)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              name = VALUES(name),
              pct_change = VALUES(pct_change),
              hot = VALUES(hot),
              sort_value = VALUES(sort_value),
              strength = VALUES(strength),
              z_t_num = VALUES(z_t_num),
              main_change = VALUES(main_change),
              lead_stock = VALUES(lead_stock),
              lead_stock_code = VALUES(lead_stock_code),
              lead_stock_pct_change = VALUES(lead_stock_pct_change),
              updated_time = CURRENT_TIMESTAMP
        """
        rows = [
            (
                db_value(row.theme_code),
                db_value(row.trade_date),
                db_value(row.name),
                db_value(row.pct_change),
                db_value(row.hot),
                db_value(row.sort_value),
                db_value(row.strength),
                db_value(row.z_t_num),
                db_value(row.main_change),
                db_value(row.lead_stock),
                db_value(row.lead_stock_code),
                db_value(row.lead_stock_pct_change),
            )
            for row in df.itertuples(index=False)
        ]
        with self.connection.cursor() as cursor:
            cursor.executemany(sql, rows)
        logger.info("题材数据入库完成：%s 条", len(rows))

    def insert_theme_stock(self, df: pd.DataFrame):
        if df.empty:
            logger.warning("题材成分数据为空，跳过入库")
            return

        df = df.copy()
        df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d').dt.date
        df['hot_num'] = pd.to_numeric(df['hot_num'], errors='coerce').astype('Int64')
        df = df.where(pd.notnull(df), None)

        sql = """
            INSERT INTO trade_market_dc_theme_stock
            (ts_code, trade_date, name, theme_code, industry_code, industry, reason, hot_num)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              name = VALUES(name),
              industry_code = VALUES(industry_code),
              industry = VALUES(industry),
              reason = VALUES(reason),
              hot_num = VALUES(hot_num),
              updated_time = CURRENT_TIMESTAMP
        """
        rows = [
            (
                db_value(row.ts_code),
                db_value(row.trade_date),
                db_value(row.name),
                db_value(row.theme_code),
                db_value(row.industry_code),
                db_value(row.industry),
                db_value(row.reason),
                db_value(row.hot_num),
            )
            for row in df.itertuples(index=False)
        ]
        with self.connection.cursor() as cursor:
            cursor.executemany(sql, rows)
        logger.info("题材成分数据入库完成：%s 条", len(rows))

    def update_date(self, trade_date: str):
        logger.info("开始更新 %s 东方财富题材数据", trade_date)
        theme_df = self.fetch_theme(trade_date)
        logger.info("获取题材数据 %s 条", len(theme_df))
        self.insert_theme(theme_df)
        time.sleep(0.3)

        theme_codes = theme_df['theme_code'].dropna().drop_duplicates().tolist() if not theme_df.empty else []
        total_stock_count = 0
        for idx, theme_code in enumerate(theme_codes, 1):
            try:
                stock_df = self.fetch_theme_stock(trade_date, theme_code)
                if not stock_df.empty:
                    self.insert_theme_stock(stock_df)
                    total_stock_count += len(stock_df)
                if idx % 50 == 0 or idx == len(theme_codes):
                    logger.info("题材成分处理进度 %s/%s，累计 %s 条", idx, len(theme_codes), total_stock_count)
                time.sleep(0.12)
            except Exception as exc:
                logger.warning("获取题材 %s 成分失败：%s", theme_code, exc)
        logger.info("获取并入库题材成分数据累计 %s 条", total_stock_count)

    def close(self):
        if self.connection:
            self.connection.close()


def normalize_date(value: str) -> str:
    return value.replace('-', '')


def main():
    parser = argparse.ArgumentParser(description='同步东方财富题材库和题材成分数据')
    parser.add_argument('--date', help='单个交易日，格式 YYYYMMDD 或 YYYY-MM-DD')
    parser.add_argument('--start-date', help='开始交易日，格式 YYYYMMDD 或 YYYY-MM-DD')
    parser.add_argument('--end-date', help='结束交易日，格式 YYYYMMDD 或 YYYY-MM-DD')
    parser.add_argument('--latest', action='store_true', help='同步最近交易日')
    parser.add_argument('--delay', type=float, default=0.5, help='交易日之间的请求间隔秒数')
    args = parser.parse_args()

    cleaner = DCThemeCleaner()
    try:
        cleaner.ensure_tables()
        if args.date:
            dates = [normalize_date(args.date)]
        elif args.start_date and args.end_date:
            dates = cleaner.get_trading_dates(normalize_date(args.start_date), normalize_date(args.end_date))
        else:
            dates = [cleaner.get_latest_trading_date()]

        if not dates:
            logger.warning("没有需要同步的交易日")
            return

        for idx, trade_date in enumerate(dates, 1):
            logger.info("处理交易日 %s/%s：%s", idx, len(dates), trade_date)
            try:
                cleaner.update_date(trade_date)
            except Exception as exc:
                logger.exception("处理交易日 %s 失败：%s", trade_date, exc)
            time.sleep(args.delay)
    finally:
        cleaner.close()


if __name__ == '__main__':
    main()
