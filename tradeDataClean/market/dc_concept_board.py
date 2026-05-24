#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
东方财富概念板块和概念成分入库。

数据源：
- Tushare dc_index：东方财富概念板块
- Tushare dc_member：东方财富板块每日成分
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime
from typing import List

import pandas as pd
import pymysql
import tushare as ts

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

from config import config

logs_dir = os.path.join(project_root, 'logs/tradeDataClean')
os.makedirs(logs_dir, exist_ok=True)

log_filename = os.path.join(logs_dir, f'dc_concept_board_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
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


class DCConceptBoardCleaner:
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
                CREATE TABLE IF NOT EXISTS trade_market_dc_concept (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
                    code VARCHAR(60) NOT NULL COMMENT '概念板块代码',
                    trade_date DATE NOT NULL COMMENT '交易日期',
                    name VARCHAR(100) NOT NULL COMMENT '概念板块名称',
                    leading_name VARCHAR(100) DEFAULT NULL COMMENT '领涨股票名称',
                    leading_code VARCHAR(60) DEFAULT NULL COMMENT '领涨股票代码',
                    pct_change DECIMAL(10,4) DEFAULT NULL COMMENT '涨跌幅',
                    leading_pct DECIMAL(10,4) DEFAULT NULL COMMENT '领涨股票涨跌幅',
                    total_mv DECIMAL(20,2) DEFAULT NULL COMMENT '总市值（万元）',
                    turnover_rate DECIMAL(10,4) DEFAULT NULL COMMENT '换手率',
                    volume BIGINT DEFAULT 0 COMMENT '成交量',
                    amount DECIMAL(20,2) DEFAULT 0.00 COMMENT '成交额',
                    up_num INT DEFAULT NULL COMMENT '上涨家数',
                    down_num INT DEFAULT NULL COMMENT '下跌家数',
                    net_amount DECIMAL(20,2) DEFAULT NULL COMMENT '今日主力净流入净额',
                    net_amount_rate DECIMAL(10,4) DEFAULT NULL COMMENT '今日主力净流入净占比',
                    buy_elg_amount DECIMAL(20,2) DEFAULT NULL COMMENT '今日超大单净流入净额',
                    buy_elg_amount_rate DECIMAL(10,4) DEFAULT NULL COMMENT '今日超大单净流入净占比',
                    buy_lg_amount DECIMAL(20,2) DEFAULT NULL COMMENT '今日大单净流入净额',
                    buy_lg_amount_rate DECIMAL(10,4) DEFAULT NULL COMMENT '今日大单净流入净占比',
                    buy_md_amount DECIMAL(20,2) DEFAULT NULL COMMENT '今日中单净流入净额',
                    buy_md_amount_rate DECIMAL(10,4) DEFAULT NULL COMMENT '今日中单净流入净占比',
                    buy_sm_amount DECIMAL(20,2) DEFAULT NULL COMMENT '今日小单净流入净额',
                    buy_sm_amount_rate DECIMAL(10,4) DEFAULT NULL COMMENT '今日小单净流入净占比',
                    buy_sm_amount_stock VARCHAR(100) DEFAULT NULL COMMENT '今日主力净流入最大股',
                    rank_value INT DEFAULT 0 COMMENT '资金流入排名',
                    idx_type VARCHAR(50) DEFAULT NULL COMMENT '板块类型',
                    level VARCHAR(50) DEFAULT NULL COMMENT '行业层级',
                    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                    UNIQUE KEY uk_code_trade_date (code, trade_date),
                    INDEX idx_trade_date (trade_date),
                    INDEX idx_name_date (name, trade_date),
                    INDEX idx_code (code),
                    INDEX idx_idx_type (idx_type)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='东财概念板块数据表'
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS trade_market_dc_concept_stock (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
                    trade_date DATE NOT NULL COMMENT '交易日期',
                    concept_code VARCHAR(60) NOT NULL COMMENT '概念板块代码',
                    stock_code VARCHAR(60) NOT NULL COMMENT '成分股票代码',
                    stock_name VARCHAR(100) DEFAULT NULL COMMENT '成分股票名称',
                    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                    UNIQUE KEY uk_concept_stock_date (concept_code, stock_code, trade_date),
                    INDEX idx_trade_date (trade_date),
                    INDEX idx_concept_date (concept_code, trade_date),
                    INDEX idx_stock_code (stock_code)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='东方财富概念板块成分'
                """
            )
        self.ensure_column('trade_market_dc_concept', 'idx_type', "VARCHAR(50) DEFAULT NULL COMMENT '板块类型'")
        self.ensure_column('trade_market_dc_concept', 'level', "VARCHAR(50) DEFAULT NULL COMMENT '行业层级'")

    def ensure_column(self, table_name: str, column_name: str, column_def: str):
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = %s
                  AND COLUMN_NAME = %s
                """,
                (table_name, column_name),
            )
            exists = cursor.fetchone()[0] > 0
            if not exists:
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}")
                logger.info("已为 %s 增加字段 %s", table_name, column_name)

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

    def fetch_concepts(self, trade_date: str) -> pd.DataFrame:
        fields = [
            'ts_code',
            'trade_date',
            'name',
            'leading',
            'leading_code',
            'pct_change',
            'leading_pct',
            'total_mv',
            'turnover_rate',
            'up_num',
            'down_num',
            'idx_type',
            'level',
        ]
        return self.pro.dc_index(
            trade_date=trade_date,
            idx_type='概念板块',
            fields=','.join(fields),
        )

    def fetch_members(self, trade_date: str, concept_code: str) -> pd.DataFrame:
        return self.pro.dc_member(
            trade_date=trade_date,
            ts_code=concept_code,
            fields='trade_date,ts_code,con_code,name',
        )

    def insert_concepts(self, df: pd.DataFrame):
        if df.empty:
            logger.warning("概念板块数据为空，跳过入库")
            return

        df = df.rename(columns={'ts_code': 'code', 'leading': 'leading_name'}).copy()
        df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d').dt.date
        for col in ['pct_change', 'leading_pct', 'total_mv', 'turnover_rate']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        for col in ['up_num', 'down_num']:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
        df = df.where(pd.notnull(df), None)

        sql = """
            INSERT INTO trade_market_dc_concept
            (code, trade_date, name, leading_name, leading_code, pct_change, leading_pct,
             total_mv, turnover_rate, up_num, down_num, idx_type, level)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              name = VALUES(name),
              leading_name = VALUES(leading_name),
              leading_code = VALUES(leading_code),
              pct_change = VALUES(pct_change),
              leading_pct = VALUES(leading_pct),
              total_mv = VALUES(total_mv),
              turnover_rate = VALUES(turnover_rate),
              up_num = VALUES(up_num),
              down_num = VALUES(down_num),
              idx_type = VALUES(idx_type),
              level = VALUES(level),
              updated_time = CURRENT_TIMESTAMP
        """
        rows = [
            (
                db_value(row.code),
                db_value(row.trade_date),
                db_value(row.name),
                db_value(row.leading_name),
                db_value(row.leading_code),
                db_value(row.pct_change),
                db_value(row.leading_pct),
                db_value(row.total_mv),
                db_value(row.turnover_rate),
                db_value(row.up_num),
                db_value(row.down_num),
                db_value(row.idx_type),
                db_value(row.level),
            )
            for row in df.itertuples(index=False)
        ]
        with self.connection.cursor() as cursor:
            cursor.executemany(sql, rows)
        logger.info("概念板块数据入库完成：%s 条", len(rows))

    def insert_members(self, df: pd.DataFrame):
        if df.empty:
            return

        df = df.rename(columns={'ts_code': 'concept_code', 'con_code': 'stock_code', 'name': 'stock_name'}).copy()
        df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d').dt.date
        df = df.where(pd.notnull(df), None)

        sql = """
            INSERT INTO trade_market_dc_concept_stock
            (trade_date, concept_code, stock_code, stock_name)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              stock_name = VALUES(stock_name),
              updated_time = CURRENT_TIMESTAMP
        """
        rows = [
            (
                db_value(row.trade_date),
                db_value(row.concept_code),
                db_value(row.stock_code),
                db_value(row.stock_name),
            )
            for row in df.itertuples(index=False)
        ]
        with self.connection.cursor() as cursor:
            cursor.executemany(sql, rows)
        logger.info("概念成分数据入库完成：%s 条", len(rows))

    def update_date(self, trade_date: str, request_delay: float):
        logger.info("开始更新 %s 东方财富概念板块数据", trade_date)
        concept_df = self.fetch_concepts(trade_date)
        logger.info("获取概念板块 %s 条", len(concept_df))
        self.insert_concepts(concept_df)

        concept_codes = concept_df['ts_code'].dropna().drop_duplicates().tolist() if not concept_df.empty else []
        total_member_count = 0
        for idx, concept_code in enumerate(concept_codes, 1):
            try:
                member_df = self.fetch_members(trade_date, concept_code)
                if not member_df.empty:
                    self.insert_members(member_df)
                    total_member_count += len(member_df)
                if idx % 50 == 0 or idx == len(concept_codes):
                    logger.info("概念成分处理进度 %s/%s，累计 %s 条", idx, len(concept_codes), total_member_count)
                time.sleep(request_delay)
            except Exception as exc:
                logger.warning("获取概念 %s 成分失败：%s", concept_code, exc)
        logger.info("获取并入库概念成分数据累计 %s 条", total_member_count)

    def close(self):
        if self.connection:
            self.connection.close()


def normalize_date(value: str) -> str:
    return value.replace('-', '')


def main():
    parser = argparse.ArgumentParser(description='同步东方财富概念板块和概念成分数据')
    parser.add_argument('--date', help='单个交易日，格式 YYYYMMDD 或 YYYY-MM-DD')
    parser.add_argument('--start-date', help='开始交易日，格式 YYYYMMDD 或 YYYY-MM-DD')
    parser.add_argument('--end-date', help='结束交易日，格式 YYYYMMDD 或 YYYY-MM-DD')
    parser.add_argument('--latest', action='store_true', help='同步最近交易日')
    parser.add_argument('--delay', type=float, default=0.12, help='板块成分请求间隔秒数')
    parser.add_argument('--date-delay', type=float, default=0.5, help='交易日之间的请求间隔秒数')
    args = parser.parse_args()

    cleaner = DCConceptBoardCleaner()
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
                cleaner.update_date(trade_date, args.delay)
            except Exception as exc:
                logger.exception("处理交易日 %s 失败：%s", trade_date, exc)
            time.sleep(args.date_delay)
    finally:
        cleaner.close()


if __name__ == '__main__':
    main()
