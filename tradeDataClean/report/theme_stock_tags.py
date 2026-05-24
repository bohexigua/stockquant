#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每日更新投研题材成分股标签。

自动标签：
- trend_leader：题材内近 X 个交易日涨幅排名靠前
- former_popular：历史人气股，要求过去 60 天至少 3 次进入东财热榜前 20

人工标签：
- pure_play：题材最正宗，不由脚本自动生成
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from typing import List

import pandas as pd
import pymysql

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

from config import config

logs_dir = os.path.join(project_root, 'logs/tradeDataClean')
os.makedirs(logs_dir, exist_ok=True)

log_filename = os.path.join(logs_dir, f'theme_stock_tags_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


class ThemeStockTagUpdater:
    def __init__(self):
        self.db_config = config.database
        self.connection = pymysql.connect(
            host=self.db_config.host,
            port=self.db_config.port,
            user=self.db_config.user,
            password=self.db_config.password,
            database=self.db_config.database,
            charset=self.db_config.charset,
            autocommit=False,
        )

    def ensure_table(self):
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS trade_research_theme_stock_tag (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
                    trade_date DATE NOT NULL COMMENT '标签日期',
                    theme_code VARCHAR(60) NOT NULL COMMENT '东财题材代码',
                    theme_name VARCHAR(100) DEFAULT NULL COMMENT '题材名称',
                    stock_code VARCHAR(60) NOT NULL COMMENT '股票代码',
                    stock_name VARCHAR(100) DEFAULT NULL COMMENT '股票名称',
                    tag_type VARCHAR(50) NOT NULL COMMENT '标签类型：trend_leader/pure_play/former_popular',
                    tag_source VARCHAR(20) NOT NULL DEFAULT 'auto' COMMENT '标签来源：auto/manual',
                    tag_note VARCHAR(500) DEFAULT NULL COMMENT '标签说明',
                    score DECIMAL(12,4) DEFAULT NULL COMMENT '标签评分',
                    rank_value INT DEFAULT NULL COMMENT '标签排名',
                    window_days INT DEFAULT NULL COMMENT '计算窗口交易日数',
                    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                    UNIQUE KEY uk_tag (trade_date, theme_code, stock_code, tag_type),
                    INDEX idx_trade_theme (trade_date, theme_code),
                    INDEX idx_stock_code (stock_code),
                    INDEX idx_tag_type (tag_type)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='投研题材成分股标签表'
                """
            )
        self.connection.commit()

    def latest_dc_theme_date(self, trade_date: str = '') -> str:
        with self.connection.cursor() as cursor:
            if trade_date:
                cursor.execute(
                    "SELECT MAX(trade_date) FROM trade_market_dc_theme WHERE trade_date <= %s",
                    (trade_date,),
                )
            else:
                cursor.execute("SELECT MAX(trade_date) FROM trade_market_dc_theme")
            row = cursor.fetchone()
            if not row or not row[0]:
                raise RuntimeError("trade_market_dc_theme 没有可用数据")
            return row[0].strftime('%Y-%m-%d')

    def window_start_date(self, trade_date: str, window_days: int) -> str:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT cal_date
                FROM (
                    SELECT cal_date
                    FROM trade_market_calendar
                    WHERE is_open = 1 AND cal_date <= %s
                    ORDER BY cal_date DESC
                    LIMIT %s
                ) recent_days
                ORDER BY cal_date ASC
                LIMIT 1
                """,
                (trade_date, max(1, window_days + 1)),
            )
            row = cursor.fetchone()
            return row[0].strftime('%Y-%m-%d') if row and row[0] else trade_date

    def cleanup_auto_tags(self, trade_date: str):
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM trade_research_theme_stock_tag
                WHERE trade_date = %s AND tag_source = 'auto'
                  AND tag_type IN ('trend_leader', 'former_popular')
                """,
                (trade_date,),
            )

    def build_trend_leaders(self, trade_date: str, start_date: str, window_days: int, top_n: int) -> pd.DataFrame:
        sql = """
        WITH members AS (
            SELECT
                s.trade_date,
                s.theme_code,
                t.name AS theme_name,
                s.ts_code AS stock_code,
                s.name AS stock_name
            FROM trade_market_dc_theme_stock s
            INNER JOIN trade_market_dc_theme t
              ON t.theme_code = s.theme_code AND t.trade_date = s.trade_date
            WHERE s.trade_date = %s
        ),
        prices AS (
            SELECT
                sd.code,
                sd.trade_date,
                sd.close,
                ROW_NUMBER() OVER (PARTITION BY sd.code ORDER BY sd.trade_date ASC) AS rn_asc,
                ROW_NUMBER() OVER (PARTITION BY sd.code ORDER BY sd.trade_date DESC) AS rn_desc
            FROM trade_market_stock_daily sd
            INNER JOIN (SELECT DISTINCT stock_code FROM members) m ON m.stock_code = sd.code
            WHERE sd.trade_date >= %s AND sd.trade_date <= %s
        ),
        stock_perf AS (
            SELECT
                code,
                MAX(CASE WHEN rn_asc = 1 THEN close END) AS start_close,
                MAX(CASE WHEN rn_desc = 1 THEN close END) AS end_close
            FROM prices
            GROUP BY code
        ),
        ranked AS (
            SELECT
                m.trade_date,
                m.theme_code,
                m.theme_name,
                m.stock_code,
                m.stock_name,
                ROUND((sp.end_close / NULLIF(sp.start_close, 0) - 1) * 100, 4) AS score,
                ROW_NUMBER() OVER (
                    PARTITION BY m.theme_code
                    ORDER BY ROUND((sp.end_close / NULLIF(sp.start_close, 0) - 1) * 100, 4) DESC, m.stock_code ASC
                ) AS rank_value
            FROM members m
            INNER JOIN stock_perf sp ON sp.code = m.stock_code
            WHERE sp.start_close IS NOT NULL AND sp.end_close IS NOT NULL
        )
        SELECT *
        FROM ranked
        WHERE rank_value <= %s
        """
        return pd.read_sql(sql, self.connection, params=(trade_date, start_date, trade_date, top_n))

    def build_former_popular(self, trade_date: str, start_date: str, lookback_days: int, top_n: int) -> pd.DataFrame:
        sql = """
        WITH members AS (
            SELECT
                s.trade_date,
                s.theme_code,
                t.name AS theme_name,
                s.ts_code AS stock_code,
                s.name AS stock_name
            FROM trade_market_dc_theme_stock s
            INNER JOIN trade_market_dc_theme t
              ON t.theme_code = s.theme_code AND t.trade_date = s.trade_date
            WHERE s.trade_date = %s
        ),
        hot_stats AS (
            SELECT
                h.code AS stock_code,
                COUNT(*) AS hot_days,
                MIN(h.hot_rank) AS best_hot_rank,
                AVG(CASE WHEN h.hot_rank <= 100 THEN 1 ELSE 0 END) AS top100_rate
            FROM trade_market_dc_stock_hot h
            WHERE h.trade_date >= DATE_SUB(%s, INTERVAL %s DAY)
              AND h.trade_date < %s
                AND h.hot_rank <= 20
            GROUP BY h.code
        ),
        limit_stats AS (
            SELECT
                m.stock_code,
                COUNT(*) AS limit_days,
                MAX(CAST(SUBSTRING_INDEX(m.most_related_theme_strength, '_', 1) AS UNSIGNED)) AS max_strength
            FROM trade_factor_most_related_theme m
            WHERE m.trade_date >= DATE_SUB(%s, INTERVAL %s DAY)
              AND m.trade_date < %s
              AND m.most_related_theme_strength IS NOT NULL
              AND m.most_related_theme_strength != ''
            GROUP BY m.stock_code
        ),
        recent_perf AS (
            SELECT
                sd.code AS stock_code,
                SUM(sd.amount) AS amount_sum,
                SUM(CASE WHEN sd.chg_pct >= 9.8 THEN 1 ELSE 0 END) AS limit_up_days,
                MAX(sd.chg_pct) AS max_chg_pct
            FROM trade_market_stock_daily sd
            WHERE sd.trade_date >= DATE_SUB(%s, INTERVAL %s DAY)
              AND sd.trade_date < %s
            GROUP BY sd.code
        ),
        candidates AS (
            SELECT
                m.trade_date,
                m.theme_code,
                m.theme_name,
                m.stock_code,
                m.stock_name,
                COALESCE(h.hot_days, 0) AS hot_days,
                COALESCE(h.best_hot_rank, 999999) AS best_hot_rank,
                (
                    COALESCE(h.hot_days, 0) * 20
                    + GREATEST(0, 25 - COALESCE(h.best_hot_rank, 999999)) * 2
                    + LOG10(COALESCE(r.amount_sum, 0) + 10)
                ) AS score
            FROM members m
            LEFT JOIN hot_stats h ON h.stock_code = m.stock_code
            LEFT JOIN limit_stats l ON l.stock_code = m.stock_code
            LEFT JOIN recent_perf r ON r.stock_code = m.stock_code
            WHERE
              COALESCE(h.hot_days, 0) >= 3
              AND COALESCE(h.best_hot_rank, 999999) <= 20
        ),
        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY theme_code ORDER BY score DESC, best_hot_rank ASC, stock_code ASC) AS rank_value
            FROM candidates
        )
        SELECT *
        FROM ranked
        WHERE rank_value <= %s
        """
        return pd.read_sql(
            sql,
            self.connection,
            params=(
                trade_date,
                trade_date,
                lookback_days,
                trade_date,
                trade_date,
                lookback_days,
                trade_date,
                trade_date,
                lookback_days,
                trade_date,
                top_n,
            ),
        )

    def insert_tags(self, df: pd.DataFrame, tag_type: str, window_days: int):
        if df.empty:
            logger.info("%s 无候选标签", tag_type)
            return

        sql = """
        INSERT INTO trade_research_theme_stock_tag
        (trade_date, theme_code, theme_name, stock_code, stock_name, tag_type, tag_source,
         tag_note, score, rank_value, window_days)
        VALUES (%s, %s, %s, %s, %s, %s, 'auto', %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          theme_name = VALUES(theme_name),
          stock_name = VALUES(stock_name),
          tag_source = VALUES(tag_source),
          tag_note = VALUES(tag_note),
          score = VALUES(score),
          rank_value = VALUES(rank_value),
          window_days = VALUES(window_days),
          updated_time = CURRENT_TIMESTAMP
        """
        note = '近窗口涨幅题材内靠前' if tag_type == 'trend_leader' else '过去60天至少3次进入东财热榜前20'
        rows = [
            (
                row.trade_date,
                row.theme_code,
                row.theme_name,
                row.stock_code,
                row.stock_name,
                tag_type,
                note,
                float(row.score) if pd.notna(row.score) else None,
                int(row.rank_value) if pd.notna(row.rank_value) else None,
                window_days,
            )
            for row in df.itertuples(index=False)
        ]
        with self.connection.cursor() as cursor:
            cursor.executemany(sql, rows)
        logger.info("%s 入库 %s 条", tag_type, len(rows))

    def update(self, trade_date: str, trend_window: int, trend_top_n: int, popular_lookback: int, popular_top_n: int):
        self.ensure_table()
        actual_date = self.latest_dc_theme_date(trade_date)
        start_date = self.window_start_date(actual_date, trend_window)
        logger.info("更新标签日期：%s，趋势窗口起点：%s", actual_date, start_date)

        self.cleanup_auto_tags(actual_date)
        trend_df = self.build_trend_leaders(actual_date, start_date, trend_window, trend_top_n)
        self.insert_tags(trend_df, 'trend_leader', trend_window)
        self.connection.commit()

        former_df = self.build_former_popular(actual_date, start_date, popular_lookback, popular_top_n)
        self.insert_tags(former_df, 'former_popular', popular_lookback)
        self.connection.commit()

    def close(self):
        if self.connection:
            self.connection.close()


def main():
    parser = argparse.ArgumentParser(description='更新投研题材成分股自动标签')
    parser.add_argument('--date', help='标签日期，默认取最新东财题材日期')
    parser.add_argument('--trend-window', type=int, default=5, help='趋势龙头涨幅窗口交易日数')
    parser.add_argument('--trend-top-n', type=int, default=3, help='每个题材趋势龙头数量')
    parser.add_argument('--popular-lookback', type=int, default=60, help='前人气股历史观察自然日')
    parser.add_argument('--popular-top-n', type=int, default=5, help='每个题材前人气股数量')
    args = parser.parse_args()

    updater = ThemeStockTagUpdater()
    try:
        trade_date = args.date or ''
        updater.update(
            trade_date=trade_date,
            trend_window=args.trend_window,
            trend_top_n=args.trend_top_n,
            popular_lookback=args.popular_lookback,
            popular_top_n=args.popular_top_n,
        )
    except Exception:
        updater.connection.rollback()
        logger.exception("更新题材成分股标签失败")
        raise
    finally:
        updater.close()


if __name__ == '__main__':
    main()
