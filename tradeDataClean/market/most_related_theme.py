#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
个股近期最相关题材清洗脚本
从Tushare开盘啦榜单数据(kpl_list)获取并写入数据库

最相关题材来源：接口字段 lu_desc（涨停原因说明）
全部题材来源：接口字段 theme（多个题材以中文顿号/逗号分隔），清洗为英文逗号分隔字符串
"""

import sys
import os
import logging
import argparse
from datetime import datetime, timedelta
from typing import Optional, List

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

import pandas as pd
import pymysql
import tushare as ts
import re
from config import config

# 创建logs目录
logs_dir = os.path.join(project_root, 'logs/tradeDataClean')
os.makedirs(logs_dir, exist_ok=True)

# 配置日志 - 输出到文件和控制台
log_filename = os.path.join(logs_dir, f'most_related_theme_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class MostRelatedThemeCleaner:
    """个股近期最相关题材数据清洗器"""

    def __init__(self):
        self.db_config = config.database
        self.tushare_token = config.tushare.token
        self.connection = None
        self.tushare_api = None

        self._init_tushare()
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
                sql = (
                    "SELECT cal_date FROM trade_market_calendar "
                    "WHERE is_open = 1 AND cal_date <= CURDATE() "
                    "ORDER BY cal_date DESC LIMIT 1"
                )
                cursor.execute(sql)
                result = cursor.fetchone()
                if result:
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
                sql = (
                    "SELECT MIN(cal_date) as start_date, MAX(cal_date) as end_date "
                    "FROM trade_market_calendar WHERE is_open = 1"
                )
                cursor.execute(sql)
                result = cursor.fetchone()
                if result and result[0] and result[1]:
                    start_date = result[0].strftime('%Y%m%d')
                    end_date = result[1].strftime('%Y%m%d')
                    return start_date, end_date
                else:
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
                sql = (
                    "SELECT cal_date FROM trade_market_calendar "
                    "WHERE is_open = 1 AND cal_date >= STR_TO_DATE(%s, '%%Y%%m%%d') "
                    "AND cal_date <= STR_TO_DATE(%s, '%%Y%%m%%d') ORDER BY cal_date"
                )
                cursor.execute(sql, (start_date, end_date))
                results = cursor.fetchall()
                if results:
                    return [r[0].strftime('%Y%m%d') for r in results]
                else:
                    return []
        except Exception as e:
            logger.error(f"获取交易日期失败: {e}")
            return []

    def _check_date_exists(self, trade_date: str) -> bool:
        """检查指定日期是否已存在数据"""
        try:
            with self.connection.cursor() as cursor:
                sql = (
                    "SELECT COUNT(*) FROM trade_factor_most_related_theme "
                    "WHERE trade_date = STR_TO_DATE(%s, '%%Y%%m%%d')"
                )
                cursor.execute(sql, (trade_date,))
                result = cursor.fetchone()
                return bool(result and result[0] > 0)
        except Exception as e:
            logger.error(f"检查日期{trade_date}是否存在失败: {e}")
            return False

    @staticmethod
    def _sanitize_themes(theme: Optional[str]) -> str:
        """清洗题材字段为英文逗号分隔字符串"""
        if not theme:
            return ''
        # 常见分隔符替换为逗号
        s = str(theme)
        for sep in ['、', '，', ';', '；', '|', '/', '\\']:
            s = s.replace(sep, ',')
        # 去除多余空格与重复逗号
        parts = [p.strip() for p in s.split(',') if p and p.strip()]
        return ','.join(parts)

    @staticmethod
    def _truncate(text: Optional[str], max_len: int = 100) -> str:
        """安全截断文本到指定长度"""
        if text is None:
            return ''
        s = str(text).strip()
        return s[:max_len]

    @staticmethod
    def _status_to_strength(status: Optional[str]) -> Optional[str]:
        if status is None:
            return None
        s = str(status).strip()
        if not s:
            return None
        strength = None
        m_days_boards = re.search(r"(\d+)\s*天\s*(\d+)\s*板", s)
        if m_days_boards:
            try:
                strength = int(m_days_boards.group(2))
            except Exception:
                strength = None
        else:
            if '首板' in s:
                strength = 1
            else:
                m = re.search(r"(\d+)\s*连板", s)
                if m:
                    try:
                        strength = int(m.group(1))
                    except Exception:
                        strength = None
        if strength is None:
            return None
        return f"{strength}_{s}"

    def fetch_most_related_theme_by_date(self, trade_date: str, tag: str = '涨停') -> pd.DataFrame:
        """获取指定交易日的个股最相关题材数据"""
        try:
            logger.info(f"获取{trade_date}的最相关题材数据，tag={tag}")
            df = self.tushare_api.kpl_list(
                trade_date=trade_date,
                tag=tag,
                fields='ts_code,name,trade_date,lu_desc,theme,status'
            )
            if df is None or df.empty:
                logger.info(f"{trade_date}未返回kpl_list数据")
                return pd.DataFrame()

            # 字段重命名与清洗
            df_cleaned = df.rename(columns={
                'ts_code': 'stock_code',
                'name': 'stock_name',
                'trade_date': 'trade_date',
                'lu_desc': 'most_related_theme_name',
                'theme': 'all_themes_name'
            })

            # 转换日期格式
            df_cleaned['trade_date'] = pd.to_datetime(df_cleaned['trade_date'], format='%Y%m%d').dt.date

            # 清洗字段
            df_cleaned['most_related_theme_name'] = df_cleaned['most_related_theme_name'].apply(lambda x: self._truncate(x, 100))
            df_cleaned['all_themes_name'] = df_cleaned['all_themes_name'].apply(self._sanitize_themes)
            strength_series = df['status'].apply(self._status_to_strength)
            df_cleaned['most_related_theme_strength'] = strength_series

            # 去除重复（同日同股）
            df_cleaned = df_cleaned.drop_duplicates(subset=['trade_date', 'stock_code'])

            # 仅保留入库所需字段
            df_cleaned = df_cleaned[['trade_date', 'stock_code', 'stock_name', 'most_related_theme_name', 'most_related_theme_strength', 'all_themes_name']]

            logger.info(f"{trade_date}清洗后记录数: {len(df_cleaned)}")
            return df_cleaned
        except Exception as e:
            logger.error(f"获取或清洗{trade_date}数据失败: {e}")
            return pd.DataFrame()

    def insert_most_related_theme(self, df: pd.DataFrame) -> bool:
        """批量插入/更新到trade_factor_most_related_theme表"""
        if df is None or df.empty:
            logger.warning("没有数据需要插入")
            return False
        try:
            with self.connection.cursor() as cursor:
                sql = (
                    "INSERT INTO trade_factor_most_related_theme "
                    "(trade_date, stock_code, stock_name, most_related_theme_name, most_related_theme_strength, all_themes_name) "
                    "VALUES (%s, %s, %s, %s, %s, %s) "
                    "ON DUPLICATE KEY UPDATE "
                    "stock_name = VALUES(stock_name), "
                    "most_related_theme_name = VALUES(most_related_theme_name), "
                    "most_related_theme_strength = VALUES(most_related_theme_strength), "
                    "all_themes_name = VALUES(all_themes_name), "
                    "updated_time = CURRENT_TIMESTAMP"
                )

                params = [
                    (
                        row['trade_date'],
                        row['stock_code'],
                        row['stock_name'],
                        row['most_related_theme_name'],
                        row['most_related_theme_strength'],
                        row['all_themes_name']
                    )
                    for _, row in df.iterrows()
                ]

                cursor.executemany(sql, params)
                logger.info(f"成功写入/更新 {len(params)} 条记录到trade_factor_most_related_theme")
                return True
        except Exception as e:
            logger.error(f"插入最相关题材数据失败: {e}")
            return False

    def fetch_most_related_theme_range(self, start_date: str, end_date: str, tag: str = '涨停') -> pd.DataFrame:
        """按日期范围获取并入库个股最相关题材数据（逐日处理）"""
        trading_dates = self._get_trading_dates_in_range(start_date, end_date)
        if not trading_dates:
            logger.warning(f"在{start_date}-{end_date}范围内未找到交易日")
            return pd.DataFrame()

        total = 0
        for trade_date in trading_dates:
            try:
                # 若该日已有数据则跳过，避免重复
                if self._check_date_exists(trade_date):
                    logger.info(f"{trade_date}数据已存在，跳过")
                    continue

                df_day = self.fetch_most_related_theme_by_date(trade_date, tag)
                if df_day is None or df_day.empty:
                    continue

                ok = self.insert_most_related_theme(df_day)
                if ok:
                    total += len(df_day)
                    logger.info(f"{trade_date}入库完成，共{len(df_day)}条")
            except Exception as e:
                logger.error(f"处理{trade_date}失败: {e}")
                continue

        logger.info(f"范围{start_date}-{end_date}处理完成，总入库{total}条")
        return pd.DataFrame({'total_count': [total]})

    def close(self):
        if self.connection:
            self.connection.close()
            logger.info("数据库连接已关闭")


def parse_args():
    parser = argparse.ArgumentParser(description='个股近期最相关题材数据清洗入库')
    parser.add_argument('--trade_date', type=str, help='交易日期YYYYMMDD，若提供则仅处理该日')
    parser.add_argument('--start_date', type=str, help='开始日期YYYYMMDD（未提供trade_date时生效）')
    parser.add_argument('--end_date', type=str, help='结束日期YYYYMMDD（未提供trade_date时生效）')
    parser.add_argument('--tag', type=str, default='涨停', help='榜单类型（涨停/炸板/跌停/自然涨停/竞价），默认涨停')
    return parser.parse_args()


def main():
    cleaner = None
    try:
        args = parse_args()
        cleaner = MostRelatedThemeCleaner()

        if args.trade_date:
            df = cleaner.fetch_most_related_theme_by_date(args.trade_date, args.tag)
            if not df.empty:
                cleaner.insert_most_related_theme(df)
                logger.info("处理完成")
            else:
                logger.info("无数据可写入")
        else:
            # 若未提供日期范围，默认按最近一个自然月的交易日处理
            start_date = args.start_date
            end_date = args.end_date
            if not start_date or not end_date:
                start_date, end_date = cleaner.get_trading_date_range()
            cleaner.fetch_most_related_theme_range(start_date, end_date, args.tag)

    except Exception as e:
        logger.error(f"程序执行失败: {e}")
    finally:
        if cleaner:
            cleaner.close()


if __name__ == '__main__':
    main()