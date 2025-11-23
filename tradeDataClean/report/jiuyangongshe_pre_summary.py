#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
韭研公社盘前纪要抓取脚本
每日08:10执行，仅交易日抓取并写入trade_market_research_report表
"""
import sys
import os
import re
import json
import logging
import argparse
import requests
import demjson3
from datetime import datetime, timedelta
from typing import List, Dict, Optional

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

import pymysql
from config import config

# 创建logs目录
logs_dir = os.path.join(project_root, 'logs/tradeDataClean/report')
os.makedirs(logs_dir, exist_ok=True)

# 配置日志
log_filename = os.path.join(logs_dir, f'jiuyangongshe_pre_summary_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class JiuYanGongShePreSummarySpider:
    """韭研公社盘前纪要爬虫"""

    SEARCH_URL = 'https://www.jiuyangongshe.com/search/new'
    BASE_DETAIL_URL = 'https://www.jiuyangongshe.com/a/{}'
    KEYWORD = '盘前纪要'

    def __init__(self):
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

    def is_trading_day(self, date: datetime) -> bool:
        """判断是否为交易日"""
        try:
            with self.connection.cursor() as cursor:
                sql = (
                    "SELECT COUNT(*) FROM trade_market_calendar "
                    "WHERE cal_date = %s AND is_open = 1"
                )
                cursor.execute(sql, (date.date(),))
                result = cursor.fetchone()
                return bool(result and result[0] > 0)
        except Exception as e:
            logger.error(f"判断交易日失败: {e}")
            return False

    def fetch_search_page(self) -> Optional[str]:
        """抓取搜索页面HTML"""
        params = {'k': self.KEYWORD}
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Referer': 'https://www.jiuyangongshe.com/',
        }
        try:
            resp = requests.get(self.SEARCH_URL, params=params, headers=headers, timeout=15)
            resp.raise_for_status()
            logger.info(f"成功抓取搜索页面，状态码: {resp.status_code}")
            return resp.text
        except Exception as e:
            logger.error(f"抓取搜索页面失败: {e}")
            return None

    def parse_nuxt_data(self, html: str) -> List[Dict]:
        """一次性提取所有盘前纪要（单页多日）"""
        # 直接匹配包含盘前的标题和对应的article_id、sync_time
        title_pattern = re.compile(r'title\s*:\s*"([^"]*盘前[^"]*)".*?article_id\s*:\s*"([a-zA-Z0-9]+)".*?sync_time\s*:\s*"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"', re.DOTALL)
        matches = title_pattern.findall(html)
        
        logger.debug(f"抓取到匹配项数: {len(matches)}")
        if matches:
            logger.debug(f"第一个匹配标题: {matches[0][0]}")
        else:
            logger.debug("正则未命中，打印 HTML 前 512 字符:\n" + html[:512])

        pre_summary_list = []
        for title, article_id, sync_time in matches:
            # 只打印含“18日”的标题，减少日志
            if '18日' in title:
                logger.debug(f"18日标题: {title}")
            
            detail_url = self.BASE_DETAIL_URL.format(article_id)
            pre_summary_list.append({
                'title': title,
                'article_id': article_id,
                'url': detail_url,
                'sync_time': sync_time
            })
        
        logger.info(f"提取到盘前纪要数量: {len(pre_summary_list)}")
        if not pre_summary_list:
            logger.debug("HTML 片段前 1024 字符：\n" + html[:1024])
        return pre_summary_list

    def report_exists(self, trade_date: str, title: str) -> bool:
        """检查报告是否已存在"""
        try:
            with self.connection.cursor() as cursor:
                sql = (
                    "SELECT COUNT(*) FROM trade_market_research_report "
                    "WHERE trade_date = %s AND pre_summary_report_title = %s"
                )
                cursor.execute(sql, (trade_date, title))
                result = cursor.fetchone()
                return bool(result and result[0] > 0)
        except Exception as e:
            logger.error(f"检查报告存在失败: {e}")
            return False

    def insert_report(self, trade_date: str, title: str, url: str) -> bool:
        """插入报告记录"""
        try:
            with self.connection.cursor() as cursor:
                sql = (
                    "INSERT INTO trade_market_research_report "
                    "(trade_date, pre_summary_report_title, pre_summary_report_url) "
                    "VALUES (%s, %s, %s) "
                    "ON DUPLICATE KEY UPDATE "
                    "pre_summary_report_url = VALUES(pre_summary_report_url), "
                    "updated_time = CURRENT_TIMESTAMP"
                )
                cursor.execute(sql, (trade_date, title, url))
                logger.info(f"成功写入盘前纪要: {title}")
                return True
        except Exception as e:
            logger.error(f"插入盘前纪要失败: {e}")
            return False

    def run_single_day(self, target_date: datetime) -> int:
        """抓取并入库指定日期的盘前纪要（单页多日，一次请求）"""
        if not self.is_trading_day(target_date):
            logger.info(f"{target_date.strftime('%Y-%m-%d')} 非交易日，跳过")
            return 0

        html = self.fetch_search_page()
        if not html:
            return 0

        reports = self.parse_nuxt_data(html)
        if not reports:
            logger.warning("未提取到任何盘前纪要")
            return 0

        inserted = 0
        for rpt in reports:
            title = rpt['title']
            # 从标题提取“月”“日”
            date_match = re.search(r'(\d{1,2})月(\d{1,2})日', title)
            if not date_match:
                logger.warning(f"标题中未提取到日期: {title}")
                continue
            month, day = int(date_match.group(1)), int(date_match.group(2))
            report_date = datetime(target_date.year, month, day)
            # 仅入库目标日期
            if report_date.date() != target_date.date():
                continue
            if self.report_exists(report_date.strftime('%Y-%m-%d'), title):
                logger.info(f"报告已存在，跳过: {title}")
                continue
            if self.insert_report(report_date.strftime('%Y-%m-%d'), title, rpt['url']):
                inserted += 1
        logger.info(f"{target_date.strftime('%Y-%m-%d')} 完成，新增记录: {inserted}")
        return inserted

    def run_range(self, start_date: datetime, end_date: datetime) -> int:
        """按日期范围补录数据（仅一次请求，逐日过滤入库）"""
        # 先抓一页，包含多日数据
        html = self.fetch_search_page()
        if not html:
            return 0
        reports = self.parse_nuxt_data(html)
        if not reports:
            logger.warning("未提取到任何盘前纪要")
            return 0

        total = 0
        for curr in (start_date + timedelta(n) for n in range((end_date - start_date).days + 1)):
            if not self.is_trading_day(curr):
                continue
            curr_str = curr.strftime('%Y-%m-%d')
            for rpt in reports:
                title = rpt['title']
                date_match = re.search(r'(\d{1,2})月(\d{1,2})日', title)
                if not date_match:
                    continue
                month, day = int(date_match.group(1)), int(date_match.group(2))
                report_date = datetime(curr.year, month, day)
                if report_date.date() != curr.date():
                    continue
                if self.report_exists(curr_str, title):
                    continue
                if self.insert_report(curr_str, title, rpt['url']):
                    total += 1
        logger.info(f"范围 {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')} 完成，总新增: {total}")
        return total

    def close(self):
        if self.connection:
            self.connection.close()
            logger.info("数据库连接已关闭")


def parse_args():
    parser = argparse.ArgumentParser(description='韭研公社盘前纪要抓取入库')
    parser.add_argument('--trade_date', type=str, help='指定日期 YYYYMMDD，默认今天')
    parser.add_argument('--start_date', type=str, help='补录开始日期 YYYYMMDD')
    parser.add_argument('--end_date', type=str, help='补录结束日期 YYYYMMDD')
    return parser.parse_args()


def main():
    spider = None
    try:
        args = parse_args()
        spider = JiuYanGongShePreSummarySpider()

        if args.trade_date:
            target = datetime.strptime(args.trade_date, '%Y%m%d')
            spider.run_single_day(target)
        elif args.start_date and args.end_date:
            start = datetime.strptime(args.start_date, '%Y%m%d')
            end = datetime.strptime(args.end_date, '%Y%m%d')
            spider.run_range(start, end)
        else:
            # 默认今天
            today = datetime.now()
            spider.run_single_day(today)

    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        sys.exit(1)
    finally:
        if spider:
            spider.close()


if __name__ == '__main__':
    main()