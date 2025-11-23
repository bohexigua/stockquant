#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI板块投研分析报告生成脚本
使用Coze API分析盘前纪要和热点题材，生成板块投资建议
"""
import sys
import os
import json
import re
import logging
import argparse
import pymysql
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from dataclasses import dataclass

# 添加项目根目录到Python路径
project_root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
sys.path.append(project_root)

from config import config

# 创建logs目录
logs_dir = os.path.join(project_root, "logs/tradeDataClean/report")
os.makedirs(logs_dir, exist_ok=True)

# 配置日志
log_filename = os.path.join(
    logs_dir, f'ai_sector_analysis_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_filename, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

from cozepy import Coze, TokenAuth, Message, ChatStatus, MessageType  # noqa
from cozepy import COZE_CN_BASE_URL

coze_api_token = "pat_202Dik1x8fvnoazeA2vJGmRLNiFo5YCGIl4gkVMrFgIE5sBMdtL60urLowaC1RCd"
coze_bot_id = "7575842388766195712"
coze_user_id = "lxp"
coze = Coze(auth=TokenAuth(token=coze_api_token), base_url=COZE_CN_BASE_URL)


@dataclass
class SectorAnalysis:
    """板块分析结果"""

    name: str
    strength: str
    description: str
    related_themes: List[str]


class AISectorAnalysisGenerator:
    """AI板块投研分析报告生成器"""

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
                autocommit=True,
            )
            logger.info("数据库连接初始化成功")
        except Exception as e:
            logger.error(f"数据库连接初始化失败: {e}")
            raise

    def get_pre_summary_report(self, trade_date: datetime) -> Optional[Dict]:
        """获取指定日期的盘前纪要报告"""
        try:
            with self.connection.cursor() as cursor:
                sql = """
                SELECT pre_summary_report_title, pre_summary_report_url 
                FROM trade_market_research_report 
                WHERE trade_date = %s 
                ORDER BY created_time DESC 
                LIMIT 1
                """
                cursor.execute(sql, (trade_date.date(),))
                result = cursor.fetchone()

                if result:
                    return {"title": result[0], "url": result[1]}
                else:
                    logger.warning(
                        f"未找到 {trade_date.strftime('%Y-%m-%d')} 的盘前纪要"
                    )
                    return None
        except Exception as e:
            logger.error(f"获取盘前纪要失败: {e}")
            return None

    def get_recent_themes(self, days_back: int = 30) -> List[str]:
        """获取最近N天的热点题材，去重后返回列表"""
        try:
            start_date = datetime.now() - timedelta(days=days_back)
            themes_raw = []

            with self.connection.cursor() as cursor:
                # 获取最相关题材
                sql1 = """
                SELECT DISTINCT most_related_theme_name 
                FROM trade_factor_most_related_theme 
                WHERE trade_date >= %s AND most_related_theme_name IS NOT NULL
                """
                cursor.execute(sql1, (start_date.date(),))
                for row in cursor.fetchall():
                    if row[0]:
                        themes_raw.append(row[0].strip())

            themes_list = list(dict.fromkeys(themes_raw))
            logger.info(f"获取到 {len(themes_list)} 个热点题材")
            return themes_list

        except Exception as e:
            logger.error(f"获取热点题材失败: {e}")
            return []

    def build_coze_request_content(self, pre_summary: Dict, themes: List[str]) -> str:
        """构建Coze API请求内容"""
        themes_str = "，".join(themes)  # 限制题材数量，避免请求过长

        content = f"""盘前纪要：{pre_summary['url']}
热点题材：{themes_str}"""

        return content.strip()

    def call_coze_api(self, content: str):
        """调用Coze API进行分析"""
        try:
            chat_poll = coze.chat.create_and_poll(
                bot_id=coze_bot_id,
                user_id=coze_user_id,
                additional_messages=[Message.build_user_question_text(content)],
            )
            result = None, None, None
            for message in chat_poll.messages:
                print("-----message-----")
                print(message.type)
                if message.type == MessageType.ANSWER:
                    print(message.content, end="", flush=True)
                    result = message.chat_id, message.conversation_id, message.content
            if chat_poll.chat.status == ChatStatus.COMPLETED:
                print()
                print("token usage:", chat_poll.chat.usage.token_count)

            logger.info("Coze API调用成功")
            return result

        except Exception as e:
            logger.error(f"调用Coze API失败: {e}")
            return None

    def parse_coze_response(self, answer_content: str) -> Optional[Dict]:
        """解析Coze API响应，提取分析结果"""
        try:
            if answer_content is None:
                logger.error("Coze API answer响应为空")
                return None

            # 提取JSON部分
            json_match = re.search(r"```json\s*([\s\S]*?)\s*```", answer_content)
            if json_match:
                analysis_json = json_match.group(1)
            else:
                logger.warning("未找到JSON内容")
                analysis_json = "[]"

            print('-----analysis_json-------')
            print(analysis_json)
            return {"analysis_detail": answer_content, "analysis_json": analysis_json}

        except Exception as e:
            logger.error(f"解析Coze响应失败: {e}")
            return None

    def save_analysis_result(
        self,
        trade_date: datetime,
        chat_id: Optional[str],
        conversation_id: Optional[str],
        analysis_json: str,
        analysis_detail: str,
    ) -> bool:
        """保存分析结果到数据库"""
        try:
            with self.connection.cursor() as cursor:
                sql = """
                INSERT INTO trade_market_ai_theme_analysis 
                (trade_date, chat_id, conversation_id, analysis_json, analysis_detail)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                analysis_json = VALUES(analysis_json),
                analysis_detail = VALUES(analysis_detail),
                updated_time = CURRENT_TIMESTAMP
                """

                cursor.execute(
                    sql,
                    (
                        trade_date.date(),
                        chat_id,
                        conversation_id,
                        analysis_json,
                        analysis_detail,
                    ),
                )

                logger.info(f"成功保存板块分析结果")
                return True

        except Exception as e:
            logger.error(f"保存分析结果失败: {e}")
            return False

    def generate_sector_analysis(self, trade_date: datetime) -> int:
        """生成指定日期的板块分析报告"""
        logger.info(f"开始生成 {trade_date.strftime('%Y-%m-%d')} 的AI板块分析报告")

        # 获取盘前纪要
        pre_summary = self.get_pre_summary_report(trade_date)
        if not pre_summary:
            logger.warning("未找到盘前纪要，跳过分析")
            return 0

        # 获取热点题材
        themes = self.get_recent_themes()
        if not themes:
            logger.warning("未找到热点题材，跳过分析")
            return 0

        # 构建请求内容
        content = self.build_coze_request_content(pre_summary, themes)

        # 调用Coze API
        chat_id, conversation_id, answer_content = self.call_coze_api(content)
        if not chat_id or not conversation_id or not answer_content:
            return 0
        # 解析响应
        parsed_result = self.parse_coze_response(answer_content)
        if not parsed_result:
            return 0
        print(type(parsed_result["analysis_json"]))
        # 保存到数据库
        if self.save_analysis_result(
            trade_date,
            chat_id,
            conversation_id,
            parsed_result["analysis_json"],
            parsed_result["analysis_detail"],
        ):
            logger.info(f"完成分析")

    def close(self):
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()
            logger.info("数据库连接已关闭")


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description="AI板块投研分析报告生成器")
    parser.add_argument("--trade_date", type=str, help="指定日期 YYYYMMDD，默认今天")
    return parser.parse_args()


def main():
    """主函数"""
    args = parse_args()

    generator = None
    try:
        generator = AISectorAnalysisGenerator()

        # 确定分析日期
        if args.trade_date:
            trade_date = datetime.strptime(args.trade_date, "%Y%m%d")
        else:
            # trade_date = datetime.now()
            trade_date = datetime.strptime("20251121", "%Y%m%d")

        # 生成分析报告
        generator.generate_sector_analysis(trade_date)

    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        sys.exit(1)
    finally:
        if generator:
            generator.close()


if __name__ == "__main__":
    main()
