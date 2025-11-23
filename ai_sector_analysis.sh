#!/bin/bash
# AI板块投研分析报告生成定时脚本（每日 08:30）

set -euo pipefail

PROJECT_DIR="/Users/zwldqp/work/stockquant"
SCRIPT_REL="tradeDataClean/report/ai_sector_analysis_generator.py"
LOG_DIR="$PROJECT_DIR/logs/tradeDataClean/report"
mkdir -p "$LOG_DIR"

cd "$PROJECT_DIR"

TODAY=$(date +%Y%m%d)

echo "[$(date '+%F %T')] 运行AI板块分析生成脚本，日期=$TODAY" >> "$LOG_DIR/ai_sector_analysis.log"
python "$SCRIPT_REL" --trade_date "$TODAY" >> "$LOG_DIR/ai_sector_analysis.log" 2>&1

exit $?