#!/bin/bash
# 韭研公社盘前纪要抓取脚本定时执行（每日 08:25）

set -euo pipefail

PROJECT_DIR="/Users/zwldqp/work/stockquant"
SCRIPT_REL="tradeDataClean/report/jiuyangongshe_pre_summary.py"
LOG_DIR="$PROJECT_DIR/logs/tradeDataClean/report"
mkdir -p "$LOG_DIR"

cd "$PROJECT_DIR"

TODAY=$(date +%Y%m%d)

echo "[$(date '+%F %T')] 运行盘前纪要抓取脚本，日期=$TODAY" >> "$LOG_DIR/jiuyangongshe_pre_summary.log"
python "$SCRIPT_REL" --trade_date "$TODAY" >> "$LOG_DIR/jiuyangongshe_pre_summary.log" 2>&1

exit $?