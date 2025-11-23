#!/bin/bash

# 每日凌晨4点执行个股近期最相关题材清洗脚本
# Cron示例：在crontab中加入以下条目
# 0 4 * * * /Users/zwldqp/work/stockquant/most_related_theme_daily.sh

# 设置项目根目录
PROJECT_ROOT="/Users/zwldqp/work/stockquant"

# 设置日志目录
LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"

# 设置日志文件名（带时间戳）
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="$LOG_DIR/most_related_theme_$TIMESTAMP.log"

# 记录任务开始时间
echo "========================================" >> "$LOG_FILE"
echo "most_related_theme 任务开始执行: $(date)" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

# 切换到项目目录
cd "$PROJECT_ROOT"

# 执行清洗脚本（默认tag为涨停，可按需调整）
echo "开始执行 most_related_theme.py..." >> "$LOG_FILE"
python3 "$PROJECT_ROOT/tradeDataClean/market/most_related_theme.py" >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "most_related_theme.py 执行成功" >> "$LOG_FILE"
else
    echo "most_related_theme.py 执行失败，退出码: $EXIT_CODE" >> "$LOG_FILE"
fi

# 记录任务结束时间
echo "========================================" >> "$LOG_FILE"
echo "任务执行完成: $(date)" >> "$LOG_FILE"
echo "退出码: $EXIT_CODE" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

exit $EXIT_CODE