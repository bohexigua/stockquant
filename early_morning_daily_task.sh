#!/bin/bash

# 每天早上8:20执行个股近期最相关题材清洗脚本
# Cron示例：在crontab中加入以下条目
# 20 8 * * * /Users/zwldqp/work/stockquant/early_morning_daily_task.sh

# 设置项目根目录
PROJECT_ROOT="/Users/zwldqp/work/stockquant"

# 设置日志目录
LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"

# 设置日志文件名（带时间戳）
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="$LOG_DIR/early_morning_daily_task_$TIMESTAMP.log"

# 记录任务开始时间
echo "========================================" >> "$LOG_FILE"
echo "early_morning_daily_task 任务开始执行: $(date)" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

# 切换到项目目录
cd "$PROJECT_ROOT"

# 执行清洗脚本（默认tag为涨停，可按需调整）
echo "开始执行 most_related_theme.py..." >> "$LOG_FILE"
python3 "$PROJECT_ROOT/tradeDataClean/market/most_related_theme.py" >> "$LOG_FILE" 2>&1
MOST_RELATED_THEME_EXIT_CODE=$?

if [ $MOST_RELATED_THEME_EXIT_CODE -eq 0 ]; then
    echo "most_related_theme.py 执行成功" >> "$LOG_FILE"
else
    echo "most_related_theme.py 执行失败，退出码: $MOST_RELATED_THEME_EXIT_CODE" >> "$LOG_FILE"
fi

# 早盘回补东财热榜：晚间任务拿不到当天完整数据时，次日早上自动补最近3天
DC_STOCK_HOT_START_DATE=$(date -v-3d +"%Y%m%d")
DC_STOCK_HOT_END_DATE=$(date +"%Y%m%d")
echo "开始执行 dc_stock_hot.py，回补日期范围: ${DC_STOCK_HOT_START_DATE}-${DC_STOCK_HOT_END_DATE}..." >> "$LOG_FILE"
python3 "$PROJECT_ROOT/tradeDataClean/market/dc_stock_hot.py" --start_date "$DC_STOCK_HOT_START_DATE" --end_date "$DC_STOCK_HOT_END_DATE" >> "$LOG_FILE" 2>&1
DC_STOCK_HOT_EXIT_CODE=$?

if [ $DC_STOCK_HOT_EXIT_CODE -eq 0 ]; then
    echo "dc_stock_hot.py 执行成功" >> "$LOG_FILE"
else
    echo "dc_stock_hot.py 执行失败，退出码: $DC_STOCK_HOT_EXIT_CODE" >> "$LOG_FILE"
fi

# 记录任务结束时间
echo "========================================" >> "$LOG_FILE"
echo "任务执行完成: $(date)" >> "$LOG_FILE"
echo "most_related_theme.py 退出码: $MOST_RELATED_THEME_EXIT_CODE" >> "$LOG_FILE"
echo "dc_stock_hot.py 退出码: $DC_STOCK_HOT_EXIT_CODE" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

if [ $MOST_RELATED_THEME_EXIT_CODE -ne 0 ] || [ $DC_STOCK_HOT_EXIT_CODE -ne 0 ]; then
    exit 1
fi

exit 0