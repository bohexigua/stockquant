#!/bin/bash

# 个股tick数据调度定时任务脚本（每日 09:15-11:30，内部20s轮询）

PROJECT_ROOT="/Users/zwldqp/work/stockquant"
LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="$LOG_DIR/stock_tick_schedule_$TIMESTAMP.log"

echo "========================================" >> "$LOG_FILE"
echo "stock_tick_schedule 任务开始执行: $(date)" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

cd "$PROJECT_ROOT"

python3 "$PROJECT_ROOT/tradeDataClean/market/stock_tick.py" >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

echo "========================================" >> "$LOG_FILE"
echo "任务执行完成: $(date)" >> "$LOG_FILE"
echo "退出码: $EXIT_CODE" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

exit $EXIT_CODE

