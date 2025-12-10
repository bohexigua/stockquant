#!/bin/bash

# 韭研公社盘前纪要抓取定时任务脚本（参考 daily_task.sh 风格）

# 项目根目录
PROJECT_ROOT="/Users/zwldqp/work/stockquant"

# 为cron环境补充PATH，确保python3与依赖可用
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"

# 日志目录与文件
LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="$LOG_DIR/jiuyangongshe_pre_summary_$TIMESTAMP.log"

# 记录任务开始
echo "========================================" >> "$LOG_FILE"
echo "盘前纪要任务开始执行: $(date)" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

# 切换到项目目录
cd "$PROJECT_ROOT"

TODAY=$(date +"%Y%m%d")
echo "开始执行盘前纪要抓取..." >> "$LOG_FILE"

OVERALL_EXIT=0
for RUN_IDX in 1 2 3; do
    echo "第${RUN_IDX}次执行开始: $(date)" >> "$LOG_FILE"
    python3 "$PROJECT_ROOT/tradeDataClean/report/jiuyangongshe_pre_summary.py" --trade_date "$TODAY" >> "$LOG_FILE" 2>&1
    RUN_EXIT=$?
    echo "第${RUN_IDX}次执行结束，退出码: $RUN_EXIT" >> "$LOG_FILE"
    if [ $RUN_EXIT -ne 0 ]; then
        OVERALL_EXIT=$RUN_EXIT
    fi
    if [ $RUN_IDX -lt 3 ]; then
        sleep 10
    fi
done
EXIT_CODE=$OVERALL_EXIT

# 记录任务结束
echo "========================================" >> "$LOG_FILE"
echo "盘前纪要任务执行完成: $(date)" >> "$LOG_FILE"
echo "退出码: $EXIT_CODE" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

exit $EXIT_CODE
