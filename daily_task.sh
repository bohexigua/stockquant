#!/bin/bash

# 股票量化数据处理定时任务脚本
# 每个工作日19:30执行数据清洗和因子计算

# 设置项目根目录
PROJECT_ROOT="/Users/zwldqp/work/stockquant"

# 设置日志目录
LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"

# 设置日志文件名（带时间戳）
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="$LOG_DIR/daily_task_$TIMESTAMP.log"

# 记录任务开始时间
echo "========================================" >> "$LOG_FILE"
echo "定时任务开始执行: $(date)" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

# 切换到项目目录
cd "$PROJECT_ROOT"

# 执行数据清洗脚本
echo "开始执行数据清洗脚本..." >> "$LOG_FILE"
python3 "$PROJECT_ROOT/tradeDataClean/main.py" >> "$LOG_FILE" 2>&1
DATA_CLEAN_EXIT_CODE=$?

if [ $DATA_CLEAN_EXIT_CODE -eq 0 ]; then
    echo "数据清洗脚本执行成功" >> "$LOG_FILE"
else
    echo "数据清洗脚本执行失败，退出码: $DATA_CLEAN_EXIT_CODE" >> "$LOG_FILE"
    echo "任务终止" >> "$LOG_FILE"
    exit $DATA_CLEAN_EXIT_CODE
fi

# 执行因子计算脚本
echo "开始执行因子计算脚本..." >> "$LOG_FILE"
python3 "$PROJECT_ROOT/factors/main.py" >> "$LOG_FILE" 2>&1
FACTORS_EXIT_CODE=$?

if [ $FACTORS_EXIT_CODE -eq 0 ]; then
    echo "因子计算脚本执行成功" >> "$LOG_FILE"
else
    echo "因子计算脚本执行失败，退出码: $FACTORS_EXIT_CODE" >> "$LOG_FILE"
fi

# 记录任务结束时间
echo "========================================" >> "$LOG_FILE"
echo "定时任务执行完成: $(date)" >> "$LOG_FILE"
echo "数据清洗退出码: $DATA_CLEAN_EXIT_CODE" >> "$LOG_FILE"
echo "因子计算退出码: $FACTORS_EXIT_CODE" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

# 如果任一脚本失败，返回非零退出码
if [ $DATA_CLEAN_EXIT_CODE -ne 0 ] || [ $FACTORS_EXIT_CODE -ne 0 ]; then
    exit 1
fi

exit 0