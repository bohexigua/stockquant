#!/bin/bash

PROJECT_ROOT="/Users/zwldqp/work/stockquant"
LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="$LOG_DIR/quant_trading_$TIMESTAMP.log"

echo "========================================" >> "$LOG_FILE"
echo "quant_trading 任务开始执行: $(date)" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

cd "$PROJECT_ROOT"
python3 "$PROJECT_ROOT/tradeDataClean/positions/quant_trading.py" >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

echo "========================================" >> "$LOG_FILE"
echo "任务执行完成: $(date)" >> "$LOG_FILE"
echo "退出码: $EXIT_CODE" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

exit $EXIT_CODE

