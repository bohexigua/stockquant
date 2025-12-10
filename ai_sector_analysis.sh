#!/bin/bash
# AI板块投研分析报告生成定时脚本（每日 08:30）

# 项目根目录
PROJECT_ROOT="/Users/zwldqp/work/stockquant"

# 为cron环境补充PATH，确保python3与依赖可用
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"

# 日志目录与文件（与盘前纪要脚本一致的时间戳日志）
LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="$LOG_DIR/ai_sector_analysis_$TIMESTAMP.log"

# 记录任务开始
echo "========================================" >> "$LOG_FILE"
echo "AI板块分析任务开始执行: $(date)" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

# 切换到项目目录
cd "$PROJECT_ROOT"

# 执行分析生成脚本（使用python3）
TODAY=$(date +%Y%m%d)
echo "开始执行AI板块投研分析生成..." >> "$LOG_FILE"
python3 "$PROJECT_ROOT/tradeDataClean/report/ai_sector_analysis_generator.py" --trade_date "$TODAY" >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

# 记录任务结束
echo "========================================" >> "$LOG_FILE"
echo "AI板块分析任务执行完成: $(date)" >> "$LOG_FILE"
echo "退出码: $EXIT_CODE" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

exit $EXIT_CODE
