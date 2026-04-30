#!/bin/bash
# run_all_new_templates.sh — 持续跑完所有新模板的完整 Pipeline
# 用法: bash run_all_new_templates.sh
# 推荐: tmux new-session -d -s alphamining 'bash run_all_new_templates.sh'

set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

LOG_DIR="pipeline_logs"
REPORT_DIR="pipeline_reports"
mkdir -p "$LOG_DIR" "$REPORT_DIR"

# 用 uv 管理包环境（GCP VM 上 python 命令不存在，需用 python3 或 uv run）
PYTHON_CMD="uv run python3"

# 定义 pv1 模板（6 个）
PV1_TEMPLATES="TPL_PV_TURNOVER_TS_V1,\
TPL_PV_PRICE_RANGE_V1,\
TPL_PV_AMIHUD_ILLIQ_V1,\
TPL_PV_REL_VOLUME_V1,\
TPL_PV_RETURN_VOL_V1,\
TPL_PV_SIZE_FACTOR_V1"

# 定义 fundamental6 模板（4 个）
F6_TEMPLATES="TPL_FUND_DEBT_EQUITY_TS_V1,\
TPL_FUND_GROWTH_RATE_TS_V1,\
TPL_FUND_EARNINGS_QUALITY_TS_V1,\
TPL_FUND_INVESTMENT_TS_V1"

# ──────────────────────────────────────────────────
# Pipeline 1: pv1
# ──────────────────────────────────────────────────
echo "========================================"
echo " Pipeline 1/2: pv1 (6 new templates)"
echo "========================================"

nohup $PYTHON_CMD run_pipeline.py \
  --dataset-id pv1 \
  --data-type MATRIX \
  --template-ids "$PV1_TEMPLATES" \
  --expand-min-sharpe 0.7 \
  --expand-min-fitness 0.45 \
  --expand-max-turnover 0.7 \
  --watch-min-sharpe 0.5 \
  --min-probe-count 1 \
  --max-workers 3 \
  --max-retries 5 \
  --retry-sleep 5 \
  --relogin-interval-seconds 13800 \
  --log-level INFO \
  --probe-batches-dir factor_batches/probe/pv1 \
  --probe-results-dir backtest_results/probe/pv1 \
  --expand-batches-dir factor_batches/expand/pv1 \
  --expand-results-dir backtest_results/expand/pv1 \
  --report-file "$REPORT_DIR/pv1_report.json" \
  > "$LOG_DIR/pipeline_pv1.log" 2>&1 &

PV1_PID=$!
echo "pv1 pipeline started (PID=$PV1_PID)"
echo "  log:    $LOG_DIR/pipeline_pv1.log"
echo "  report: $REPORT_DIR/pv1_report.json"
echo ""
echo "  Monitor: tail -f $LOG_DIR/pipeline_pv1.log"
echo ""

# wait 返回非零时不中断脚本（pipeline 可能部分失败但仍要继续跑 fundamental6）
wait $PV1_PID
PV1_EXIT=$?
echo "pv1 pipeline finished (exit code: $PV1_EXIT)"

# ──────────────────────────────────────────────────
# Pipeline 2: fundamental6
# ──────────────────────────────────────────────────
echo ""
echo "========================================"
echo " Pipeline 2/2: fundamental6 (4 new templates)"
echo "========================================"

nohup $PYTHON_CMD run_pipeline.py \
  --dataset-id fundamental6 \
  --data-type MATRIX \
  --template-ids "$F6_TEMPLATES" \
  --expand-min-sharpe 0.7 \
  --expand-min-fitness 0.45 \
  --expand-max-turnover 0.7 \
  --watch-min-sharpe 0.5 \
  --min-probe-count 1 \
  --max-workers 3 \
  --max-retries 5 \
  --retry-sleep 5 \
  --relogin-interval-seconds 13800 \
  --log-level INFO \
  --probe-batches-dir factor_batches/probe/fundamental6 \
  --probe-results-dir backtest_results/probe/fundamental6 \
  --expand-batches-dir factor_batches/expand/fundamental6 \
  --expand-results-dir backtest_results/expand/fundamental6 \
  --report-file "$REPORT_DIR/fundamental6_report.json" \
  > "$LOG_DIR/pipeline_fundamental6.log" 2>&1 &

F6_PID=$!
echo "fundamental6 pipeline started (PID=$F6_PID)"
echo "  log:    $LOG_DIR/pipeline_fundamental6.log"
echo "  report: $REPORT_DIR/fundamental6_report.json"
echo ""
echo "  Monitor: tail -f $LOG_DIR/pipeline_fundamental6.log"
echo ""

wait $F6_PID
F6_EXIT=$?
echo "fundamental6 pipeline finished (exit code: $F6_EXIT)"

# ──────────────────────────────────────────────────
# 汇总
# ──────────────────────────────────────────────────
echo ""
echo "========================================"
echo " ALL PIPELINES COMPLETE"
echo "========================================"
echo "  pv1:           exit=$PV1_EXIT  report=$REPORT_DIR/pv1_report.json"
echo "  fundamental6:  exit=$F6_EXIT  report=$REPORT_DIR/fundamental6_report.json"
echo ""
echo "  Logs: $LOG_DIR/"
echo ""
echo "  Post-analysis:"
echo "    python3 result_filter.py --results-dir backtest_results/expand/pv1 --group-by-core --top-n 20"
echo "    python3 result_filter.py --results-dir backtest_results/expand/fundamental6 --group-by-core --top-n 20"
echo "========================================"
