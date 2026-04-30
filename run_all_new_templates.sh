#!/bin/bash
  # run_all_new_templates.sh — 持续跑完所有新模板的完整 Pipeline
  set -e

  PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
  cd "$PROJECT_DIR"

  LOG_DIR="pipeline_logs"
  mkdir -p "$LOG_DIR"

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

  echo "========================================"
  echo " Pipeline 1/2: pv1 (6 templates)"
  echo "========================================"
  nohup python run_pipeline.py \
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
    --report-file pipeline_reports/pv1_report.json \
    > "$LOG_DIR/pipeline_pv1.log" 2>&1 &

  PV1_PID=$!
  echo "pv1 pipeline started (PID=$PV1_PID), log: $LOG_DIR/pipeline_pv1.log"
  wait $PV1_PID
  echo "pv1 pipeline finished (exit code: $?)"

  echo "========================================"
  echo " Pipeline 2/2: fundamental6 (4 templates)"
  echo "========================================"
  nohup python run_pipeline.py \
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
    --report-file pipeline_reports/fundamental6_report.json \
    > "$LOG_DIR/pipeline_fundamental6.log" 2>&1 &

  F6_PID=$!
  echo "fundamental6 pipeline started (PID=$F6_PID), log: $LOG_DIR/pipeline_fundamental6.log"
  wait $F6_PID
  echo "fundamental6 pipeline finished (exit code: $?)"

  echo "========================================"
  echo " ALL PIPELINES COMPLETE"
  echo " Reports:"
  echo "   pv1:           pipeline_reports/pv1_report.json"
  echo "   fundamental6:  pipeline_reports/fundamental6_report.json"
  echo " Logs:"
  echo "   $LOG_DIR/"
  echo "========================================"