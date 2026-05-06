#!/bin/bash
# run_all_new_templates.sh — 跑完 template_catalog_mixed.json 全部 15 个模板的完整 Pipeline
# 用法: bash run_all_new_templates.sh
# 推荐: tmux new-session -d -s alphamining 'bash run_all_new_templates.sh'

set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

LOG_DIR="pipeline_logs"
REPORT_DIR="pipeline_reports"
mkdir -p "$LOG_DIR" "$REPORT_DIR"

# Python 命令（按环境调整）
PYTHON_CMD="python"

# ══════════════════════════════════════════════════════════════════════
# 模板分组（template_catalog_mixed.json）
# ══════════════════════════════════════════════════════════════════════

PV1_TEMPLATES="\
TPL_PV_VOLUME_RETURN_CORR_V1,\
TPL_PV_ACCUM_TS_V1,\
TPL_PV_REVERSAL_CORR_V1"

F6_TEMPLATES="\
TPL_FUND_EARNINGS_YIELD_TS_V1,\
TPL_FUND_INTEREST_COVER_TS_V1,\
TPL_FUND_CASHFLOW_YIELD_TS_V1,\
TPL_FUND_SALES_TURNOVER_TS_V1"

OPT8_TEMPLATES="\
TPL_OPTION_TERM_SLOPE_TS_V1,\
TPL_OPTION_VOLOFVOL_TS_V1"

OPT9_TEMPLATES="\
TPL_OPTION_PCR_DELTA_TS_V1,\
TPL_OPTION_PCRSPREAD_TS_V1"

SENT1_TEMPLATES="\
TPL_SENTIMENT_REVISION_TS_V1,\
TPL_SENTIMENT_CONSENSUS_TS_V1"

SOCIAL12_TEMPLATES="\
TPL_SENTIMENT_SOCIAL_TS_V1"

NEWS12_TEMPLATES="\
TPL_NEWS_REACTION_TS_V1"

# ══════════════════════════════════════════════════════════════════════
# 通用 Pipeline 函数
# ══════════════════════════════════════════════════════════════════════

run_pipeline() {
    local dataset_id=$1
    local template_ids=$2
    local label=$3

    echo ""
    echo "========================================"
    echo " Pipeline $label: $dataset_id"
    echo "========================================"

    nohup $PYTHON_CMD run_pipeline.py \
      --dataset-id "$dataset_id" \
      --template-doc template_catalog_mixed.json \
      --template-ids "$template_ids" \
      --expand-min-sharpe 0.8 \
      --expand-min-fitness 0.45 \
      --expand-max-turnover 0.7 \
      --watch-min-sharpe 0.5 \
      --min-probe-count 1 \
      --max-workers 3 \
      --max-retries 5 \
      --retry-sleep 5 \
      --relogin-interval-seconds 13800 \
      --log-level INFO \
      --probe-batches-dir "factor_batches/probe/${dataset_id}" \
      --probe-results-dir "backtest_results/probe/${dataset_id}" \
      --expand-batches-dir "factor_batches/expand/${dataset_id}" \
      --expand-results-dir "backtest_results/expand/${dataset_id}" \
      --report-file "${REPORT_DIR}/${dataset_id}_report.json" \
      > "${LOG_DIR}/pipeline_${dataset_id}.log" 2>&1 &

    local pid=$!
    echo "  PID=$pid"
    echo "  log:    ${LOG_DIR}/pipeline_${dataset_id}.log"
    echo "  report: ${REPORT_DIR}/${dataset_id}_report.json"

    wait $pid
    local exit_code=$?
    echo "  exit code: $exit_code"
    return $exit_code
}

# ══════════════════════════════════════════════════════════════════════
# 执行全部 7 个 Pipeline（串行）
# ══════════════════════════════════════════════════════════════════════

ALL_EXIT=0

run_pipeline "pv1"           "$PV1_TEMPLATES"     "1/7"   || ALL_EXIT=1
run_pipeline "fundamental6"  "$F6_TEMPLATES"      "2/7"   || ALL_EXIT=1
run_pipeline "option8"       "$OPT8_TEMPLATES"    "3/7"   || ALL_EXIT=1
run_pipeline "option9"       "$OPT9_TEMPLATES"    "4/7"   || ALL_EXIT=1
run_pipeline "sentiment1"    "$SENT1_TEMPLATES"   "5/7"   || ALL_EXIT=1
run_pipeline "socialmedia12" "$SOCIAL12_TEMPLATES" "6/7"   || ALL_EXIT=1
run_pipeline "news12"        "$NEWS12_TEMPLATES"  "7/7"   || ALL_EXIT=1

# ══════════════════════════════════════════════════════════════════════
# 汇总
# ══════════════════════════════════════════════════════════════════════

echo ""
echo "========================================"
echo " ALL PIPELINES COMPLETE"
echo "   15 templates across 7 datasets"
echo "========================================"
echo ""
for ds in pv1 fundamental6 option8 option9 sentiment1 socialmedia12 news12; do
    echo "  $ds:  report=${REPORT_DIR}/${ds}_report.json"
done
echo ""
echo "  Logs: ${LOG_DIR}/"
echo ""
echo "  Post-analysis examples:"
echo "    python result_filter.py --results-dir backtest_results/expand/pv1 --group-by-core --top-n 20"
echo "    python result_filter.py --results-dir backtest_results/expand/fundamental6 --group-by-core --top-n 20"
echo "========================================"

exit $ALL_EXIT
