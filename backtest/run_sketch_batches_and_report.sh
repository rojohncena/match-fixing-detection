#!/usr/bin/env bash
# Regenerate ROU1 + CSL + J2 batch CSVs using histogram large-trade cutoff, then COMBINED_SKETCH_REPORT.md.
# Requires network. Run from repo root:  bash backtest/run_sketch_batches_and_report.sh
set -euo pipefail
cd "$(dirname "$0")"
YEAR="${YEAR:-2026}"
python3 polymarket_smart_money_backtest.py \
  --rou1-week3-csv out_sketch_histogram.csv \
  --year "$YEAR" \
  --large-trade-cut histogram
python3 polymarket_smart_money_backtest.py \
  --csl-mar-apr-csv csl_backtest_sketch_histogram.csv \
  --year "$YEAR" \
  --large-trade-cut histogram
python3 polymarket_smart_money_backtest.py \
  --j2-feb28-csv j2_backtest_sketch_histogram.csv \
  --year "$YEAR" \
  --large-trade-cut histogram
python3 combined_backtest_report.py \
  --rou1 out_sketch_histogram.csv \
  --csl csl_backtest_sketch_histogram.csv \
  --j2 j2_backtest_sketch_histogram.csv \
  --heading "# Combined backtest report (histogram large-trade cutoff)" \
  --preamble "> **Variant:** \`--large-trade-cut histogram\` — prefix **90th-percentile** trade size uses \`sketch_quantile.histogram_large_cut_fixed\` with **fixed** log-spaced edges (\`build_log_spaced_edges\`, default 64 bins over production min/max). Flow medians, \`MIN_DELTA_P\`, and other logic match the **exact** \`--large-trade-cut exact\` backtest." \
  -o COMBINED_SKETCH_REPORT.md
echo "Wrote COMBINED_SKETCH_REPORT.md and *_sketch_histogram.csv"
