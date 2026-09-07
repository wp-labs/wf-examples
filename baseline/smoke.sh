#!/usr/bin/env bash
# ===========================================================================
# baseline — 在线基线数据生产（step 1）
#   1. wfgen lint    校验 scenario / 规则 / schema
#   2. wfgen gen     确定性生成 metrics_stream 事件（JSONL）
#   3. wfusion batch 回放：baseline_producer（stats 1m 固定窗 + sumsq）
#                     → baseline_out → file_json_sink 落 data/baseline/baseline.ndjson
#   4. verify        输入事件逐 (entity, metric) 总量与基线输出对拍
#
# 前置: wfgen / wfusion 在 PATH（含 sumsq 聚合的本地引擎重建版）
# ===========================================================================
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

CASE="metrics_baseline"
GEN_DIR="data/generated"
BASELINE_DIR="data/baseline"

mkdir -p "$GEN_DIR" "$BASELINE_DIR" data/logs
rm -f "$BASELINE_DIR"/*.ndjson data/logs/wfusion.log

echo "1> lint scenario: $CASE"
wfgen lint "models/scenarios/$CASE.wfg"

echo "2> generate metrics events"
wfgen gen --scenario "models/scenarios/$CASE.wfg" --out "$GEN_DIR" --format jsonl

echo "3> run wfusion batch replay"
wfusion batch --config test/wfusion.batch.toml --work-dir .

if [[ ! -s "$BASELINE_DIR/baseline.ndjson" ]]; then
    echo "ERROR: 基线输出为空: $BASELINE_DIR/baseline.ndjson" >&2
    exit 1
fi

echo "4> verify baseline totals (n/sum/sum_sq)"
python3 scripts/verify_baseline.py "$GEN_DIR/$CASE.jsonl" "$BASELINE_DIR/baseline.ndjson"

echo "5> baseline record count"
wc -l "$BASELINE_DIR/baseline.ndjson"
