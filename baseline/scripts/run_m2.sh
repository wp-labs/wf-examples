#!/usr/bin/env bash
# ===========================================================================
# S2-M2 最小闭环（近端 B：共享 BaselineStore warm + baseline_dev judge 对拍）
#   1. gen    受控数据（复用 S2-M3a：5 实体历史 + live，svc_e 9000=9×）
#   2. producer batch → baseline.ndjson（逐窗记录）
#   3. export 逐窗历史 CSV（entity,metric,win_start,win_end,n,sum,sum_sq）
#   4. judge  batch：runtime.baseline_history warm store → baseline_judge
#      （on each where |baseline_dev|>3）→ judge.ndjson
#   5. verify 断言仅 svc_e（z≈500+）告警、svc_a..d 不告警
# ===========================================================================
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

mkdir -p data/detect data/logs
rm -f data/baseline/baseline.ndjson data/detect/judge.ndjson data/detect/baseline_history.csv \
      data/logs/wfusion_m2_judge.log data/logs/wfusion_m3a_producer.log

echo "1> gen 受控数据"
python3 scripts/gen_detect_data.py

echo "2> producer batch（受控历史 → 逐窗基线记录）"
wfusion batch --config test/wfusion.m3a_producer.batch.toml --work-dir .
[[ -s data/baseline/baseline.ndjson ]] || { echo "ERROR: producer 无输出" >&2; exit 1; }
wc -l data/baseline/baseline.ndjson

echo "3> export 逐窗历史 CSV（warm 输入）"
python3 scripts/export_baseline_history.py

echo "4> judge batch（BaselineStore warm + baseline_dev z 判定）"
wfusion batch --config test/wfusion.m2_judge.batch.toml --work-dir .

echo "5> verify"
python3 scripts/verify_m2.py
