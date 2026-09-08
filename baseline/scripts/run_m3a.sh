#!/usr/bin/env bash
# ===========================================================================
# S2-M3a 最小验证（全局周期基线供给通道：knowdb CSV → ProviderWindow → join 判定）
#   1. gen     受控数据（5 条线路历史 + live 判定事件，5号线 故意 9x）
#   2. producer baseline_producer 聚合受控历史 → baseline.ndjson
#   3. export  聚合成 provider CSV（三元组 + mu/sigma，契约 §11.3）
#   4. detect  knowdb 加载 CSV → baseline_detect 每事件 join + 越界 where → 告警
#   5. verify  断言仅 5号线 告警、CSV 契约自洽
# ===========================================================================
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

mkdir -p data/detect data/logs
rm -f data/baseline/baseline.ndjson data/detect/*.ndjson data/logs/wfusion_m3a*.log

echo "1> gen 受控数据"
python3 scripts/gen_detect_data.py

echo "2> producer batch（受控历史 → 基线记录）"
wfusion batch --config test/wfusion.m3a_producer.batch.toml --work-dir .
[[ -s data/baseline/baseline.ndjson ]] || { echo "ERROR: producer 无输出" >&2; exit 1; }
wc -l data/baseline/baseline.ndjson

echo "3> export provider CSV（三元组 + mu/sigma）"
python3 scripts/export_baseline_ref.py

echo "4> detect batch（knowdb CSV → ProviderWindow → join 判定）"
wfusion batch --config test/wfusion.m3a_detect.batch.toml --work-dir .

echo "5> verify"
python3 scripts/verify_detect.py
