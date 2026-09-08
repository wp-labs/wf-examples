#!/usr/bin/env bash
# ===========================================================================
# S2-M3b refresh 实证（daemon：knowdb CSV 周期重载 ProviderWindow，全局周期基线）
#
#   conf/refresh.wfusion.toml → baseline_detect 规则 + baseline_ref
#   refresh="1s"（knowdb.toml [[tables]]）。三阶段：
#     phase1: 正常 live 注入 → 旧表 mu≈1000，dev<5% → 无告警
#     phase2: 覆盖 baseline_ref.csv（仅 5号线: mu 1000→5）→ 等 ≥3 个刷新 tick
#     phase3: 同批再注入 → 仅 5号线 告警（dev≈199）——证明 join 判定读到的是
#             刷新后数据（旧表下同一批事件 dev≈0，不可能告警）
#   日志断言: "provider refresh loaded table=baseline_ref" 至少出现 2 次
#   （即 daemon 运行期确实周期重读了数据源）
#
# 用法: ./scripts/run_refresh.sh
# 产物: data/logs/wfusion_refresh.log、data/detect/alerts.ndjson
# 环境: WFUSION/WFGEN/PYTHON 可覆盖（默认 ~/bin 的 wfusion/wfgen）
# ===========================================================================
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

WFUSION=${WFUSION:-wfusion}
WFGEN=${WFGEN:-wfgen}
PY=${PYTHON:-python3}
PORT=9800
CONF=conf/refresh.wfusion.toml
LOG=data/logs/wfusion_refresh.log
ALERTS=data/detect/alerts.ndjson
CSV=data/detect/baseline_ref.csv

mkdir -p data/logs data/detect
rm -f "$LOG" "$ALERTS" data/daemon.log data/live.jsonl

CSV_BAK=$(mktemp)
cp "$CSV" "$CSV_BAK"

echo "==> 0. 启动 daemon（conf/refresh.wfusion.toml，baseline_ref refresh=1s）"
"$WFUSION" daemon --config "$CONF" --work-dir . > data/daemon.log 2>&1 &
DAEMON_PID=$!
restore() {
  kill "$DAEMON_PID" 2>/dev/null || true
  cp "$CSV_BAK" "$CSV"
  rm -f "$CSV_BAK"
}
trap restore EXIT

for i in $(seq 1 60); do
  nc -z 127.0.0.1 "$PORT" 2>/dev/null && break
  sleep 0.2
done
nc -z 127.0.0.1 "$PORT" || { echo "ERROR: TCP 源未就绪"; tail -30 "$LOG" 2>/dev/null || true; exit 1; }

send_batch() {
  local offset=$1 count=1500 span=90
  "$PY" scripts/gen_metrics_live.py "$count" "$span" "$offset" > data/live.jsonl
  "$WFGEN" send --scenario models/scenarios/metrics_baseline.wfg \
    --input data/live.jsonl --addr 127.0.0.1:$PORT \
    --ws models/schemas/metrics.wfs 2>&1 | tail -1
}

alert_count() {
  if [[ -f "$ALERTS" ]]; then wc -l < "$ALERTS" | tr -d ' '; else echo 0; fi
}

echo "==> 1. phase1：正常 live 注入（旧表，预期无告警）"
send_batch 60
sleep 4
N1=$(alert_count)
echo "   phase1 alerts count = $N1 (expect 0)"
if [[ "$N1" != "0" ]]; then
  echo "FAIL: phase1 不应告警（旧表 mu≈1000 时 dev<5%）" >&2
  exit 1
fi

echo "==> 2. phase2：覆盖 baseline_ref.csv（5号线 mu: 1000 → 5）"
"$PY" - <<'PYEOF'
import csv
p = "data/detect/baseline_ref.csv"
with open(p, encoding="utf-8") as f:
    rows = list(csv.DictReader(f))
for r in rows:
    if r["entity"] == "5号线":
        r.update(n="1", sum="5.0", sum_sq="25.0", mu="5.0", sigma="0.0")
with open(p, "w", encoding="utf-8", newline="") as f:
    w = csv.DictWriter(f, fieldnames=rows[0].keys())
    w.writeheader()
    w.writerows(rows)
PYEOF
sleep 4   # ≥3 个 1s 刷新 tick

echo "==> 3. phase3：同批 live 注入（预期仅 5号线 告警，dev≈199）"
send_batch 60
sleep 4

RELOADS=$(grep -c "provider refresh loaded table=baseline_ref" "$LOG" || true)
echo "   refresh reload count in log = $RELOADS (expect >=2)"
if (( RELOADS < 2 )); then
  echo "FAIL: 未见周期重载日志" >&2
  grep "provider refresh" "$LOG" | tail -5
  exit 1
fi

N2=$(alert_count)
echo "   phase3 alerts count = $N2"
if (( N2 < 1 )); then
  echo "FAIL: phase3 应有告警（5号线 相对刷新后 mu=5 偏离 ~199x）" >&2
  exit 1
fi

# 断言：告警实体只有 5号线，且 deviation ≈ 199
"$PY" - <<'PYEOF'
import json, os, sys
p = "data/detect/alerts.ndjson"
alerts = []
if os.path.exists(p):
    with open(p, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                alerts.append(json.loads(line))
bad = []
by_entity = {}
for a in alerts:
    by_entity.setdefault(a.get("entity"), []).append(a)
if set(by_entity) != {"5号线"}:
    bad.append(f"告警实体集合不符: {sorted(by_entity)} 期望仅 5号线")
for a in alerts:
    dev = float(a.get("deviation", 0.0))
    if a.get("mu") != 5.0:
        bad.append(f"mu 应来自刷新后表(5.0)，实际 {a.get('mu')}")
    if not (150 <= dev <= 250):
        bad.append(f"deviation 期望≈199，实际 {dev}")
if bad:
    print("FAIL:")
    for b in bad:
        print("  -", b)
    sys.exit(1)
print(f"PASS: phase1 0 告警 → phase3 仅 5号线 {len(alerts)} 条（mu=5.0, dev≈199）")
PYEOF

echo ""
echo "PASS: refresh daemon 实证闭环——CSV 覆盖 → 周期重载 → join 判定用新数据"
