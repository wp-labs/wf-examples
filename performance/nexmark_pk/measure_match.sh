#!/usr/bin/env bash
# 精确计时：ingest 时间 + 规则 match 追平时间（区分开）。
#
# 用法: ./measure_match.sh <wfusion-bin> <wfgen-bin> [N]
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

WFUSION_BIN="$1"; WFGEN_BIN="$2"; N="${3:-2000000}"
PY=${PYTHON:-python3}; PORT=9800
METRICS=data/metrics.ndjson

mkdir -p data
rm -f "$METRICS" data/wfusion.log data/daemon.log data/default.ndjson data/burst.jsonl data/burst.frames

# sum_metric <name> <label>  — 按 name+label 求和
sum_metric() {
  "$PY" - "$METRICS" "$1" "$2" <<'EOF'
import json, sys
name, label = sys.argv[2], sys.argv[3]
s = 0
try:
    for line in open(sys.argv[1]):
        try: o = json.loads(line)
        except Exception: continue
        if o.get("name") == name and o.get("label", "") == label:
            s += int(o.get("value", 0))
except FileNotFoundError:
    pass
print(s)
EOF
}

# sum_all <name>  — 该 name 所有 label 求和
sum_all() {
  "$PY" - "$METRICS" "$1" <<'EOF'
import json, sys
name = sys.argv[2]
s = 0
try:
    for line in open(sys.argv[1]):
        try: o = json.loads(line)
        except Exception: continue
        if o.get("name") == name:
            s += int(o.get("value", 0))
except FileNotFoundError:
    pass
print(s)
EOF
}

"$WFUSION_BIN" daemon --config conf/wfusion.toml --work-dir . > data/daemon.log 2>&1 &
DAEMON_PID=$!
trap 'kill $DAEMON_PID 2>/dev/null || true' EXIT

for i in $(seq 1 50); do nc -z 127.0.0.1 "$PORT" 2>/dev/null && break; sleep 0.2; done

T0=$("$PY" -c 'import time; print(time.time())')
"$WFGEN_BIN" gen-nexmark "$N" > data/burst.jsonl
"$WFGEN_BIN" dump-frames --scenario scenarios/nexmark.wfg --input data/burst.jsonl \
  --ws models/schemas/nexmark.wfs --addr 127.0.0.1:$PORT --output data/burst.frames \
  --chunk 1000000 > /dev/null 2>&1
"$WFGEN_BIN" send-arrow --input data/burst.frames --addr 127.0.0.1:$PORT > /dev/null 2>&1

# ingest 追平
INGEST_T=-1
for i in $(seq 1 600); do
  RX=$(sum_metric rows_total ingress)
  if [ "${RX:-0}" -ge "$N" ]; then INGEST_T=$("$PY" -c "import time; print(round(time.time() - $T0, 2))"); break; fi
  sleep 0.2
done

# matches 追平：连续 3 次(1s)不变则视为完成
MATCH_T=-1; LAST=-1; STABLE=0
for i in $(seq 1 600); do
  M=$(sum_all matches_total)
  if [ "$M" = "$LAST" ]; then STABLE=$((STABLE+1)); else STABLE=0; LAST=$M; fi
  if [ "$STABLE" -ge 3 ]; then MATCH_T=$("$PY" -c "import time; print(round(time.time() - $T0, 2))"); break; fi
  sleep 1
done

echo "events=$N"
echo "ingest 追平: ${INGEST_T}s"
echo "match  追平: ${MATCH_T}s  (matches=$LAST)"
kill $DAEMON_PID 2>/dev/null || true
