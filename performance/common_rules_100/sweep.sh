#!/usr/bin/env bash
# common_rules_100 — 限定 EPS 下的资源消耗统计（速率档位扫描）
#
# 用 daemon 端 max_ingest_rate 把引擎消化速率限定为目标 EPS（qradar/nexmark
# MAX_INGEST_RATE 同款口径），注入端全速 send → 引擎按目标速率稳态消化；
# 统计该速率下的 CPU（核占数 avg/max）/ RSS 峰值 / allocator commit /
# e2e 延迟 p50/p99，并判定「跟上 / 达上限」（目标 > 引擎能力时实际 EPS 到顶）。
#
# 用法:
#   ./sweep.sh                    # 默认档位 1w,2w,5w,10w
#   ./sweep.sh 1w,2w,10w          # 指定档位（k/w/m 后缀）
#   ./sweep.sh all                # 预设 1w,2w,5w,10w,20w,50w
# 环境: WFUSION/WFGEN；PROFILE=release|debug
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

PROFILE="${PROFILE:-release}"
REPO_ROOT="${REPO_ROOT:-}"
if [ -z "$REPO_ROOT" ] && [ -d "../../../warp-fusion" ]; then
  REPO_ROOT="$(cd ../../../warp-fusion && pwd)"
fi
WFUSION="${WFUSION:-}"; WFGEN="${WFGEN:-}"
if [ -z "$WFUSION" ] && [ -n "$REPO_ROOT" ] && [ -x "$REPO_ROOT/target/$PROFILE/wfusion" ]; then
  WFUSION="$REPO_ROOT/target/$PROFILE/wfusion"
fi
if [ -z "$WFGEN" ] && [ -n "$REPO_ROOT" ] && [ -x "$REPO_ROOT/target/$PROFILE/wfgen" ]; then
  WFGEN="$REPO_ROOT/target/$PROFILE/wfgen"
fi
WFUSION="${WFUSION:-$(command -v wfusion 2>/dev/null || true)}"
WFGEN="${WFGEN:-$(command -v wfgen 2>/dev/null || true)}"
[ -n "$WFUSION" ] && [ -n "$WFGEN" ] || { echo "错误: 找不到 wfusion/wfgen" >&2; exit 1; }

PY=${PYTHON:-python3}
LIB="../scripts/bench_lib.py"     # rss-sampler：0.1s 周期 RSS+CPU 核占采样
PORT=9800
METRICS=data/metrics.ndjson
RSS_SAMPLES=/tmp/cr100_sweep_rss.txt
REPORT=data/sweep_eps.txt

to_eps() { "$PY" -c 'import sys
v = sys.argv[1].lower()
mul = {"k": 1_000, "w": 10_000, "m": 1_000_000}.get(v[-1], 1)
num = v[:-1] if v[-1] in "kwm" else v
print(int(float(num) * mul))' "$1"; }

# ---- 指标读取 ----
received() { "$PY" - "$METRICS" <<'EOF'
import json, sys
s = 0
try:
    for line in open(sys.argv[1]):
        try: o = json.loads(line)
        except Exception: continue
        if o.get("stage") == "receiver" and o.get("name") == "rows_total" and o.get("label") == "ingress":
            s += int(o.get("value", 0))
except FileNotFoundError:
    pass
print(s)
EOF
}
acked_lag() { "$PY" - "$METRICS" <<'EOF'
import json, sys
last = -1
try:
    for line in open(sys.argv[1]):
        try: o = json.loads(line)
        except Exception: continue
        if o.get("stage") == "window" and o.get("name") == "acked_lag" and o.get("label") == "conn_events":
            last = int(o.get("value", -1))
except FileNotFoundError:
    pass
print(last)
EOF
}
delivered() { "$PY" - "$METRICS" <<'EOF'
import json, sys
s = 0
try:
    for line in open(sys.argv[1]):
        try: o = json.loads(line)
        except Exception: continue
        if o.get("stage") == "router" and o.get("name") == "delivered_total":
            s += int(o.get("value", 0))
except FileNotFoundError:
    pass
print(s)
EOF
}
cm=0; read_latest() { "$PY" - "$METRICS" "$1" "$2" <<'EOF'
import json, sys
last = None
try:
    for line in open(sys.argv[1]):
        try: o = json.loads(line)
        except Exception: continue
        if o.get("stage") == sys.argv[2] and o.get("name") == sys.argv[3] and o.get("label") is None:
            last = o.get("value")
except FileNotFoundError:
    pass
print(last if last is not None else "0")
EOF
}

# ---- 档位 ----
TARGETS_ARG="${1:-1w,2w,5w,10w}"
[ "$TARGETS_ARG" = "all" ] && TARGETS_ARG="1w,2w,5w,10w,20w,50w"
IFS=',' read -ra TARGETS <<< "$TARGETS_ARG"
[ ${#TARGETS[@]} -ge 1 ] || { echo "用法: ./sweep.sh [档位如 1w,2w,10w|all]" >&2; exit 1; }

mkdir -p data
EPS_LIST=()
for t in "${TARGETS[@]}"; do EPS_LIST+=("$(to_eps "$t")"); done

echo "== sweep: 100 条常见规则 × 限定 EPS（max_ingest_rate 引擎限速）资源统计 =="
printf "%-7s %-9s %-9s %-14s %-9s %-9s %-7s %s\n" \
  "档" "EPS目标" "实际EPS" "CPU% avg/max" "RSS" "commit" "跟上" "行数"
echo "---"
: > "$REPORT"

for i in "${!EPS_LIST[@]}"; do
  EPS="${EPS_LIST[$i]}"
  EPS_DISP="${TARGETS[$i]}"
  # 每档行数 = min(EPS×8, 40 万)：低档 ≥8s 稳态窗，高档数据量有界
  N_LINES=$(( EPS * 8 )); [ "$N_LINES" -gt 400000 ] && N_LINES=400000
  N_LINES=$(( (N_LINES / 1000) * 1000 )); [ "$N_LINES" -lt 1000 ] && N_LINES=1000

  rm -f "$METRICS" data/*.ndjson data/wfusion.log data/daemon.log data/sweep.jsonl data/sweep.toml
  "$PY" scripts/gen_events.py "$N_LINES" > data/sweep.jsonl
  LINES=$(wc -l < data/sweep.jsonl)
  # 注入 max_ingest_rate = 目标 EPS、指标 200ms（判定分辨率）；临时 conf
  awk -v r="max_ingest_rate = $EPS" '/^rule_exec_timeout = /{print; print r; next} {print}' \
    conf/wfusion.toml > data/sweep.toml
  awk '/report_interval = /{print "report_interval = \"200ms\""; next} {print}' \
    data/sweep.toml > data/sweep2.toml && mv data/sweep2.toml data/sweep.toml

  "$WFUSION" daemon --config data/sweep.toml --work-dir . > data/daemon.log 2>&1 &
  DAEMON_PID=$!
  READY=0
  for k in $(seq 1 50); do
    if nc -z 127.0.0.1 "$PORT" 2>/dev/null; then READY=1; break; fi
    sleep 0.2
  done
  [ "$READY" = 1 ] || { echo "档 $EPS_DISP：daemon 未就绪"; tail -5 data/daemon.log; kill $DAEMON_PID 2>/dev/null || true; continue; }

  : > "$RSS_SAMPLES"
  "$PY" "$LIB" rss-sampler "$DAEMON_PID" "$RSS_SAMPLES" 0.1 > /dev/null 2>&1 &
  SAMPLER_PID=$!
  for k in $(seq 1 40); do [ -s "$RSS_SAMPLES" ] && break; sleep 0.1; done

  START_NS=$($PY -c 'import time; print(time.time_ns())')
  "$WFGEN" send --scenario scenarios/common.wfg --input data/sweep.jsonl \
    --addr 127.0.0.1:$PORT --ws models/schemas/network.wfs \
    --chunk 20000 > /dev/null 2>&1
  DIGEST_START_W=$($PY -c 'import time; print(time.time())')
  # 等引擎消化完：delivered 累计 ≥ LINES（窗口收全）且 acked_lag=0（规则读完）
  WAIT_ITERS=$(( N_LINES / EPS * 3 ))
  [ "$WAIT_ITERS" -gt 500 ] && WAIT_ITERS=500
  [ "$WAIT_ITERS" -lt 10 ] && WAIT_ITERS=10
  for k in $(seq 1 "$WAIT_ITERS"); do
    LAG=$(acked_lag)
    if [ "$(delivered)" -ge "$LINES" ] && [ "${LAG:-1}" = "0" ]; then break; fi
    sleep 0.2
  done
  ACK0_W=$($PY -c 'import time; print(time.time())')
  sleep 2
  END_NS=$($PY -c 'import time; print(time.time_ns())')
  kill "$SAMPLER_PID" 2>/dev/null || true

  # 实际 EPS = 纯引擎消化段速率（send 完成 → acked_lag=0），平台期不计入
  DIGEST_W=$($PY -c "print(max($ACK0_W - $DIGEST_START_W, 0.05))")
  ACT_EPS=$($PY -c "print(int($LINES / $DIGEST_W))" 2>/dev/null || echo 0)
  # 跟上 = 实际消化速率达到目标限速；目标 > 引擎能力时实际到顶 → 达上限
  if $PY -c "exit(0 if $ACT_EPS >= $EPS * 0.8 else 1)" 2>/dev/null; then FOLLOW="跟上"; else FOLLOW="达上限"; fi

  WS=$(( START_NS - 1000000000 )); WE="$END_NS"
  CPU_AVG=$("$PY" - "$RSS_SAMPLES" "$WS" "$WE" <<'EOF'
import sys
s = n = 0
for line in open(sys.argv[1]):
    p = line.split()
    if len(p) >= 3:
        try:
            if int(p[0]) < int(sys.argv[2]) or int(p[0]) > int(sys.argv[3]): continue
            s += float(p[2]); n += 1
        except ValueError: pass
print(int(s / n) if n else "n/a")
EOF
)
  CPU_MAX=$("$PY" - "$RSS_SAMPLES" "$WS" "$WE" <<'EOF'
import sys
mx = n = 0
for line in open(sys.argv[1]):
    p = line.split()
    if len(p) >= 3:
        try:
            if int(p[0]) < int(sys.argv[2]) or int(p[0]) > int(sys.argv[3]): continue
            mx = max(mx, float(p[2])); n += 1
        except ValueError: pass
print(int(mx) if n else "n/a")
EOF
)
  PEAK_RSS=$("$PY" - "$RSS_SAMPLES" <<'EOF'
import sys
mx = 0
for line in open(sys.argv[1]):
    p = line.split()
    if len(p) >= 2:
        try: mx = max(mx, float(p[1]))
        except ValueError: pass
print(int(mx))
EOF
)
  CMIT=$(read_latest alloc current_commit_bytes); [ "${CMIT:-0}" -gt 0 ] 2>/dev/null || CMIT=0
  CMIT_MB=$(( CMIT / 1048576 ))

  printf "%-7s %-9s %-9s %-14s %-9s %-9s %-7s %s\n" \
    "$EPS_DISP" "$EPS" "$ACT_EPS" "$CPU_AVG/$CPU_MAX" "${PEAK_RSS}M" "${CMIT_MB}M" \
    "$FOLLOW" "$LINES"
  printf "%s %s %s %s/%s %s %s %s %s\n" \
    "$EPS_DISP" "$EPS" "$ACT_EPS" "$CPU_AVG" "$CPU_MAX" "$PEAK_RSS" "$CMIT_MB" \
    "$FOLLOW" "$LINES" >> "$REPORT"

  kill "$DAEMON_PID" 2>/dev/null || true
  wait "$DAEMON_PID" 2>/dev/null || true
  sleep 1
done
echo "== 完成：结果在 $REPORT =="
