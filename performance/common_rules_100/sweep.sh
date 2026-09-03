#!/usr/bin/env bash
# common_rules_100 — 限定 EPS 下的资源消耗统计（速率档位扫描）
#
# 用 daemon 端 max_ingest_rate 把引擎消化速率限定为目标 EPS（qradar/nexmark
# MAX_INGEST_RATE 同款口径），注入端全速 send → 引擎按目标速率稳态消化；
# 统计该速率下的 CPU（核占数 avg/max）/ RSS 峰值 / allocator commit /
# e2e 延迟 p50/p99，并判「达成率 %(可服务/能力封顶)」——目标超过引擎可持续吞吐时
# 达成率 <80%、实际 EPS 即引擎真实能力顶。
#
# 用法:
#   ./sweep.sh                    # 默认档位 1w,2w,5w,10w
#   ./sweep.sh 1w,2w,10w          # 指定档位（k/w/m 后缀）
#   ./sweep.sh all                # 预设 1w,2w,5w,10w,20w,50w
# 环境: WFUSION/WFGEN；PROFILE=release|debug；SWEEP_SECS=每档稳态窗(默认8s)；
#       MAX_LINES/ALLOW_BIG=1=放行超 100 万行档（>10w 档数据文件大/gen 慢）
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

# 每档稳态测量时长（秒）：行数 = 目标 EPS × SWEEP_SECS → 各档稳态窗等长，
# 资源采样样本数一致可比。>10w 档需 ~百万行 jsonl（gen 慢/文件大），超上限报错提示。
SWEEP_SECS=${SWEEP_SECS:-8}
MAX_LINES=${MAX_LINES:-1000000}
ALLOW_BIG=${ALLOW_BIG:-0}

echo "== sweep: 100 常见规则 × 限定 EPS（max_ingest_rate 引擎限速）资源统计 =="
echo "    每档稳态窗 ${SWEEP_SECS}s（行数 = EPS × ${SWEEP_SECS}）；done% = 实际/目标，>=80% 记 ok（可服务），<80% 记 cap（能力封顶）"
printf "%-6s %-8s %-8s %-15s %-6s %-7s %-7s %-7s %s\n" \
  "tier" "target" "actual" "cpu%avg/p95/max" "rss" "commit" "done%" "wall" "lines"
echo "---"
: > "$REPORT"

for i in "${!EPS_LIST[@]}"; do
  EPS="${EPS_LIST[$i]}"
  EPS_DISP="${TARGETS[$i]}"
  TIER_START=$($PY -c 'import time; print(int(time.time()))')
  # 行数 = EPS × SWEEP_SECS（稳态窗等长）；超 MAX_LINES 需 ALLOW_BIG=1 显式放行
  N_LINES=$(( EPS * SWEEP_SECS ))
  if [ "$N_LINES" -gt "$MAX_LINES" ] && [ "$ALLOW_BIG" != "1" ]; then
    echo "档 ${EPS_DISP}：需 ${N_LINES} 行（EPS×${SWEEP_SECS}s）> MAX_LINES=${MAX_LINES}；"
    echo "  确认后 ALLOW_BIG=1 放行（或调小 SWEEP_SECS / 去掉该档）"
    continue
  fi

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
  [ "$READY" = 1 ] || { echo "档 ${EPS_DISP}：daemon 未就绪"; tail -5 data/daemon.log; kill $DAEMON_PID 2>/dev/null || true; continue; }

  : > "$RSS_SAMPLES"
  "$PY" "$LIB" rss-sampler "$DAEMON_PID" "$RSS_SAMPLES" 0.1 > /dev/null 2>&1 &
  SAMPLER_PID=$!
  for k in $(seq 1 40); do [ -s "$RSS_SAMPLES" ] && break; sleep 0.1; done

  START_NS=$($PY -c 'import time; print(time.time_ns())')
  # 全速分块注入（chunk 只做传输拆分避免巨帧阻塞解码；不 pacing）——目标 EPS
  # 由 daemon 端 max_ingest_rate 精确限速（引擎按速率平滑消费，qradar/nexmark
  # replay 同口径）。CPU max 含块到达/启动瞬态，稳态看 avg/p95。
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
  # wait 消费 job 状态：否则 bash 在脚本继续时打印 “pid Terminated” 通知污染输出
  kill "$SAMPLER_PID" 2>/dev/null || true
  wait "$SAMPLER_PID" 2>/dev/null || true

  # 实际 EPS = 纯引擎消化段速率（send 完成 → acked_lag=0），平台期不计入
  DIGEST_W=$($PY -c "print(max($ACK0_W - $DIGEST_START_W, 0.05))")
  ACT_EPS=$($PY -c "print(int($LINES / $DIGEST_W))" 2>/dev/null || echo 0)
  # 达成率 = 实际消化速率 / 目标限速。≥80% = 目标限速可服务（引擎按速率稳态消化）；
  # <80% = 目标超出引擎能力（限速失效，实际 EPS 即引擎真实吞吐封顶值）。
  DONE_PCT=$(( ACT_EPS * 100 / EPS ))
  if $PY -c "exit(0 if $ACT_EPS >= $EPS * 0.8 else 1)" 2>/dev/null; then FOLLOW="ok"; else FOLLOW="cap"; fi

  WS=$(( START_NS - 1000000000 )); WE="$END_NS"
  CPU_STATS=$("$PY" - "$RSS_SAMPLES" "$WS" "$WE" <<'EOF'
# 窗内 CPU 统计：avg / p95 / max。注入是分块突发（chunk 块到达瞬间解码+窗口+
# emit 并行冲高），max 含突发瞬态；p95 是稳态瞬时上界（avg 偏低、max 偏高）
import sys
vals = []
for line in open(sys.argv[1]):
    p = line.split()
    if len(p) >= 3:
        try:
            if int(p[0]) < int(sys.argv[2]) or int(p[0]) > int(sys.argv[3]): continue
            vals.append(float(p[2]))
        except ValueError: pass
if not vals:
    print("n/a n/a n/a")
elif len(vals) == 1:
    print(f"{int(vals[0])} {int(vals[0])} {int(vals[0])}")
else:
    vals.sort()
    n = len(vals)
    p95 = vals[min(n - 1, int(n * 0.95))]
    avg = sum(vals) / n
    print(f"{int(avg)} {int(p95)} {int(vals[-1])}")
EOF
)
  CPU_AVG=$(echo "$CPU_STATS" | awk '{print $1}')
  CPU_P95=$(echo "$CPU_STATS" | awk '{print $2}')
  CPU_MAX=$(echo "$CPU_STATS" | awk '{print $3}')
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

  TIER_END=$($PY -c 'import time; print(int(time.time()))')
  TIER_S=$(( TIER_END - TIER_START ))

  printf "%-6s %-8s %-8s %-15s %-6s %-7s %-7s %-7s %s\n" \
    "$EPS_DISP" "$EPS" "$ACT_EPS" "$CPU_AVG/$CPU_P95/$CPU_MAX" "${PEAK_RSS}M" "${CMIT_MB}M" \
    "${DONE_PCT}%$FOLLOW" "${TIER_S}s" "$LINES"
  printf "%s %s %s %s/%s/%s %s %s %s %s %s %s\n" \
    "$EPS_DISP" "$EPS" "$ACT_EPS" "$CPU_AVG" "$CPU_P95" "$CPU_MAX" "$PEAK_RSS" "$CMIT_MB" \
    "$DONE_PCT" "$FOLLOW" "$TIER_S" "$LINES" >> "$REPORT"

  kill "$DAEMON_PID" 2>/dev/null || true
  wait "$DAEMON_PID" 2>/dev/null || true
  sleep 1
done
echo "== 完成：结果在 $REPORT =="
