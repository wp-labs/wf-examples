#!/bin/bash
# 跑验证形态（qN-verify.wfl）30M replay——与标准形态 qN.wfl 交叉验算（2026-08-23）。
# 标准形态（stats/CEP 最优版）已统一为 qN.wfl（bench.sh 直接支持），本脚本只跑
# `qN-verify` 版用于双实现对拍。用法: ./run_stats_bench.sh q15-verify q18-verify
# 复用 bench.sh 的 replay 流程: write_conf → daemon → send-arrow 重放 → 追平等待 → 采样。
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

PY="${PYTHON:-python3}"
PORT=9800
WFUSION="${WFUSION:-/Users/zuowenjian/devspace/rust/wfusion/warp-fusion/target/release/wfusion}"
WFGEN="${WFGEN:-/Users/zuowenjian/devspace/rust/wfusion/warp-fusion/target/release/wfgen}"
TOTAL_N="${TOTAL_N:-30000000}"
DATA_VER="${DATA_VER:-v5}"
# 帧文件命名按 bench.sh 的 TOTAL 别名（10m/30m/100m）
TOTAL="${TOTAL:-30m}"
case "$TOTAL" in
  10m) TOTAL_N=10000000;; 30m) TOTAL_N=30000000;; 100m) TOTAL_N=100000000;;
  *) echo "bad total '$TOTAL' (10m|30m|100m)"; exit 1;;
esac
MAX_FRAME_BYTES="${MAX_FRAME_BYTES:-8388608}"
CONNECTIONS="${CONNECTIONS:-1}"
PARSE="${PARSE_PARALLELISM:-}"
RULE="${RULE_PARALLELISM:-}"
if [ "$MAX_FRAME_BYTES" = "8388608" ]; then
  FRAMES="data/bench_${TOTAL}_${DATA_VER}.frames"
else
  FRAMES="data/bench_${TOTAL}_mb${MAX_FRAME_BYTES}_${DATA_VER}.frames"
fi
echo "== stats bench: frames=$FRAMES connections=$CONNECTIONS =="

cleanup() { pkill -9 -f "wfusion daemon" 2>/dev/null; pkill -9 -f "wfgen send-arrow" 2>/dev/null; sleep 1; }
cleanup
trap cleanup EXIT INT TERM

start_rss() { # $1=PID → /tmp/bench_rss.txt
  "$PY" - "$1" > /tmp/bench_rss.txt 2>&1 <<'EOF' &
import re, subprocess, sys, time
pid = int(sys.argv[1])
def secs(s):
    v = 0.0
    for x in s.split(':'):
        v = v * 60 + float(x)
    return v
UNITS = {'': 1, 'K': 1, 'M': 1024, 'G': 1024*1024, 'T': 1024*1024*1024}
def rss_kb_footprint():
    r = subprocess.run(['footprint', str(pid)], capture_output=True, text=True)
    m = re.search(r'Footprint:\s*([\d.]+)\s*([KMGT]?)B', r.stdout)
    if not m: return None
    return int(float(m.group(1)) * UNITS[m.group(2)])
prev, prev_t = None, time.time()
while True:
    try:
        r = subprocess.run(['ps','-o','rss=,cputime=','-p',str(pid)],capture_output=True,text=True)
        parts = r.stdout.split()
        if len(parts) == 2:
            rss, ct = int(parts[0]), secs(parts[1])
            cur, now = ct, time.time()
            if prev is not None:
                dt = now - prev_t
                cpu = (cur - prev) / dt * 100.0 if dt > 0 else 0.0
                print(f"{rss//1024} {cpu:.1f}", flush=True)
            prev, prev_t = cur, now
        else:
            rss = rss_kb_footprint()
            if rss is not None: print(f"{rss//1024} n/a", flush=True)
            prev, prev_t = None, time.time()
    except Exception:
        prev, prev_t = None, time.time()
    time.sleep(1)
EOF
}

stat_samples() {
  PEAK=$(awk 'NF==2 && $1>m {m=$1} END {print (m?m:"n/a")}' /tmp/bench_rss.txt)
  CPU_AVG=$(awk 'NF==2 && $2 ~ /^[0-9.]+$/ {s+=$2;n++} END {if(n) printf "%d", s/n; else print "n/a"}' /tmp/bench_rss.txt)
  CPU_MAX=$(awk 'NF==2 && $2 ~ /^[0-9.]+$/ && $2>m {m=$2} END {if(m) printf "%d", m; else print "n/a"}' /tmp/bench_rss.txt)
}

write_conf() { # $1=wfl 文件名
  local Q="$1"
  local P="${PARSE:-$(sed -n 's/^parse_parallelism = *//p' conf/wfusion.toml | head -1)}"
  local R="${RULE:-$(sed -n 's/^rule_parallelism = *//p' conf/wfusion.toml | head -1)}"
  sed -e "s|^rules = .*|rules = \"models/queries/$Q.wfl\"|" \
      -e "s|^parse_parallelism = .*|parse_parallelism = ${P}|" \
      -e "s|^rule_parallelism = .*|rule_parallelism = ${R}|" \
      conf/wfusion.toml > /tmp/bench_conf.toml
}

start_daemon() {
  rm -f data/metrics.ndjson data/wfusion.log data/daemon.log data/stream.log
  "$WFUSION" daemon --config /tmp/bench_conf.toml --work-dir . > data/daemon.log 2>&1 &
  local D=$!
  local i
  for i in $(seq 1 40); do
    if ! kill -0 "$D" 2>/dev/null; then
      echo "    错误: daemon 启动失败" >&2; tail -20 data/daemon.log >&2; return 1
    fi
    nc -z 127.0.0.1 "$PORT" 2>/dev/null && { echo "$D"; return 0; }
    sleep 0.2
  done
  echo "    错误: daemon 启动超时" >&2; tail -20 data/daemon.log >&2; return 1
}

engine_appended() {
  "$PY" -c "
import json
s=0
for line in open('data/metrics.ndjson'):
    try: o=json.loads(line)
    except: continue
    if o.get('name')=='append_total' and o.get('label') in ('auction_events','bid_events','person_events'):
        s+=int(o.get('value',0))
print(s)"
}

engine_acked_lag() {
  "$PY" -c "
import json
lag={}
for line in open('data/metrics.ndjson'):
    try: o=json.loads(line)
    except: continue
    if o.get('name')=='acked_lag' and o.get('label') in ('auction_events','bid_events','person_events'):
        lag[o.get('label')]=int(o.get('value',0))
print(sum(lag.values()))"
}

correctness_summary() {
  "$PY" - <<'EOF'
import json
from collections import defaultdict
emitted = defaultdict(int); bad = defaultdict(int)
for line in open('data/metrics.ndjson'):
    try: o = json.loads(line)
    except Exception: continue
    n, l = o.get('name'), o.get('label', '')
    try: v = int(float(o.get('value', 0) or 0))
    except (TypeError, ValueError): continue
    if n == 'emitted_total': emitted[l] += v
    elif n in ('serialize_failed_total', 'dropped_late_total',
               'memory_evicted_total') and v: bad[n] += v
    elif n == 'cursor_gap_total' and v: bad['cursor_gap[%s]' % l] += v
bad_str = ' '.join('%s=%d' % kv for kv in sorted(bad.items())) or 'clean'
print('SUMMARY %s' % bad_str)
for k in sorted(emitted): print('EMIT %s %d' % (k, emitted[k]))
EOF
}

comma() { "$PY" -c 'import sys
v=sys.argv[1]
try: print(f"{int(float(v)):,}")
except Exception: print(v)' "$1" 2>/dev/null || echo "$1"; }

run_one() {
  local Q="$1"
  local OUT="data/bench_${Q}_replay.txt"
  write_conf "$Q"
  local D
  D=$(start_daemon) || exit 1
  start_rss "$D"; local SP=$!
  local T0=$("$PY" -c 'import time; print(time.time())')
  "$WFGEN" send-arrow --input "$FRAMES" --addr 127.0.0.1:$PORT --connections "$CONNECTIONS" > /dev/null 2>&1 &
  local CLIENT=$!
  local MAX_SEC=$(( TOTAL_N / 100000 + 600 ))
  local T2=0 APP=0
  for j in $(seq 1 $(( MAX_SEC * 10 ))); do
    APP=$(engine_appended); DRAINED=$(engine_acked_lag)
    if [ "${APP:-0}" -ge "$TOTAL_N" ] && [ "${DRAINED:-1}" = "0" ]; then
      T2=$("$PY" -c 'import time; print(time.time())'); break
    fi
    if [ $(( j % 100 )) -eq 0 ]; then
      echo "  $Q ingest: $(comma "${APP:-0}")/$(comma "$TOTAL_N") ack_lag=${DRAINED:-1}"
    fi
    sleep 0.1
  done
  # 追平后**不 kill 客户端**: 等 send-arrow 自然推完 → receiver 自然完成 →
  # EOS 触发 stats flush（此时 alert 通道仍开, 产出可靠落盘）。提前 kill 会让
  # EOS 由 shutdown cancel 触发, stats flush（构建百万级 alert 需数秒）与 alert
  # 通道 join-timeout 关闭竞争 → 产出被丢（10M+30m 窗 EMIT=0 根因）。
  echo "  $Q 等 send-arrow 自然推完（receiver → EOS flush）..."
  wait "$CLIENT" 2>/dev/null
  if [ "$T2" = 0 ]; then T2=$("$PY" -c 'import time; print(time.time())'); fi
  local FLUSH_WAIT="${FLUSH_WAIT:-8}"
  echo "  $Q EOS flush 落盘等待 ${FLUSH_WAIT}s"
  sleep "$FLUSH_WAIT"
  kill $SP 2>/dev/null; wait $SP 2>/dev/null
  local D_PID=$D; D=
  kill "$D_PID" 2>/dev/null
  # 优雅关闭宽限: stats close flush 构建百万级 alert 需数秒-数十秒（q19 30M
  # ≈ 8M 条 ~13s, GROUP_JOIN_TIMEOUT=60s）; 过早 SIGKILL 截断 flush 丢尾部产出。
  for i in $(seq 1 400); do kill -0 "$D_PID" 2>/dev/null || break; sleep 0.2; done
  kill -9 "$D_PID" 2>/dev/null
  for i in $(seq 1 25); do nc -z 127.0.0.1 "$PORT" 2>/dev/null || break; sleep 0.2; done
  local EPS=$("$PY" -c "print(int($APP/($T2-$T0)))")
  stat_samples
  local EV=$(grep -c 'memory eviction' data/wfusion.log 2>/dev/null || true); EV=${EV:-0}
  : > "$OUT"
  echo "$Q/replay: EPS=$(comma "$EPS") · RSS_peak=$(comma "$PEAK")MB · CPU ${CPU_AVG}%avg/${CPU_MAX}%max · evict=$EV · appended=$(comma "$APP")/$(comma "$TOTAL_N")" >> "$OUT"
  { echo "-- correctness --"; correctness_summary; } >> "$OUT"
  local SUM; SUM=$(grep '^SUMMARY' "$OUT" | tail -1 | cut -d' ' -f2-)
  echo "$Q/replay: EPS=$(comma "$EPS") · RSS_peak=$(comma "$PEAK")MB · CPU ${CPU_AVG}%avg/${CPU_MAX}%max · evict=$EV · [$SUM]"
  cleanup
}

for Q in "$@"; do
  [ -f "models/queries/$Q.wfl" ] || { echo "跳过: models/queries/$Q.wfl 不存在" >&2; continue; }
  run_one "$Q"
done
echo "== done: 结果在 data/bench_*_replay.txt =="
