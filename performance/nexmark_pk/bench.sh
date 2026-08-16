#!/usr/bin/env bash
# nexmark_pk bench — 参数化吞吐/内存测试（send-arrow 连续流 或 wfgen stream 实时生成）
#
# feed:
#   cont   = send-arrow 连续流：100M 唯一事件预编码成帧文件，一条 TCP 连接连续推
#            （3M+，事件时间固定为预生成数据的 ~30min span）—— 测引擎峰值持续能力
#   stream = wfgen stream 实时生成：事件时间随 slice 推进、按 RATE 目标速率注入
#            （~760k，客户端实时编码受限）—— 测长时实时流稳定性/内存有界
#
# 用法:
#   ./bench.sh [query=q1|q2|q3|q4|q5|q7|q9|all] [feed=cont|stream] [total=100m|30m|10m]
#   调优用环境变量（并行度默认取 conf/wfusion.toml）:
#     PARSE_PARALLELISM / RULE_PARALLELISM / MAX_FRAME_BYTES / MAX_FRAME_ROWS
#     MAX_INGEST_RATE（引擎端限速）/ RATE / SLICE_MS（stream）
#     WARMUP=1（cont：先跑一轮预热不计结果——stash 重建后首跑系统性偏低，须剔除）
# 示例:
#   PARSE_PARALLELISM=6 RULE_PARALLELISM=6 MAX_FRAME_BYTES=204800 ./bench.sh q1 cont 100m
#   PARSE_PARALLELISM=6 RULE_PARALLELISM=6 MAX_INGEST_RATE=5000000 ./bench.sh q1 cont 100m
#   WARMUP=1 ./bench.sh all cont 30m
#
# 输出每查询: EPS（引擎 append 数/墙钟，端到端口径）+ RSS 峰值 + 驱逐数
#   + 口径上下文（并行度/帧大小/时间戳）+ 正确性计数器摘要
# 计时终点 = window append_total 三输入流追平 TOTAL（非 receiver 预读游标）
# 结果写 data/bench_<query>_<feed>.txt（含完整 correctness 明细附录）
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

QUERY="${1:-all}"
FEED="${2:-cont}"
TOTAL="${3:-100m}"
# 调优参数：环境变量（并行度不设默认——write_conf 从 conf/wfusion.toml 读取，env 才覆盖）
PARSE="${PARSE_PARALLELISM:-}"
RULE="${RULE_PARALLELISM:-}"
MAX_FRAME_BYTES="${MAX_FRAME_BYTES:-8388608}"
MAX_FRAME_ROWS="${MAX_FRAME_ROWS:-100000}"
MAX_INGEST_RATE="${MAX_INGEST_RATE:-}"
RATE="${RATE:-3000000}"
SLICE_MS="${SLICE_MS:-1000}"
WARMUP="${WARMUP:-0}"

REPO="${REPO:-$(cd ../../../warp-fusion && pwd)}"
WFUSION="${WFUSION:-$REPO/target/release/wfusion}"
WFGEN="${WFGEN:-$REPO/target/release/wfgen}"
PY="${PYTHON:-python3}"
PORT=9800

# ---- 校验 ----
case "$TOTAL" in
  10m) TOTAL_N=10000000;; 30m) TOTAL_N=30000000;; 100m) TOTAL_N=100000000;;
  *) echo "bad total '$TOTAL' (10m|30m|100m)"; exit 1;;
esac
case "$QUERY" in
  q1|q2|q3|q4|q5|q7|q9) QUERIES=("$QUERY");;
  all) QUERIES=(q1 q2 q3 q4 q5 q7 q9);;
  *) echo "bad query '$QUERY' (q1|q2|q3|q4|q5|q7|q9|all)"; exit 1;;
esac
case "$FEED" in
  cont|stream) ;;
  *) echo "bad feed '$FEED' (cont|stream)"; exit 1;;
esac

mkdir -p data
CORES=$(sysctl -n hw.ncpu 2>/dev/null || echo "?")
echo "== bench: query=$QUERY feed=$FEED total=$TOTAL_N rate=$RATE slice_ms=$SLICE_MS cores=$CORES =="

# 等 daemon 释放端口（kill 后优雅关闭可能慢，尤其高内存 daemon）。否则下一个
# daemon bind 9800 失败 → accept 任务退出 → source 通道永久关闭（"connection
# channel closed"，后续连接全收不到）。
wait_port_free() {
  for i in $(seq 1 50); do
    if ! nc -z 127.0.0.1 "$PORT" 2>/dev/null; then return 0; fi
    sleep 0.2
  done
  echo "    警告: 端口 $PORT 超时未释放" >&2
}

# 强杀单个 daemon：SIGTERM → 轮询最多 5s → SIGKILL → 确认进程消失。
# 满负荷 daemon 的优雅退出可能 >5s；只等端口（wait_port_free）会漏掉"端口已
# 释放但进程未退出"的孤儿（listener 关闭后进程仍在烧 CPU），污染后续测量。
# 宽限 10s：GROUP_JOIN_TIMEOUT(3s) + abort 确认(0.5s) + 同步收尾(unwind/flush
# ~1-2s) + 进程拆解（GB 级内存释放、阻塞线程回收 ~0.5-1s）实测 30M 约 4.6s+ε。
kill_daemon() {
  local PID="$1"
  [ -n "$PID" ] || return 0
  kill "$PID" 2>/dev/null
  local i
  for i in $(seq 1 50); do
    kill -0 "$PID" 2>/dev/null || { sleep 1; return 0; }
    sleep 0.2
  done
  echo "    警告: daemon $PID SIGTERM 后 10s 未退出, 强制 SIGKILL" >&2
  kill -9 "$PID" 2>/dev/null
  for i in $(seq 1 25); do
    kill -0 "$PID" 2>/dev/null || { sleep 1; return 0; }
    sleep 0.2
  done
  echo "    错误: daemon $PID 连 SIGKILL 都未退出" >&2
  return 1
}

# 清理所有残留 daemon（含被 kill 的 bench.sh 孤儿化的）：SIGTERM → SIGKILL 兜底。
# 脚本开头与 EXIT trap 各调用一次，幂等。
cleanup_daemons() {
  pkill -f "wfusion daemon" 2>/dev/null
  sleep 2
  pkill -9 -f "wfusion daemon" 2>/dev/null
  wait_port_free
}

cleanup_daemons
trap cleanup_daemons EXIT

# RSS + 瞬时 CPU% 采样（后台，1s 周期，调用方 kill 结束）。
# - ps %cpu 是生命周期平均，无意义；这里取 cputime 差分 / 墙钟差分 = 瞬时核占数%。
# - 输出每行 "RSS_MB CPU_PCT"；ps 失败（权限受限环境）静默跳过，不打印
#   traceback——否则污染 /tmp/bench_rss.txt 使提取错乱。
start_rss() {
  local PID="$1"
  "$PY" - "$PID" > /tmp/bench_rss.txt 2>&1 <<'EOF' &
import re, subprocess, sys, time
pid = int(sys.argv[1])
def secs(s):
    v = 0.0
    for x in s.split(':'):
        v = v * 60 + float(x)
    return v
UNITS = {'': 1, 'K': 1, 'M': 1024, 'G': 1024*1024, 'T': 1024*1024*1024}
def rss_kb_footprint():
    # ps 被权限拒绝时的回退：macOS footprint 工具输出 "Footprint: 912 KB" 等
    r = subprocess.run(['footprint', str(pid)], capture_output=True, text=True)
    m = re.search(r'Footprint:\s*([\d.]+)\s*([KMGT]?)B', r.stdout)
    if not m:
        return None
    return int(float(m.group(1)) * UNITS[m.group(2)])
prev, prev_t = None, time.time()
def sample_ps():
    # 返回 (rss_kb, cputime_secs) 或 None（ps 被权限拒绝/进程不在时）
    try:
        r = subprocess.run(['ps','-o','rss=,cputime=','-p',str(pid)],capture_output=True,text=True)
    except Exception:
        return None
    parts = r.stdout.split()
    if len(parts) != 2:
        return None
    def secs(s):
        v = 0.0
        for x in s.split(':'):
            v = v * 60 + float(x)
        return v
    return int(parts[0]), secs(parts[1])
while True:
    try:
        got = sample_ps()
        if got is not None:
            rss, ct = got
            cur, now = ct, time.time()
            if prev is not None:
                dt = now - prev_t
                cpu = (cur - prev) / dt * 100.0 if dt > 0 else 0.0
                print(f"{rss//1024} {cpu:.1f}", flush=True)
            prev, prev_t = cur, now
        else:
            rss = rss_kb_footprint()
            if rss is not None:
                print(f"{rss//1024} n/a", flush=True)
            prev, prev_t = None, time.time()
    except Exception:
        prev, prev_t = None, time.time()
    time.sleep(1)
EOF
}

# 从采样文件提取 PEAK_RSS / CPU_AVG / CPU_MAX（缺样本时给 n/a）
stat_samples() {
  PEAK=$(awk 'NF==2 && $1>m {m=$1} END {print (m?m:"n/a")}' /tmp/bench_rss.txt)
  CPU_AVG=$(awk 'NF==2 && $2 ~ /^[0-9.]+$/ {s+=$2;n++} END {if(n) printf "%d", s/n; else print "n/a"}' /tmp/bench_rss.txt)
  CPU_MAX=$(awk 'NF==2 && $2 ~ /^[0-9.]+$/ && $2>m {m=$2} END {if(m) printf "%d", m; else print "n/a"}' /tmp/bench_rss.txt)
}

# ---- 写查询 conf：基于 conf/wfusion.toml，覆盖 rules + 并行度（+ 可选限速） ----
# 并行度默认取 conf/wfusion.toml；-p/-r flag 或环境变量覆盖。
write_conf() {
  local Q="$1" M="$2"
  PARSE_V_EFF="${PARSE:-$(sed -n 's/^parse_parallelism = //p' conf/wfusion.toml | head -1)}"
  RULE_V_EFF="${RULE:-$(sed -n 's/^rule_parallelism = //p' conf/wfusion.toml | head -1)}"
  sed -e "s|^rules = .*|rules = \"models/queries/$Q.wfl\"|" \
      -e "s|^parse_parallelism = .*|parse_parallelism = ${PARSE_V_EFF}|" \
      -e "s|^rule_parallelism = .*|rule_parallelism = ${RULE_V_EFF}|" \
      conf/wfusion.toml > /tmp/bench_conf.toml
  # 限速：MAX_INGEST_RATE 设置时在 [runtime] 注入 max_ingest_rate
  if [ -n "${MAX_INGEST_RATE:-}" ]; then
    awk -v r="max_ingest_rate = ${MAX_INGEST_RATE}" '/^rule_exec_timeout = /{print; print r; next} {print}' \
      /tmp/bench_conf.toml > /tmp/bench_conf.toml.tmp
    mv /tmp/bench_conf.toml.tmp /tmp/bench_conf.toml
  fi
}

start_daemon() {
  rm -f data/metrics.ndjson data/wfusion.log data/daemon.log data/stream.log
  "$WFUSION" daemon --config /tmp/bench_conf.toml --work-dir . > data/daemon.log 2>&1 &
  local D=$!
  for i in $(seq 1 40); do nc -z 127.0.0.1 $PORT 2>/dev/null && break; sleep 0.2; done
  echo "$D"
}

# 引擎端到端游标：window append_total 按输入流求和（auction/bid/person）。
# receiver rows_total 是无界预读游标（cont 下 15s 即可拉完 100M 堆内存），
# 用它当计时终点测的是预读速度而非处理能力。window→rule 推送通道有界
# （RULE_CHANNEL_CAPACITY=32，满则 send().await 阻塞），append 被最慢 rule
# 反压，故 append 追平 ≈ 规则真正吃完（误差 ≤32 批次，100M 下 <0.5%）。
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

# ---- 正确性摘要：emitted_total（按规则）+ 致命计数器 ----
# 致命计数器（serialize_failed/dropped_late/cursor_gap/memory_evicted）非零
# 即跑批作废——测量纪律：数字可信的前提。time_evicted 有值属正常窗口关闭。
# 输出两行：SUMMARY 行（进结果行）+ 各规则 emitted（进结果文件）。
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

# 轮末报告：单行结果（stdout + 结果文件）+ correctness 附录（结果文件）。
# 上下文字段（p/r/帧大小/时间戳）写进结果行——事后可追溯口径，防 1MiB/8MiB、
# 并行度混淆（曾因口径混杂误判 ±8%）。
report_result() {
  local Q="$1" FEED="$2" OUT="$3" BODY="$4"
  local CTX="p=${PARSE_V_EFF} r=${RULE_V_EFF} frame_mb=$((MAX_FRAME_BYTES/1048576)) $(date +%m-%d_%H:%M:%S)"
  { echo "$BODY $CTX"; echo "-- correctness --"; correctness_summary; } >> "$OUT"
  # SUMMARY 行回显到 stdout（EMIT 行只进文件）
  local SUM
  SUM=$(grep '^SUMMARY' "$OUT" | tail -1 | cut -d' ' -f2-)
  echo "$BODY [$SUM] $CTX"
  case "$SUM" in
    clean) ;;
    *) echo "    ⚠ 正确性计数器非零，本跑批作废: $SUM" >&2 ;;
  esac
}

# ---- feed=cont：send-arrow 连续流（预编码帧，一条连接推完） ----
# 默认帧大小（8MiB）复用 bench_${TOTAL}.frames；非默认大小用带后缀名（避免覆盖）。
if [ "$MAX_FRAME_BYTES" = "8388608" ]; then
  FRAMES=data/bench_${TOTAL}.frames
else
  FRAMES=data/bench_${TOTAL}_mb${MAX_FRAME_BYTES}.frames
fi
if [ "$FEED" = "cont" ] && [ ! -f "$FRAMES" ]; then
  echo "==> 预编码帧（gen-nexmark ${TOTAL_N} → dump-frames, max_frame_bytes=${MAX_FRAME_BYTES}）"
  "$WFGEN" gen-nexmark "$TOTAL_N" > data/burst_bench.jsonl
  write_conf q1 cont
  local_dummy=$(start_daemon)
  "$WFGEN" dump-frames --scenario scenarios/nexmark.wfg --input data/burst_bench.jsonl \
    --ws models/schemas/nexmark.wfs --addr 127.0.0.1:$PORT --output "$FRAMES" --chunk 1000000 \
    --max-frame-bytes "$MAX_FRAME_BYTES" --max-frame-rows "$MAX_FRAME_ROWS" > /dev/null 2>&1
  # kill_daemon（非裸 kill）：只等端口会漏掉"端口已释放但进程未退出"的孤儿，
  # 孤儿继续烧 CPU 会污染本轮首跑测量
  kill_daemon "$local_dummy"; wait_port_free
  rm -f data/burst_bench.jsonl
  echo "   frames: $FRAMES ($(du -h "$FRAMES" | cut -f1))"
fi

run_cont_one() {
  local Q="$1" OUT_TAG="${2:-cont}"
  local OUT="data/bench_${Q}_${OUT_TAG}.txt"
  write_conf "$Q" cont
  local D=$(start_daemon)
  start_rss "$D"; local SP=$!

  local T0=$("$PY" -c 'import time; print(time.time())')
  "$WFGEN" send-arrow --input "$FRAMES" --addr 127.0.0.1:$PORT > /dev/null 2>&1
  # 等引擎真正消化完（append 追平 TOTAL，而非 ingress 预读）。
  # 超时自适应：按 100k/s 诚实下限 + 600s 余量（on-each 单线程 ~0.3M/s，
  # 100M 需 ~333s；旧的 300s 上限会在真实负载下提前超时）。
  local MAX_SEC=$(( TOTAL_N / 100000 + 600 ))
  local T2=0 APP=0 TIMEOUT=0
  for j in $(seq 1 $(( MAX_SEC * 2 ))); do
    APP=$(engine_appended)
    if [ "${APP:-0}" -ge "$TOTAL_N" ]; then T2=$("$PY" -c 'import time; print(time.time())'); break; fi
    sleep 0.5
  done
  if [ "$T2" = 0 ]; then T2=$("$PY" -c 'import time; print(time.time())'); TIMEOUT=1; fi
  sleep 3
  kill $SP 2>/dev/null; wait $SP 2>/dev/null; kill_daemon $D; wait_port_free

  # EPS 按 append 计：追平时 == TOTAL；超时时 = 实际处理速率（不含预读水分）
  local EPS=$("$PY" -c "print(int($APP/($T2-$T0)))")
  stat_samples
  # grep -c 无匹配时退出码 1 但仍输出 0——`|| echo 0` 会叠出双 0，改为兜底默认
  local EV=$(grep -c 'memory eviction' data/wfusion.log 2>/dev/null || true); EV=${EV:-0}
  : > "$OUT"   # 预清空，防追加残留上一轮
  local TO=""; [ "$TIMEOUT" = 1 ] && TO=" ⚠TIMEOUT(appended未追平,EPS=实际速率)"
  report_result "$Q" cont "$OUT" \
    "$Q/cont: EPS=$EPS RSS_peak=${PEAK}MB cpu_avg=${CPU_AVG}% cpu_max=${CPU_MAX}% evict=$EV appended=$APP/$TOTAL_N$TO"
}

# ---- feed=stream：wfgen stream 实时生成（事件时间推进） ----
run_stream_one() {
  local Q="$1"
  local OUT="data/bench_${Q}_stream.txt"
  write_conf "$Q" stream
  local D=$(start_daemon)
  start_rss "$D"; local SP=$!

  local T0=$("$PY" -c 'import time; print(time.time())')
  "$WFGEN" stream --scenario-dir scenarios --ws models/schemas/nexmark.wfs \
    --wfl models/queries/$Q.wfl --addr 127.0.0.1:$PORT \
    --rate "$RATE" --slice-ms "$SLICE_MS" > data/stream.log 2>&1 &
  local S=$!

  # 等引擎消化完（append 追平 TOTAL）。若引擎持续能力 < RATE，backlog 会
  # 一直堆积、append 永远追不上 → 超时退出，此时 EPS 按实际 append 数计算，
  # 即"撑不住目标速率"的诚实信号。
  local MAX_SEC=900
  if [ "$RATE" -gt 0 ] 2>/dev/null; then MAX_SEC=$(( TOTAL_N / RATE * 3 + 60 ))
  else MAX_SEC=$(( TOTAL_N / 100000 + 600 )); fi   # 不限速：与 cont 同款自适应
  local T2=0 APP=0 TIMEOUT=0
  for j in $(seq 1 $(( MAX_SEC * 2 ))); do
    APP=$(engine_appended)
    if [ "${APP:-0}" -ge "$TOTAL_N" ]; then T2=$("$PY" -c 'import time; print(time.time())'); break; fi
    sleep 0.5
  done
  if [ "$T2" = 0 ]; then T2=$("$PY" -c 'import time; print(time.time())'); TIMEOUT=1; fi
  kill $S 2>/dev/null; wait $S 2>/dev/null
  sleep 3
  kill $SP 2>/dev/null; wait $SP 2>/dev/null; kill_daemon $D; wait_port_free

  local EPS=$("$PY" -c "print(int($APP/($T2-$T0)))")
  stat_samples
  local EV=$(grep -c 'memory eviction' data/wfusion.log 2>/dev/null || true); EV=${EV:-0}
  : > "$OUT"
  local TO=""; [ "$TIMEOUT" = 1 ] && TO=" ⚠TIMEOUT(未追平,引擎撑不住目标速率或超时)"
  report_result "$Q" stream "$OUT" \
    "$Q/stream: EPS=$EPS RSS_peak=${PEAK}MB cpu_avg=${CPU_AVG}% cpu_max=${CPU_MAX}% evict=$EV appended=$APP/$TOTAL_N target_rate=$RATE$TO"
}

# ---- 预热轮（WARMUP=1）：stash 重建后首跑系统性偏低（曾三次复现），须剔除 ----
if [ "$WARMUP" = "1" ] && [ "$FEED" = "cont" ]; then
  echo "==> warmup 轮（结果丢弃, 写 /tmp/bench_warmup_q1.txt）"
  run_cont_one q1 warmup_q1
  mv -f data/bench_q1_warmup_q1.txt /tmp/bench_warmup_q1.txt 2>/dev/null || true
fi

for Q in "${QUERIES[@]}"; do
  if [ "$FEED" = "cont" ]; then
    run_cont_one "$Q"
  else
    run_stream_one "$Q"
  fi
done
echo "== done: 结果在 data/bench_*_${FEED}.txt =="
