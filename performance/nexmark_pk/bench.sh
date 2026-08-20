#!/usr/bin/env bash
# nexmark_pk bench — 参数化吞吐/内存测试（send-arrow 连续流 或 wfgen stream 实时生成）
#
# feed:
#   replay = send-arrow 重放预编码帧（旧名 cont）：100M 唯一事件预编码成帧文件，CONNECTIONS 条 TCP
#            连接并发推（默认 1；每条连接推完整帧文件 —— C-UCP 供给并发，配套
#            引擎 source `instances` 消化，见 wp-reactor docs/design/concurrency-scaling.md）
#            （3M+，事件时间固定为预生成数据的 ~30min span）—— 测引擎峰值持续能力
#   stream = wfgen stream 实时生成：事件时间随 slice 推进、按 RATE 目标速率注入
#            （~760k，客户端实时编码受限）—— 测长时实时流稳定性/内存有界
#
# 用法:
#   ./bench.sh [query=q1|..|q22|all] [feed=replay|stream] [total=100m|30m|10m]   (旧名 cont 已移除)
#   ./bench.sh clean [cache|all]   清除生成数据：
#       cache（默认）= 预编码帧/分片缓存 + 日志 + 临时文件（可再生，磁盘大头，
#                     典型 ~10G/100m）；保留结果文件 data/bench_*.txt
#       all          = 连结果文件 data/bench_*.txt、data/verify_*.txt 一起删
#   调优用环境变量（并行度默认取 conf/wfusion.toml）:
#     PARSE_PARALLELISM / RULE_PARALLELISM / MAX_FRAME_BYTES / MAX_FRAME_ROWS
#     MAX_INGEST_RATE（引擎端限速）/ RATE / SLICE_MS（stream）
#     CONNECTIONS（replay 并发连接数，默认 1——2026-08-20 起默认单连接：
#       gen-nexmark 输出已按事件时间排序（v2 数据），单连接整文件推保持时间
#       有序 → over=10m 时间驱逐生效 → 窗口只持 ~10 分钟数据 → 内存/吞吐双赢
#       （q1 100M：RSS 24GB→3GB、EPS 11M→26M，正确性 clean）。多连接
#       （显式 CONNECTIONS>1）会让批次时间乱序（时间驱逐失效、窗口持全量、
#       内存膨胀），仅在有状态负载需要键闭包分片时使用，并配 SHARD_KEYS）
#     SHARD_KEYS（键闭包分片键，默认空=不分片单连接推整文件；CONNECTIONS>1 时
#       配 "bid_events:auction,auction_events:id,person_events:id" 走生成时
#       shard-frames --shard-files，同 key 同连接，有状态负载也安全）
#     WARMUP=1（replay：先跑一轮预热不计结果——stash 重建后首跑系统性偏低，须剔除）
# 示例:
#   PARSE_PARALLELISM=6 RULE_PARALLELISM=6 MAX_FRAME_BYTES=204800 ./bench.sh q1 replay 100m
#   WARMUP=1 ./bench.sh all replay 30m
#   CONNECTIONS=4 SHARD_KEYS="bid_events:auction,auction_events:id,person_events:id" ./bench.sh q2 replay 30m  # 有状态:键闭包多连接
#   DATA_VER=old ./bench.sh q1 replay 100m   # 强制用旧乱序数据复现对比
#
# 输出每查询: EPS（引擎 append 数/墙钟，端到端口径）+ RSS 峰值 + 驱逐数
#   + 口径上下文（并行度/帧大小/时间戳）+ 正确性计数器摘要
# 计时终点 = window append_total 三输入流追平 TOTAL（非 receiver 预读游标）
# 结果写 data/bench_<query>_<feed>.txt（含完整 correctness 明细附录）
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

# ---- clean：清除生成数据（缓存/日志/临时 → 结果文件） ----
# 必须在 QUERY 校验之前拦截（clean 不是 query）。
# 缓存按 TOTAL×DATA_VER×帧大小×分片键可再生成（gen-nexmark → dump-frames/
# shard-frames），是磁盘大头（100m 帧缓存 ~7.2G）；结果 txt 是测量记录保留。
# 内联 daemon 清理（不依赖后文函数定义）：删除前确保没有进程在写。
if [ "${1:-}" = "clean" ]; then
  CLEAN_MODE="${2:-cache}"
  case "$CLEAN_MODE" in
    cache|all)
      echo "== bench.sh clean ${CLEAN_MODE}: 清除生成数据 =="
      pkill -9 -f "wfusion daemon" 2>/dev/null
      pkill -9 -f "wfgen send-arrow" 2>/dev/null
      sleep 1
      # 大缓存：预编码帧 + 键闭包分片帧（可再生）
      rm -f data/bench_*.frames data/shard_*.frames
      # 日志/临时：运行残留（start_daemon 每次 rm -f 重写，可任意删）
      rm -f data/metrics.ndjson data/wfusion.log data/daemon.log data/stream.log \
            data/error.ndjson data/burst_bench.jsonl data/bench_q1q21_100m.log \
            data/daemon_file.log data/wfusion_file.log \
            /tmp/bench_rss.txt /tmp/bench_conf.toml /tmp/bench_conf.toml.tmp \
            /tmp/bench_gt_verify.json /tmp/bench_warmup_q1.txt
      if [ "$CLEAN_MODE" = "all" ]; then
        # 结果文件 + 旧验证产物（--verify 输出在 /tmp 已清，data/verify_*.txt 是旧命名的残留）
        rm -f data/bench_*_replay.txt data/bench_*_stream.txt data/verify_*.txt \
              data/window_shard_bench_*.txt
        echo "  → 结果文件已删"
      else
        echo "  → 保留结果文件 data/bench_*.txt（要连结果一起删用: ./bench.sh clean all）"
      fi
      echo "  → data/ 剩余 $(du -sh data 2>/dev/null | cut -f1)"
      exit 0
      ;;
    *) echo "bad clean mode '$CLEAN_MODE' (cache|all)"; exit 1;;
  esac
fi

QUERY="${1:-all}"
FEED="${2:-replay}"
TOTAL="${3:-100m}"
# --verify：跑批后用 `wfgen verify-nexmark` ground truth 对拍 EMIT（回归验证）。
# 作为任意位置参数（如 `./bench.sh all replay 30m --verify`）或 VERIFY=1。
VERIFY="${VERIFY:-0}"
for arg in "$@"; do [ "$arg" = "--verify" ] && VERIFY=1; done
# 调优参数：环境变量（并行度不设默认——write_conf 从 conf/wfusion.toml 读取，env 才覆盖）
PARSE="${PARSE_PARALLELISM:-}"
RULE="${RULE_PARALLELISM:-}"
MAX_FRAME_BYTES="${MAX_FRAME_BYTES:-8388608}"
MAX_FRAME_ROWS="${MAX_FRAME_ROWS:-100000}"
MAX_INGEST_RATE="${MAX_INGEST_RATE:-}"
RATE="${RATE:-3000000}"
SLICE_MS="${SLICE_MS:-1000}"
WARMUP="${WARMUP:-0}"
CONNECTIONS="${CONNECTIONS:-1}"
# 数据版本指纹：gen-nexmark 排序输出（2026-08-20）后，帧/分片缓存必须带版本号，
# 否则旧乱序缓存被静默复用（时间驱逐失效、窗口持全量、RSS 20GB+ 的根因）。
# 换 DATA_VER 即强制重新生成对应版本缓存。
DATA_VER="${DATA_VER:-v2}"
# 键闭包分片键：默认空 = 单连接整文件推（时间有序 → 时间驱逐生效）。
# CONNECTIONS>1 时配三流各自按键分，走生成时 shard-frames（同 key 同连接）。
SHARD_KEYS="${SHARD_KEYS:-}"

# 二进制来源：优先本地 warp-fusion 的 target/release 构建（仅当存在时）；否则回退 PATH。
# 不把路径固化为 ../../../warp-fusion —— 脚本可复制到任意目录运行，只要 wfusion/wfgen 在 PATH。
REPO="${REPO:-}"
if [ -z "$REPO" ] && [ -d "../../../warp-fusion" ]; then
  REPO="$(cd ../../../warp-fusion && pwd)"
fi
WFUSION="${WFUSION:-}"
WFGEN="${WFGEN:-}"
if [ -z "$WFUSION" ] && [ -n "$REPO" ] && [ -x "$REPO/target/release/wfusion" ]; then
  WFUSION="$REPO/target/release/wfusion"
fi
if [ -z "$WFGEN" ] && [ -n "$REPO" ] && [ -x "$REPO/target/release/wfgen" ]; then
  WFGEN="$REPO/target/release/wfgen"
fi
if [ -z "$WFUSION" ]; then WFUSION="$(command -v wfusion 2>/dev/null || true)"; fi
if [ -z "$WFGEN" ]; then WFGEN="$(command -v wfgen 2>/dev/null || true)"; fi
if [ -z "$WFUSION" ] || [ -z "$WFGEN" ]; then
  echo "错误: 找不到 wfusion/wfgen 二进制（设置 REPO/WFUSION/WFGEN，或加入 PATH）" >&2
  exit 1
fi
PY="${PYTHON:-python3}"
PORT=9800

# ---- 校验 ----
case "$TOTAL" in
  10m) TOTAL_N=10000000;; 30m) TOTAL_N=30000000;; 100m) TOTAL_N=100000000;;
  *) echo "bad total '$TOTAL' (10m|30m|100m)"; exit 1;;
esac
case "$QUERY" in
  q1|q2|q3|q4|q5|q6|q7|q8|q9|q10|q11|q12|q13|q14|q15|q16|q17|q18|q19|q20|q21|q22) QUERIES=("$QUERY");;
  all) QUERIES=(q1 q2 q3 q4 q5 q6 q7 q8 q9 q10 q11 q12 q13 q14 q15 q16 q17 q18 q19 q20 q21 q22);;
  *) echo "bad query '$QUERY' (q1..q22|all)"; exit 1;;
esac
case "$FEED" in
  replay|stream) ;;
  *) echo "bad feed '$FEED' (replay|stream；旧名 cont 已于 2026-08-20 移除，用 replay)"; exit 1;;
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
# 脚本开头与 EXIT/INT/TERM trap 各调用一次，幂等。
cleanup_daemons() {
  # SIGKILL 直接兜底：高内存 daemon 优雅关闭可能 >10s，EXIT 时不留活口
  pkill -9 -f "wfusion daemon" 2>/dev/null
  pkill -9 -f "wfgen send-arrow" 2>/dev/null
  sleep 1
  pkill -9 -f "wfusion daemon" 2>/dev/null
  wait_port_free
}

cleanup_daemons
# INT/TERM 也清理：Ctrl-C 打断脚本时 SIGINT 不保证触发 EXIT trap（bash
# 对被中断命令的 trap 行为），残留 daemon 会继续占端口/烧 CPU。
trap cleanup_daemons EXIT INT TERM

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

# 引擎端到端游标（pull 模型）：
# - append_total：三输入流已 append 的行数累计（EPS 口径 + "数据已全部进入 window"）。
# - acked_lag：每窗口 next_seq - min_acked（未 ack 批数，0 = 所有规则已消费到最新）。
# pull 下 actor 与 rule 解耦，append 追平 ≠ 规则吃完（曾致 Q3 metrics 漏报尾部 emit）。
# 完成条件 = append 追平 TOTAL 且 acked_lag 归零。
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

# pull 完成信号：三输入窗口最新 acked_lag 之和（0 = 规则全消费完）。
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
  # loadavg（1-min）随结果记录：本机是常载开发机（Zed/VM/WorkBuddy 等后台 ~6-7），
  # 同一配置的 EPS 随后台干扰在 43↔55M 间摆动（见 q1-throughput-bisection.md §9），
  # 结果行不带负载上下文无法解释相位差异。
  local LD; LD=$(sysctl -n vm.loadavg 2>/dev/null | awk '{printf "%.1f", $2}')
  local CTX="p=${PARSE_V_EFF} r=${RULE_V_EFF} c=${CONNECTIONS}${SHARD_KEYS:+" s=${SHARD_KEYS%%:*}"} frame_mb=$((MAX_FRAME_BYTES/1048576)) load=${LD:-n/a} $(date +%m-%d_%H:%M:%S)"
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

# ---- feed=replay：send-arrow 重放预编码帧（旧名 cont） ----
# 默认帧大小（8MiB）复用 bench_${TOTAL}_${DATA_VER}.frames；非默认大小用带后缀名（避免覆盖）。
if [ "$MAX_FRAME_BYTES" = "8388608" ]; then
  FRAMES=data/bench_${TOTAL}_${DATA_VER}.frames
else
  FRAMES=data/bench_${TOTAL}_mb${MAX_FRAME_BYTES}_${DATA_VER}.frames
fi
if [ "$FEED" = "replay" ] && [ ! -f "$FRAMES" ]; then
  echo "==> 预编码帧（gen-nexmark ${TOTAL_N} → dump-frames, max_frame_bytes=${MAX_FRAME_BYTES}）"
  "$WFGEN" gen-nexmark "$TOTAL_N" > data/burst_bench.jsonl
  write_conf q1 replay
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

run_replay_one() {
  local Q="$1" OUT_TAG="${2:-replay}"
  local OUT="data/bench_${Q}_${OUT_TAG}.txt"
  write_conf "$Q" replay
  local D=$(start_daemon)
  start_rss "$D"; local SP=$!

  local T0=$("$PY" -c 'import time; print(time.time())')
  if [ -n "$SHARD_KEYS" ] && [ "$CONNECTIONS" -gt 1 ]; then
    # 生成时分片(shard-frames)→ 纯 copy 多连接发送(键闭包,零解码):
    # 先检查分片文件缓存(同 TOTAL×CONNECTIONS×shard-keys 复用),缺则 shard-frames
    # 一次生成。缓存 key 必须带 shard-keys 指纹——换键会静默复用旧分片文件(曾踩坑)。
    local SHARD_KEY_FP
    SHARD_KEY_FP=$("$PY" -c 'import hashlib,sys;print(hashlib.md5(sys.argv[1].encode()).hexdigest()[:8])' "$SHARD_KEYS")
    local SHARD_PREFIX="data/shard_${TOTAL}_${DATA_VER}_c${CONNECTIONS}_k${SHARD_KEY_FP}"
    local SHARD_FILES=""
    local i
    for i in $(seq 0 $(( CONNECTIONS - 1 ))); do
      [ -f "${SHARD_PREFIX}.s${i}.frames" ] || { SHARD_FILES=""; break; }
      SHARD_FILES="${SHARD_FILES:+$SHARD_FILES,}${SHARD_PREFIX}.s${i}.frames"
    done
    if [ -z "$SHARD_FILES" ]; then
      echo "==> shard-frames(${CONNECTIONS} 分片, key=${SHARD_KEYS%%:*})"
      "$WFGEN" shard-frames --input "$FRAMES" --shards "$CONNECTIONS" \
        --shard-keys "$SHARD_KEYS" --output-prefix "$SHARD_PREFIX" > /dev/null 2>&1 || {
        echo "    ⚠ shard-frames 失败(退出;EXIT trap 清理 daemon)" >&2
        exit 1
      }
      SHARD_FILES=""
      for i in $(seq 0 $(( CONNECTIONS - 1 ))); do
        SHARD_FILES="${SHARD_FILES:+$SHARD_FILES,}${SHARD_PREFIX}.s${i}.frames"
      done
    fi
    "$WFGEN" send-arrow --input "$FRAMES" --addr 127.0.0.1:$PORT --shard-files "$SHARD_FILES" > /dev/null 2>&1 &
  elif [ -n "$SHARD_KEYS" ]; then
    # 单连接 + shard-keys:无需分区,退化为普通发送
    "$WFGEN" send-arrow --input "$FRAMES" --addr 127.0.0.1:$PORT > /dev/null 2>&1 &
  else
    "$WFGEN" send-arrow --input "$FRAMES" --addr 127.0.0.1:$PORT --connections "$CONNECTIONS" > /dev/null 2>&1 &
  fi
  local CLIENT=$!
  # 等引擎真正消化完（append 追平 TOTAL 且所有规则 ack 追平，而非 ingress 预读）。
  # 超时自适应：按 100k/s 诚实下限 + 600s 余量（on-each 单线程 ~0.3M/s，
  # 100M 需 ~333s；旧的 300s 上限会在真实负载下提前超时）。
  local MAX_SEC=$(( TOTAL_N / 100000 + 600 ))
  local T2=0 APP=0 TIMEOUT=0
  # 轮询 0.1s：metrics exporter 100ms 落盘（conf/wfusion.toml report_interval），
  # T2 误差 ≤ exporter 间隔 + 轮询间隔 ≈ 200ms——短跑（~1s feed）读数才可信
  # （旧 1s 落盘 + 0.5s 轮询把 EPS 量化成 43/55/84M 伪档，见 wp-reactor §12）。
  for j in $(seq 1 $(( MAX_SEC * 10 ))); do
    APP=$(engine_appended)
    DRAINED=$(engine_acked_lag)
    if [ "${APP:-0}" -ge "$TOTAL_N" ] && [ "${DRAINED:-1}" = "0" ]; then
      T2=$("$PY" -c 'import time; print(time.time())')
      break
    fi
    sleep 0.1
  done
  # 追平后 kill 客户端：CONNECTIONS>1 时客户端会推 CONNECTIONS×TOTAL 事件，
  # 引擎只需消化 TOTAL（口径统一）；单连接时客户端已推完，kill 无害。
  kill "$CLIENT" 2>/dev/null; wait "$CLIENT" 2>/dev/null
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
  report_result "$Q" replay "$OUT" \
    "$Q/replay: EPS=$EPS RSS_peak=${PEAK}MB cpu_avg=${CPU_AVG}% cpu_max=${CPU_MAX}% evict=$EV appended=$APP/$TOTAL_N$TO"
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

  # 等引擎消化完（append 追平 TOTAL 且规则 ack 追平）。若引擎持续能力 < RATE，backlog 会
  # 一直堆积、append 永远追不上 → 超时退出，此时 EPS 按实际 append 数计算，
  # 即"撑不住目标速率"的诚实信号。
  local MAX_SEC=900
  if [ "$RATE" -gt 0 ] 2>/dev/null; then MAX_SEC=$(( TOTAL_N / RATE * 3 + 60 ))
  else MAX_SEC=$(( TOTAL_N / 100000 + 600 )); fi   # 不限速：与 replay 同款自适应
  local T2=0 APP=0 TIMEOUT=0
  for j in $(seq 1 $(( MAX_SEC * 2 ))); do
    APP=$(engine_appended)
    DRAINED=$(engine_acked_lag)
    if [ "${APP:-0}" -ge "$TOTAL_N" ] && [ "${DRAINED:-1}" = "0" ]; then
      T2=$("$PY" -c 'import time; print(time.time())')
      break
    fi
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
if [ "$WARMUP" = "1" ] && [ "$FEED" = "replay" ]; then
  echo "==> warmup 轮（结果丢弃, 写 /tmp/bench_warmup_q1.txt）"
  run_replay_one q1 warmup_q1
  mv -f data/bench_q1_warmup_q1.txt /tmp/bench_warmup_q1.txt 2>/dev/null || true
fi

for Q in "${QUERIES[@]}"; do
  if [ "$FEED" = "replay" ]; then
    run_replay_one "$Q"
  else
    run_stream_one "$Q"
  fi
done
echo "== done: 结果在 data/bench_*_${FEED}.txt =="

# ---- --verify：用 wfgen verify-nexmark ground truth 对拍 EMIT ----------------
# 映射引擎 EMIT 规则名 → verify-nexmark 输出 key。q16/q21 是已知边界
# （模拟器给理想/朴素值，引擎按设计不同——见 wfgen 文档），q1/q11/q12/
# q14/q22 无模拟器（未建模），仅作 clean/确定性验证。
if [ "$VERIFY" = "1" ] && [ "$FEED" = "replay" ]; then
  echo "== verify: wfgen verify-nexmark ${TOTAL_N} 对拍 EMIT =="
  "$WFGEN" verify-nexmark "$TOTAL_N" > /tmp/bench_gt_verify.json 2>/dev/null
  "$PY" - /tmp/bench_gt_verify.json <<'VERIFYEOF'
import json, re, sys, glob
gt = json.load(open(sys.argv[1]))
M = {
 "q2_mod_123":"q2_mod123", "q3_auction_seller":"q3_auction_seller",
 "q4_real_avg_100":"q4_real_avg_100", "q5_bidcount_10":"q5_bidcount_10",
 "q5_bidcount_50":"q5_bidcount_50", "q5_bidcount_100":"q5_bidcount_100",
 "q6_avg_price_200":"q6_avg_price_200", "q7_maxbid_200":"q7_maxbid_200",
 "q7_maxbid_500":"q7_maxbid_500", "q7_maxbid_1000":"q7_maxbid_1000",
 "q8_monitor_new_user":"q8_monitor_new_user", "q10_arbitrary_selection":"q10_arbitrary_selection",
 "q13_bid_person_join":"q13_bid_person_join", "q15_high_bid_count_5":"q15_high_bid_count_5",
 "q16_sum_price_1000":"q16_sum_price_1000", "q17_distinct_bidders_20":"q17_distinct_bidders_20",
 "q18_accumulate_fires":"q18_accumulate_fires", "q19_seq_two_bids":"q19_seq_two_bids",
 "q20_any_count_3":"q20_any_count_3", "q21_anti_person":"q21_anti_person",
 "q9_seller_count":"q3_auction_seller",
}
# 模拟器不精确（理想/朴素值），引擎按设计不同——不算回归失败
KNOWN = {"q16_sum_price_1000", "q21_anti_person"}
emitted = {}
for path in glob.glob("data/bench_*_replay.txt"):
    for line in open(path):
        m = re.match(r"EMIT (\S+) (\d+)", line)
        if m:
            emitted[m.group(1)] = int(m.group(2))
n_match = n_diff = n_skip = 0
for rule, key in sorted(M.items()):
    if rule not in emitted:
        n_skip += 1; print(f"  {rule:<26} 无引擎 EMIT（未跑/无输出）"); continue
    if key not in gt:
        n_skip += 1; print(f"  {rule:<26} 无 ground truth"); continue
    ev, gv = emitted[rule], gt[key]
    if ev == gv:
        n_match += 1; print(f"  {rule:<26} {ev:>12} == {gv:>12}  ✅")
    elif rule in KNOWN:
        n_match += 1; print(f"  {rule:<26} {ev:>12} != {gv:>12}  ⚠ 已知边界（模拟器理想/朴素值）")
    elif abs(ev - gv) <= 50 or abs(ev - gv) <= max(gv, 1) * 0.005:
        n_match += 1; print(f"  {rule:<26} {ev:>12} ≈ {gv:>12}  ✅（已知波动带 ±0.5%）")
    else:
        n_diff += 1; print(f"  {rule:<26} {ev:>12} != {gv:>12}  ❌")
print(f"== verify 结果: {n_match} 匹配, {n_diff} 差异, {n_skip} 跳过 ==")
VERIFYEOF
fi
