#!/usr/bin/env bash
# memory_stability — 长时间运行内存稳定性验证
#
# 两个验证目标：
#   A. 逻辑释放：burst → 实例增长；输入停止 → 窗口 TTL 到期后实例自动过期（rule.instances→0）
#   B. 泄漏检测：重复 K 轮 burst+idle，用 **allocator 口径**（alloc.current_commit_bytes /
#      current_rss_bytes，mimalloc 实占）判定是否持续增长——预热轮 + 每轮平台期 settled
#      值 + 引擎逻辑释放确认（instances 每轮回 0）区分「真泄漏」与「分配器不归还 OS」。
#
# 用法:
#   ./run.sh [all|demo|leak|smoke]   （`--` 前缀兼容：--smoke/--demo/--leak）
#   ./run.sh --demo      # 只跑 A：单周期逻辑释放演示（~2.5 分钟）
#   ./run.sh --leak      # 只跑 B：多周期泄漏检测（默认 6 轮含预热，~10 分钟）
#   ./run.sh --smoke     # 快速冒烟（~20 秒）
# 环境: N CYCLES CYCLE_IDLE(默认70>TTL60) SETTLE GROW_TOL_MB（判定容忍）IDLE_SEC(demo)
#       OFFSET_STEP（每轮事件时间前移量，须 > allowed_lateness）
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

WFUSION=${WFUSION:-wfusion}
WFGEN=${WFGEN:-wfgen}
PY=${PYTHON:-python3}
PORT=9800
METRICS=data/metrics.ndjson
SAMPLES=data/mem_samples.tsv
MODE="${1:-all}"; MODE="${MODE#--}"
N=${N:-10000}
TTL_SEC=60

mkdir -p data
rm -f "$METRICS" data/*.ndjson data/wfusion.log data/daemon.log data/*.jsonl "$SAMPLES"

echo "==> 0. 启动 daemon（TCP 源 + 指标监控）"
"$WFUSION" daemon --config conf/wfusion.toml --work-dir . > data/daemon.log 2>&1 &
DAEMON_PID=$!
trap 'kill $DAEMON_PID 2>/dev/null || true' EXIT

# ---- 指标读取（label `-` = 无 label 指标）----
m() { "$PY" scripts/read_metrics.py "$METRICS" "$1" "$2" "${3:--}"; }
instances()   { m rule instances instance_growth; }
commit_bytes(){ m alloc current_commit_bytes; }
arss_bytes()  { m alloc current_rss_bytes; }
wmem_bytes()  { m window memory_bytes conn_events; }
ps_rss_kb()   { ps -o rss= -p "$DAEMON_PID" 2>/dev/null | tr -d ' '; }

# ---- 1s 采样器（全时段轨迹，供报告/事后分析）----
echo -e "epoch\tps_rss_kb\talloc_rss\talloc_commit\tinstances\twmem_conn" > "$SAMPLES"
(
  while kill -0 "$DAEMON_PID" 2>/dev/null; do
    printf "%s\t%s\t%s\t%s\t%s\t%s\n" \
      "$(date +%s)" "$(ps_rss_kb)" "$(arss_bytes)" "$(commit_bytes)" \
      "$(instances)" "$(wmem_bytes)" >> "$SAMPLES"
    sleep 1
  done
) &
SAMPLER_PID=$!

echo "==> 1. 等待 TCP 源就绪 (port $PORT)"
READY=0
for i in $(seq 1 50); do
  if nc -z 127.0.0.1 "$PORT" 2>/dev/null; then READY=1; break; fi
  sleep 0.2
done
if [ "$READY" != 1 ]; then echo "ERROR: TCP 源未就绪"; tail -20 data/daemon.log; exit 1; fi

send_burst() {
  # 事件时间随轮次前移（offset_seconds）：让窗口 watermark 推进、上一轮窗口行
  # 按 allowed_lateness 过期驱逐——否则每轮都打在同一固定基准时间，窗口行只增
  # 不减，commit 增长是窗口保留而非分配器泄漏（见 leak 模式 B1 说明）。
  local offset=${1:-0}
  "$PY" scripts/gen_events.py "$N" "$offset" > data/burst.jsonl
  "$WFGEN" send --scenario scenarios/memory.wfg --input data/burst.jsonl \
    --addr 127.0.0.1:$PORT --ws models/schemas/network.wfs 2>&1 | tail -1
}

if [ "$MODE" = "smoke" ]; then
  N=100
  send_burst
  sleep 4
  HIGH=$(instances)
  if [ "$HIGH" -ge 50 ]; then
    echo "SMOKE OK: 配置加载成功，$N 实例已创建并上报 (instances=$HIGH, commit=$(commit_bytes) rss_kb=$(ps_rss_kb))"
  else
    echo "SMOKE FAIL: 实例数过低 (instances=$HIGH)"; tail -20 data/daemon.log; exit 1
  fi
  kill "$SAMPLER_PID" 2>/dev/null || true
  exit 0
fi

# ---------- A. 单周期逻辑释放演示 ----------
if [ "$MODE" = "all" ] || [ "$MODE" = "demo" ]; then
  IDLE_SEC=${IDLE_SEC:-130}
  echo "==> A1. 发送 burst（$N 个 distinct sip）"
  send_burst
  sleep 4
  HIGH=$(instances)
  RSS_A1=$(ps_rss_kb); CM_A1=$(commit_bytes)
  echo "    burst 后 instances=$HIGH  commit=${CM_A1}  rss_kb=$RSS_A1"
  [ "$HIGH" -ge $(( N / 2 )) ] || { echo "ERROR: burst 后实例数过低: $HIGH (N=$N)"; tail -20 data/daemon.log; exit 1; }

  echo "==> A2. 停止输入，等待 2× 窗口 TTL（${IDLE_SEC}s）"
  sleep "$IDLE_SEC"
  STILL=$(instances)
  RSS_A2=$(ps_rss_kb); CM_A2=$(commit_bytes)
  echo "    停止后 instances=$STILL  commit=${CM_A2}  rss_kb=$RSS_A2"
  # macOS bash 3.2 非 UTF-8 locale 会把变量名后紧跟的全角字符并入名字 → 用 ${} 界定。
  echo "    （实例已逻辑释放到 ${STILL}；commit 回落/持平是分配器行为）"
  [ "$STILL" -lt "$HIGH" ] || { echo "ERROR: 停止后实例未释放: $STILL"; exit 1; }
  echo "OK: 逻辑释放验证通过（instances $HIGH → ${STILL}）"
  if [ "$MODE" = "demo" ]; then kill "$SAMPLER_PID" 2>/dev/null || true; exit 0; fi
fi

# ---------- B. 多周期泄漏检测（allocator 口径） ----------
echo "==> B1. 重复 K 轮 burst + idle，观察 allocator commit 是否持续增长"
CYCLES=${CYCLES:-6}          # 第 1 轮为预热（分配器爬升）；至少 6 轮才能判稳态
CYCLE_IDLE=${CYCLE_IDLE:-70} # 必须 > TTL(60s)：让实例过期释放后再采样 settled
SETTLE=${SETTLE:-3}
GROW_TOL_MB=${GROW_TOL_MB:-8}  # 末轮增量/零输入 drain 增量的容忍（MB）
OFFSET_STEP=${OFFSET_STEP:-7200} # 每轮事件时间前移量(s)，须 > allowed_lateness(30m)
                                # 让上一轮窗口行过期驱逐（否则窗口只增不减 → commit
                                # 随窗口保留增长，非分配器泄漏）
mb() { local b=$1; [ -n "$b" ] && [ "$b" -gt 0 ] 2>/dev/null && echo $(( b / 1048576 )) || echo 0; }

wmem_conn() { m window memory_bytes conn_events; }

echo "    轮次(TTL=${TTL_SEC}s, idle=${CYCLE_IDLE}s, settle=${SETTLE}s, tol=${GROW_TOL_MB}MB, offset_step=${OFFSET_STEP}s)"
echo "    轮次     instances   commit(MB)  wmem(MB)  alloc_rss(MB)  ps_rss(MB)"
BASELINE=0
PREV_CM=0
PREV2_CM=0
PREV_AR=0
PREV2_AR=0
LEAK=0
LOGICAL=0
for k in $(seq 1 "$CYCLES"); do
  # 事件时间前移 offset_step·(k-1)：上一轮窗口行随 watermark 推进过期（见 send_burst）。
  send_burst $(( OFFSET_STEP * (k - 1) )) > /dev/null
  sleep "$CYCLE_IDLE"   # 实例 TTL 到期
  sleep "$SETTLE"       # 平台期：分配器回收（settled 值）
  INST=$(instances)
  CM=$(commit_bytes); WM=$(wmem_conn); AR=$(arss_bytes); PS=$(ps_rss_kb)
  echo "    $k/$CYCLES     ${INST}        $(mb "$CM")        $(mb "$WM")        $(mb "$AR")            $(mb $(( PS * 1024 )))"
  if [ "$k" = 1 ]; then
    BASELINE=$CM   # 预热后的 settled 基线（仅用于报告 ratchet 幅度）
  fi
  # 引擎逻辑释放确认：每轮 idle 后实例都应已过期回 0。
  [ "$INST" -le 5 ] || { LOGICAL=1; echo "    ⚠ 轮次 $k 后实例未释放: instances=$INST"; }
  PREV2_CM=$PREV_CM; PREV_CM=$CM
  PREV2_AR=$PREV_AR; PREV_AR=$AR
done

# 末轮后再加一段 drain，确认 commit 收敛（不继续爬升 = 无逐轮残留）。
echo "==> B2. 末轮后追加 drain（${CYCLE_IDLE}s，零输入）"
sleep "$CYCLE_IDLE"
FINAL_INST=$(instances); FINAL_CM=$(commit_bytes)
echo "    drain 后 instances=$FINAL_INST commit=$(mb "$FINAL_CM")MB"
[ "$FINAL_INST" -le 5 ] || LOGICAL=1

# 泄漏判定。实测（N=10000，TTL 60s）：实例每轮释放回 0 后，allocator 预热会
# 把 commit ratchet 爬升到稳态平台（±10MB/轮噪声、RSS 增量收敛），不是泄漏；
# commit 的末轮增量噪声大 → 用两条更稳的信号判泄漏：
#   1) RSS 末轮增量 ≤ tol：末两轮 RSS 已收敛（不再触碰新内存）；
#   2) 零输入 drain 增量 ≤ tol：无输入期间 commit 不增长（无后台残留）。
# 真正的逐轮线性泄漏（实例滞留等）会被「实例未回 0」（LOGICAL）与 RSS 不收敛抓到。
AR_LAST_DELTA=$(( PREV_AR - PREV2_AR ))
echo "    预热基线 $(mb "$BASELINE")MB → 末轮 commit $(mb "$PREV_CM")MB / rss $(mb "$PREV_AR")MB（ratchet +$(mb $((PREV_CM - BASELINE)))MB）"
if [ "$AR_LAST_DELTA" -gt $(( GROW_TOL_MB * 1048576 )) ]; then
  LEAK=1
  echo "    ⚠ RSS 末轮仍在增长：第 $((CYCLES-1)) 轮 $(mb "$PREV2_AR")MB → 第 $CYCLES 轮 $(mb "$PREV_AR")MB（+$(mb "$AR_LAST_DELTA")MB > tol）" >&2
fi
if [ $(( FINAL_CM - PREV_CM )) -gt $(( GROW_TOL_MB * 1048576 )) ]; then
  LEAK=1
  echo "    ⚠ 零输入 drain 后 commit 仍在增长：$(mb "$PREV_CM")MB → $(mb "$FINAL_CM")MB（+$(mb $((FINAL_CM - PREV_CM)))MB > tol）" >&2
fi

echo ""
if [ "$LEAK" = 0 ] && [ "$LOGICAL" = 0 ]; then
  echo "OK: 无泄漏迹象——$CYCLES 轮后 RSS 收敛（末轮增量 ≤ ${GROW_TOL_MB}MB）且零输入 drain commit 不增长，实例每轮逻辑释放回 0"
  echo "    （commit/RSS 不回落 OS 且预热 ratchet 爬升到稳态平台都是分配器复用行为，非泄漏；轨迹见 ${SAMPLES}）"
else
  [ "$LEAK" = 1 ] && echo "ERROR: allocator 疑似泄漏（RSS 未收敛或零输入 drain 仍增长 > ${GROW_TOL_MB}MB，见 ${SAMPLES}）" >&2
  [ "$LOGICAL" = 1 ] && echo "ERROR: 实例未逻辑释放（instances 未回 0），疑似引擎侧滞留" >&2
  exit 1
fi
echo "==> 完成。"
