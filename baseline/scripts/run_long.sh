#!/usr/bin/env bash
# ===========================================================================
# baseline 长跑验证（daemon + TCP 注入）——producer 窗推进 + 内存平台
#
# 验证目标（对齐 memory_stability 方法论）：
#   A. 窗推进不漏：事件时间每轮前移 offset，stats 15s 固定窗持续收盘；
#      每次收盘每键一条基线记录 → baseline.ndjson 行数单调增长
#      （每轮增长 ≥ 4×5 窗键，防"事件打在同一时间基、窗永不收盘"的假绿）。
#   B. 内存平台：状态每窗重置（reset_window）→ alloc commit/RSS 不随已收盘
#      窗数累积增长（末段两次平台采样差 < GROW_MB 判定）。
#
# 用法: ./scripts/run_long.sh [rounds]        （默认 3 轮，~1 分钟）
# 环境: GROW_MB（内存增长容忍，默认 80MB）；PORT=9800
# 产物: data/metrics.ndjson（alloc/window 指标采样）、data/long_samples.tsv、
#       data/baseline/baseline.ndjson（累计基线记录）、data/logs/wfusion_long.log
#
# 注：判定规则/全局周期基线供给的 refresh 不在本脚本范围（供给表按启动装载一次，
# refresh 属 S2-M3b）。
# ===========================================================================
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

WFUSION=${WFUSION:-wfusion}
WFGEN=${WFGEN:-wfgen}
PY=${PYTHON:-python3}
PORT=9800
ROUNDS="${1:-3}"
GROW_MB="${GROW_MB:-80}"
CONF=conf/long.wfusion.toml
OUT=data/baseline/baseline.ndjson
METRICS=data/metrics.ndjson
SAMPLES=data/long_samples.tsv
LOG=data/logs/wfusion_long.log

mkdir -p data/logs
rm -f "$METRICS" data/long_samples.tsv "$LOG" data/baseline/baseline.ndjson data/daemon.log data/live.jsonl

# 指标读取（label `-` = 无 label）
m() { "$PY" scripts/read_metrics.py "$METRICS" "$1" "$2" "${3:--}"; }
commit_bytes() { m alloc current_commit_bytes; }
arss_bytes()   { m alloc current_rss_bytes; }

echo "==> 0. 启动 daemon（TCP 源 + 指标监控，conf/long.wfusion.toml，15s 窗）"
"$WFUSION" daemon --config "$CONF" --work-dir . > data/daemon.log 2>&1 &
DAEMON_PID=$!
trap 'kill $DAEMON_PID 2>/dev/null || true' EXIT

# 1s 采样器（RSS/commit/行数轨迹）
echo -e "epoch\tps_rss_kb\talloc_rss\talloc_commit\tbaseline_rows" > "$SAMPLES"
(
  while kill -0 "$DAEMON_PID" 2>/dev/null; do
    ROWS=0; [[ -f "$OUT" ]] && ROWS=$(wc -l < "$OUT" | tr -d ' ')
    printf "%s\t%s\t%s\t%s\t%s\n" \
      "$(date +%s)" "$(ps -o rss= -p "$DAEMON_PID" 2>/dev/null | tr -d ' ')" \
      "$(arss_bytes)" "$(commit_bytes)" "$ROWS" >> "$SAMPLES"
    sleep 1
  done
) &
SAMPLER_PID=$!

echo "==> 1. 等待 TCP 源就绪 (port $PORT)"
for i in $(seq 1 50); do
  if nc -z 127.0.0.1 "$PORT" 2>/dev/null; then break; fi
  sleep 0.2
done
nc -z 127.0.0.1 "$PORT" 2>/dev/null || { echo "ERROR: TCP 源未就绪"; tail -20 data/daemon.log; exit 1; }

send_round() {
  local offset=$1 span=90 count=3000
  "$PY" scripts/gen_metrics_live.py "$count" "$span" "$offset" > data/live.jsonl
  "$WFGEN" send --scenario models/scenarios/metrics_baseline.wfg --input data/live.jsonl \
    --addr 127.0.0.1:$PORT --ws models/schemas/metrics.wfs 2>&1 | tail -1
}

prev_rows=0
echo "==> 2. 分轮注入（每轮事件时间 +120s，覆盖 90s → 跨多窗推进）"
for r in $(seq 1 "$ROUNDS"); do
  offset=$((r * 120))
  echo "-- round $r (offset +${offset}s)"
  send_round "$offset"
  sleep 4   # 收盘 → sink 消费落盘
  ROWS=0; [[ -f "$OUT" ]] && ROWS=$(wc -l < "$OUT" | tr -d ' ')
  echo "   baseline.ndjson rows = $ROWS"
  if (( r > 1 && ROWS <= prev_rows )); then
    echo "FAIL: 第 $r 轮无新收盘记录（窗未推进? rows $prev_rows -> $ROWS）" >&2
    tail -30 "$LOG" 2>/dev/null || true
    exit 1
  fi
  prev_rows=$ROWS
done

echo "==> 3. 平台期采样（末段两次 6s 间隔，判内存是否随窗数累积）"
sleep 2
C1=$(commit_bytes); R1=$(ps -o rss= -p "$DAEMON_PID" 2>/dev/null | tr -d ' ')
sleep 6
C2=$(commit_bytes); R2=$(ps -o rss= -p "$DAEMON_PID" 2>/dev/null | tr -d ' ')
DC=$(( (C2 - C1) / 1024 / 1024 )); DR=$(( (R2 - R1) / 1024 ))
echo "   alloc commit: ${C1} -> ${C2} (Δ ${DC}MB); ps rss: ${R1} -> ${R2} (Δ ${DR}MB)"

kill "$SAMPLER_PID" 2>/dev/null || true

if (( ROWS < 20 )); then
  echo "FAIL: 基线记录过少（$ROWS），producer 未正常收盘" >&2; tail -30 "$LOG"; exit 1
fi
if (( DC > GROW_MB || DR > GROW_MB )); then
  echo "FAIL: 末段内存仍在增长（Δcommit ${DC}MB / Δrss ${DR}MB > ${GROW_MB}MB）——疑似状态未随窗回收" >&2
  exit 1
fi

kill "$DAEMON_PID" 2>/dev/null || true
echo ""
echo "PASS: 窗推进 $ROWS 条（≥20），末段内存平台（Δcommit ${DC}MB / Δrss ${DR}MB ≤ ${GROW_MB}MB）"
