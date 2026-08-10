#!/usr/bin/env bash
# memory_stability — 长时间运行内存稳定性验证
#
# 两个验证目标：
#   A. 逻辑释放：burst → 实例增长；输入停止 → 窗口 TTL 到期后实例自动过期（rule.instances→0）
#   B. 泄漏检测：重复 K 轮 burst+idle，观察 daemon 进程 RSS 是否持续增长（区分"分配器不归还
#      OS"与"真泄漏"：逻辑释放后 RSS 不回落是 allocator 行为；RSS 随轮次持续增长才是泄漏）
#
# 用法:
#   ./run.sh             # 完整：A（~3 分钟）+ B（3 轮，~4 分钟）
#   ./run.sh --demo      # 只跑 A：单周期逻辑释放演示（~3 分钟）
#   ./run.sh --leak      # 只跑 B：多周期 RSS 泄漏检测（~4 分钟）
#   ./run.sh --smoke     # 快速冒烟（~20 秒）
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

WFUSION=${WFUSION:-wfusion}
WFGEN=${WFGEN:-wfgen}
PY=${PYTHON:-python3}
PORT=9800
METRICS=data/metrics.ndjson
MODE="${1:-all}"
N=10000

mkdir -p data
rm -f "$METRICS" data/*.ndjson data/wfusion.log data/daemon.log data/*.jsonl

echo "==> 0. 启动 daemon（TCP 源 + 指标监控）"
"$WFUSION" daemon --config conf/wfusion.toml --work-dir . > data/daemon.log 2>&1 &
DAEMON_PID=$!
trap 'kill $DAEMON_PID 2>/dev/null || true' EXIT

rss_kb() { ps -o rss= -p "$DAEMON_PID" 2>/dev/null | tr -d ' '; }

echo "==> 1. 等待 TCP 源就绪 (port $PORT)"
READY=0
for i in $(seq 1 50); do
  if nc -z 127.0.0.1 "$PORT" 2>/dev/null; then READY=1; break; fi
  sleep 0.2
done
if [ "$READY" != 1 ]; then echo "ERROR: TCP 源未就绪"; tail -20 data/daemon.log; exit 1; fi

send_burst() {
  "$PY" scripts/gen_events.py "$N" 0 > data/burst.jsonl
  "$WFGEN" send --scenario scenarios/memory.wfg --input data/burst.jsonl \
    --addr 127.0.0.1:$PORT --ws models/schemas/network.wfs 2>&1 | tail -1
}

if [ "$MODE" = "smoke" ]; then
  N=100
  send_burst
  sleep 4
  HIGH=$("$PY" scripts/read_metrics.py "$METRICS" rule instances instance_growth)
  if [ "$HIGH" -ge 50 ]; then
    echo "SMOKE OK: 配置加载成功，$N 实例已创建并上报 (instances=$HIGH, rss_kb=$(rss_kb))"
  else
    echo "SMOKE FAIL: 实例数过低 (instances=$HIGH)"; tail -20 data/daemon.log; exit 1
  fi
  exit 0
fi

# ---------- A. 单周期逻辑释放演示 ----------
if [ "$MODE" = "all" ] || [ "$MODE" = "demo" ]; then
  echo "==> A1. 发送 burst（$N 个 distinct sip）"
  send_burst
  sleep 4
  HIGH=$("$PY" scripts/read_metrics.py "$METRICS" rule instances instance_growth)
  RSS_A1=$(rss_kb)
  echo "    burst 后 instances=$HIGH  rss_kb=$RSS_A1"
  [ "$HIGH" -ge 5000 ] || { echo "ERROR: burst 后实例数过低: $HIGH"; tail -20 data/daemon.log; exit 1; }

  echo "==> A2. 停止输入，等待 2× 窗口 TTL（130s）"
  sleep 130
  STILL=$("$PY" scripts/read_metrics.py "$METRICS" rule instances instance_growth)
  RSS_A2=$(rss_kb)
  echo "    停止后 instances=$STILL  rss_kb=$RSS_A2"
  echo "    （实例已逻辑释放到 $STILL；RSS 是否回落是 allocator 行为）"
  [ "$STILL" -lt "$HIGH" ] || { echo "ERROR: 停止后实例未释放: $STILL"; exit 1; }
  echo "OK: 逻辑释放验证通过（instances $HIGH → $STILL）"
  if [ "$MODE" = "demo" ]; then exit 0; fi
fi

# ---------- B. 多周期 RSS 泄漏检测 ----------
echo "==> B1. 重复 K 轮 burst + idle，观察 RSS 是否持续增长"
CYCLES=3
PREV_RSS=0
LEAK=0
for k in $(seq 1 "$CYCLES"); do
  send_burst > /dev/null
  sleep 65   # 略大于窗口 TTL（60s），让上一轮实例过期
  INST=$("$PY" scripts/read_metrics.py "$METRICS" rule instances instance_growth)
  RSS=$(rss_kb)
  echo "    轮次 $k/$CYCLES: instances=$INST  rss_kb=$RSS"
  if [ "$PREV_RSS" -gt 0 ] && [ "$RSS" -gt $((PREV_RSS + 5000)) ]; then
    LEAK=1
  fi
  PREV_RSS=$RSS
done
if [ "$LEAK" = 0 ]; then
  echo "OK: $CYCLES 轮后 RSS 未持续增长（无泄漏；RSS 不回落是 allocator 复用行为）"
else
  echo "ERROR: RSS 持续增长，疑似泄漏（末轮 RSS=$PREV_RSS KB）" >&2
  exit 1
fi
echo "==> 完成。"
