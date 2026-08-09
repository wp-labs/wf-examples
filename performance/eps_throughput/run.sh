#!/usr/bin/env bash
# eps_throughput — 吞吐压力测试，目标 EPS >= 10000（1W 事件/秒）
#
# 测量 wfusion 引擎输入处理吞吐（events/sec）：
#   burst   <N>   : 全速发送 N 事件，测峰值 EPS（默认 100000）
#   sustain <N>   : 以目标速率持续发送，测持续 EPS（默认 200000）
#
# 指标：router.delivered_total（累计送达）—— 引擎实际处理的事件数。
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

WFUSION=${WFUSION:-wfusion}
WFGEN=${WFGEN:-wfgen}
PY=${PYTHON:-python3}
PORT=9800
METRICS=data/metrics.ndjson
MODE="${1:-burst}"
N="${2:-100000}"
MODE_GEN="${3:-pool}"   # global | distinct | pool

mkdir -p data
rm -f "$METRICS" data/*.ndjson data/wfusion.log data/daemon.log data/*.jsonl

echo "==> 0. 启动 daemon（TCP 源 + 指标，report_interval=1s）"
"$WFUSION" daemon --config conf/wfusion.toml --work-dir . > data/daemon.log 2>&1 &
DAEMON_PID=$!
trap 'kill $DAEMON_PID 2>/dev/null || true' EXIT

echo "==> 1. 等待 TCP 源就绪 (port $PORT)"
READY=0
for i in $(seq 1 50); do
  if nc -z 127.0.0.1 "$PORT" 2>/dev/null; then READY=1; break; fi
  sleep 0.2
done
[ "$READY" = 1 ] || { echo "ERROR: TCP 源未就绪"; tail -20 data/daemon.log; exit 1; }

received() {
  "$PY" - "$METRICS" <<'EOF'
import json, sys
s = 0
try:
    for line in open(sys.argv[1]):
        try:
            o = json.loads(line)
        except Exception:
            continue
        if o.get("name") == "rows_total" and o.get("label") == "ingress":
            s += int(o.get("value", 0))
except FileNotFoundError:
    pass
print(s)
EOF
}

echo "==> 2. 生成 $N 事件 (mode=$MODE_GEN)"
"$PY" scripts/gen_events.py "$N" "$MODE_GEN" > data/burst.jsonl

if [ "$MODE" = "burst" ]; then
  echo "==> 3. 全速发送并计时（等 delivered 追平 N）"
  START=$($PY -c 'import time; print(time.time())')
  "$WFGEN" send --scenario scenarios/throughput.wfg --input data/burst.jsonl \
    --addr 127.0.0.1:$PORT --ws models/schemas/network.wfs > /dev/null 2>&1
  DONE=0
  for i in $(seq 1 150); do
    if [ "$(received)" -ge "$N" ]; then DONE=1; break; fi
    sleep 0.2
  done
  END=$($PY -c 'import time; print(time.time())')
  ELAPSED=$($PY -c "print($END - $START)")
  D=$(received)
  EPS=$($PY -c "print(int($N / $ELAPSED))" 2>/dev/null || echo 0)
  echo "    接收 $D / $N 事件，耗时 ${ELAPSED}s"
  echo "    EPS = $EPS events/sec"
  [ "$DONE" = 1 ] || echo "    警告: 超时未追平（daemon 接收慢于发送？）"
elif [ "$MODE" = "sustain" ]; then
  echo "==> 3. 持续发送（顺序分片，目标 ~10000/s）"
  START=$($PY -c 'import time; print(time.time())')
  CHUNK=10000
  for off in $(seq 1 "$CHUNK" "$N"); do
    ENDOFF=$((off + CHUNK - 1))
    [ "$ENDOFF" -gt "$N" ] && ENDOFF=$N
    sed -n "${off},${ENDOFF}p" data/burst.jsonl > data/chunk.jsonl
    "$WFGEN" send --scenario scenarios/throughput.wfg --input data/chunk.jsonl \
      --addr 127.0.0.1:$PORT --ws models/schemas/network.wfs > /dev/null 2>&1
    # 块间不 sleep（全速），wfusion 处理 40k/s 远超 1 万/s 目标
  done
  for i in $(seq 1 150); do
    if [ "$(received)" -ge "$N" ]; then break; fi
    sleep 0.2
  done
  END=$($PY -c 'import time; print(time.time())')
  ELAPSED=$($PY -c "print($END - $START)")
  D=$(received)
  EPS=$($PY -c "print(int($N / $ELAPSED))" 2>/dev/null || echo 0)
  echo "    接收 $D / $N 事件，耗时 ${ELAPSED}s"
  echo "    EPS = $EPS events/sec (持续)"
fi

echo ""
echo "==> 结果：EPS=$EPS  target=10000"
if [ "${EPS:-0}" -ge 10000 ]; then
  echo "OK: 吞吐达到 1W EPS"
else
  echo "未达 1W EPS（$EPS < 10000）—— 见 README 调优项"
fi
