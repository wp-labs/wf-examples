#!/usr/bin/env bash
# nexmark_pk — NEXMark 基准查询（Q1/Q2/Q4/Q5/Q7）吞吐，对齐 Flink Nexmark 基线
#
# 3 事件流（Person/Auction/Bid，10m 窗口），12 条查询规则覆盖 pass-through /
# filter / 窗口聚合 / 计数 / MAX。数据由 scripts/gen_nexmark.py 确定性生成，
# 事件占比 Person 2% / Auction 6% / Bid 92%（bid firehose）。
#
# 用法:
#   ./run.sh                          # 默认 stream 200000 normal（单连接流式持续）
#   ./run.sh peak 200000 normal       # 峰值突发
#   ./run.sh replay 200000 normal     # P0: 预编码 Arrow 帧字节回放（测引擎真实上限）
#   CHUNK=1000 RATE_MS=50 ./run.sh stream ...  # 受控持续入流速率
#   PROFILE=debug ./run.sh ...        # debug 对比
#   WFUSION=... WFGEN=... ./run.sh    # 指定二进制
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

PROFILE="${PROFILE:-release}"   # release | debug
REPO_ROOT="${REPO_ROOT:-$(cd ../../../warp-fusion && pwd)}"  # 默认 warp-fusion 根
if [ -x "$REPO_ROOT/target/$PROFILE/wfusion" ] && [ -x "$REPO_ROOT/target/$PROFILE/wfgen" ]; then
  WFUSION="${WFUSION:-$REPO_ROOT/target/$PROFILE/wfusion}"
  WFGEN="${WFGEN:-$REPO_ROOT/target/$PROFILE/wfgen}"
else
  # 注：用 ${VAR} 花括号包裹，避免 macOS bash 3.2 在全角 `）` 前的多字节变量名 bug
  WFUSION="${WFUSION:-wfusion}"
  WFGEN="${WFGEN:-wfgen}"
  echo "   （未找到 $PROFILE 二进制，回退 PATH：${WFUSION} / ${WFGEN}）" >&2
fi
PY=${PYTHON:-python3}
PORT=9800
# 模式命名：发送方式 peak（峰值一次性灌入）/ stream（流式分片持续）；
# 数据形态 normal（sip 复用，正常流量）/ flood（唯一 sip，洪水压力）/ single（单键）。
# 旧名 alias：burst→peak, sustain→stream, pool→normal, distinct→flood, global→single。
MODE="${1:-stream}"
case "$MODE" in burst) MODE=peak;; sustain) MODE=stream;; esac
N="${2:-200000}"
MODE_GEN="${3:-normal}"
case "$MODE_GEN" in
  pool) MODE_GEN=normal;;
  distinct) MODE_GEN=flood;;
  global) MODE_GEN=single;;
esac
METRICS=data/metrics.ndjson

mkdir -p data
rm -f "$METRICS" data/*.ndjson data/wfusion.log data/daemon.log data/*.jsonl

echo "==> 0. 启动 daemon（TCP 源 + 指标，report_interval=1s） profile=$PROFILE"
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

echo "==> 2. 生成 $N Nexmark 事件（wfgen gen-nexmark，Person 2%/Auction 6%/Bid 92%）"
"$WFGEN" gen-nexmark "$N" > data/burst.jsonl

# 送达计数（metrics 中 rows_total 为每区间 delta，累加得总送达）
received() {
  "$PY" - "$METRICS" <<'EOF'
import json, sys
s = 0
try:
    for line in open(sys.argv[1]):
        try: o = json.loads(line)
        except Exception: continue
        if o.get("name") == "rows_total" and o.get("label") == "ingress":
            s += int(o.get("value", 0))
except FileNotFoundError:
    pass
print(s)
EOF
}

if [ "$MODE" = "peak" ]; then
  echo "==> 3. 全速发送并计时（等 delivered 追平 N）"
  START=$($PY -c 'import time; print(time.time())')
  "$WFGEN" send --scenario scenarios/nexmark.wfg --input data/burst.jsonl \
    --addr 127.0.0.1:$PORT --ws models/schemas/nexmark.wfs > /dev/null 2>&1
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
elif [ "$MODE" = "replay" ]; then
  # P0 方案 A：预编码 Arrow 帧回放，测引擎真实摄取上限。
  # dump-frames 一次性把 JSONL 编成 Arrow IPC + RFC6587 帧（字节与 send 完全一致），
  # send-arrow 直接回放字节，省掉每次 send 的 JSON 解析 + Arrow 编码。
  # EPS 按 send-arrow 墙钟计时（dump 是一次性 setup，不计入）。
  CHUNK="${CHUNK:-1000000}"
  FRAMES=data/burst.frames
  echo "==> 3. 预编码 Arrow 帧（dump-frames --chunk ${CHUNK}）"
  "$WFGEN" dump-frames --scenario scenarios/nexmark.wfg --input data/burst.jsonl \
    --ws models/schemas/nexmark.wfs --addr 127.0.0.1:$PORT --output "$FRAMES" \
    --chunk "$CHUNK" > /dev/null 2>&1
  echo "==> 4. 字节回放 send-arrow（计时）"
  START=$($PY -c 'import time; print(time.time())')
  "$WFGEN" send-arrow --input "$FRAMES" --addr 127.0.0.1:$PORT > /dev/null 2>&1
  END=$($PY -c 'import time; print(time.time())')
  for i in $(seq 1 150); do
    if [ "$(received)" -ge "$N" ]; then break; fi
    sleep 0.2
  done
  D=$(received)
  ELAPSED=$($PY -c "print($END - $START)")
  EPS=$($PY -c "print(int($N / $ELAPSED))" 2>/dev/null || echo 0)
  echo "    接收 $D / $N 事件，send-arrow 墙钟 ${ELAPSED}s"
  echo "    EPS = $EPS events/sec (字节回放，无 JSON 解析/Arrow 编码)"
elif [ "$MODE" = "stream" ]; then
  # 单连接流式：一个 wfgen 进程（wfgen send --chunk 分批 --rate-ms 节拍），
  # EPS 按 send 墙钟计时（避免 metrics 1s 上报拖慢 elapsed）。
  # CHUNK 越大越接近 peak；RATE_MS>0 模拟真实持续入流速率。
  CHUNK="${CHUNK:-10000}"
  RATE_MS="${RATE_MS:-0}"
  echo "==> 3. 单连接流式发送（--chunk ${CHUNK} --rate-ms ${RATE_MS}）"
  START=$($PY -c 'import time; print(time.time())')
  "$WFGEN" send --scenario scenarios/nexmark.wfg --input data/burst.jsonl \
    --addr 127.0.0.1:$PORT --ws models/schemas/nexmark.wfs \
    --chunk "$CHUNK" --rate-ms "$RATE_MS" > /dev/null 2>&1
  END=$($PY -c 'import time; print(time.time())')
  ELAPSED=$($PY -c "print($END - $START)")
  for i in $(seq 1 150); do
    if [ "$(received)" -ge "$N" ]; then break; fi
    sleep 0.2
  done
  D=$(received)
  EPS=$($PY -c "print(int($N / $ELAPSED))" 2>/dev/null || echo 0)
  echo "    接收 $D / $N 事件，send 墙钟 ${ELAPSED}s"
  echo "    EPS = $EPS events/sec (单连接流式)"
else
  echo "ERROR: 未知模式 '$MODE'（peak | stream）" >&2
  exit 1
fi

sleep 2  # 等告警落盘

# ---- 基本健康检查（PK case 关注吞吐，不做 #18 门禁） ----
echo ""
SINK_TYPE=$(grep -o '^connect = "[a-z_]*"' topology/sinks/infra.d/default.toml 2>/dev/null | head -1 | cut -d'"' -f2)
if [ "$SINK_TYPE" = "blackhole_sink" ]; then
  echo "==> 健康检查（blackhole sink：输出已丢弃，只看驱逐）"
else
  echo "==> 健康检查（驱逐告警 = 0 且总告警 > 0）"
fi
EVICT=$(grep -c "in memory eviction" data/wfusion.log 2>/dev/null || true)
echo "    内存驱逐告警: $EVICT"

if [ "$SINK_TYPE" = "blackhole_sink" ]; then
  if [ "${EVICT:-0}" -eq 0 ]; then
    echo "OK: 健康 — 无驱逐（blackhole 输出已丢弃，告警不落盘）"
  else
    echo "FAIL: 驱逐=${EVICT}"
  fi
else
  ALERT_SUMMARY=$("$PY" <<'EOF'
import json, collections
c = collections.Counter()
try:
    for line in open("data/default.ndjson"):
        try: c[json.loads(line).get("__wfu_rule_name", "?")] += 1
        except Exception: pass
except FileNotFoundError:
    pass
print(f"total={sum(c.values())}")
print("    per_rule: " + ", ".join(f"{k}={v}" for k, v in sorted(c.items())) or "    (none)")
EOF
)
  TOTAL_ALERTS=$(echo "$ALERT_SUMMARY" | grep -o 'total=[0-9]*' | cut -d= -f2)
  echo "    告警: $ALERT_SUMMARY"
  if [ "${EVICT:-0}" -eq 0 ] && [ "${TOTAL_ALERTS:-0}" -gt 0 ]; then
    echo "OK: 健康 — 无驱逐，规则正常触发（alerts=${TOTAL_ALERTS}）"
  elif [ "${EVICT:-0}" -eq 0 ] && [ "$MODE_GEN" = "flood" ]; then
    echo "OK: 健康 — 无驱逐（flood 模式每个 sip 仅 1-2 事件，阈值不触发为预期）"
  else
    echo "FAIL: 驱逐=${EVICT} alerts=${TOTAL_ALERTS}"
  fi
fi

echo ""
echo "==> 结果：EPS=$EPS  target=10000"
if [ "${EPS:-0}" -ge 10000 ]; then
  echo "OK: 吞吐达到 1W EPS"
else
  echo "未达 1W EPS（$EPS < 10000）—— 见 README 调优项"
fi
