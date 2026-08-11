#!/usr/bin/env bash
# eps_throughput_rules100 — 300 规则高压吞吐 + 内存扩展性（目标 EPS >= 10000）
#
# 300 条规则（scripts/gen_rules.py 生成）覆盖主要引擎路径：count/sum/avg/
# min/max/distinct/accu/guard（bool/float/object 嵌套/array/字符串/数学函数）/
# close/多事件/序列/pipeline，多 key × 阈值网格，6 类事件源。
#
# 验证（wp-reactor#19 共享解析后）：
#   1. 高规则量下吞吐（300 规则 EPS 应与 20 规则相当，因事件解析已共享）
#   2. 内存亚线性扩展（normal 模式 RSS ~0.7GB；flood 模式 ~14GB 实例内存）
#   3. #18 门禁（object 大批次不被窗口内存驱逐）
#
# 用法:
#   ./run.sh                          # 默认 peak 200000 normal（复现 #18 所需规模）
#   ./run.sh stream 200000 normal     # 持续吞吐
#   ./run.sh peak 200000 flood        # 洪水压力（100k 独立 sip，实例 churn）
#   ./run.sh peak 50000 normal        # 小规模快速验证
#   PROFILE=debug ./run.sh ...        # debug 对比
#   WFUSION=... WFGEN=... ./run.sh    # 指定二进制（如修复前/修复后对比）
#   兼容旧名：burst/peak, sustain/stream, pool/normal, distinct/flood, global/single
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
MODE="${1:-peak}"
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

echo "==> 2. 生成 $N 事件 (mode=$MODE_GEN)"
"$PY" scripts/gen_events.py "$N" "$MODE_GEN" > data/burst.jsonl

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
elif [ "$MODE" = "stream" ]; then
  echo "==> 3. 持续发送（顺序分片，目标 ~10000/s）"
  START=$($PY -c 'import time; print(time.time())')
  CHUNK=10000
  for off in $(seq -f "%1.f" 1 "$CHUNK" "$N"); do
    ENDOFF=$((off + CHUNK - 1))
    [ "$ENDOFF" -gt "$N" ] && ENDOFF=$N
    sed -n "${off},${ENDOFF}p" data/burst.jsonl > data/chunk.jsonl
    "$WFGEN" send --scenario scenarios/throughput.wfg --input data/chunk.jsonl \
      --addr 127.0.0.1:$PORT --ws models/schemas/network.wfs > /dev/null 2>&1
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
else
  echo "ERROR: 未知模式 '$MODE'（peak | stream）" >&2
  exit 1
fi

sleep 2  # 等告警落盘

# ---- #18 回归门禁 ----
echo ""
echo "==> #18 回归检查（object 大批次是否被窗口内存驱逐丢弃）"
EVICT=$(grep -c "in memory eviction" data/wfusion.log 2>/dev/null || true)
ALERT_SUMMARY=$("$PY" <<'EOF'
import json, collections
c = collections.Counter()
try:
    for line in open("data/default.ndjson"):
        try: c[json.loads(line).get("__wfu_rule_name", "?")] += 1
        except Exception: pass
except FileNotFoundError:
    pass
# conn 规则 = 全部告警 - auth_* - dns_* - pr_* - fw_* - fl_*（生成器按前缀命名：
# conn 规则无前缀，auth/dns/proxy/firewall/file 各带前缀）。若 conn 大批次被内存
# 驱逐丢弃，conn 规则归零 → 门禁 FAIL。
conn = sum(v for k, v in c.items()
           if not k.startswith(("auth_", "dns_", "pr_", "fw_", "fl_")))
print(f"conn_rules={conn} total={sum(c.values())}")
print("    per_rule: " + ", ".join(f"{k}={v}" for k, v in sorted(c.items())) or "    (none)")
EOF
)
CONN_ALERTS=$(echo "$ALERT_SUMMARY" | grep -o 'conn_rules=[0-9]*' | cut -d= -f2)
echo "    内存驱逐告警: $EVICT"
echo "    告警: $ALERT_SUMMARY"

if [ "${EVICT:-0}" -eq 0 ]; then
  if [ "$MODE_GEN" = "flood" ]; then
    # flood 模式每个 sip 仅 1-2 事件，规则阈值（如 100 conn/sip）不触发属预期
    echo "OK: #18 回归通过 — 无内存驱逐丢批（flood 模式 conn 规则阈值不触发为预期）"
  elif [ "${CONN_ALERTS:-0}" -gt 0 ]; then
    echo "OK: #18 回归通过 — object 大批次未被驱逐，conn 规则正常触发（alerts=${CONN_ALERTS}）"
  else
    echo "FAIL: #18 回归失败 — eviction=0 但 conn_alerts=0"
    echo "    （object 大批次可能被窗口内存驱逐丢弃，检查 wfusion.log 与二进制是否含 wp-reactor#18 修复）"
  fi
else
  echo "FAIL: #18 回归失败 — eviction=${EVICT} conn_alerts=${CONN_ALERTS}"
  echo "    （object 大批次被窗口内存驱逐丢弃，检查 wfusion.log 与二进制是否含 wp-reactor#18 修复）"
fi

echo ""
echo "==> 结果：EPS=$EPS  target=10000"
if [ "${EPS:-0}" -ge 10000 ]; then
  echo "OK: 吞吐达到 1W EPS"
else
  echo "未达 1W EPS（$EPS < 10000）—— 见 README 调优项"
fi
