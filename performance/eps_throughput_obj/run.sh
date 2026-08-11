#!/usr/bin/env bash
# eps_throughput_obj — object 字段高压回归 + 吞吐（目标 EPS >= 10000）
#
# 与 eps_throughput 相同，但 conn_events 额外带 `conn_info: object` 字段。
# 大规模 object 批次（>~112000 行）在修复前会因 Arrow IPC 解码膨胀
# get_array_memory_size 超过 max_window_bytes=256MB 被窗口内存驱逐静默丢弃
# （wp-labs/wp-reactor#18）。本场景验证修复后：
#   1. 无内存驱逐告警（window ... dropped ... in memory eviction）
#   2. conn 规则正常产生告警（accu_tracker/denied_probe/traffic_sum 等）
#   3. EPS 仍达到 1W（吞吐无回归）
#
# 用法:
#   ./run.sh                       # 默认 burst 200000 pool（复现 #18 所需规模）
#   ./run.sh sustain 200000 pool   # 持续吞吐
#   ./run.sh burst 200000 distinct # 实例 churn 压力
#   ./run.sh burst 50000 pool      # 小规模快速验证
#   PROFILE=debug ./run.sh ...     # debug 对比
#   WFUSION=... WFGEN=... ./run.sh # 指定二进制（如修复前/修复后对比）
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
MODE="${1:-burst}"
N="${2:-200000}"
MODE_GEN="${3:-pool}"
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
  echo "ERROR: 未知模式 '$MODE'（burst | sustain）" >&2
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
# conn 规则 = conn_events 窗口的规则告警（不含 auth 的 login_brute / dns 的 dns_avg_tunnel）。
# 若 conn 大批次被内存驱逐丢弃，所有 conn 规则归零，仅 login_brute / dns_avg_tunnel 存活 → 门禁 FAIL。
conn_rules_set = {"global_throughput","per_sip_instances","denied_probe","traffic_sum",
                  "accu_tracker","max_bytes_spike","min_duration_probe","chain_attack",
                  "port_scan_distinct","high_packet_rate","blocked_flag","object_nested_path",
                  "array_tag_member","hex_app_id","string_func_guard","math_func_guard",
                  "close_threshold","pipeline_aggregate"}
conn = sum(v for k, v in c.items() if k in conn_rules_set)
print(f"conn_rules={conn} total={sum(c.values())}")
print("    per_rule: " + ", ".join(f"{k}={v}" for k, v in sorted(c.items())) or "    (none)")
EOF
)
CONN_ALERTS=$(echo "$ALERT_SUMMARY" | grep -o 'conn_rules=[0-9]*' | cut -d= -f2)
echo "    内存驱逐告警: $EVICT"
echo "    告警: $ALERT_SUMMARY"

if [ "${EVICT:-0}" -eq 0 ]; then
  if [ "$MODE_GEN" = "distinct" ]; then
    # distinct 模式每个 sip 仅 1 事件，规则阈值（如 100 conn/sip）不触发属预期
    echo "OK: #18 回归通过 — 无内存驱逐丢批（distinct 模式 conn 规则阈值不触发为预期）"
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
