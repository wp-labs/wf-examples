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
#   ./run.sh                # 默认 200000 事件 burst（复现 #18 所需规模）
#   ./run.sh <N>            # 指定事件数
#   PROFILE=debug ./run.sh  # debug 对比
#   WFUSION=... WFGEN=... ./run.sh   # 指定二进制（如修复前/修复后对比）
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
N="${1:-200000}"
MODE_GEN="${2:-pool}"
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

echo "==> 3. 全速发送并计时（等 delivered 追平 N）"
START=$($PY -c 'import time; print(time.time())')
"$WFGEN" send --scenario scenarios/throughput.wfg --input data/burst.jsonl \
  --addr 127.0.0.1:$PORT --ws models/schemas/network.wfs > /dev/null 2>&1
DONE=0
for i in $(seq 1 150); do
  if [ "$("$PY" - "$METRICS" <<'EOF'
import json, sys
s = 0
try:
    for line in open(sys.argv[1]):
        try: o = json.loads(line)
        except Exception: continue
        if o.get("name") == "rows_total" and o.get("label") == "ingress":
            s = max(s, int(o.get("value", 0)))
except FileNotFoundError:
    pass
print(s)
EOF
)" -ge "$N" ]; then DONE=1; break; fi
  sleep 0.2
done
END=$($PY -c 'import time; print(time.time())')
ELAPSED=$($PY -c "print($END - $START)")
D=$("$PY" - "$METRICS" <<'EOF'
import json, sys
s = 0
try:
    for line in open(sys.argv[1]):
        try: o = json.loads(line)
        except Exception: continue
        if o.get("name") == "rows_total" and o.get("label") == "ingress":
            s = max(s, int(o.get("value", 0)))
except FileNotFoundError:
    pass
print(s)
EOF
)
EPS=$($PY -c "print(int($N / $ELAPSED))" 2>/dev/null || echo 0)
echo "    接收 $D / $N 事件，耗时 ${ELAPSED}s"
echo "    EPS = $EPS events/sec"
[ "$DONE" = 1 ] || echo "    警告: 超时未追平（daemon 接收慢于发送？）"

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
conn = c["accu_tracker"] + c["denied_probe"] + c["traffic_sum"] \
     + c["global_throughput"] + c["per_sip_instances"]
print(f"conn_rules={conn} total={sum(c.values())}")
print("    per_rule: " + ", ".join(f"{k}={v}" for k, v in sorted(c.items())) or "    (none)")
EOF
)
CONN_ALERTS=$(echo "$ALERT_SUMMARY" | grep -o 'conn_rules=[0-9]*' | cut -d= -f2)
echo "    内存驱逐告警: $EVICT"
echo "    告警: $ALERT_SUMMARY"

if [ "${EVICT:-0}" -eq 0 ] && [ "${CONN_ALERTS:-0}" -gt 0 ]; then
  echo "OK: #18 回归通过 — object 大批次未被驱逐，conn 规则正常触发（alerts=${CONN_ALERTS}）"
else
  echo "FAIL: #18 回归失败 — eviction=${EVICT} conn_alerts=${CONN_ALERTS}"
  echo "    （object 大批次可能被窗口内存驱逐丢弃，检查 wfusion.log 与二进制是否含 wp-reactor#18 修复）"
fi

echo ""
echo "==> 结果：EPS=$EPS  target=10000"
if [ "${EPS:-0}" -ge 10000 ]; then
  echo "OK: 吞吐达到 1W EPS"
else
  echo "未达 1W EPS（$EPS < 10000）—— 见 README 调优项"
fi
