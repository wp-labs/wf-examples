#!/usr/bin/env bash
# nexmark_pk — NEXMark 基准门禁（送达/告警/驱逐/RSS/EPS）
#
# 走 P0 方案 A：gen-nexmark 生成 → dump-frames 一次性预编码 Arrow 帧 →
# send-arrow 字节回放（无 JSON 解析/Arrow 编码），EPS 反映引擎真实摄取，
# 而非 wfgen JSONL 客户端的 ~480k 上限。汇总送达/驱逐/RSS/EPS。
#
# 用法:
#   ./validate.sh <wfusion-bin> <wfgen-bin> [N]
#   N 默认 200000；CHUNK 控制 dump 单批行数（默认 1000000）
#
# 依赖: nc（macOS/Linux 自带）、python3、ps（RSS 采样）。端口 9800 空闲。
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

WFUSION_BIN="$1"
WFGEN_BIN="$2"
N="${3:-200000}"
PY=${PYTHON:-python3}
PORT=9800
CHUNK="${CHUNK:-1000000}"
METRICS=data/metrics.ndjson

mkdir -p data
rm -f "$METRICS" data/wfusion.log data/daemon.log data/default.ndjson \
      data/burst.jsonl data/burst.frames

"$WFUSION_BIN" daemon --config conf/wfusion.toml --work-dir . > data/daemon.log 2>&1 &
DAEMON_PID=$!
trap 'kill $DAEMON_PID 2>/dev/null || true' EXIT

READY=0
for i in $(seq 1 50); do
  if nc -z 127.0.0.1 "$PORT" 2>/dev/null; then READY=1; break; fi
  sleep 0.2
done
[ "$READY" = 1 ] || { echo "ERROR: TCP 源未就绪"; tail -20 data/daemon.log; exit 1; }

"$WFGEN_BIN" gen-nexmark "$N" > data/burst.jsonl

# 一次性预编码（不计时）；dump-frames 复用 send 的编码路径，字节一致
"$WFGEN_BIN" dump-frames --scenario scenarios/nexmark.wfg --input data/burst.jsonl \
  --ws models/schemas/nexmark.wfs --addr 127.0.0.1:$PORT --output data/burst.frames \
  --chunk "$CHUNK" > /dev/null 2>&1
# 字节回放并计时（send-arrow 子进程墙钟；大 N 时受 daemon backpressure 约束，
# EPS = 引擎真实摄取。不要算到 metrics 追平——指标 1s 上报一次会吃掉计时）
START=$("$PY" -c 'import time; print(time.time())')
"$WFGEN_BIN" send-arrow --input data/burst.frames --addr 127.0.0.1:$PORT > /dev/null 2>&1
END=$("$PY" -c 'import time; print(time.time())')
ELAPSED=$("$PY" -c "print(round($END - $START, 2))")
# 送达正确性检查（不计时）+ daemon RSS 峰值采样
RX=0
PEAK_RSS=0
for i in $(seq 1 300); do
  RX=$("$PY" - <<'EOF'
import json
s = 0
try:
    for line in open("data/metrics.ndjson"):
        try: o = json.loads(line)
        except Exception: continue
        if o.get("name") == "rows_total" and o.get("label") == "ingress":
            s += int(o.get("value", 0))
except FileNotFoundError:
    pass
print(s)
EOF
)
  RSS=$(ps -o rss= -p $DAEMON_PID 2>/dev/null | tr -d ' ' || echo 0)
  [ -n "$RSS" ] && [ "$RSS" -gt "$PEAK_RSS" ] && PEAK_RSS=$RSS
  [ "${RX:-0}" -ge "$N" ] && break
  sleep 0.2
done
sleep 2   # 等告警落盘
kill $DAEMON_PID 2>/dev/null || true
wait $DAEMON_PID 2>/dev/null || true

WINDOW_BYTES=$("$PY" - <<'EOF'
import json
v = 0
try:
    for line in open("data/metrics.ndjson"):
        try: o = json.loads(line)
        except Exception: continue
        if o.get("name") == "window_bytes":
            v = max(v, int(o.get("value", 0)))
except FileNotFoundError:
    pass
print(v)
EOF
)
EVICT=$(grep -c "in memory eviction" data/wfusion.log 2>/dev/null | head -1 || true)
EVICT=${EVICT:-0}
ALERT_SUMMARY=$("$PY" - <<'EOF'
import json, collections
c = collections.Counter()
try:
    for line in open("data/default.ndjson"):
        try: o = json.loads(line)
        except Exception: continue
        c[o.get("__wfu_rule_name", "?")] += 1
except FileNotFoundError:
    pass
conn = sum(v for k, v in c.items()
           if not k.startswith(("auth_", "dns_", "pr_", "fw_", "fl_")))
print(f"conn_rules={conn} total={sum(c.values())}")
EOF
)
MB=$("$PY" -c "print(round($WINDOW_BYTES / 1048576, 1))")
RSS_MB=$("$PY" -c "print(round($PEAK_RSS / 1024, 1))")
EPS=$("$PY" -c "print(int($N / $ELAPSED))" 2>/dev/null || echo 0)

echo "── $WFUSION_BIN ──"
echo "  events      : $N (sent)"
echo "  delivered   : ${RX:-0} / $N"
echo "  window_bytes: ${MB}MB   (conn_events max_window_bytes=256MB)"
echo "  evict_warn  : $EVICT"
echo "  alerts      : $ALERT_SUMMARY"
echo "  daemon RSS  : ${RSS_MB}MB peak"
echo "  elapsed     : ${ELAPSED}s   EPS=$EPS"
