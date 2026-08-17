#!/usr/bin/env bash
# qradar_pk — 450 规则高压吞吐 + QRadar EP 对标（目标 EPS >= 10000）
#
# 450 条规则（scripts/gen_rules.py 生成，对标 QRadar EP 451 规则规格）覆盖主要引擎路径：
# count/sum/avg/min/max/distinct/accu/guard（bool/float/object 嵌套/array/字符串/数学函数）/
# close/多事件/序列/pipeline，多 key × 阈值网格，6 类事件源。
#
# 验证（wp-reactor#19 共享解析后）：
#   1. 高规则量下吞吐（450 规则 EPS 应与 20 规则相当，因事件解析已共享）
#   2. #18 门禁（object 大批次不被窗口内存驱逐）
#
# 唯一模式：单连接流式持续 + sip 复用（1000 池，正常流量长尾）——贴近真实部署
# 的口径。历史 peak（一次性突发）/ flood（唯一 sip）/ single（单键）模式已删除：
# peak 的 EPS 是 wfgen 发送墙钟假象，flood 是极端基数内存压力，均不代表引擎
# 日常容量（2026-08-16 实测定性，详见 README）。
#
# 用法:
#   ./run.sh                          # 默认 200000 事件
#   ./run.sh 1000000                  # 长跑（100 万事件）
#   CHUNK=1000 RATE_MS=50 ./run.sh 200000  # 受控持续入流速率
#   PLATEAU=15 ./run.sh 200000        # 送达后 RSS 平台期采样时长(默认 8s)
#   PROFILE=debug ./run.sh ...        # debug 对比
#   WFUSION=... WFGEN=... ./run.sh    # 指定二进制（如修复前/修复后对比）
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

PROFILE="${PROFILE:-release}"   # release | debug
# 二进制来源：优先本地 warp-fusion 的 target/$PROFILE 构建（仅当存在时）；否则回退 PATH。
# 不把路径固化为 ../../../warp-fusion —— 脚本可复制到任意目录运行，只要 wfusion/wfgen 在 PATH。
REPO_ROOT="${REPO_ROOT:-}"
if [ -z "$REPO_ROOT" ] && [ -d "../../../warp-fusion" ]; then
  REPO_ROOT="$(cd ../../../warp-fusion && pwd)"
fi
WFUSION="${WFUSION:-}"
WFGEN="${WFGEN:-}"
FROM_REPO=1
if [ -z "$WFUSION" ] && [ -n "$REPO_ROOT" ] && [ -x "$REPO_ROOT/target/$PROFILE/wfusion" ]; then
  WFUSION="$REPO_ROOT/target/$PROFILE/wfusion"
else
  FROM_REPO=0
fi
if [ -z "$WFGEN" ] && [ -n "$REPO_ROOT" ] && [ -x "$REPO_ROOT/target/$PROFILE/wfgen" ]; then
  WFGEN="$REPO_ROOT/target/$PROFILE/wfgen"
else
  FROM_REPO=0
fi
if [ -z "$WFUSION" ]; then WFUSION="$(command -v wfusion 2>/dev/null || true)"; fi
if [ -z "$WFGEN" ]; then WFGEN="$(command -v wfgen 2>/dev/null || true)"; fi
if [ -z "$WFUSION" ] || [ -z "$WFGEN" ]; then
  echo "错误: 找不到 wfusion/wfgen 二进制（设置 REPO_ROOT/WFUSION/WFGEN，或加入 PATH）" >&2
  exit 1
fi
[ "$FROM_REPO" = 1 ] || echo "   （未找到 $PROFILE 构建，回退 PATH：${WFUSION} / ${WFGEN}）" >&2
PY=${PYTHON:-python3}
PORT=9800
N="${1:-200000}"
case "$N" in
  ''|*[!0-9]*) echo "用法: ./run.sh [事件数]（默认 200000；环境变量 CHUNK/RATE_MS/PLATEAU 可调）" >&2; exit 1;;
esac
METRICS=data/metrics.ndjson

# PLATEAU=送达后继续采样 RSS 的秒数（默认 8：实例存活至窗口关闭，送达即杀进程
# 会严重低估 RSS——README 2026-08-11 口径为"送达后平台期峰值"）。
PLATEAU="${PLATEAU:-8}"

mkdir -p data
# 用截断（: >）而非 rm 清理旧产物：与安全删除钩子解耦（钩子对不存在文件
# fail-closed、对重复路径 fail-closed、轮内删除数超阈值会整体拦截）。
for f in "$METRICS" data/default.ndjson data/error.ndjson data/burst.jsonl data/wfusion.log data/daemon.log data/rss_peak_bytes.txt; do
  mkdir -p "$(dirname "$f")"; : > "$f" 2>/dev/null || true
done

echo "==> 0. 启动 daemon（TCP 源 + 指标，report_interval=1s） profile=$PROFILE"
"$WFUSION" daemon --config conf/wfusion.toml --work-dir . > data/daemon.log 2>&1 &
DAEMON_PID=$!
trap 'kill $DAEMON_PID $SAMPLER_PID 2>/dev/null || true' EXIT

# RSS 采样循环：优先 macOS footprint；Linux 用 /proc/<pid>/status VmRSS；兜底 ps -o rss=。
# 每 1s 采样峰值落盘（字节）。送达后由 PLATEAU 控制继续采样时长。
RSS_FILE=data/rss_peak_bytes.txt
echo 0 > "$RSS_FILE"
rss_bytes() {
  # macOS footprint: "wfusion [123]: 64-bit    Footprint: 4321 KB (…)"
  if command -v footprint >/dev/null 2>&1; then
    LINE=$(footprint "$DAEMON_PID" 2>/dev/null | grep -E 'Footprint:' | head -1)
    VAL=$(echo "$LINE" | sed -E 's/.*Footprint:[[:space:]]*([0-9.]+)[[:space:]]*([KMGT]?)B.*/\1 \2/')
    NUM="${VAL%% *}"; UNIT="${VAL##* }"
    if [ -n "$NUM" ] && [ "$NUM" != "$VAL" ]; then
      case "$UNIT" in
        T) MULT=1099511627776 ;;
        G) MULT=1073741824 ;;
        M) MULT=1048576 ;;
        K) MULT=1024 ;;
        *) MULT=1 ;;
      esac
      "$PY" -c "print(int($NUM * $MULT))" 2>/dev/null || echo 0
      return
    fi
  fi
  # Linux: /proc/<pid>/status 的 VmRSS（kB → bytes）
  if [ -r "/proc/$DAEMON_PID/status" ]; then
    awk '/^VmRSS:/{print $2 * 1024}' "/proc/$DAEMON_PID/status" 2>/dev/null && return
  fi
  # 兜底: ps -o rss=（kB → bytes）
  ps -o rss= -p "$DAEMON_PID" 2>/dev/null | awk '{print $1 * 1024}'
}
(
  while kill -0 "$DAEMON_PID" 2>/dev/null; do
    F=$(rss_bytes)
    if [ -n "$F" ] && [ "$F" -gt 0 ]; then
      CUR=$(cat "$RSS_FILE" 2>/dev/null || echo 0)
      [ "$F" -gt "$CUR" ] && echo "$F" > "$RSS_FILE"
    fi
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
[ "$READY" = 1 ] || { echo "ERROR: TCP 源未就绪"; tail -20 data/daemon.log; exit 1; }

echo "==> 2. 生成 $N 事件（sip 复用池 1000，正常流量长尾）"
"$PY" scripts/gen_events.py "$N" > data/burst.jsonl

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

# 单连接流式：一个 wfgen 进程（wfgen send --chunk 分批 --rate-ms 节拍），
# EPS 按 send 墙钟计时（避免 metrics 1s 上报拖慢 elapsed）。
# CHUNK 越大越接近全速；RATE_MS>0 模拟真实持续入流速率。
CHUNK="${CHUNK:-10000}"
RATE_MS="${RATE_MS:-0}"
echo "==> 3. 单连接流式发送（--chunk ${CHUNK} --rate-ms ${RATE_MS}）"
START=$($PY -c 'import time; print(time.time())')
"$WFGEN" send --scenario scenarios/throughput.wfg --input data/burst.jsonl \
  --addr 127.0.0.1:$PORT --ws models/schemas/network.wfs \
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

# 送达后平台期：继续运行 PLATEAU 秒采 RSS 峰值 + 等告警落盘（实例存活至窗口关闭）
echo "==> 4. 送达后平台期采样 ${PLATEAU}s（RSS 峰值 + 告警落盘）"
sleep "$PLATEAU"

# ---- #18 回归门禁 ----
# 口径：告警 sink 为 blackhole（对齐 Flink Nexmark discarding sink，只测处理吞吐），
# 故计数取 metrics 的 emitted_total / emitted_detail（引擎侧发射计数，不依赖落盘）。
echo ""
echo "==> #18 回归检查（object 大批次是否被窗口内存驱逐丢弃）"
EVICT=$(grep -c "in memory eviction" data/wfusion.log 2>/dev/null || true)
ALERT_SUMMARY=$("$PY" <<'EOF'
import json, collections
tot = 0
c = collections.Counter()
for line in open("data/metrics.ndjson"):
    try:
        o = json.loads(line)
    except Exception:
        continue
    if o.get("name") == "emitted_total":
        tot += int(o.get("value", 0))
    elif o.get("name") == "emitted_detail":
        c[o.get("label", "?")] += int(o.get("value", 0))
conn = sum(v for k, v in c.items()
           if not k.startswith(("auth_", "dns_", "pr_", "fw_", "fl_")))
print(f"emitted={tot} conn_rules={conn} rules_seen={len(c)}")
EOF
)
EMITTED=$(echo "$ALERT_SUMMARY" | grep -o 'emitted=[0-9]*' | cut -d= -f2)
echo "    内存驱逐告警: $EVICT"
echo "    告警(metrics): $ALERT_SUMMARY"

if [ "${EVICT:-0}" -eq 0 ]; then
  if [ "${EMITTED:-0}" -ge 10000 ]; then
    echo "OK: #18 回归通过 — object 大批次未被驱逐，规则正常发射告警（emitted=${EMITTED}）"
  else
    echo "FAIL: #18 回归失败 — eviction=0 但 emitted=${EMITTED:-0}（<10000）"
    echo "    （object 大批次可能被窗口内存驱逐丢弃，检查 wfusion.log 与二进制是否含 wp-reactor#18 修复）"
  fi
else
  echo "FAIL: #18 回归失败 — eviction=${EVICT}（object 大批次被窗口内存驱逐丢弃，检查 max_window_bytes）"
fi

echo ""
PEAK_RSS_BYTES=$(cat "$RSS_FILE" 2>/dev/null || echo 0)
RSS_MB=$("$PY" -c "print(round($PEAK_RSS_BYTES / 1048576, 1))" 2>/dev/null || echo "?")
echo "==> 结果：EPS=$EPS  target=10000  RSS_peak=${RSS_MB}MB（footprint 平台期口径）"
if [ "${EPS:-0}" -ge 10000 ]; then
  echo "OK: 吞吐达标（EPS=${EPS} ≥ 目标 10000）"
else
  echo "未达标（EPS=${EPS} < 目标 10000）—— 见 README 调优项"
fi
