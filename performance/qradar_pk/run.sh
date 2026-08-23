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

# send-arrow 注入（方向 B）：dump-frames 预编码 + 多连接 raw-copy，绕开 `wfgen send`
# 的 JSONL 实时编码客户端墙。CONNECTIONS>1 是多连接注入；SHARD_KEYS 按流指定
# match key 做键闭包分片（同 key 同连接，保证有状态规则正确）。
#
# 注入路径（2026-08-23 修正）：CONNECTIONS>1 时走 `shard-frames` 预分片 +
# `send-arrow --shard-files` 纯 copy（nexmark bench.sh 同款，零解码）。已弃用
# `send-arrow --shard-keys` 动态分片路径：其连接 0 承载全部未分片流（此前
# SHARD_KEYS 漏了 file_events）+ shard-0 桶，可阻塞整条发送链，且该路径忽略
# `--rate-bytes`（1M 实测卡死在 711k、file_events=0，TEST_PLAN §7.3 已知边界）。
CONNECTIONS="${CONNECTIONS:-4}"
SHARD_KEYS="${SHARD_KEYS:-conn_events:sip,dns_events:sip,proxy_events:sip,firewall_events:sip,auth_events:source_ip,file_events:user}"
# RATE_BYTES：send-arrow 持续注入速率（bytes/秒），默认 0=不限速（nexmark 用）。
# 对 qradar 测速务必设 >0（持续注入，禁用 burst——burst 会窗口积压失真，见 TEST_PLAN §3.4）。
# 注入速率 ≈ 目标EPS × 每事件字节(~244B)。脚本输出「注入墙钟 vs 全墙钟」判断引擎是否跟上。
RATE_BYTES="${RATE_BYTES:-0}"

mkdir -p data
# 用截断（: >）而非 rm 清理旧产物：与安全删除钩子解耦（钩子对不存在文件
# fail-closed、对重复路径 fail-closed、轮内删除数超阈值会整体拦截）。
for f in "$METRICS" data/default.ndjson data/error.ndjson data/burst.jsonl data/burst_*.frames data/wfusion.log data/daemon.log data/rss_peak_bytes.txt; do
  mkdir -p "$(dirname "$f")"; : > "$f" 2>/dev/null || true
done

echo "==> 0. 启动 daemon（TCP 源 + 指标，report_interval=1s） profile=$PROFILE"
"$WFUSION" daemon --config conf/wfusion.toml --work-dir . > data/daemon.log 2>&1 &
DAEMON_PID=$!
trap 'kill ${SEND_PID:-} $DAEMON_PID $SAMPLER_PID 2>/dev/null || true' EXIT

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

echo "==> 2. 生成 $N 事件（源 IP 长尾 10100 + 泊松时间 12min，符合现实）"
"$PY" scripts/gen_events.py "$N" > data/burst.jsonl

# 预编码成 Arrow frames（绕开 `wfgen send` JSONL 实时编码客户端墙）。
# 必须在 step 0 的 daemon 运行期间 dump，且 send-arrow 回放到同一 daemon（schema 一致）。
FRAMES=data/burst_${N}.frames
echo "==> 2b. 预编码帧（dump-frames → ${FRAMES}）"
"$WFGEN" dump-frames --scenario scenarios/throughput.wfg --input data/burst.jsonl \
  --addr 127.0.0.1:$PORT --ws models/schemas/network.wfs --output "$FRAMES" \
  --chunk 10000 --max-frame-bytes 8388608 --max-frame-rows 100000 > /dev/null 2>&1
rm -f data/burst.jsonl

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

# 引擎消化信号（pull 模型，与 nexmark bench.sh 同口径）：
# - append_total：各输入流已 append 的行数累计（EPS 口径 + “数据已全部进入 window”）。
# - acked_lag：每窗口 next_seq - min_acked（未 ack 批数，0 = 所有规则已消费到最新）。
# 完成条件 = append 追平 N 且 acked_lag 归零（append 追平 ≠ 规则吃完，曾致尾部 emit 漏报）。
# 注意：新引擎 metrics 为区间差值，append_total/emitted_total 均须**全文件求和**
# （旧版读尾部 2MB + emitted 停滞判定在 30MB 级文件下恒判停滞，2026-08-23 修复）。
STREAMS="auth_events conn_events dns_events file_events firewall_events proxy_events"
engine_appended() {
  "$PY" - "$METRICS" "$STREAMS" <<'EOF'
import json, sys
path, streams = sys.argv[1], set(sys.argv[2].split())
s = 0
try:
    for line in open(path, errors="replace"):
        try: o = json.loads(line)
        except Exception: continue
        if o.get("name") == "append_total" and o.get("label") in streams:
            s += int(o.get("value", 0))
except FileNotFoundError:
    pass
print(s)
EOF
}
engine_acked_lag() {
  "$PY" - "$METRICS" "$STREAMS" <<'EOF'
import json, sys
path, streams = sys.argv[1], set(sys.argv[2].split())
lag = {}
try:
    for line in open(path, errors="replace"):
        try: o = json.loads(line)
        except Exception: continue
        if o.get("name") == "acked_lag" and o.get("label") in streams:
            lag[o["label"]] = int(o.get("value", 0))
except FileNotFoundError:
    pass
print(sum(lag.values()))
EOF
}

# send-arrow 多连接注入（后台化），再等引擎消化（append 追平 + acked_lag 归零）。
# CONNECTIONS>1：shard-frames 预分片（键闭包，同 key 同连接）→ send-arrow --shard-files
# 纯 copy 零解码（分片文件按 N×CONNECTIONS×shard-keys 指纹缓存，换键不静默复用）。
# CONNECTIONS=1：单连接 raw-copy（无需分片，顺序天然保证键闭包）。
# rate 参数：RATE_BYTES>0 时加 --rate-bytes（限速匀速注入）；否则不限速。
RATE_ARG=""
if [ "$RATE_BYTES" -gt 0 ]; then RATE_ARG="--rate-bytes $RATE_BYTES"; fi
if [ "$CONNECTIONS" -gt 1 ]; then
  echo "==> 3. shard-frames(${CONNECTIONS} 分片) + send-arrow --shard-files 注入（SHARD_KEYS=${SHARD_KEYS%%:*},...）"
  SHARD_KEY_FP=$($PY -c 'import hashlib,sys;print(hashlib.md5(sys.argv[1].encode()).hexdigest()[:8])' "$SHARD_KEYS")
  SHARD_PREFIX="data/shard_${N}_c${CONNECTIONS}_k${SHARD_KEY_FP}"
  SHARD_FILES=""
  i=0
  while [ "$i" -lt "$CONNECTIONS" ]; do
    [ -s "${SHARD_PREFIX}.s${i}.frames" ] || { SHARD_FILES=""; break; }
    SHARD_FILES="${SHARD_FILES:+$SHARD_FILES,}${SHARD_PREFIX}.s${i}.frames"
    i=$(( i + 1 ))
  done
  if [ -z "$SHARD_FILES" ]; then
    echo "    预分片 → ${SHARD_PREFIX}.s0..s$(( CONNECTIONS - 1 )).frames"
    "$WFGEN" shard-frames --input "$FRAMES" --shards "$CONNECTIONS" \
      --shard-keys "$SHARD_KEYS" --output-prefix "$SHARD_PREFIX" > /dev/null 2>&1 || {
      echo "ERROR: shard-frames 失败（检查 SHARD_KEYS 的 key 字段是否在对应流 schema）" >&2
      exit 1
    }
    SHARD_FILES=""
    i=0
    while [ "$i" -lt "$CONNECTIONS" ]; do
      SHARD_FILES="${SHARD_FILES:+$SHARD_FILES,}${SHARD_PREFIX}.s${i}.frames"
      i=$(( i + 1 ))
    done
  fi
  "$WFGEN" send-arrow --input "$FRAMES" --addr 127.0.0.1:$PORT \
    --shard-files "$SHARD_FILES" $RATE_ARG > /dev/null 2>&1 &
else
  echo "==> 3. send-arrow 单连接注入（raw-copy）"
  "$WFGEN" send-arrow --input "$FRAMES" --addr 127.0.0.1:$PORT $RATE_ARG > /dev/null 2>&1 &
fi
SEND_PID=$!
START=$($PY -c 'import time; print(time.time())')
# 等引擎消化：append 追平 N 且 acked_lag 归零（超时保护：上限按最低能力外推）。
# 长等待期每 10s 报一次进度（0.5s 轮询），避免全程静默像挂死。
# INJ_END = append 首次追平 N（注入+append 完成）——注入墙钟的计时点；
# END = 再等 acked_lag 归零（规则全部消化）——全墙钟的计时点。
MAX_SEC=$(( N / 100000 + 600 ))
TIMEOUT=0; END=""; INJ_END=""; DRAINED=1
for i in $(seq 1 $((MAX_SEC * 2))); do
  APP=$(engine_appended)
  DRAINED=$(engine_acked_lag)
  if [ -z "$INJ_END" ] && [ "${APP:-0}" -ge "$N" ]; then
    INJ_END=$($PY -c 'import time; print(time.time())')
  fi
  if [ "${APP:-0}" -ge "$N" ] && [ "${DRAINED:-1}" = "0" ]; then
    END=$($PY -c 'import time; print(time.time())'); break
  fi
  if [ $(( i % 20 )) -eq 0 ]; then
    echo "  ingest: ${APP:-0}/${N} ack_lag=${DRAINED:-1}（等待引擎消化，超时上限 ${MAX_SEC}s）"
  fi
  sleep 0.5
done
[ -n "$END" ] || { END=$($PY -c 'import time; print(time.time())'); TIMEOUT=1; }
[ -n "$INJ_END" ] || INJ_END="$END"
# 追平后收掉注入客户端（多连接分片路径已推完；保险清理，防端口占用）。
# sender 可能已自行退出（kill 返回 1）；wait 返回信号退出码——两者都必须 || true，
# 否则 set -e 会在此处杀掉脚本、丢掉结果输出。
kill "$SEND_PID" 2>/dev/null || true; wait "$SEND_PID" 2>/dev/null || true
ELAPSED=$($PY -c "print($END - $START)")
D=$(received)
APP=$(engine_appended)
EPS=$($PY -c "print(int($APP / $ELAPSED))" 2>/dev/null || echo 0)
TO_MARK=""; [ "$TIMEOUT" = 1 ] && TO_MARK=" ⚠TIMEOUT(appended未追平,EPS=实际处理速率)"
INJ_S=$($PY -c "print(f'{$INJ_END-$START:.1f}')")
echo "    接收 $D / $N 事件：注入墙钟 ${INJ_S}s，全墙钟(引擎消化完)${ELAPSED}s (appended=$APP, acked_lag=$DRAINED, 限速=${RATE_BYTES}B/s)$TO_MARK"
echo "    EPS = $EPS events/sec (send-arrow ${CONNECTIONS}连接 / 引擎消化口径)"

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
