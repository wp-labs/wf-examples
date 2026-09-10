#!/usr/bin/env bash
# Current sdm_event -> sdm_alert correctness verifier.
#
# This is the direct daemon/TCP verification entry point corresponding to
# performance/nexmark_pk/verify_daemon.sh.  It uses the current rule and
# schema files in this directory and never invokes wfusion_eps_bench.sh.
set -u -o pipefail

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
ROOT="$SCRIPT_DIR"
cd "$ROOT" || exit 1
source "$ROOT/scripts/direct_case_lib.sh"

usage() {
  cat <<'EOF'
用法: ./verify_daemon.sh [query] [events] [选项]

当前 query 固定为 alert（sdm_alert/mix/all 是兼容别名）。
脚本固定使用 file sink，验证 TCP 注入、规则消费、SIGTERM flush 和
sdm_alert JSONL 内容。

选项:
  --addr HOST:PORT          默认 127.0.0.1:9800
  --rule-shards N           默认 4
  --sink-parallel N         默认 2
  --tcp-instances N         默认 4
  --max-total-bytes SIZE    默认 1536MB
  --event-window-bytes SIZE 默认 512MB
  --allowed-lateness D      默认 30s
  --wait-seconds N          默认 300
  --wfusion-bin PATH        指定 wfusion
  --wfgen-bin PATH          指定 wfgen
  --help                    显示帮助

示例:
  ./verify_daemon.sh alert 100k
  ./verify_daemon.sh alert 1m --rule-shards 4 --sink-parallel 2
EOF
}

QUERY=alert
TOTAL_SPEC=100k
ADDR=$CASE_ADDR
RULE_SHARDS=4
SINK_PARALLEL=2
TCP_INSTANCES=4
MAX_TOTAL_BYTES=1536MB
EVENT_WINDOW_BYTES=512MB
ALLOWED_LATENESS=30s
WAIT_SECONDS=300
WFUSION_BIN=$CASE_WFUSION
WFGEN_BIN=$CASE_WFGEN

if [[ "$#" -gt 0 && "$1" != -* ]]; then QUERY=$1; shift; fi
if [[ "$#" -gt 0 && "$1" != -* ]]; then TOTAL_SPEC=$1; shift; fi
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --addr) [[ "$#" -ge 2 ]] || { echo "错误: --addr 需要参数" >&2; exit 2; }; ADDR=$2; shift 2 ;;
    --rule-shards) [[ "$#" -ge 2 ]] || { echo "错误: --rule-shards 需要参数" >&2; exit 2; }; RULE_SHARDS=$2; shift 2 ;;
    --sink-parallel) [[ "$#" -ge 2 ]] || { echo "错误: --sink-parallel 需要参数" >&2; exit 2; }; SINK_PARALLEL=$2; shift 2 ;;
    --tcp-instances) [[ "$#" -ge 2 ]] || { echo "错误: --tcp-instances 需要参数" >&2; exit 2; }; TCP_INSTANCES=$2; shift 2 ;;
    --max-total-bytes) [[ "$#" -ge 2 ]] || { echo "错误: --max-total-bytes 需要参数" >&2; exit 2; }; MAX_TOTAL_BYTES=$2; shift 2 ;;
    --event-window-bytes) [[ "$#" -ge 2 ]] || { echo "错误: --event-window-bytes 需要参数" >&2; exit 2; }; EVENT_WINDOW_BYTES=$2; shift 2 ;;
    --allowed-lateness) [[ "$#" -ge 2 ]] || { echo "错误: --allowed-lateness 需要参数" >&2; exit 2; }; ALLOWED_LATENESS=$2; shift 2 ;;
    --wait-seconds) [[ "$#" -ge 2 ]] || { echo "错误: --wait-seconds 需要参数" >&2; exit 2; }; WAIT_SECONDS=$2; shift 2 ;;
    --wfusion-bin) [[ "$#" -ge 2 ]] || { echo "错误: --wfusion-bin 需要参数" >&2; exit 2; }; WFUSION_BIN=$2; shift 2 ;;
    --wfgen-bin) [[ "$#" -ge 2 ]] || { echo "错误: --wfgen-bin 需要参数" >&2; exit 2; }; WFGEN_BIN=$2; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "错误: 未知参数 $1（使用 --help 查看用法）" >&2; exit 2 ;;
  esac
done

N=$(case_parse_count "$TOTAL_SPEC") || { echo "错误: 事件数格式错误: $TOTAL_SPEC" >&2; exit 2; }
[[ "$N" =~ ^[0-9]+$ && "$N" -gt 0 ]] || { echo "错误: 事件数必须大于 0" >&2; exit 2; }
[[ "$RULE_SHARDS" =~ ^[0-9]+$ && "$RULE_SHARDS" -gt 0 ]] || { echo "错误: --rule-shards 必须是正整数" >&2; exit 2; }
[[ "$SINK_PARALLEL" =~ ^[0-9]+$ && "$SINK_PARALLEL" -gt 0 ]] || { echo "错误: --sink-parallel 必须是正整数" >&2; exit 2; }
[[ "$TCP_INSTANCES" =~ ^[0-9]+$ && "$TCP_INSTANCES" -gt 0 ]] || { echo "错误: --tcp-instances 必须是正整数" >&2; exit 2; }
[[ "$WAIT_SECONDS" =~ ^[0-9]+$ && "$WAIT_SECONDS" -gt 0 ]] || { echo "错误: --wait-seconds 必须是正整数" >&2; exit 2; }
case "$QUERY" in alert|sdm_alert|mix|all) ;; *) echo "错误: 当前 case 只有 alert（兼容别名: sdm_alert/mix/all）" >&2; exit 2 ;; esac

CASE_ADDR=$ADDR
case_parse_addr "$ADDR" || { echo "错误: --addr 必须是 HOST:PORT" >&2; exit 2; }
CASE_TCP_INSTANCES=$TCP_INSTANCES
CASE_SHUTDOWN_SECS=$WAIT_SECONDS
CASE_WFUSION=$(case_resolve_binary "$WFUSION_BIN" wfusion)
CASE_WFGEN=$(case_resolve_binary "$WFGEN_BIN" wfgen)
[[ -x "$CASE_WFUSION" ]] || { echo "错误: 找不到可执行 wfusion" >&2; exit 1; }
[[ -x "$CASE_WFGEN" ]] || { echo "错误: 找不到可执行 wfgen" >&2; exit 1; }

EVENT_LABEL=$(printf '%s' "$TOTAL_SPEC" | tr '[:upper:]' '[:lower:]' | tr -c '[:alnum:]_.-' '_')
mkdir -p "$ROOT/data/out_dat"

# Frames are generated from the current event generator and current schema,
# rather than from any Nexmark cache.
case_prepare_runtime blackhole "$RULE_SHARDS" "$SINK_PARALLEL" "$MAX_TOTAL_BYTES" "$EVENT_WINDOW_BYTES" "$ALLOWED_LATENESS" ||
  { echo "错误: 无法创建 frame 生成配置" >&2; exit 1; }
FRAME_FILE=$(case_prepare_frames "$N" "$EVENT_LABEL" "$CASE_FRAME_BYTES" "$CASE_FRAME_ROWS") ||
  { case_cleanup_runtime; echo "错误: 当前 sdm_event frame 生成失败" >&2; exit 1; }
case_cleanup_runtime

METRICS="$ROOT/data/metrics.ndjson"
ALERTS="$ROOT/data/out_dat/sdm_alert.json"
SUMMARY_FILE="$ROOT/data/verify_daemon_all.txt"
DETAIL_FILE="$ROOT/data/verify_daemon_alert.txt"
LOG_FILE="$ROOT/data/verify_daemon_alert.log"
rm -f "$METRICS" "$ALERTS" "$ROOT/data/perf_sentinel.ndjson" "$SUMMARY_FILE" "$DETAIL_FILE" "$LOG_FILE"
: > "$ROOT/data/perf_sentinel.ndjson"

ACTIVE_DAEMON=""
cleanup() {
  local rc=$?
  set +e
  if [[ -n "$ACTIVE_DAEMON" ]]; then case_stop_daemon "$ACTIVE_DAEMON" "$WAIT_SECONDS"; fi
  case_cleanup_runtime
  trap - EXIT INT TERM
  exit "$rc"
}
trap cleanup EXIT INT TERM

case_prepare_runtime file "$RULE_SHARDS" "$SINK_PARALLEL" "$MAX_TOTAL_BYTES" "$EVENT_WINDOW_BYTES" "$ALLOWED_LATENESS" ||
  { echo "错误: 无法创建 file sink 配置" >&2; exit 1; }
case_start_daemon "$LOG_FILE" || { case_cleanup_runtime; exit 1; }
DAEMON=$CASE_DAEMON_PID
ACTIVE_DAEMON=$DAEMON

echo "== verify_daemon: query=alert events=$N sink=file rule_shards=$RULE_SHARDS =="
echo "frames=$FRAME_FILE"
"$CASE_WFGEN" send-arrow --input "$FRAME_FILE" --addr "$CASE_CONNECT_ADDR" \
  --connections 1 > "$ROOT/data/verify_daemon_sender.log" 2>&1 &
SENDER=$!

drained=0
app=0
lag=1
for i in $(seq 1 $((WAIT_SECONDS * 10))); do
  app=$(case_metric_appended)
  lag=$(case_metric_acked_lag)
  if [[ "$app" =~ ^[0-9]+$ && "$app" -ge "$N" && "$lag" == 0 ]]; then
    drained=1
    break
  fi
  if (( i % 50 == 0 )); then echo "  progress=$app/$N ack_lag=$lag"; fi
  sleep 0.1
done
if [[ "$drained" != 1 ]]; then
  kill "$SENDER" 2>/dev/null || true
  wait "$SENDER" 2>/dev/null || true
  echo "错误: 在" "$WAIT_SECONDS" "s 内未达到 appended=$N 且 acked_lag=0" >&2
  case_stop_daemon "$DAEMON" "$WAIT_SECONDS"
  ACTIVE_DAEMON=""
  exit 1
fi
wait "$SENDER" 2>/dev/null || true

# SIGTERM lets the current file sink flush the final alert rows.
case_stop_daemon "$DAEMON" "$WAIT_SECONDS"
ACTIVE_DAEMON=""
sleep 1

CORRECTNESS=$(case_metric_correctness)
APP=$(case_metric_appended)
LAG=$(case_metric_acked_lag)
ALERT_COUNT=$(case_count_alerts)
EMIT_COUNT=$(printf '%s\n' "$CORRECTNESS" | awk '$1 == "EMIT" && $2 == "sdm_alert" {s += $3} END {print s + 0}')
CONTENT_FILE="$ROOT/data/verify_daemon_content.txt"
"$CASE_PYTHON" - "$ALERTS" "$N" "$CONTENT_FILE" <<'PY'
import json
import sys

path, expected, report = sys.argv[1], int(sys.argv[2]), sys.argv[3]
count = 0
ids = set()
bad = []
try:
    stream = open(path, encoding="utf-8", errors="replace")
except FileNotFoundError:
    stream = []
for line_no, line in enumerate(stream, 1):
    try:
        obj = json.loads(line)
    except Exception:
        bad.append("line %d: invalid JSON" % line_no)
        continue
    count += 1
    if not isinstance(obj, dict):
        bad.append("line %d: JSON value is not an object" % line_no)
        continue
    alert_id = obj.get("alert_id")
    if not alert_id:
        bad.append("line %d: missing alert_id" % line_no)
    else:
        ids.add(str(alert_id))
    for key in ("tenant_id", "created_time"):
        if key not in obj:
            bad.append("line %d: missing %s" % (line_no, key))
if stream != []:
    stream.close()
ok = count == expected and len(ids) == expected and not bad
with open(report, "w", encoding="utf-8") as out:
    out.write("count=%d expected=%d unique_alert_id=%d\n" % (count, expected, len(ids)))
    for item in bad[:20]:
        out.write("error=%s\n" % item)
    out.write("content=%s\n" % ("PASS" if ok else "FAIL"))
print("PASS" if ok else "FAIL")
raise SystemExit(0 if ok else 1)
PY
CONTENT_RC=$?
CONTENT=$(cat "$CONTENT_FILE" 2>/dev/null || echo "content=FAIL")

VERDICT=PASS
[[ "$APP" =~ ^[0-9]+$ && "$APP" -ge "$N" ]] || VERDICT=FAIL
[[ "$LAG" == 0 ]] || VERDICT=FAIL
[[ "$EMIT_COUNT" -ge "$N" ]] || VERDICT=FAIL
[[ "$ALERT_COUNT" -eq "$N" ]] || VERDICT=FAIL
[[ "$CONTENT_RC" -eq 0 ]] || VERDICT=FAIL
printf '%s\n' "$CORRECTNESS" | grep -q '^SUMMARY clean$' || VERDICT=DIRTY

{
  echo "query=alert events=$N sink=file"
  echo "frame=$FRAME_FILE"
  echo "appended=$APP acked_lag=$LAG emitted=$EMIT_COUNT alerts=$ALERT_COUNT"
  echo "rule_shards=$RULE_SHARDS sink_parallel=$SINK_PARALLEL tcp_instances=$TCP_INSTANCES"
  echo "max_total_bytes=$MAX_TOTAL_BYTES event_window_bytes=$EVENT_WINDOW_BYTES allowed_lateness=$ALLOWED_LATENESS"
  echo "verdict=$VERDICT"
  echo "-- correctness --"
  printf '%s\n' "$CORRECTNESS"
  echo "-- content --"
  printf '%s\n' "$CONTENT"
} | tee "$SUMMARY_FILE" | tee "$DETAIL_FILE"

if [[ "$VERDICT" == PASS ]]; then
  echo "== verify_daemon: PASS =="
  exit 0
fi
echo "== verify_daemon: FAIL（见 $DETAIL_FILE） =="
exit 1
