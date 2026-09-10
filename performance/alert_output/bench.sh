#!/usr/bin/env bash
# WFusion current-case benchmark, directly adapted from performance/nexmark_pk/bench.sh.
#
# This entry point runs the current sdm_event -> sdm_alert project in this
# directory.  It does not call scripts/wfusion_eps_bench.sh.
set -u -o pipefail

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
ROOT="$SCRIPT_DIR"
cd "$ROOT" || exit 1
source "$ROOT/scripts/direct_case_lib.sh"

usage() {
  cat <<'EOF'
用法: ./bench.sh [query] [feed] [events] [选项]

query:
  alert|sdm_alert（当前 models/rules/alert.wfl；mix/all 为兼容别名）
feed:
  replay（先 dump-frames，再 send-arrow）或 stream（wfgen stream）

选项:
  --mode blackhole|file     输出 sink，默认 blackhole
  --trials N                重复次数，默认 1
  --addr HOST:PORT          TCP 地址，默认 127.0.0.1:9800
  --rule-shards N           规则分片，默认 4
  --sink-parallel N         sink 并行度，默认 2
  --tcp-instances N         TCP reader 实例数，默认 4
  --input-connections N     replay 连接数，默认 1
  --max-total-bytes SIZE    全局窗口上限，默认 1536MB
  --event-window-bytes SIZE sdm_event 窗口上限，默认 512MB
  --allowed-lateness D      允许迟到，默认 30s
  --send-eps N|max           stream 目标速率；max 使用场景速率
  --rate N                  stream 目标速率别名
  --slice-ms N              stream 批次时间片，默认 1000
  --frame-rows N            replay frame 行数，默认 100000
  --frame-bytes N           replay frame 字节数，默认 8388608
  --wait-seconds N          每轮等待上限，默认 300
  --wfusion-bin PATH        指定 wfusion
  --wfgen-bin PATH          指定 wfgen
  --help                    显示帮助

示例:
  ./bench.sh alert replay 1m --mode blackhole
  ./bench.sh alert stream 1m --mode file --rate 300000
  ./bench.sh mix replay 10m --rule-shards 4 --input-connections 1
EOF
}

if [[ "$#" -gt 0 && "$1" == clean ]]; then
  rm -f "$ROOT"/data/bench_*.txt "$ROOT"/data/bench_*.samples \
        "$ROOT"/data/bench_*.log "$ROOT"/data/bench_*.jsonl \
        "$ROOT"/data/bench_*.frames "$ROOT"/data/perf_sentinel.ndjson \
        "$ROOT"/data/metrics.ndjson "$ROOT"/data/out_dat/sdm_alert.json
  rm -rf "$ROOT"/data/.wfusion-direct-*
  echo "== bench clean: 已清理本 case 生成物 =="
  exit 0
fi

QUERY=alert
FEED=replay
TOTAL_SPEC=1m
MODE=blackhole
TRIALS=1
ADDR=$CASE_ADDR
RULE_SHARDS=4
SINK_PARALLEL=2
TCP_INSTANCES=4
INPUT_CONNECTIONS=1
MAX_TOTAL_BYTES=1536MB
EVENT_WINDOW_BYTES=512MB
ALLOWED_LATENESS=30s
SEND_EPS=max
RATE=0
SLICE_MS=1000
FRAME_ROWS=100000
FRAME_BYTES=8388608
WAIT_SECONDS=300
WFUSION_BIN=$CASE_WFUSION
WFGEN_BIN=$CASE_WFGEN

if [[ "$#" -gt 0 && "$1" != -* ]]; then QUERY=$1; shift; fi
if [[ "$#" -gt 0 && "$1" != -* ]]; then FEED=$1; shift; fi
if [[ "$#" -gt 0 && "$1" != -* ]]; then TOTAL_SPEC=$1; shift; fi

while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --events|-n) [[ "$#" -ge 2 ]] || { echo "错误: $1 需要参数" >&2; exit 2; }; TOTAL_SPEC=$2; shift 2 ;;
    --mode|--sink) [[ "$#" -ge 2 ]] || { echo "错误: $1 需要参数" >&2; exit 2; }; MODE=$2; shift 2 ;;
    --trials) [[ "$#" -ge 2 ]] || { echo "错误: --trials 需要参数" >&2; exit 2; }; TRIALS=$2; shift 2 ;;
    --addr) [[ "$#" -ge 2 ]] || { echo "错误: --addr 需要参数" >&2; exit 2; }; ADDR=$2; shift 2 ;;
    --rule-shards) [[ "$#" -ge 2 ]] || { echo "错误: --rule-shards 需要参数" >&2; exit 2; }; RULE_SHARDS=$2; shift 2 ;;
    --sink-parallel) [[ "$#" -ge 2 ]] || { echo "错误: --sink-parallel 需要参数" >&2; exit 2; }; SINK_PARALLEL=$2; shift 2 ;;
    --tcp-instances) [[ "$#" -ge 2 ]] || { echo "错误: --tcp-instances 需要参数" >&2; exit 2; }; TCP_INSTANCES=$2; shift 2 ;;
    --input-connections|--connections) [[ "$#" -ge 2 ]] || { echo "错误: $1 需要参数" >&2; exit 2; }; INPUT_CONNECTIONS=$2; shift 2 ;;
    --max-total-bytes) [[ "$#" -ge 2 ]] || { echo "错误: --max-total-bytes 需要参数" >&2; exit 2; }; MAX_TOTAL_BYTES=$2; shift 2 ;;
    --event-window-bytes) [[ "$#" -ge 2 ]] || { echo "错误: --event-window-bytes 需要参数" >&2; exit 2; }; EVENT_WINDOW_BYTES=$2; shift 2 ;;
    --allowed-lateness) [[ "$#" -ge 2 ]] || { echo "错误: --allowed-lateness 需要参数" >&2; exit 2; }; ALLOWED_LATENESS=$2; shift 2 ;;
    --send-eps|--rate) [[ "$#" -ge 2 ]] || { echo "错误: $1 需要参数" >&2; exit 2; }; SEND_EPS=$2; RATE=$2; shift 2 ;;
    --slice-ms) [[ "$#" -ge 2 ]] || { echo "错误: --slice-ms 需要参数" >&2; exit 2; }; SLICE_MS=$2; shift 2 ;;
    --frame-rows) [[ "$#" -ge 2 ]] || { echo "错误: --frame-rows 需要参数" >&2; exit 2; }; FRAME_ROWS=$2; shift 2 ;;
    --frame-bytes) [[ "$#" -ge 2 ]] || { echo "错误: --frame-bytes 需要参数" >&2; exit 2; }; FRAME_BYTES=$2; shift 2 ;;
    --wait-seconds) [[ "$#" -ge 2 ]] || { echo "错误: --wait-seconds 需要参数" >&2; exit 2; }; WAIT_SECONDS=$2; shift 2 ;;
    --wfusion-bin) [[ "$#" -ge 2 ]] || { echo "错误: --wfusion-bin 需要参数" >&2; exit 2; }; WFUSION_BIN=$2; shift 2 ;;
    --wfgen-bin) [[ "$#" -ge 2 ]] || { echo "错误: --wfgen-bin 需要参数" >&2; exit 2; }; WFGEN_BIN=$2; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "错误: 未知参数 $1（使用 --help 查看用法）" >&2; exit 2 ;;
  esac
done

N=$(case_parse_count "$TOTAL_SPEC") || { echo "错误: 事件数格式错误: $TOTAL_SPEC" >&2; exit 2; }
[[ "$N" =~ ^[0-9]+$ && "$N" -gt 0 ]] || { echo "错误: 事件数必须大于 0" >&2; exit 2; }
[[ "$TRIALS" =~ ^[0-9]+$ && "$TRIALS" -gt 0 ]] || { echo "错误: --trials 必须是正整数" >&2; exit 2; }
[[ "$RULE_SHARDS" =~ ^[0-9]+$ && "$RULE_SHARDS" -gt 0 ]] || { echo "错误: --rule-shards 必须是正整数" >&2; exit 2; }
[[ "$SINK_PARALLEL" =~ ^[0-9]+$ && "$SINK_PARALLEL" -gt 0 ]] || { echo "错误: --sink-parallel 必须是正整数" >&2; exit 2; }
[[ "$TCP_INSTANCES" =~ ^[0-9]+$ && "$TCP_INSTANCES" -gt 0 ]] || { echo "错误: --tcp-instances 必须是正整数" >&2; exit 2; }
[[ "$INPUT_CONNECTIONS" =~ ^[0-9]+$ && "$INPUT_CONNECTIONS" -gt 0 ]] || { echo "错误: --input-connections 必须是正整数" >&2; exit 2; }
[[ "$FRAME_ROWS" =~ ^[0-9]+$ && "$FRAME_ROWS" -gt 0 ]] || { echo "错误: --frame-rows 必须是正整数" >&2; exit 2; }
[[ "$FRAME_BYTES" =~ ^[0-9]+$ && "$FRAME_BYTES" -gt 0 ]] || { echo "错误: --frame-bytes 必须是正整数" >&2; exit 2; }
[[ "$SLICE_MS" =~ ^[0-9]+$ && "$SLICE_MS" -gt 0 ]] || { echo "错误: --slice-ms 必须是正整数" >&2; exit 2; }
[[ "$WAIT_SECONDS" =~ ^[0-9]+$ && "$WAIT_SECONDS" -gt 0 ]] || { echo "错误: --wait-seconds 必须是正整数" >&2; exit 2; }
case "$QUERY" in alert|sdm_alert|mix|all) QUERY_LABEL=alert ;; *) echo "错误: 当前 case 只有 alert（兼容别名: sdm_alert/mix/all）" >&2; exit 2 ;; esac
case "$FEED" in replay|stream) ;; *) echo "错误: feed 只能是 replay 或 stream" >&2; exit 2 ;; esac
case "$MODE" in blackhole|file) ;; *) echo "错误: --mode 只能是 blackhole 或 file" >&2; exit 2 ;; esac
if [[ "$SEND_EPS" != max ]]; then
  [[ "$SEND_EPS" =~ ^[0-9]+$ ]] || { echo "错误: --send-eps/--rate 必须是 max 或正整数" >&2; exit 2; }
  RATE=$SEND_EPS
else
  RATE=0
fi

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
FRAME_FILE=""
if [[ "$FEED" == replay ]]; then
  printf '== 准备当前 sdm_event 的 Arrow frame（事件数=%s） ==\n' "$N"
  case_prepare_runtime blackhole "$RULE_SHARDS" "$SINK_PARALLEL" "$MAX_TOTAL_BYTES" "$EVENT_WINDOW_BYTES" "$ALLOWED_LATENESS" ||
    { echo "错误: 无法创建临时运行配置" >&2; exit 1; }
  FRAME_FILE=$(case_prepare_frames "$N" "$EVENT_LABEL" "$FRAME_BYTES" "$FRAME_ROWS") ||
    { case_cleanup_runtime; echo "错误: 当前 sdm_event frame 生成失败" >&2; exit 1; }
  case_cleanup_runtime
  echo "frames=$FRAME_FILE"
fi

ACTIVE_DAEMON=""
ACTIVE_SAMPLER=""
cleanup() {
  local rc=$?
  set +e
  if [[ -n "$ACTIVE_SAMPLER" ]]; then kill "$ACTIVE_SAMPLER" 2>/dev/null || true; wait "$ACTIVE_SAMPLER" 2>/dev/null || true; fi
  if [[ -n "$ACTIVE_DAEMON" ]]; then case_stop_daemon "$ACTIVE_DAEMON" "$WAIT_SECONDS"; fi
  case_cleanup_runtime
  trap - EXIT INT TERM
  exit "$rc"
}
trap cleanup EXIT INT TERM

run_trial() {
  local sink="$1" trial="$2"
  local out="$ROOT/data/bench_"$QUERY_LABEL"_"$FEED"_"$sink".txt"
  local log="$ROOT/data/bench_"$EVENT_LABEL"_"$FEED"_"$sink"_"$trial".log"
  local sample="$ROOT/data/bench_"$EVENT_LABEL"_"$FEED"_"$sink"_"$trial".samples"
  local sender="" sender_rc=0
  local expected="$N" tuple="" sent_n="" sent_start="" sent_end="" sent_count=""
  local eps=0 eps_mode=timeout t0=0 t2=0 app=0 lag=1
  local daemon="" sampler="" i
  local daemon_log=""
  local stream_rate=0
  local correctness="" summary="" alerts=0 rss_peak="" cpu_avg="" cpu_max="" evict=""
  case_prepare_runtime "$sink" "$RULE_SHARDS" "$SINK_PARALLEL" "$MAX_TOTAL_BYTES" "$EVENT_WINDOW_BYTES" "$ALLOWED_LATENESS" ||
    { echo "错误: trial=$trial 无法创建临时运行配置" >&2; return 1; }
  rm -f "$CASE_METRICS" "$ROOT/data/perf_sentinel.ndjson" "$CASE_ALERTS"
  : > "$ROOT/data/perf_sentinel.ndjson"
  daemon_log="$ROOT/data/daemon_"$EVENT_LABEL"_"$FEED"_"$sink"_"$trial".log"
  case_start_daemon "$daemon_log" "$ROOT/conf/perf-diag.toml" || { case_cleanup_runtime; return 1; }
  daemon=$CASE_DAEMON_PID
  ACTIVE_DAEMON=$daemon
  "$CASE_PYTHON" "$CASE_LIB" rss-sampler "$daemon" "$sample" 0.1 >/dev/null 2>&1 &
  sampler=$!
  ACTIVE_SAMPLER=$sampler
  sleep 0.3
  t0=$("$CASE_PYTHON" "$CASE_LIB" now)

  if [[ "$FEED" == replay ]]; then
    expected=$((N * INPUT_CONNECTIONS))
    "$CASE_WFGEN" send-arrow --input "$FRAME_FILE" --addr "$CASE_CONNECT_ADDR" \
      --connections "$INPUT_CONNECTIONS" --sentinel "$expected" > "$log" 2>&1 &
    sender=$!
  else
    stream_rate=$RATE
    "$CASE_WFGEN" stream --scenario-dir "$ROOT/models/scenarios" \
      --ws "$ROOT/models/schemas/sdm_event.wfs" --wfl "$ROOT/models/rules/alert.wfl" \
      --addr "$CASE_CONNECT_ADDR" --rate "$stream_rate" --slice-ms "$SLICE_MS" \
      --sentinel "$N" > "$log" 2>&1 &
    sender=$!
  fi

  for i in $(seq 1 $((WAIT_SECONDS * 10))); do
    tuple=$(case_sentinel_tuple)
    if [[ -n "$tuple" ]]; then
      read -r sent_n sent_start sent_end sent_count <<< "$tuple"
      eps=$("$CASE_PYTHON" "$CASE_LIB" eps "$sent_n" "$sent_start" "$sent_end")
      eps_mode=sentinel
      app=$sent_n
      t2=$("$CASE_PYTHON" "$CASE_LIB" now)
      break
    fi
    app=$(case_metric_appended)
    lag=$(case_metric_acked_lag)
    if [[ "$app" =~ ^[0-9]+$ && "$app" -ge "$expected" && "$lag" == 0 ]]; then
      t2=$("$CASE_PYTHON" "$CASE_LIB" now)
      eps=$("$CASE_PYTHON" "$CASE_LIB" eps "$expected" "$t0" "$t2")
      eps_mode=metrics
      break
    fi
    if (( i % 50 == 0 )); then
      echo "  trial=$trial sink=$sink progress=$app/$expected ack_lag=$lag"
    fi
    sleep 0.1
  done
  if [[ "$eps_mode" == timeout ]]; then
    [[ -n "$sender" ]] && kill "$sender" 2>/dev/null || true
  fi
  if [[ -n "$sender" ]]; then
    wait "$sender" 2>/dev/null || sender_rc=$?
  fi
  if [[ "$eps_mode" == timeout ]]; then
    app=$(case_metric_appended)
    t2=$("$CASE_PYTHON" "$CASE_LIB" now)
    eps=$("$CASE_PYTHON" "$CASE_LIB" eps "$app" "$t0" "$t2")
    echo "  ⚠ trial=$trial sink=$sink 在" "$WAIT_SECONDS" "s 内未完成" >&2
  fi
  sleep 1
  case_stop_daemon "$daemon" "$WAIT_SECONDS"
  ACTIVE_DAEMON=""
  if [[ -n "$sampler" ]]; then kill "$sampler" 2>/dev/null || true; wait "$sampler" 2>/dev/null || true; fi
  ACTIVE_SAMPLER=""

  app=$(case_metric_appended)
  lag=$(case_metric_acked_lag)
  correctness=$(case_metric_correctness)
  summary=$(printf '%s\n' "$correctness" | sed -n 's/^SUMMARY //p' | tail -1)
  alerts=$(case_count_alerts)
  rss_peak=$(awk 'NF >= 2 && $2 ~ /^[0-9.]+$/ && $2 > m {m=$2} END {if (m) printf "%.1f", m; else print "n/a"}' "$sample" 2>/dev/null)
  cpu_avg=$(awk 'NF >= 3 && $3 ~ /^[0-9.]+$/ {s += $3; n++} END {if (n) printf "%.0f", s/n; else print "n/a"}' "$sample" 2>/dev/null)
  cpu_max=$(awk 'NF >= 3 && $3 ~ /^[0-9.]+$/ && $3 > m {m=$3} END {if (m) printf "%.0f", m; else print "n/a"}' "$sample" 2>/dev/null)
  evict=$(grep -ci 'evict' "$daemon_log" 2>/dev/null || true)
  [[ -n "$rss_peak" ]] || rss_peak=n/a
  [[ -n "$cpu_avg" ]] || cpu_avg=n/a
  [[ -n "$cpu_max" ]] || cpu_max=n/a

  verdict=PASS
  [[ "$eps_mode" == timeout ]] && verdict=TIMEOUT
  [[ "$sender_rc" == 0 ]] || verdict=FAIL
  [[ "$app" =~ ^[0-9]+$ && "$app" -ge "$expected" ]] || verdict=FAIL
  [[ "$lag" == 0 ]] || verdict=DIRTY
  [[ "$summary" == clean ]] || verdict=DIRTY
  if [[ "$sink" == file && "$alerts" -lt "$expected" ]]; then verdict=FAIL; fi
  {
    echo "trial=$trial query=$QUERY_LABEL feed=$FEED sink=$sink"
    echo "events=$expected sender_rc=$sender_rc eps=$eps eps_mode=$eps_mode"
    echo "appended=$app acked_lag=$lag alerts=$alerts"
    echo "rule_shards=$RULE_SHARDS sink_parallel=$SINK_PARALLEL tcp_instances=$TCP_INSTANCES input_connections=$INPUT_CONNECTIONS"
    echo "max_total_bytes=$MAX_TOTAL_BYTES event_window_bytes=$EVENT_WINDOW_BYTES allowed_lateness=$ALLOWED_LATENESS"
    echo "CPU_avg=$cpu_avg% CPU_max=$cpu_max% RSS_peak=$rss_peak""MiB evict=$evict verdict=$verdict"
    echo "-- correctness --"
    printf '%s\n' "$correctness"
  } >> "$out"
  echo "$QUERY_LABEL/$FEED sink=$sink trial=$trial EPS=$eps appended=$app/$expected alerts=$alerts CPU=$cpu_avg%/$cpu_max% RSS_peak=$rss_peak MiB evict=$evict [$verdict]"
  case_cleanup_runtime
  [[ "$verdict" == PASS ]]
}

overall=0
echo "== bench: query=$QUERY_LABEL feed=$FEED events=$N sink=$MODE rule_shards=$RULE_SHARDS sink_parallel=$SINK_PARALLEL =="
for trial in $(seq 1 "$TRIALS"); do
  if ! run_trial "$MODE" "$trial"; then overall=1; fi
done
echo "== done: 结果在 $ROOT/data/bench_"$QUERY_LABEL"_"$FEED"_"$MODE".txt =="
exit "$overall"
