#!/usr/bin/env bash
# wfusion_new 性能墙定位（直接适配 performance/nexmark_pk/diag.sh）
#
# 复用 wfusion 的 perf-diag/sentinel 协议，把同一份预编码帧按
# recv/decode/floor/rules/full 逐档重放，计算每段增量并检查完成计数。
# 每次运行使用 data/perf_diag_runs/<run-id>/ 下的配置副本，不修改原工程。
#
# 用法：
#   ./diag.sh                         # 100k，默认 BlackHole sink
#   ./diag.sh 1m --sink file          # 1m，包含 JSON 序列化和文件写
#   ./diag.sh --events 500k --no-warmup
#   ./diag.sh 200k --stages floor,rules,emit,full --rule-shards 4
#
# 结果：
#   data/perf_diag_<N>_<run-id>.txt       可读报告
#   data/perf_diag_wall_<N>_<run-id>.txt  wfgen 原始墙表
#   data/perf_diag_runs/<run-id>/         事件、帧、sentinel、metrics 快照、日志、采样
#
# 默认 --sink blackhole 只测引擎输出链；--sink file 才把 file_json_sink 的
# 序列化/写盘纳入 full。单轮测量只用于定位方向，建议用 1m 或更大 N 重跑。
set -u -o pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
ROOT="$SCRIPT_DIR"
cd "$ROOT" || exit 1

PYTHON_BIN="${PYTHON_BIN:-${PYTHON:-python3}}"
WFUSION_BIN="${WFUSION_BIN:-${WFUSION:-}}"
WFGEN_BIN="${WFGEN_BIN:-${WFGEN:-}}"
ADDR="${WF_DIAG_ADDR:-127.0.0.1:9800}"
SINK_MODE="blackhole"
STAGES_CSV="recv,decode,floor,rules,full"
WARMUP=1
FRAME_ROWS=100000
FRAME_BYTES=8388608
SAMPLE_MS=100
TIMEOUT_SECS=""
SHUTDOWN_SECS="${WF_DIAG_SHUTDOWN_SECS:-}"
RULE_SHARDS="${WF_DIAG_RULE_SHARDS:-4}"
MAX_TOTAL_BYTES="${WF_DIAG_MAX_TOTAL_BYTES:-1536MB}"
OUTPUT=""
CLEANUP=0
EVENTS_SPEC=""

usage() {
  cat <<'EOF'
用法: ./diag.sh [事件数=100k] [选项]

选项:
  --events N              事件数；支持整数、k/m/g（如 100k、1m）
  --sink blackhole|file   full 档 sink（默认 blackhole）
  --stages CSV            墙梯，默认 recv,decode,floor,rules,full
  --warmup / --no-warmup  是否加入预热档（默认加入）
  --addr HOST:PORT         TCP 监听地址（默认 127.0.0.1:9800）
  --frame-rows N          每个 Arrow frame 最大行数（默认 100000）
  --frame-bytes N         每个 Arrow frame 最大字节数（默认 8388608）
  --sample-ms N            CPU/RSS 采样间隔（默认 100）
  --timeout-secs N        每档 sentinel 等待超时（默认按事件量计算）
  --shutdown-secs N        daemon 优雅停止宽限（默认 max(60, timeout-secs)）
  --rule-shards N         覆盖副本 overlay 中的 rule_shards
  --max-total-bytes SIZE  设置 WF_DIAG_MAX_TOTAL_BYTES（如 0、8GB、60%）
  --wfusion-bin PATH       指定 wfusion 二进制
  --wfgen-bin PATH         指定 wfgen 二进制
  --output PATH            报告输出路径（默认 data/perf_diag_<N>_<run-id>.txt）
  --cleanup                分析结束后删除本次临时运行目录
  --keep                   保留临时运行目录（默认）
  -h, --help               显示帮助

recv/decode/floor/rules/emit/full 是叠加式档位。每档发送同一份帧，只有
sentinel 流不受切口影响；recv/decode 档的业务事件按设计不进入窗口。
出现大幅负增量时，报告会标记测量不满足单调性，不硬选“主墙”。
EOF
}

die() {
  echo "错误：$*" >&2
  EXIT_STATUS=2
  exit 2
}

parse_count() {
  local raw="${1:-}" number suffix multiplier
  raw="$(printf '%s' "$raw" | tr '[:upper:]' '[:lower:]')"
  if [[ "$raw" =~ ^([0-9]+)([kmgw]?)$ ]]; then
    number="${BASH_REMATCH[1]}"
    suffix="${BASH_REMATCH[2]}"
  else
    return 1
  fi
  case "$suffix" in
    "") multiplier=1 ;;
    k) multiplier=1000 ;;
    m) multiplier=1000000 ;;
    g) multiplier=1000000000 ;;
    w) multiplier=10000 ;;
    *) return 1 ;;
  esac
  printf '%s\n' "$((10#$number * multiplier))"
}

resolve_binary() {
  local requested="$1" name="$2" candidate
  if [[ -n "$requested" ]]; then
    [[ -x "$requested" ]] || die "$name 不可执行：$requested"
    printf '%s/%s\n' "$(cd -- "$(dirname -- "$requested")" && pwd -P)" "$(basename -- "$requested")"
    return 0
  fi
  for candidate in \
    "$ROOT/../../warp-fusion/target/release/$name" \
    "$ROOT/../warp-fusion/target/release/$name" \
    "/Users/dy_xuyuhao/bin/$name"; do
    if [[ -x "$candidate" ]]; then
      printf '%s/%s\n' "$(cd -- "$(dirname -- "$candidate")" && pwd -P)" "$(basename -- "$candidate")"
      return 0
    fi
  done
  command -v "$name" 2>/dev/null || true
}

if [[ "$#" -gt 0 && "$1" != -* ]]; then
  EVENTS_SPEC="$1"
  shift
fi

while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --events|-n)
      [[ "$#" -ge 2 ]] || die "$1 需要参数"
      EVENTS_SPEC="$2"; shift 2 ;;
    --sink)
      [[ "$#" -ge 2 ]] || die "--sink 需要参数"
      SINK_MODE="$2"; shift 2 ;;
    --stages)
      [[ "$#" -ge 2 ]] || die "--stages 需要参数"
      STAGES_CSV="$2"; shift 2 ;;
    --warmup) WARMUP=1; shift ;;
    --no-warmup) WARMUP=0; shift ;;
    --addr)
      [[ "$#" -ge 2 ]] || die "--addr 需要参数"
      ADDR="$2"; shift 2 ;;
    --frame-rows)
      [[ "$#" -ge 2 ]] || die "--frame-rows 需要参数"
      FRAME_ROWS="$2"; shift 2 ;;
    --frame-bytes)
      [[ "$#" -ge 2 ]] || die "--frame-bytes 需要参数"
      FRAME_BYTES="$2"; shift 2 ;;
    --sample-ms)
      [[ "$#" -ge 2 ]] || die "--sample-ms 需要参数"
      SAMPLE_MS="$2"; shift 2 ;;
    --timeout-secs)
      [[ "$#" -ge 2 ]] || die "--timeout-secs 需要参数"
      TIMEOUT_SECS="$2"; shift 2 ;;
    --shutdown-secs)
      [[ "$#" -ge 2 ]] || die "--shutdown-secs 需要参数"
      SHUTDOWN_SECS="$2"; shift 2 ;;
    --rule-shards)
      [[ "$#" -ge 2 ]] || die "--rule-shards 需要参数"
      RULE_SHARDS="$2"; shift 2 ;;
    --max-total-bytes)
      [[ "$#" -ge 2 ]] || die "--max-total-bytes 需要参数"
      MAX_TOTAL_BYTES="$2"; shift 2 ;;
    --wfusion-bin)
      [[ "$#" -ge 2 ]] || die "--wfusion-bin 需要参数"
      WFUSION_BIN="$2"; shift 2 ;;
    --wfgen-bin)
      [[ "$#" -ge 2 ]] || die "--wfgen-bin 需要参数"
      WFGEN_BIN="$2"; shift 2 ;;
    --output)
      [[ "$#" -ge 2 ]] || die "--output 需要参数"
      OUTPUT="$2"; shift 2 ;;
    --cleanup) CLEANUP=1; shift ;;
    --keep) CLEANUP=0; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "未知参数：${1}（使用 --help 查看用法）" ;;
  esac
done

EVENTS_SPEC="${EVENTS_SPEC:-100k}"
N="$(parse_count "$EVENTS_SPEC")" || die "事件数格式错误：${EVENTS_SPEC}（示例：100k、1m）"
(( N > 0 )) || die "事件数必须大于 0"

case "$SINK_MODE" in
  blackhole|file) ;;
  *) die "--sink 只能是 blackhole 或 file" ;;
esac

if [[ "$FRAME_ROWS" =~ ^[0-9]+$ ]]; then
  FRAME_ROWS=$((10#$FRAME_ROWS))
  (( FRAME_ROWS > 0 )) || die "--frame-rows 必须是正整数"
else
  die "--frame-rows 必须是正整数"
fi
if [[ "$FRAME_BYTES" =~ ^[0-9]+$ ]]; then
  FRAME_BYTES=$((10#$FRAME_BYTES))
  (( FRAME_BYTES > 0 )) || die "--frame-bytes 必须是正整数"
else
  die "--frame-bytes 必须是正整数"
fi
if [[ "$SAMPLE_MS" =~ ^[0-9]+$ ]]; then
  SAMPLE_MS=$((10#$SAMPLE_MS))
  (( SAMPLE_MS > 0 )) || die "--sample-ms 必须是正整数"
else
  die "--sample-ms 必须是正整数"
fi
if [[ -n "$TIMEOUT_SECS" ]]; then
  [[ "$TIMEOUT_SECS" =~ ^[0-9]+$ ]] || die "--timeout-secs 必须是正整数"
  TIMEOUT_SECS=$((10#$TIMEOUT_SECS))
  (( TIMEOUT_SECS > 0 )) || die "--timeout-secs 必须是正整数"
else
  TIMEOUT_SECS=$((N / 50000 + 120))
  (( TIMEOUT_SECS < 120 )) && TIMEOUT_SECS=120
fi
if [[ -n "$SHUTDOWN_SECS" ]]; then
  [[ "$SHUTDOWN_SECS" =~ ^[0-9]+$ ]] || die "--shutdown-secs 必须是正整数"
  SHUTDOWN_SECS=$((10#$SHUTDOWN_SECS))
  (( SHUTDOWN_SECS > 0 )) || die "--shutdown-secs 必须是正整数"
else
  SHUTDOWN_SECS="$TIMEOUT_SECS"
  (( SHUTDOWN_SECS < 60 )) && SHUTDOWN_SECS=60
fi
if [[ -n "$RULE_SHARDS" ]]; then
  [[ "$RULE_SHARDS" =~ ^[0-9]+$ ]] || die "--rule-shards 必须是正整数"
  RULE_SHARDS=$((10#$RULE_SHARDS))
  (( RULE_SHARDS > 0 )) || die "--rule-shards 必须是正整数"
fi

if [[ "$ADDR" =~ ^\[([^]]+)\]:([0-9]+)$ ]]; then
  LISTEN_HOST="${BASH_REMATCH[1]}"
  PORT="${BASH_REMATCH[2]}"
elif [[ "$ADDR" =~ ^([^:]+):([0-9]+)$ ]]; then
  LISTEN_HOST="${BASH_REMATCH[1]}"
  PORT="${BASH_REMATCH[2]}"
else
  die "--addr 必须是 HOST:PORT（IPv6 使用 [addr]:port）"
fi
PORT=$((10#$PORT))
(( PORT >= 1 && PORT <= 65535 )) || die "端口范围错误：$PORT"
case "$LISTEN_HOST" in
  "") LISTEN_HOST="0.0.0.0" ;;
  "*") LISTEN_HOST="0.0.0.0" ;;
esac
case "$LISTEN_HOST" in
  0.0.0.0) CONNECT_HOST="127.0.0.1" ;;
  ::) CONNECT_HOST="::1" ;;
  *) CONNECT_HOST="$LISTEN_HOST" ;;
esac
if [[ "$CONNECT_HOST" == *:* ]]; then
  CONNECT_ADDR="[$CONNECT_HOST]:$PORT"
else
  CONNECT_ADDR="$CONNECT_HOST:$PORT"
fi

IFS=',' read -r -a STAGE_LIST <<< "$STAGES_CSV"
(( ${#STAGE_LIST[@]} >= 2 )) || die "--stages 至少需要两个档位"
STAGE_SEEN=""
for stage_index in "${!STAGE_LIST[@]}"; do
  stage="${STAGE_LIST[$stage_index]}"
  stage="${stage//[[:space:]]/}"
  STAGE_LIST[$stage_index]="$stage"
  [[ -n "$stage" ]] || die "--stages 含空档位"
  case "$stage" in
    recv|decode|floor|rules|emit|full) ;;
    warmup) die "warmup 由 --warmup/--no-warmup 控制，不要放进 --stages" ;;
    *) die "未知诊断档：${stage}（可用 recv|decode|floor|rules|emit|full）" ;;
  esac
  case ",$STAGE_SEEN," in
    *,"$stage",*) die "--stages 含重复档位：$stage" ;;
  esac
  if [[ -n "$STAGE_SEEN" ]]; then STAGE_SEEN="$STAGE_SEEN,$stage"; else STAGE_SEEN="$stage"; fi
done

WFUSION_BIN="$(resolve_binary "$WFUSION_BIN" wfusion)"
WFGEN_BIN="$(resolve_binary "$WFGEN_BIN" wfgen)"
[[ -x "$WFUSION_BIN" ]] || die "找不到可执行 wfusion；请用 --wfusion-bin 指定"
[[ -x "$WFGEN_BIN" ]] || die "找不到可执行 wfgen；请用 --wfgen-bin 指定"
command -v "$PYTHON_BIN" >/dev/null 2>&1 || die "找不到 Python：$PYTHON_BIN"

RUN_BASE="$ROOT/data/perf_diag_runs"
RUN_ID="$(date +%Y%m%d-%H%M%S)-$$"
RUN_DIR="$RUN_BASE/$RUN_ID"
mkdir -p "$RUN_BASE" || die "无法创建 $RUN_BASE"
mkdir "$RUN_DIR" || die "运行目录已存在：$RUN_DIR"
REPORT_LABEL="$(printf '%s' "$EVENTS_SPEC" | tr '[:upper:]' '[:lower:]')"
REPORT_LABEL="${REPORT_LABEL//[^a-zA-Z0-9_.-]/_}"
if [[ -z "$OUTPUT" ]]; then
  OUTPUT="$ROOT/data/perf_diag_${REPORT_LABEL}_${RUN_ID}.txt"
elif [[ "$OUTPUT" != /* ]]; then
  OUTPUT="$ROOT/$OUTPUT"
fi
mkdir -p "$(dirname -- "$OUTPUT")" || die "无法创建报告目录：$(dirname -- "$OUTPUT")"

DAEMON_PID=""
SAMPLER_PID=""
EXIT_STATUS=0

stop_pid() {
  local pid="${1:-}" timeout_secs="${2:-10}" i ticks
  [[ -n "$pid" ]] || return 0
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    ticks=$((10#$timeout_secs * 10))
    (( ticks < 1 )) && ticks=1
    for i in $(seq 1 "$ticks"); do
      kill -0 "$pid" 2>/dev/null || break
      sleep 0.1
    done
    if kill -0 "$pid" 2>/dev/null; then
      echo "⚠ 进程 $pid 在 ${timeout_secs}s 内未退出，执行 SIGKILL" >&2
      kill -9 "$pid" 2>/dev/null || true
    fi
  fi
  wait "$pid" 2>/dev/null || true
}

cleanup() {
  local rc
  rc=$?
  if (( EXIT_STATUS != 0 )); then rc="$EXIT_STATUS"; fi
  set +e
  stop_pid "$SAMPLER_PID" 5
  stop_pid "$DAEMON_PID" "$SHUTDOWN_SECS"
  SAMPLER_PID=""
  DAEMON_PID=""
  if (( CLEANUP == 1 )) && [[ -n "$RUN_DIR" && -d "$RUN_DIR" && "$RUN_DIR" == "$RUN_BASE"/* ]]; then
    rm -rf -- "$RUN_DIR"
  fi
  trap - EXIT
  if (( rc != 0 )); then
    if (( CLEANUP == 1 )); then
      echo "诊断未完全成功（退出码 ${rc}）；本次运行目录已按 --cleanup 删除" >&2
    else
      echo "诊断未完全成功（退出码 ${rc}）；原始证据保留在 $RUN_DIR" >&2
    fi
  fi
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

port_available() {
  "$PYTHON_BIN" - "$1" "$2" <<'PY'
import socket
import sys

host, port = sys.argv[1], int(sys.argv[2])
try:
    with socket.socket(socket.AF_INET6 if ":" in host else socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
except OSError:
    raise SystemExit(1)
PY
}

copy_project() {
  local item
  for item in conf connectors models topology; do
    [[ -d "$ROOT/$item" ]] || die "工程目录缺失：$ROOT/$item"
    cp -R "$ROOT/$item" "$RUN_DIR/" || die "复制 $item 失败"
  done
  mkdir -p "$RUN_DIR/data/out_dat" "$RUN_DIR/logs" || die "无法创建运行数据目录"
}

rewrite_source_addr() {
  local source="$RUN_DIR/topology/sources/auth_tcp.toml"
  local tmp="$source.tmp"
  [[ -f "$source" ]] || die "找不到 TCP 源配置：$source"
  awk -v host="$LISTEN_HOST" -v port="$PORT" '
    /^[[:space:]]*addr[[:space:]]*=/ { printf "addr = \"%s\"\n", host; next }
    /^[[:space:]]*port[[:space:]]*=/ { printf "port = %s\n", port; next }
    /^[[:space:]]*framing[[:space:]]*=/ { print "framing = \"len\""; next }
    /^[[:space:]]*data_format[[:space:]]*=/ { print "data_format = \"arrow_framed\""; next }
    /^[[:space:]]*stream_tag[[:space:]]*=/ { print "stream_tag = \"\""; next }
    { print }
  ' "$source" > "$tmp" || die "改写 TCP 源配置失败"
  mv "$tmp" "$source" || die "替换 TCP 源配置失败"
}

rewrite_business_sink() {
  [[ "$SINK_MODE" == blackhole ]] || return 0
  local sink="$RUN_DIR/topology/sinks/business.d/sdm_alert.toml"
  local tmp="$sink.tmp"
  [[ -f "$sink" ]] || die "找不到业务 sink 配置：$sink"
  # 保留 sink_group 的窗口、并行度和 wf_meta_disable，只替换第一个业务 sink。
  awk '
    /^\[\[sink_group\.sinks\]\]/ { exit }
    { print }
  ' "$sink" > "$tmp" || die "生成 BlackHole sink 失败"
  cat >> "$tmp" <<'EOF'
[[sink_group.sinks]]
connect = "blackhole_sink"
name = "sdm_alert_blackhole"
EOF
  mv "$tmp" "$sink" || die "替换 BlackHole sink 失败"
}

rewrite_metrics_and_shards() {
  local conf="$RUN_DIR/conf/wfusion.toml" overlay="$RUN_DIR/conf/wfusion-perf-overlay.toml" tmp
  tmp="$conf.tmp"
  # metrics 是间隔 delta；100ms 粒度让尾部计数不会被 1s 报告周期截断。
  awk '
    /^report_interval[[:space:]]*=/ { print "report_interval = \"100ms\""; next }
    { print }
  ' "$conf" > "$tmp" || die "调整 metrics 粒度失败"
  mv "$tmp" "$conf" || die "替换 metrics 配置失败"
  if [[ -n "$RULE_SHARDS" ]]; then
    tmp="$overlay.tmp"
    awk -v shards="$RULE_SHARDS" '
      /^rule_shards[[:space:]]*=/ { print "rule_shards = " shards; next }
      { print }
    ' "$overlay" > "$tmp" || die "调整 rule_shards 失败"
    mv "$tmp" "$overlay" || die "替换 rule_shards 配置失败"
  fi
}

write_diag_config() {
  local config="$RUN_DIR/conf/perf-diag-wall.toml" stage
  {
    echo "# diag.sh 生成；daemon 和 wfgen 共读此文件。"
    echo "# 诊断配置仅存在于本次运行副本，不修改 wfusion_new 原始工程。"
    if (( WARMUP == 1 )); then
      cat <<'EOF'

[[stages]]
name = "warmup"
cut_rules = false
cut_output = false
cut_append = false
cut_recv = false
cut_sink_write = false
rules = ""
EOF
    fi
    for stage in "${STAGE_LIST[@]}"; do
      case "$stage" in
        recv)   CR=false; CO=false; CA=false; CRV=true;  CSW=false ;;
        decode) CR=false; CO=false; CA=true;  CRV=false; CSW=false ;;
        floor)  CR=true;  CO=true;  CA=false; CRV=false; CSW=false ;;
        rules)  CR=false; CO=true;  CA=false; CRV=false; CSW=false ;;
        emit)   CR=false; CO=false; CA=false; CRV=false; CSW=true  ;;
        full)   CR=false; CO=false; CA=false; CRV=false; CSW=false ;;
      esac
      cat <<EOF

[[stages]]
name = "$stage"
cut_rules = $CR
cut_output = $CO
cut_append = $CA
cut_recv = $CRV
cut_sink_write = $CSW
rules = ""
EOF
    done
  } > "$config" || die "生成 perf-diag 配置失败"
}

start_daemon() {
  local log="$RUN_DIR/logs/daemon.log" i
  : > "$log"
  (
    cd "$RUN_DIR" || exit 1
    if [[ -n "$MAX_TOTAL_BYTES" ]]; then
      export WF_DIAG_MAX_TOTAL_BYTES="$MAX_TOTAL_BYTES"
    else
      unset WF_DIAG_MAX_TOTAL_BYTES
    fi
    exec "$WFUSION_BIN" daemon \
      --config conf/wfusion.toml \
      --overlay conf/wfusion-perf-overlay.toml \
      --work-dir . \
      --perf-diag conf/perf-diag-wall.toml
  ) > "$log" 2>&1 &
  DAEMON_PID=$!
  for i in $(seq 1 100); do
    if ! kill -0 "$DAEMON_PID" 2>/dev/null; then
      echo "错误：daemon 启动失败；日志尾部：" >&2
      tail -40 "$log" >&2 || true
      return 1
    fi
    if grep -q "TCP listen" "$log" && grep -Eq "TCP listen .*:${PORT}( |$)" "$log"; then
      return 0
    fi
    sleep 0.2
  done
  echo "错误：daemon 启动超时，端口 $CONNECT_ADDR 未就绪；日志尾部：" >&2
  tail -40 "$log" >&2 || true
  return 1
}

start_sampler() {
  local sample_file="$RUN_DIR/data/samples.tsv"
  local interval
  interval="$(awk -v ms="$SAMPLE_MS" 'BEGIN { printf "%.3f", ms / 1000 }')"
  printf '# epoch_ns rss_mb cpu_pct\n' > "$sample_file"
  (
    while kill -0 "$DAEMON_PID" 2>/dev/null; do
      local epoch rss_kb cpu_pct
      epoch="$(date +%s%N)"
      rss_kb="$(ps -p "$DAEMON_PID" -o rss= 2>/dev/null | tr -d ' ' || true)"
      cpu_pct="$(ps -p "$DAEMON_PID" -o %cpu= 2>/dev/null | tr -d ' ' || true)"
      if [[ "$epoch" =~ ^[0-9]+$ && "$rss_kb" =~ ^[0-9]+([.][0-9]+)?$ && "$cpu_pct" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
        awk -v epoch="$epoch" -v rss="$rss_kb" -v cpu="$cpu_pct" 'BEGIN { printf "%s %.3f %.3f\n", epoch, rss / 1024, cpu }' >> "$sample_file"
      fi
      sleep "$interval"
    done
  ) &
  SAMPLER_PID=$!
}

stop_sampler() {
  stop_pid "$SAMPLER_PID" 5
  SAMPLER_PID=""
}

echo "== wfusion_new perf-diag =="
echo "events=$N (input=$EVENTS_SPEC) stages=$STAGES_CSV warmup=$WARMUP sink=$SINK_MODE addr=$CONNECT_ADDR timeout=${TIMEOUT_SECS}s shutdown=${SHUTDOWN_SECS}s"
echo "wfusion=$WFUSION_BIN"
echo "wfgen=$WFGEN_BIN"
echo "run=$RUN_DIR"

if ! port_available "$LISTEN_HOST" "$PORT"; then
  die "端口 $CONNECT_ADDR 已被其他进程占用；请换 --addr 或先停止占用者"
fi

copy_project
rewrite_source_addr
rewrite_business_sink
rewrite_metrics_and_shards
write_diag_config

EVENT_FILE="$RUN_DIR/data/events.jsonl"
FRAME_FILE="$RUN_DIR/data/events.frames"
echo "== 1. 生成 $N 条 sdm_event =="
"$PYTHON_BIN" "$ROOT/scripts/wfusion_perf_diag_events.py" \
  --count "$N" --prefix "wfdiag_${RUN_ID}" --output "$EVENT_FILE" \
  > "$RUN_DIR/logs/generate.log" 2>&1 || {
    tail -40 "$RUN_DIR/logs/generate.log" >&2 || true
    EXIT_STATUS=1
    exit 1
  }

# daemon 启动时会立即把 stage{current=0} 写入 sentinel 文件；必须在启动前
# 清空，启动后不能再 truncate，否则 wfgen 首个切档会永远等不到 current=0。
SENTINELS="$RUN_DIR/data/perf_sentinel.ndjson"
WALL="$RUN_DIR/data/perf_diag_wall.txt"
METRICS="$RUN_DIR/data/out_dat/metrics.ndjson"
: > "$SENTINELS"
: > "$WALL"

echo "== 2. 启动诊断 daemon =="
start_daemon || { EXIT_STATUS=1; exit 1; }
start_sampler

echo "== 3. 预编码 Arrow frames =="
"$WFGEN_BIN" dump-frames \
  --scenario "$RUN_DIR/models/scenarios/sdm_event_perf.wfg" \
  --input "$EVENT_FILE" \
  --addr "$CONNECT_ADDR" \
  --output "$FRAME_FILE" \
  --chunk "$FRAME_ROWS" \
  --max-frame-bytes "$FRAME_BYTES" \
  --max-frame-rows "$FRAME_ROWS" \
  > "$RUN_DIR/logs/dump-frames.log" 2>&1 || {
    echo "错误：dump-frames 失败；日志尾部：" >&2
    tail -60 "$RUN_DIR/logs/dump-frames.log" >&2 || true
    EXIT_STATUS=1
    exit 1
  }
[[ -s "$FRAME_FILE" ]] || { echo "错误：帧文件为空：$FRAME_FILE" >&2; EXIT_STATUS=1; exit 1; }
echo "frames=$(du -h "$FRAME_FILE" | awk '{print $1}') path=$FRAME_FILE"

echo "== 4. 运行墙梯（每档 N=${N}，timeout=${TIMEOUT_SECS}s）=="
set +e
"$WFGEN_BIN" perf-diag \
  --diag "$RUN_DIR/conf/perf-diag-wall.toml" \
  --frames "$FRAME_FILE" \
  --addr "$CONNECT_ADDR" \
  --n-list "$N" \
  --rounds 1 \
  --timeout-secs "$TIMEOUT_SECS" \
  --sentinels "$SENTINELS" \
  --output "$WALL" \
  2>&1 | tee "$RUN_DIR/logs/perf-diag.log"
PERF_RC="${PIPESTATUS[0]}"
set -e
if (( PERF_RC != 0 )); then
  echo "⚠ wfgen perf-diag 退出码 ${PERF_RC}；按已落盘 sentinel 继续分析。" >&2
fi

# 让最后一个 metrics interval 落盘，再停止采样和 daemon；只针对本次副本
# 的 PID，不影响工程外的 wfusion 进程。
sleep 2
stop_sampler
# 在停止 daemon 前冻结 metrics；某些旧 connector 在 shutdown 时会把连接
# channel 关闭记成 decode error，若读取 shutdown 后的最后一拍会污染健康度。
METRICS_SNAPSHOT="$RUN_DIR/data/metrics_snapshot.ndjson"
cp "$METRICS" "$METRICS_SNAPSHOT" 2>/dev/null || : > "$METRICS_SNAPSHOT"
stop_pid "$DAEMON_PID" "$SHUTDOWN_SECS"
DAEMON_PID=""

cores="$(sysctl -n hw.ncpu 2>/dev/null || true)"
[[ "$cores" =~ ^[0-9]+$ ]] || cores="$(getconf _NPROCESSORS_ONLN 2>/dev/null || true)"
[[ "$cores" =~ ^[0-9]+$ ]] || cores=0
load="$(sysctl -n vm.loadavg 2>/dev/null | awk '{print $2}' || true)"
[[ -n "$load" ]] || load="n/a"
stage_names=""
if (( WARMUP == 1 )); then stage_names="warmup"; fi
for stage in "${STAGE_LIST[@]}"; do
  if [[ -n "$stage_names" ]]; then
    stage_names+=",$stage"
  else
    stage_names="$stage"
  fi
done
append_cut="recv,decode"
context="run=$RUN_ID addr=$CONNECT_ADDR frame_rows=$FRAME_ROWS frame_bytes=$FRAME_BYTES sample_ms=$SAMPLE_MS cores=$cores rule_shards=${RULE_SHARDS:-config} max_total_bytes=${MAX_TOTAL_BYTES:-daemon-default} load=$load · $(date +%m-%d_%H:%M:%S)"

echo "== 5. 分析墙表和健康度 =="
set +e
"$PYTHON_BIN" "$ROOT/scripts/wfusion_perf_diag_analyze.py" \
  --sentinels "$SENTINELS" \
  --samples "$RUN_DIR/data/samples.tsv" \
  --metrics "$METRICS_SNAPSHOT" \
  --log "$RUN_DIR/logs/daemon.log" \
  --events "$N" \
  --stages "$stage_names" \
  --append-cut-stages "$append_cut" \
  --cores "$cores" \
  --sink "$SINK_MODE" \
  --context "$context" \
  | tee "$OUTPUT"
ANALYZE_RC="${PIPESTATUS[0]}"
set -e

RAW_WALL="$ROOT/data/perf_diag_wall_${REPORT_LABEL}_${RUN_ID}.txt"
cp "$WALL" "$RAW_WALL" 2>/dev/null || true
echo ""
echo "报告：$OUTPUT"
echo "原始墙表：$RAW_WALL"
if (( CLEANUP == 1 )); then
  echo "原始证据目录：${RUN_DIR}（--cleanup 后已删除）"
else
  echo "原始证据目录：$RUN_DIR"
fi

if (( PERF_RC != 0 )); then EXIT_STATUS="$PERF_RC"; fi
if (( ANALYZE_RC != 0 )); then EXIT_STATUS="$ANALYZE_RC"; fi
exit "$EXIT_STATUS"
