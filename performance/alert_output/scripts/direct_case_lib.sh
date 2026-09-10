#!/usr/bin/env bash
# Shared plumbing for the three standalone performance entry points.
#
# The entry points in this directory are direct adaptations of the Nexmark
# bench/diag/verify scripts.  This file only contains project-specific
# mechanics shared by them: resolving binaries, making a throw-away runtime
# overlay, preparing current sdm_event frames, and reading counters.

set -u

CASE_ROOT="${CASE_ROOT:-$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)}"
CASE_PYTHON="${CASE_PYTHON:-${PYTHON:-python3}}"
CASE_LIB="${CASE_ROOT}/../scripts/bench_lib.py"
CASE_WFUSION="${CASE_WFUSION:-${WFUSION:-}}"
CASE_WFGEN="${CASE_WFGEN:-${WFGEN:-}}"
CASE_ADDR="${CASE_ADDR:-127.0.0.1:9800}"
# Bump the cache key when the canonical input shape changes.  The current
# sample is the 23-field NGSOC alert shape; older synthetic files must not be
# reused by replay/verify runs.
CASE_DATA_VER="${CASE_DATA_VER:-ngsoc23-v1}"
CASE_FRAME_BYTES="${CASE_FRAME_BYTES:-8388608}"
CASE_FRAME_ROWS="${CASE_FRAME_ROWS:-100000}"
CASE_RULE_SHARDS="${CASE_RULE_SHARDS:-4}"
CASE_SINK_PARALLEL="${CASE_SINK_PARALLEL:-2}"
CASE_TCP_INSTANCES="${CASE_TCP_INSTANCES:-4}"
CASE_RUNTIME_DIR=""
CASE_OVERLAY=""
CASE_WINDOWS=""
CASE_SINKS=""
CASE_METRICS="${CASE_ROOT}/data/metrics.ndjson"
CASE_ALERTS="${CASE_ROOT}/data/out_dat/sdm_alert.json"
CASE_DAEMON_PID=""

case_die() {
    printf '错误：%s\n' "$*" >&2
    return 2
}

case_parse_count() {
    local raw="${1:-}" lower number suffix multiplier
    lower=$(printf '%s' "$raw" | tr '[:upper:]' '[:lower:]')
    if [[ "$lower" =~ ^([0-9]+)([kmgw]?)$ ]]; then
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

case_parse_size_bytes() {
    local raw="${1:-}" value suffix multiplier
    raw=$(printf '%s' "$raw" | tr -d '"' | tr '[:lower:]' '[:upper:]')
    if [[ "$raw" =~ ^([0-9]+)(B|KB|MB|GB|TB)?$ ]]; then
        value="${BASH_REMATCH[1]}"
        suffix="${BASH_REMATCH[2]}"
    else
        return 1
    fi
    case "$suffix" in
        ""|B) multiplier=1 ;;
        KB) multiplier=1024 ;;
        MB) multiplier=$((1024 * 1024)) ;;
        GB) multiplier=$((1024 * 1024 * 1024)) ;;
        TB) multiplier=$((1024 * 1024 * 1024 * 1024)) ;;
        *) return 1 ;;
    esac
    printf '%s\n' "$((10#$value * multiplier))"
}

case_bytes_label() {
    local bytes=$1 gb=$((1024 * 1024 * 1024)) mb=$((1024 * 1024))
    if (( bytes % gb == 0 )); then
        printf '%sGB\n' "$((bytes / gb))"
    elif (( bytes % mb == 0 )); then
        printf '%sMB\n' "$((bytes / mb))"
    else
        printf '%sB\n' "$bytes"
    fi
}

case_resolve_binary() {
    local requested="$1" name="$2" candidate
    if [[ -n "$requested" ]]; then
        [[ -x "$requested" ]] || return 1
        (CDPATH= cd -- "$(dirname -- "$requested")" && printf '%s/%s\n' "$PWD" "$(basename -- "$requested")")
        return 0
    fi
    for candidate in \
        "$CASE_ROOT/../../warp-fusion/target/release/$name" \
        "$CASE_ROOT/../warp-fusion/target/release/$name" \
        "/Users/dy_xuyuhao/bin/$name"; do
        if [[ -x "$candidate" ]]; then
            (CDPATH= cd -- "$(dirname -- "$candidate")" && printf '%s/%s\n' "$PWD" "$(basename -- "$candidate")")
            return 0
        fi
    done
    command -v "$name" 2>/dev/null || true
}

case_parse_addr() {
    local value="$1"
    if [[ "$value" =~ ^\[([^]]+)\]:([0-9]+)$ ]]; then
        CASE_LISTEN_HOST="${BASH_REMATCH[1]}"
        CASE_PORT="${BASH_REMATCH[2]}"
    elif [[ "$value" =~ ^([^:]+):([0-9]+)$ ]]; then
        CASE_LISTEN_HOST="${BASH_REMATCH[1]}"
        CASE_PORT="${BASH_REMATCH[2]}"
    else
        return 1
    fi
    [[ "$CASE_PORT" =~ ^[0-9]+$ ]] || return 1
    (( CASE_PORT >= 1 && CASE_PORT <= 65535 )) || return 1
    case "$CASE_LISTEN_HOST" in
        ""|\*) CASE_LISTEN_HOST="0.0.0.0" ;;
    esac
    case "$CASE_LISTEN_HOST" in
        0.0.0.0) CASE_CONNECT_HOST="127.0.0.1" ;;
        ::) CASE_CONNECT_HOST="::1" ;;
        *) CASE_CONNECT_HOST="$CASE_LISTEN_HOST" ;;
    esac
    if [[ "$CASE_CONNECT_HOST" == *:* ]]; then
        CASE_CONNECT_ADDR="[$CASE_CONNECT_HOST]:$CASE_PORT"
    else
        CASE_CONNECT_ADDR="$CASE_CONNECT_HOST:$CASE_PORT"
    fi
}

case_read_toml_value() {
    local file="$1" section="$2" key="$3"
    awk -v target="$section" -v key="$key" '
        /^\[/ { section=$0 }
        section == target && $0 ~ "^[[:space:]]*" key "[[:space:]]*=" {
            value=$0; sub(/^[^=]*=[[:space:]]*/, "", value)
            gsub(/[[:space:]\"]/, "", value); print value; exit
        }
    ' "$file"
}

case_host_memory_bytes() {
    local value pages page_size
    if command -v sysctl >/dev/null 2>&1; then
        value=$(sysctl -n hw.memsize 2>/dev/null || true)
        [[ "$value" =~ ^[0-9]+$ ]] && { printf '%s\n' "$value"; return; }
    fi
    if [[ -r /proc/meminfo ]]; then
        value=$(awk '/^MemTotal:/ { print $2 * 1024; exit }' /proc/meminfo 2>/dev/null || true)
        [[ "$value" =~ ^[0-9]+$ ]] && { printf '%s\n' "$value"; return; }
    fi
    pages=$(getconf _PHYS_PAGES 2>/dev/null || true)
    page_size=$(getconf PAGE_SIZE 2>/dev/null || true)
    if [[ "$pages" =~ ^[0-9]+$ && "$page_size" =~ ^[0-9]+$ ]]; then
        printf '%s\n' "$((pages * page_size))"
    else
        printf '0\n'
    fi
}

case_set_window_value() {
    local file="$1" total="$2" event="$3" lateness="$4" tmp="${1}.tmp.$$"
    awk -v total="$total" -v event="$event" -v lateness="$lateness" '
        function is_event(s) { return s == "[window.sdm_event]" }
        function flush() {
            if (section == "[window_defaults]") {
                if (total != "" && !seen_total) print "max_total_bytes = \"" total "\""
                if (lateness != "" && !seen_lateness) print "allowed_lateness = \"" lateness "\""
            } else if (is_event(section) && event != "" && !seen_event) {
                print "max_window_bytes = \"" event "\""
            }
        }
        /^\[/ {
            if (section != "") flush()
            section=$0; seen_total=0; seen_event=0; seen_lateness=0; print; next
        }
        section == "[window_defaults]" && /^[[:space:]]*max_total_bytes[[:space:]]*=/ {
            if (total != "") print "max_total_bytes = \"" total "\""; else print; seen_total=1; next
        }
        section == "[window_defaults]" && /^[[:space:]]*allowed_lateness[[:space:]]*=/ {
            if (lateness != "") print "allowed_lateness = \"" lateness "\""; else print; seen_lateness=1; next
        }
        is_event(section) && /^[[:space:]]*max_window_bytes[[:space:]]*=/ {
            if (event != "") print "max_window_bytes = \"" event "\""; else print; seen_event=1; next
        }
        { print }
        END { if (section != "") flush() }
    ' "$file" > "$tmp" || return 1
    mv "$tmp" "$file"
}

case_set_sink_parallel() {
    local file="$1" value="$2" tmp="${1}.tmp.$$"
    [[ -n "$value" ]] || return 0
    awk -v value="$value" '
        /^\[sink_group\][[:space:]]*$/ { in_group=1; found=0; print; next }
        /^\[/ { if (in_group && !found) print "parallel = " value; in_group=0; print; next }
        in_group && /^[[:space:]]*parallel[[:space:]]*=/ { print "parallel = " value; found=1; next }
        { print }
        END { if (in_group && !found) print "parallel = " value }
    ' "$file" > "$tmp" || return 1
    mv "$tmp" "$file"
}

case_make_blackhole_sink() {
    local file="$1" tmp="${1}.tmp.$$"
    awk '/^\[\[sink_group\.sinks\]\]/{exit} {print}' "$file" > "$tmp" || return 1
    cat >> "$tmp" <<'EOF'

[[sink_group.sinks]]
connect = "blackhole_sink"
name = "sdm_alert_blackhole"
EOF
    mv "$tmp" "$file"
}

case_prepare_runtime() {
    local sink_mode="${1:-blackhole}" shards="${2:-$CASE_RULE_SHARDS}" sink_parallel="${3:-$CASE_SINK_PARALLEL}"
    local total_bytes="${4:-}" event_bytes="${5:-}" lateness="${6:-}" source_file monitor_file business_file
    local runtime_name=".wfusion-direct-${$}-${RANDOM}"
    CASE_RUNTIME_DIR="$CASE_ROOT/data/$runtime_name"
    CASE_OVERLAY="$CASE_RUNTIME_DIR/overlay.toml"
    CASE_WINDOWS="$CASE_RUNTIME_DIR/windows.toml"
    CASE_SINKS="$CASE_RUNTIME_DIR/sinks"
    mkdir -p "$CASE_RUNTIME_DIR/sources" "$CASE_SINKS" || return 1
    cp -R "$CASE_ROOT/topology/sources/." "$CASE_RUNTIME_DIR/sources/" || return 1
    cp -R "$CASE_ROOT/topology/sinks/." "$CASE_SINKS/" || return 1
    cp "$CASE_ROOT/models/windows-perf.toml" "$CASE_WINDOWS" || return 1

    source_file="$CASE_RUNTIME_DIR/sources/auth_tcp.toml"
    [[ -f "$source_file" ]] || { printf '缺少 TCP 输入配置：%s\n' "$source_file" >&2; return 1; }
    awk -v host="$CASE_LISTEN_HOST" -v port="$CASE_PORT" -v instances="$CASE_TCP_INSTANCES" '
        /^[[:space:]]*addr[[:space:]]*=/ { print "addr = \"" host "\""; found_addr=1; next }
        /^[[:space:]]*port[[:space:]]*=/ { print "port = " port; found_port=1; next }
        /^[[:space:]]*data_format[[:space:]]*=/ { print "data_format = \"arrow_framed\""; found_format=1; next }
        /^[[:space:]]*framing[[:space:]]*=/ { print "framing = \"len\""; found_framing=1; next }
        /^[[:space:]]*stream_tag[[:space:]]*=/ { print "stream_tag = \"\""; found_tag=1; next }
        /^[[:space:]]*instances[[:space:]]*=/ { print "instances = " instances; found_instances=1; next }
        { print }
        END {
            if (!found_addr) print "addr = \"" host "\""
            if (!found_port) print "port = " port
            if (!found_format) print "data_format = \"arrow_framed\""
            if (!found_framing) print "framing = \"len\""
            if (!found_tag) print "stream_tag = \"\""
            if (!found_instances) print "instances = " instances
        }
    ' "$source_file" > "${source_file}.tmp" || return 1
    mv "${source_file}.tmp" "$source_file"

    monitor_file="$CASE_SINKS/infra.d/monitor.toml"
    if [[ -f "$monitor_file" ]]; then
        sed -E 's|^[[:space:]]*base = "data/out_dat"$|base = "data"|' "$monitor_file" > "${monitor_file}.tmp" || return 1
        mv "${monitor_file}.tmp" "$monitor_file"
    fi
    business_file="$CASE_SINKS/business.d/sdm_alert.toml"
    [[ -f "$business_file" ]] || { printf '缺少业务 sink 配置：%s\n' "$business_file" >&2; return 1; }
    case_set_sink_parallel "$business_file" "$sink_parallel" || return 1
    if [[ "$sink_mode" == "blackhole" ]]; then
        case_make_blackhole_sink "$business_file" || return 1
    fi
    case_set_window_value "$CASE_WINDOWS" "$total_bytes" "$event_bytes" "$lateness" || return 1

    cat > "$CASE_OVERLAY" <<EOF
# generated by performance/wfusion_new direct entry points
sources_dir = "$CASE_RUNTIME_DIR/sources"
sinks = "$CASE_SINKS"
windows = "$CASE_WINDOWS"

[runtime]
rule_shards = $shards
EOF
    CASE_METRICS="$CASE_ROOT/data/metrics.ndjson"
    CASE_ALERTS="$CASE_ROOT/data/out_dat/sdm_alert.json"
    return 0
}

case_port_open() {
    if command -v nc >/dev/null 2>&1; then
        nc -z "$CASE_CONNECT_HOST" "$CASE_PORT" >/dev/null 2>&1
    else
        "$CASE_PYTHON" - "$CASE_CONNECT_HOST" "$CASE_PORT" <<'PY'
import socket, sys
host, port = sys.argv[1], int(sys.argv[2])
try:
    with socket.create_connection((host, port), timeout=0.3):
        pass
except OSError:
    raise SystemExit(1)
PY
    fi
}

case_wait_port_free() {
    local i
    for i in $(seq 1 100); do
        if ! case_port_open; then return 0; fi
        sleep 0.1
    done
    return 1
}

case_start_daemon() {
    local log="$1" diag="${2:-}" i
    : > "$log"
    case_wait_port_free || { printf '端口仍被占用：%s\n' "$CASE_CONNECT_ADDR" >&2; return 1; }
    (
        cd "$CASE_ROOT" || exit 1
        if [[ -n "${CASE_DIAG_MAX_TOTAL_BYTES:-}" ]]; then
            export WF_DIAG_MAX_TOTAL_BYTES="$CASE_DIAG_MAX_TOTAL_BYTES"
        else
            unset WF_DIAG_MAX_TOTAL_BYTES
        fi
        if [[ -n "$diag" ]]; then
            exec "$CASE_WFUSION" daemon --config conf/wfusion.toml --overlay "$CASE_OVERLAY" --work-dir . --perf-diag "$diag"
        else
            exec "$CASE_WFUSION" daemon --config conf/wfusion.toml --overlay "$CASE_OVERLAY" --work-dir .
        fi
    ) > "$log" 2>&1 &
    CASE_DAEMON_PID=$!
    for i in $(seq 1 100); do
        if ! kill -0 "$CASE_DAEMON_PID" 2>/dev/null; then
            printf 'daemon 启动失败，日志尾部：\n' >&2
            tail -40 "$log" >&2 2>/dev/null || true
            return 1
        fi
        case_port_open && return 0
        sleep 0.1
    done
    printf 'daemon 启动超时，日志尾部：\n' >&2
    tail -40 "$log" >&2 2>/dev/null || true
    return 1
}

case_stop_daemon() {
    local pid="${1:-$CASE_DAEMON_PID}" timeout="${2:-60}" i
    [[ -n "$pid" ]] || return 0
    if kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null || true
        for i in $(seq 1 $((timeout * 10))); do
            kill -0 "$pid" 2>/dev/null || break
            sleep 0.1
        done
        if kill -0 "$pid" 2>/dev/null; then
            printf 'daemon %s 未在 %ss 内退出，执行 SIGKILL\n' "$pid" "$timeout" >&2
            kill -9 "$pid" 2>/dev/null || true
        fi
    fi
    wait "$pid" 2>/dev/null || true
    [[ "$pid" == "$CASE_DAEMON_PID" ]] && CASE_DAEMON_PID=""
    case_wait_port_free || true
}

case_cleanup_runtime() {
    if [[ -n "$CASE_RUNTIME_DIR" && -d "$CASE_RUNTIME_DIR" && "${CASE_KEEP_RUNTIME:-0}" != "1" ]]; then
        rm -rf "$CASE_RUNTIME_DIR"
    fi
}

case_metric_appended() {
    "$CASE_PYTHON" "$CASE_LIB" appended "$CASE_METRICS" "sdm_event"
}

case_metric_acked_lag() {
    "$CASE_PYTHON" "$CASE_LIB" acked-lag "$CASE_METRICS" ""
}

case_metric_correctness() {
    "$CASE_PYTHON" "$CASE_LIB" correctness "$CASE_METRICS"
}

case_sentinel_tuple() {
    "$CASE_PYTHON" "$CASE_LIB" sentinel-tuple "$CASE_ROOT/data/perf_sentinel.ndjson"
}

case_count_alerts() {
    "$CASE_PYTHON" - "$CASE_ALERTS" <<'PY'
import json, sys
path = sys.argv[1]
n = 0
try:
    with open(path, errors="replace") as f:
        for line in f:
            try:
                json.loads(line)
            except Exception:
                continue
            n += 1
except FileNotFoundError:
    pass
print(n)
PY
}

case_prepare_events() {
    local total="$1" label="${2:-$total}" file="${CASE_ROOT}/data/bench_${label}_${CASE_DATA_VER}.jsonl"
    mkdir -p "$CASE_ROOT/data"
    if [[ ! -s "$file" ]]; then
        "$CASE_PYTHON" "$CASE_ROOT/scripts/wfusion_perf_diag_events.py" \
            --count "$total" --prefix "wfbench_${CASE_DATA_VER}_${label}" --output "$file" || return 1
    fi
    printf '%s\n' "$file"
}

case_prepare_frames() {
    local total="$1" label="${2:-$total}" frame_bytes="${3:-$CASE_FRAME_BYTES}" frame_rows="${4:-$CASE_FRAME_ROWS}"
    local events_file="${CASE_ROOT}/data/bench_${label}_${CASE_DATA_VER}.jsonl"
    local frames="${CASE_ROOT}/data/bench_${label}_${CASE_DATA_VER}.frames" log="$CASE_ROOT/data/direct_dump_${label}.log"
    [[ -s "$frames" ]] && { printf '%s\n' "$frames"; return 0; }
    [[ -s "$events_file" ]] || { events_file=$(case_prepare_events "$total" "$label") || return 1; }
    case_start_daemon "$CASE_ROOT/data/direct_frame_daemon_${label}.log" "${CASE_ROOT}/conf/perf-diag.toml" || return 1
    "$CASE_WFGEN" dump-frames \
        --scenario "$CASE_ROOT/models/scenarios/sdm_event_perf.wfg" \
        --input "$events_file" --addr "$CASE_CONNECT_ADDR" \
        --ws "$CASE_ROOT/models/schemas/sdm_event.wfs" --output "$frames" \
        --chunk "$frame_rows" --max-frame-bytes "$frame_bytes" --max-frame-rows "$frame_rows" \
        > "$log" 2>&1
    local rc=$?
    case_stop_daemon "$CASE_DAEMON_PID" "${CASE_SHUTDOWN_SECS:-60}"
    if (( rc != 0 )) || [[ ! -s "$frames" ]]; then
        printf 'dump-frames 失败，日志尾部：\n' >&2
        tail -60 "$log" >&2 2>/dev/null || true
        rm -f "$frames"
        return 1
    fi
    printf '%s\n' "$frames"
}
