#!/usr/bin/env bash
# 直接测量业务输出文件中第一条到最后一条记录可读的时间。
# 不调用 first_last_eps.sh，也不读取 perf_sentinel.ndjson。
set -u -o pipefail

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
ROOT="$SCRIPT_DIR"
PYTHON_BIN="${PYTHON_BIN:-python3}"
EXPECTED=3000000
OUTPUT="$ROOT/data/out_dat/sdm_alert.json"
RUN_TAG=$(date +%Y%m%d-%H%M%S)-$$
LOG="$ROOT/data/time_file_3m_${RUN_TAG}.log"
TIMING="$ROOT/data/time_file_3m_${RUN_TAG}.tsv"
REPORT="$ROOT/data/time_file_3m_${RUN_TAG}.txt"
MONITOR_PID=""

mkdir -p "$ROOT/data/out_dat" || {
  echo "错误: 无法创建输出目录" >&2
  exit 1
}

cleanup() {
  if [[ -n "$MONITOR_PID" ]] && kill -0 "$MONITOR_PID" 2>/dev/null; then
    kill "$MONITOR_PID" 2>/dev/null || true
    wait "$MONITOR_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

# bench.sh 会在 trial 开始时删除该文件；这里提前删除，避免监控到上一轮残留。
rm -f "$OUTPUT" "$TIMING"

# 监控文件追加内容。时间戳取监控进程读到首个/最后一个换行符的时刻，
# 因此统计的是文件层面的可见时间，而不是 sentinel 时间。
"$PYTHON_BIN" - "$OUTPUT" "$EXPECTED" "$TIMING" <<'PY' &
import os
import sys
import time
from decimal import Decimal

path = sys.argv[1]
expected = int(sys.argv[2])
timing_path = sys.argv[3]
deadline = time.monotonic() + float(os.environ.get("FILE_MONITOR_TIMEOUT", "600"))

while True:
    try:
        stream = open(path, "rb", buffering=0)
        break
    except FileNotFoundError:
        if time.monotonic() >= deadline:
            with open(timing_path, "w", encoding="utf-8") as out:
                out.write("error\toutput file did not appear\n")
            raise SystemExit(1)
        time.sleep(0.002)

count = 0
first_ns = None
last_ns = None
while count < expected and time.monotonic() < deadline:
    chunk = stream.read(4 * 1024 * 1024)
    if chunk:
        lines = chunk.count(b"\n")
        if lines:
            observed_ns = time.time_ns()
            if first_ns is None:
                first_ns = observed_ns
            count += lines
            if count >= expected:
                last_ns = observed_ns
        continue
    time.sleep(0.002)

stream.close()
if count < expected or first_ns is None or last_ns is None:
    with open(timing_path, "w", encoding="utf-8") as out:
        out.write(f"error\toutput lines={count}, expected={expected}\n")
    raise SystemExit(1)

elapsed_ns = last_ns - first_ns
elapsed_seconds = Decimal(elapsed_ns) / Decimal(1_000_000_000)
file_eps = Decimal(count) * Decimal(1_000_000_000) / Decimal(elapsed_ns)
with open(timing_path, "w", encoding="utf-8") as out:
    print(
        count,
        first_ns,
        last_ns,
        elapsed_ns,
        f"{elapsed_seconds:.6f}",
        f"{file_eps:.2f}",
        sep="\t",
        file=out,
    )
PY
MONITOR_PID=$!

printf '== 文件计时: bench.sh mix replay 3m --mode file ==\n'
set +e
"$ROOT/bench.sh" mix replay 3m --mode file 2>&1 | tee "$LOG"
BENCH_RC=${PIPESTATUS[0]}

if (( BENCH_RC == 0 )); then
  wait "$MONITOR_PID"
  MONITOR_RC=$?
else
  kill "$MONITOR_PID" 2>/dev/null || true
  wait "$MONITOR_PID" 2>/dev/null || true
  MONITOR_RC=1
fi
MONITOR_PID=""

if (( BENCH_RC != 0 || MONITOR_RC != 0 )); then
  echo "错误: bench_exit=$BENCH_RC file_monitor_exit=$MONITOR_RC" >&2
  echo "日志: $LOG" >&2
  [[ -s "$TIMING" ]] && { echo "计时文件: $TIMING" >&2; cat "$TIMING" >&2; }
  exit "${BENCH_RC:-1}"
fi

IFS=$'\t' read -r FILE_LINES FIRST_NS LAST_NS ELAPSED_NS ELAPSED_SECONDS FILE_EPS < "$TIMING"
if [[ -z "$FILE_LINES" || -z "$ELAPSED_SECONDS" || -z "$FILE_EPS" ]]; then
  echo "错误: 计时文件格式无效: $TIMING" >&2
  exit 1
fi

{
  echo "== 文件第一条到最后一条 =="
  echo "file=$OUTPUT"
  echo "lines=$FILE_LINES"
  echo "first_file_line_ns=$FIRST_NS"
  echo "last_file_line_ns=$LAST_NS"
  echo "first_to_last_seconds=$ELAPSED_SECONDS"
  echo "file_eps=$FILE_EPS"
  echo "bench_exit=$BENCH_RC"
  echo "bench_log=$LOG"
  echo "timing=$TIMING"
} | tee "$REPORT"

echo "报告: $REPORT"
