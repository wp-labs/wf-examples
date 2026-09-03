#!/usr/bin/env bash
# nexmark_pk diag.sh — 性能墙定位（perf-diag 诊断模式驱动）
#
# 回答的问题：**这个查询的吞吐墙在管线哪一段**（注入/解码/窗口 vs 规则求值 vs 输出链），
# 而不是「吞吐是多少」（那是 bench.sh 的事）。
#
#   bench.sh  = 基准：单档全量跑，出 EPS/RSS/正确性数字（对 Flink PK）
#   diag.sh   = 诊断：墙梯逐段切除，出每段增量成本 + 墙判定（定位退化）
#                （decode→floor→rules→full 四档；decode 为 2026-08-25 新增前序档）
#
# 内存诊断模式：MEMORY=1 ./diag.sh q13 30m
#   - 默认不预热（WARMUP=0）：从空窗口开始测每档增量，避免预热档抬高基线
#   - 分析器换 diag_mem_analyze.py：报告重心从吞吐墙切到**内存墙**——每档
#     RSS 峰值增量定位内存增长段 + 成分分账（窗口 Σ/每窗明细/alloc commit/
#     parse 在途/fanout 排队），回答「内存涨在哪一段、由什么构成」。
#     输出 data/diag_mem_<q>_<total>.txt
#
# 机制：wfusion daemon --perf-diag conf/perf-diag-wall.toml（decode→floor→rules→full
#       墙梯, 哨兵驱动自切换、单 daemon 不重启）+ wfgen perf-diag 驱动。decode =
#       注入+解码（cut_append 窗口 append 前即丢）。设计见
# wp-reactor/docs/design/perf-diag-mode-design.md，用法见 docs/user-guide/perf-diag.md，
# 方法论见 wp-reactor/docs/PERF_BISECTION_METHOD.md。
#
# 度量逻辑（哨兵解析/EPS/CPU-RSS 采样/健康计数）在共享文件
# ../scripts/bench_lib.py 与 ../scripts/diag_analyze.py（两个性能 case 共用）——
# 本脚本只做流程编排，不内嵌代码。
#
# 用法:
#   ./diag.sh [query=q1|q1,q5,q9|all] [total=1m|10m|30m]
#
# 环境变量:
#   MEMORY=1           内存诊断模式：不预热（WARMUP=0）+ diag_mem_analyze.py
#                      内存墙分析器（见脚本头注释）；输出 diag_mem_<q>_<total>.txt
#   N_LIST=1m,10m       每档数据量（默认 = total）。多值 = **每值重启 daemon 跑一整套墙梯**
#                       （单次 wfgen 调用里放多个 N 会让第 2+ 个 N 吃到下一档门控，见 §坑）
#   STAGES=decode,floor,rules,full   自定义墙梯（默认用 conf/perf-diag-wall.toml）
#   WARMUP=0            关预热（默认开）。预热 = 墙梯前插一个丢弃的 warmup 档（全链路），
#                       消除首档独自承担的窗口冷分配/page fault 偏差（2026-08-24 q1 10m
#                       实测：不预热时 floor(21.2M) 反而慢于 rules(26.6M) 25%，偏差大于信号；
#                       预热后 floor 升到 31.8M、墙梯恢复单调）。关掉只为省一档时间/内存。
#   PARSE_PARALLELISM= / RULE_PARALLELISM=   并行度（默认取 conf/wfusion.toml）
#   FRAMES=path         直接指定帧文件（默认 data/bench_<total>_<DATA_VER>.frames，与 bench.sh 共享）
#   DATA_VER=v5 / MAX_FRAME_BYTES=8388608 / MAX_FRAME_ROWS=100000
#   GEN_FRAMES=1        帧缺失时自动生成（gen-nexmark + dump-frames，30m 需数分钟/数 GB）
#   EVENT_US=100        数据的事件时间步进（µs/事件，v5=100）——用于算跨度与迟到风险
#   LATENESS_FIX=1      跨度 > allowed_lateness 时自动放宽（默认开，见 §坑·迟到丢弃）
#   WF_DIAG_MAX_TOTAL_BYTES=0|8GB|60%  诊断模式全局窗口内存 cap（引擎侧）：默认 =
#                       物理内存 60%（墙梯重发同一份数据 N 次会放大窗口内存压力，
#                       cap 过小 → commit_append 停车 → 内存墙错报成计算墙，q20 已证伪）；
#                       0 = 沿用配置（测内存约束）；显式字节/百分比可调（引擎启动日志
#                       `perf-diag 内存口径` 打印实际值，报告口径行自动带上）
#   SAMPLE_MS=100       CPU%/RSS 采样周期（档时长短时决定 CPU 归属可信度）
#   TIMEOUT_SECS=       单次等待超时（默认 N/50000+120）
#   FORCE=1             跳过「跨度超窗口 over」的安全拦截
#
# 输出:
#   data/diag_<q>_<total>.txt      诊断报告（墙表 + 增量成本 + 墙判定 + 健康）
#   data/perf_sentinel.ndjson      哨兵四元组（EPS 单一事实源，每档一条）
#   data/perf_diag_wall.txt        wfgen 原始墙表（交叉校验用）
#   data/diag_daemon_<q>.log       引擎日志
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

QUERY="${1:-q1}"
TOTAL="${2:-10m}"

PY="${PYTHON:-python3}"
LIB="../scripts/bench_lib.py"          # 度量工具（comma/parse-n/diag-sampler）
# 内存诊断模式（MEMORY=1）：不预热 + 内存墙分析器（diag_mem_analyze.py）
MEMORY="${MEMORY:-0}"
if [ "$MEMORY" = "1" ]; then
  ANALYZER="${ANALYZER:-../scripts/diag_mem_analyze.py}"
else
  ANALYZER="${ANALYZER:-../scripts/diag_analyze.py}"  # 墙表 + 墙判定 + 健康分析
fi
PORT=9800
DATA_VER="${DATA_VER:-v5}"
MAX_FRAME_BYTES="${MAX_FRAME_BYTES:-8388608}"
MAX_FRAME_ROWS="${MAX_FRAME_ROWS:-100000}"
EVENT_US="${EVENT_US:-100}"
LATENESS_FIX="${LATENESS_FIX:-1}"
SAMPLE_MS="${SAMPLE_MS:-100}"
GEN_FRAMES="${GEN_FRAMES:-0}"
FORCE="${FORCE:-0}"
PARSE="${PARSE_PARALLELISM:-}"
RULE="${RULE_PARALLELISM:-}"
WINDOWS_SRC="models/schemas/windows.toml"
CONF_TMP=/tmp/diag_conf.toml
SAMPLES=/tmp/diag_samples.txt
DIRTY_SAMPLES=/tmp/diag_dirty.txt
DAEMON_PID=""
SAMPLER_PID=""
DIRTY_PID=""

# ---- 二进制来源（与 bench.sh 同款：本地 release 优先，回退 PATH）----
REPO="${REPO:-}"
if [ -z "$REPO" ] && [ -d "../../../warp-fusion" ]; then REPO="$(cd ../../../warp-fusion && pwd)"; fi
WFUSION="${WFUSION:-}"; WFGEN="${WFGEN:-}"
if [ -z "$WFUSION" ] && [ -n "$REPO" ] && [ -x "$REPO/target/release/wfusion" ]; then WFUSION="$REPO/target/release/wfusion"; fi
if [ -z "$WFGEN" ] && [ -n "$REPO" ] && [ -x "$REPO/target/release/wfgen" ]; then WFGEN="$REPO/target/release/wfgen"; fi
if [ -z "$WFUSION" ]; then WFUSION="$(command -v wfusion 2>/dev/null || true)"; fi
if [ -z "$WFGEN" ]; then WFGEN="$(command -v wfgen 2>/dev/null || true)"; fi
if [ -z "$WFUSION" ] || [ -z "$WFGEN" ]; then
  echo "错误: 找不到 wfusion/wfgen（设置 REPO/WFUSION/WFGEN 或加入 PATH）" >&2; exit 1
fi

# ---- 二进制新鲜度自检（M2, 2026-08-26）----
# 防止用不含最新源码的陈旧二进制跑出误导性性能数字（历史教训：git 依赖时改
# wp-reactor 不编进 wfusion，多轮实验白跑）。检查两件事：
#   1. warp-fusion/Cargo.toml 用 path 依赖（git 依赖 → 本地 wp-reactor 改动不生效）
#   2. 二进制 mtime ≥ wp-reactor 最近修改的 .rs（find -newer，找到第一个即停）
# 失败 → 警告（默认不阻塞）; BIN_CHECK_STRICT=1 拒绝运行; SKIP_BIN_CHECK=1 跳过。
check_binary_freshness() {
  [ "${SKIP_BIN_CHECK:-0}" = "1" ] && return 0
  [ -n "$REPO" ] && [ -n "$WFUSION" ] || return 0
  local WP_REACTOR="$REPO/../wp-reactor"
  [ -d "$WP_REACTOR" ] || return 0
  local STALE=""
  if ! grep -qE '^wf-engine = \{ *path' "$REPO/Cargo.toml" 2>/dev/null; then
    STALE="warp-fusion/Cargo.toml 未用 path 依赖（git 依赖）→ 本地 wp-reactor 改动不会编进二进制"
  fi
  local NEWER
  NEWER=$(find "$WP_REACTOR/crates" -name '*.rs' -newer "$WFUSION" -print -quit 2>/dev/null)
  if [ -n "$NEWER" ]; then
    STALE="${STALE}${STALE:+; }二进制早于源码修改: ${NEWER#*crates/} → 需 (cd $REPO && cargo build --release -p wfusion -p wfgen)"
  fi
  if [ -n "$STALE" ]; then
    echo "⚠ 二进制新鲜度自检: ${STALE}" >&2
    if [ "${BIN_CHECK_STRICT:-0}" = "1" ]; then
      echo "  错误: BIN_CHECK_STRICT=1 → 拒绝运行（构建后再跑，或 SKIP_BIN_CHECK=1 强制）" >&2
      exit 1
    fi
    echo "  （BIN_CHECK_STRICT=1 可升级为拒绝; SKIP_BIN_CHECK=1 跳过本检查）" >&2
  fi
}
check_binary_freshness

# ---- 参数校验 ----
case "$TOTAL" in
  1m) TOTAL_N=1000000;; 3m) TOTAL_N=3000000;; 10m) TOTAL_N=10000000;;
  30m) TOTAL_N=30000000;; 100m) TOTAL_N=100000000;;
  *) echo "bad total '$TOTAL' (1m|3m|10m|30m|100m)" >&2; exit 1;;
esac
ALL_Q="q1 q2 q3 q4 q5 q6 q7 q8 q9 q10 q11 q12 q13 q14 q15 q16 q17 q18 q19 q20 q21 q22"
if [ "$QUERY" = "all" ]; then
  QUERIES="$ALL_Q"
else
  QUERIES="$(echo "$QUERY" | tr ',' ' ')"
  for q in $QUERIES; do
    [ -f "models/queries/$q.wfl" ] || { echo "bad query '$q'（models/queries/$q.wfl 不存在）" >&2; exit 1; }
  done
fi

comma() { "$PY" "$LIB" comma "$1" 2>/dev/null || echo "$1"; }
parse_n() { "$PY" "$LIB" parse-n "$1"; }

N_ITEMS=""
if [ -n "${N_LIST:-}" ]; then
  for item in $(echo "$N_LIST" | tr ',' ' '); do N_ITEMS="${N_ITEMS} $(parse_n "$item")"; done
else
  N_ITEMS="$TOTAL_N"
fi
N_MAX=0
for n in $N_ITEMS; do [ "$n" -gt "$N_MAX" ] && N_MAX="$n"; done
[ "$N_MAX" -le "$TOTAL_N" ] || { echo "错误: N_LIST 最大值 $N_MAX 超过 total=${TOTAL}（帧文件行数）" >&2; exit 1; }

# ---- 墙梯配置：默认 = conf/perf-diag-wall.toml 三档 + 预热档；STAGES/WARMUP 可调 ----
# WARMUP 默认 1：不预热时首档的冷启动偏差会大于弱段的真实成本（实测墙梯倒挂）。
# 内存模式默认 0：预热档会把窗口装满再测增量、抬高基线；EPS 冷启动偏差对内存分析无意义。
DIAG_TOML="${DIAG_TOML:-conf/perf-diag-wall.toml}"
if [ "$MEMORY" = "1" ] && [ -z "${WARMUP:-}" ]; then
  WARMUP=0
else
  WARMUP="${WARMUP:-1}"
fi
mkdir -p data
if [ -n "${STAGES:-}" ] || [ "$WARMUP" = "1" ]; then
  LADDER="${STAGES:-$(grep '^name = ' "$DIAG_TOML" | sed 's/name = "\(.*\)"/\1/' | tr '\n' ',' | sed 's/,$//')}"
  DIAG_TOML=data/perf-diag-wall.toml
  : > "$DIAG_TOML"
  echo "# diag.sh 生成（ladder=${LADDER} warmup=${WARMUP}）——勿手改，改 STAGES/WARMUP 环境变量" >> "$DIAG_TOML"
  echo "mem_sample = true  # diag.sh 生成默认开（footprint dirty 采样；关用 MEM_SAMPLE=0）" >> "$DIAG_TOML"
  # 预热档：全链路（不切）——把窗口缓冲/规则状态/输出链全部跑热，分析时丢弃本档。
  [ "$WARMUP" = "1" ] && printf '\n[[stages]]\nname = "warmup"\ncut_rules = false\ncut_output = false\nrules = ""\n' >> "$DIAG_TOML"
  for st in $(echo "$LADDER" | tr ',' ' '); do
    case "$st" in
      recv)   CR=false; CO=false; CA=false; CRV=true;  CSW=false;;
      decode) CR=false; CO=false; CA=true;  CRV=false; CSW=false;;
      floor)  CR=true;  CO=true;  CA=false; CRV=false; CSW=false;;
      rules)  CR=false; CO=true;  CA=false; CRV=false; CSW=false;;
      emit)   CR=false; CO=false; CA=false; CRV=false; CSW=true;;
      full)   CR=false; CO=false; CA=false; CRV=false; CSW=false;;
      *) echo "bad stage '$st'（recv|decode|floor|rules|emit|full）" >&2; exit 1;;
    esac
    printf '\n[[stages]]\nname = "%s"\ncut_rules = %s\ncut_output = %s\ncut_append = %s\ncut_recv = %s\ncut_sink_write = %s\nrules = ""\n' "$st" "$CR" "$CO" "$CA" "$CRV" "$CSW" >> "$DIAG_TOML"
  done
fi
[ -f "$DIAG_TOML" ] || { echo "错误: 墙梯配置 $DIAG_TOML 不存在" >&2; exit 1; }
# 内存采样开关（perf-diag-wall.toml `mem_sample`，缺失/true = 开，默认开）：
# footprint dirty 采样（真持有口径）→ 报告每档 DIRTY_peak。显式 false 或
# 环境变量 MEM_SAMPLE=0 关闭（省 footprint spawn ~50% 核，非内存验证用）。
MEM_SAMPLE="${MEM_SAMPLE:-}"
if [ -z "$MEM_SAMPLE" ]; then
  _MS=$(grep '^mem_sample' "$DIAG_TOML" | head -1 | sed 's/.*= *//' | tr -d '"' | tr '[:upper:]' '[:lower:]')
  case "$_MS" in
    true|1|yes|on) MEM_SAMPLE=1;;
    false|0|no|off) MEM_SAMPLE=0;;
    *) MEM_SAMPLE=1;;  # 缺失/未知 = 开
  esac
fi
[ "$MEM_SAMPLE" = "1" ] && DIRTY_SAMPLES_PATH="$DIRTY_SAMPLES" || DIRTY_SAMPLES_PATH=""
STAGE_NAMES=$(grep '^name = ' "$DIAG_TOML" | sed 's/name = "\(.*\)"/\1/' | tr '\n' ',' | sed 's/,$//')
# cut_append/cut_recv 档（decode/recv 前序档）: 普通流不 append → appended 期望扣掉。
APPEND_CUT_STAGES=$(awk -F'"' '/^name = /{n=$2} /cut_append = true/{print n} /cut_recv = true/{print n}' "$DIAG_TOML" | sort -u | tr '\n' ',' | sed 's/,$//')
STAGE_COUNT=$(echo "$STAGE_NAMES" | tr ',' '\n' | grep -c .)
[ "$STAGE_COUNT" -ge 2 ] || { echo "错误: $DIAG_TOML 至少需 2 档才有墙梯（当前 ${STAGE_COUNT}）" >&2; exit 1; }

# ---- 迟到风险拦截：墙梯把同一份数据发 STAGE_COUNT 次 ----
# 事件时间不随重发前进（水位停在 T_end），所以每次重发的迟到量 ≤ 数据跨度。
# 跨度 > allowed_lateness → 全部被 drop（late_policy=drop）→ rules/full 档实际无数据，
# EPS 虚高、墙梯完全失真。这是把 perf_diag_case 的机制搬到 nexmark 的**头号坑**。
SPAN_SEC=$(( N_MAX * EVENT_US / 1000000 ))
LATENESS_SEC=$("$PY" -c '
import re, sys
txt = open(sys.argv[1]).read()
m = re.search(r"^allowed_lateness\s*=\s*\"([^\"]+)\"", txt, re.M)
if not m: print(0); raise SystemExit
s = m.group(1).strip(); unit = s[-1]; val = float(s[:-1]) if unit.isalpha() else float(s)
print(int(val * {"s": 1, "m": 60, "h": 3600, "d": 86400}.get(unit, 1)))' "$WINDOWS_SRC")
OVER_SEC=3600   # nexmark.wfs 三输入窗口 over=1h
WINDOWS_EFF="$WINDOWS_SRC"
if [ "$SPAN_SEC" -gt "$OVER_SEC" ] && [ "$FORCE" != "1" ]; then
  echo "错误: N=$(comma "$N_MAX") 的事件时间跨度 ${SPAN_SEC}s 超过窗口 over=${OVER_SEC}s。" >&2
  echo "      墙梯需重发数据 ${STAGE_COUNT} 次，必然出现迟到丢弃/时间驱逐 → 口径不可信。" >&2
  echo "      建议 total<=30m 定位（增量墙归属与 100m 一致）；确要跑设 FORCE=1。" >&2
  exit 1
fi
if [ "$SPAN_SEC" -gt "$LATENESS_SEC" ] && [ "$LATENESS_FIX" = "1" ]; then
  NEED=$(( SPAN_SEC + 60 ))
  WINDOWS_EFF=data/diag_windows.toml
  sed "s|^allowed_lateness = .*|allowed_lateness = \"${NEED}s\"  # diag.sh 放宽：墙梯重发 ${STAGE_COUNT} 次|" \
    "$WINDOWS_SRC" > "$WINDOWS_EFF"
  echo "  ⚠ 数据跨度 ${SPAN_SEC}s > allowed_lateness ${LATENESS_SEC}s → 放宽到 ${NEED}s（${WINDOWS_EFF}）"
  echo "    仅影响迟到判定（不影响吞吐口径）；关闭用 LATENESS_FIX=0"
fi

CORES=$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 0)
if [ "$MEMORY" = "1" ]; then
  echo "== mem-diag: query=${QUERY} total=${TOTAL} n_list=$(echo "$N_ITEMS" | tr ' ' ',' | sed 's/^,//') stages=${STAGE_NAMES} cores=${CORES}（内存墙：不预热，输出 diag_mem_*.txt）=="
else
  echo "== diag: query=${QUERY} total=${TOTAL} n_list=$(echo "$N_ITEMS" | tr ' ' ',' | sed 's/^,//') stages=${STAGE_NAMES} cores=${CORES} =="
fi

# ---- daemon 生命周期（与 bench.sh 同款纪律）----
wait_port_free() {
  for i in $(seq 1 50); do nc -z 127.0.0.1 "$PORT" 2>/dev/null || return 0; sleep 0.2; done
  echo "    警告: 端口 $PORT 超时未释放" >&2
}
kill_daemon() {
  local P="$1"; [ -n "$P" ] || return 0
  kill "$P" 2>/dev/null
  # 宽限 60s：与 wp-reactor GROUP_JOIN_TIMEOUT 对齐——stats/deferred 规则的
  # shutdown flush 需数秒~数十秒，提前 SIGKILL 会截断最终 metrics 导出。
  for i in $(seq 1 300); do kill -0 "$P" 2>/dev/null || { sleep 1; return 0; }; sleep 0.2; done
  echo "    警告: daemon $P SIGTERM 后 60s 未退出，强制 SIGKILL" >&2
  kill -9 "$P" 2>/dev/null
}
cleanup() {
  [ -n "$SAMPLER_PID" ] && kill "$SAMPLER_PID" 2>/dev/null
  [ -n "$DIRTY_PID" ] && kill "$DIRTY_PID" 2>/dev/null
  pkill -9 -f "wfusion daemon" 2>/dev/null
  sleep 1; wait_port_free
}
cleanup; trap cleanup EXIT INT TERM

# 查询 conf：基于 conf/wfusion.toml 覆盖 rules/并行度/windows（与 bench.sh write_conf 同源）
write_conf() {
  local Q="$1"
  PARSE_EFF="${PARSE:-$(sed -n 's/^parse_parallelism = *//p' conf/wfusion.toml | head -1 | tr -d ' ')}"
  RULE_EFF="${RULE:-$(sed -n 's/^rule_shards = *//p' conf/wfusion.toml | head -1 | tr -d ' ')}"
  sed -e "s|^rules = .*|rules = \"models/queries/$Q.wfl\"|" \
      -e "s|^parse_parallelism = .*|parse_parallelism = ${PARSE_EFF}|" \
      -e "s|^rule_shards = .*|rule_shards = ${RULE_EFF}|" \
      -e "s|^windows = .*|windows = \"${WINDOWS_EFF}\"|" \
      conf/wfusion.toml > "$CONF_TMP"
}

# 起 daemon（设全局 DAEMON_PID）；诊断模式入口 = --perf-diag 墙梯配置
start_daemon() {
  local LOG="$1"
  "$WFUSION" daemon --config "$CONF_TMP" --work-dir . --perf-diag "$DIAG_TOML" > "$LOG" 2>&1 &
  DAEMON_PID=$!
  for i in $(seq 1 40); do
    if ! kill -0 "$DAEMON_PID" 2>/dev/null; then
      echo "    错误: daemon 启动失败（进程已退出）——$LOG 尾部：" >&2; tail -20 "$LOG" >&2; return 1
    fi
    nc -z 127.0.0.1 "$PORT" 2>/dev/null && break
    sleep 0.2
  done
  nc -z 127.0.0.1 "$PORT" 2>/dev/null || {
    echo "    错误: daemon 启动超时（8s 端口未监听）——$LOG 尾部：" >&2; tail -20 "$LOG" >&2; return 1; }
  # 诊断模式必须真的生效，否则哨兵帧会被当未知流丢弃 → wfgen 等到超时
  grep -q "perf-diag" "$LOG" || echo "    ⚠ 启动日志无 perf-diag 字样（确认二进制含诊断模式）" >&2
  # 窗口内存口径必须真的放大（否则墙梯被内存背压污染，q20 已证伪）。二进制旧 →
  # 无此日志行 → 提示重建（M2 纪律：改动必须验证生效）。
  grep -q "perf-diag 内存口径" "$LOG" || echo "    ⚠ 启动日志无窗口内存口径行（二进制旧 → 未应用 60% 内存放量，墙梯可能被内存背压污染；需重建 wfusion）" >&2
  return 0
}

# CPU%/RSS 采样（常驻进程，脚本在 bench_lib.py）：输出 "epoch_ns rss_mb cpu_pct"，
# epoch_ns 与哨兵 start_ns/emit_ns 同域 → 分析器按档区间切分归属 CPU%/RSS。
start_sampler() {
  "$PY" "$LIB" diag-sampler "$1" "$SAMPLES" "$SAMPLE_MS" > /dev/null 2>&1 &
  SAMPLER_PID=$!
}

# 内存采样（footprint dirty，真持有口径）：输出 "epoch_ns dirty_mb"。
# 由 perf-diag-wall.toml `mem_sample` 控制（默认开）；footprint spawn ~50% 核，
# 关用 MEM_SAMPLE=0 或配置 mem_sample = false。非 macOS 无输出（分析器 n/a）。
start_dirty_sampler() {
  [ "$MEM_SAMPLE" = "1" ] || return 0
  "$PY" "$LIB" footprint-sampler "$1" "$DIRTY_SAMPLES" 0.5 > /dev/null 2>&1 &
  DIRTY_PID=$!
}

# ---- 帧文件：与 bench.sh 共享缓存 data/bench_<total>_<ver>.frames ----
ensure_frames() {
  if [ -n "${FRAMES:-}" ]; then
    [ -s "$FRAMES" ] || { echo "错误: FRAMES=$FRAMES 不存在或为空" >&2; return 1; }
    return 0
  fi
  if [ "$MAX_FRAME_BYTES" = "8388608" ]; then
    FRAMES="data/bench_${TOTAL}_${DATA_VER}.frames"
  else
    FRAMES="data/bench_${TOTAL}_mb${MAX_FRAME_BYTES}_${DATA_VER}.frames"
  fi
  [ -s "$FRAMES" ] && return 0
  # 同 total 的其它数据版本缓存：**不自动使用**。旧版本帧的 Arrow schema 与当前
  # models/schemas/nexmark.wfs 不一致时会 append 失败（window actor "schema mismatch"），
  # 数据被静默丢弃、只剩哨兵被处理 → 墙表出现 50M EPS 级假象（2026-08-24 实测踩到）。
  # 只列出候选，由使用者显式 FRAMES= 指定并自负口径。
  local ALT; ALT=$(ls -t data/bench_${TOTAL}_v*.frames 2>/dev/null | head -1)
  if [ "$GEN_FRAMES" != "1" ]; then
    echo "错误: 缺帧文件 ${FRAMES}。用以下任一方式：" >&2
    echo "      GEN_FRAMES=1 $0 $QUERY $TOTAL      # 本脚本生成（gen-nexmark + dump-frames）" >&2
    case "$TOTAL" in
      1m|10m|30m|100m) echo "      ./bench.sh q1 replay $TOTAL        # 跑一次基准顺带生成（缓存共享）" >&2;;
    esac
    if [ -n "$ALT" ]; then
      echo "      FRAMES=$ALT $0 ..." >&2
      echo "        ⚠ 该文件是其它数据版本：schema 不匹配时数据会被整批丢弃（报告的 append 校验会报）" >&2
    fi
    return 1
  fi
  echo "==> 生成帧：gen-nexmark $(comma "$TOTAL_N") --check → dump-frames（frame $((MAX_FRAME_BYTES/1048576))MiB）"
  rm -f "$FRAMES"
  "$WFGEN" gen-nexmark "$TOTAL_N" --check > data/diag_gen.jsonl || {
    echo "    错误: gen-nexmark 失败" >&2; rm -f data/diag_gen.jsonl; return 1; }
  # dump-frames 需要一个在跑的 daemon 做 schema 握手（与 bench.sh 同款临时 daemon）
  write_conf q1
  start_daemon data/diag_daemon_frames.log || return 1
  "$WFGEN" dump-frames --scenario scenarios/nexmark.wfg --input data/diag_gen.jsonl \
    --ws models/schemas/nexmark.wfs --addr 127.0.0.1:$PORT --output "$FRAMES" --chunk 1000000 \
    --max-frame-bytes "$MAX_FRAME_BYTES" --max-frame-rows "$MAX_FRAME_ROWS" > /dev/null 2>&1
  kill_daemon "$DAEMON_PID"; DAEMON_PID=""; wait_port_free
  rm -f data/diag_gen.jsonl
  [ -s "$FRAMES" ] || { echo "    错误: dump-frames 产物为空（已删坏缓存）" >&2; rm -f "$FRAMES"; return 1; }
  echo "  frames: ${FRAMES}（$(du -h "$FRAMES" | cut -f1)）"
}

# ---- 单次墙梯 ----
run_ladder() {
  local Q="$1" N="$2" OUT="$3"
  local LOG="data/diag_daemon_${Q}.log"
  local T; T="${TIMEOUT_SECS:-$(( N / 50000 + 120 ))}"

  write_conf "$Q"
  # 哨兵文件必须在 daemon 启动**前**清空：daemon 启动即写 stage{current=0}，
  # 启动后再删会擦掉这行 → wfgen 首个 wait_for_stage 直接超时（设计文档 §7 坑）。
  rm -f data/perf_sentinel.ndjson data/perf_diag_wall.txt data/metrics.ndjson data/wfusion.log "$SAMPLES" "$DIRTY_SAMPLES"
  start_daemon "$LOG" || return 1
  start_sampler "$DAEMON_PID"
  start_dirty_sampler "$DAEMON_PID"

  echo "  -- $Q · N=$(comma "$N") · 档=${STAGE_NAMES} · timeout=${T}s --"
  "$WFGEN" perf-diag --diag "$DIAG_TOML" --frames "$FRAMES" --addr "127.0.0.1:$PORT" \
    --n-list "$N" --rounds 1 --timeout-secs "$T" \
    --sentinels data/perf_sentinel.ndjson --output data/perf_diag_wall.txt
  local RC=$?
  [ "$RC" = 0 ] || echo "    ⚠ wfgen perf-diag 退出码 ${RC}（报告按已落盘哨兵记录尽力分析）" >&2

  kill "$SAMPLER_PID" 2>/dev/null; wait "$SAMPLER_PID" 2>/dev/null; SAMPLER_PID=""
  [ -n "$DIRTY_PID" ] && { kill "$DIRTY_PID" 2>/dev/null; wait "$DIRTY_PID" 2>/dev/null; DIRTY_PID=""; }
  sleep 2   # 让 metrics 最后一拍导出（report_interval=100ms）
  kill_daemon "$DAEMON_PID"; DAEMON_PID=""; wait_port_free

  local LD; LD=$(sysctl -n vm.loadavg 2>/dev/null | awk '{printf "%.1f", $2}')
  # 引擎打印的窗口内存口径（诊断模式默认 60% 物理内存 / WF_DIAG_MAX_TOTAL_BYTES 可调）
  # 先剥 ANSI 色码再截到结构字段 ` window_mem_cap=` 前，兼容无括号的来源（如 =0 关闭覆盖）。
  local MEM_CAP; MEM_CAP=$(sed 's/\x1b\[[0-9;]*m//g' "$LOG" | grep -o 'perf-diag 内存口径: max_total_bytes=.* window_mem_cap=' | head -1 | sed -e 's/^perf-diag 内存口径: //' -e 's/ window_mem_cap=$//')
  local CTX; CTX="frame_mb=$((MAX_FRAME_BYTES/1048576)) span=${SPAN_SEC}s lateness=$([ "$WINDOWS_EFF" = "$WINDOWS_SRC" ] && echo "${LATENESS_SEC}s" || echo "$(( SPAN_SEC + 60 ))s*") load=${LD:-n/a}${MEM_CAP:+ · ${MEM_CAP}} · $(date +%m-%d_%H:%M:%S)"
  [ "$MEMORY" = "1" ] && CTX="mem-diagnose · ${CTX}"
  # 分析：独立脚本 diag_analyze.py（哨兵四元组 × CPU/RSS 采样 × metrics 健康），
  # 输入走环境变量；stdout = 报告，退出码 0=健康 / 1=硬失败。
  N="$N" CTX="$CTX" QUERY="$Q" RULES_COUNT="$(grep -c '^rule ' "models/queries/$Q.wfl")" \
  STAGE_NAMES="$STAGE_NAMES" APPEND_CUT_STAGES="$APPEND_CUT_STAGES" CORES="$CORES" \
  SENT_PATH="data/perf_sentinel.ndjson" SAMPLES_PATH="$SAMPLES" \
  DIRTY_SAMPLES_PATH="$DIRTY_SAMPLES_PATH" \
  METRICS_PATH="data/metrics.ndjson" LOG_PATH="$LOG" \
  STREAMS="auction_events,bid_events,person_events" FAM_COUNTS="" \
    "$PY" "$ANALYZER" | tee -a "$OUT"
  return "${PIPESTATUS[0]}"
}

# ---- 主流程 ----
ensure_frames || exit 1
FAILED=0
for Q in $QUERIES; do
  OUT="data/diag_${Q}_${TOTAL}.txt"
  [ "$MEMORY" = "1" ] && OUT="data/diag_mem_${Q}_${TOTAL}.txt"
  : > "$OUT"
  for N in $N_ITEMS; do
    run_ladder "$Q" "$N" "$OUT" || FAILED=1
  done
  echo "  → 报告: $OUT"
done

if [ "$FAILED" = 0 ]; then
  echo "== diag 完成 =="
else
  echo "== diag 完成（存在告警/失败项，见报告）==" >&2
  exit 1
fi
