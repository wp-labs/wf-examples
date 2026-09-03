#!/usr/bin/env bash
# verify_daemon.sh — daemon（TCP 注入 + SIGTERM flush）路径的正确性验证（全深度唯一脚本）
#
# 同一份预编码帧经 TCP 注入、metrics 追平（appended ≥ N 且 acked_lag == 0）、
# 常驻进程 SIGTERM flush 收口，输出落盘 data/alerts/benchmark.ndjson 后做
# **L1（metrics.emitted_total）+ L2（内容断言）+ L3（--detail-diff 值级对拍）三层验证**。
#
# 覆盖注入/收口形态：TCP 注入 + 常驻 + 关机 flush 尾批收口（bench.sh --verify 因
# blackhole sink 只对拍 L1 计数；本脚本改用 sinks_file 落盘 → L2/L3 同样深度）。
# 历史 verify_file.sh（batch 文件源路径）已移除（2026-08-30），本脚本为唯一全深度验证入口。
#
# 用法:
#   ./verify_daemon.sh [query=q1|..|q22|all] [total=1m|10m|30m|100m]
#     默认 all + 1m（1M 数据快验）。total 对应的预编码帧
#     data/bench_<total>_v5.frames 必须存在（bench.sh 会生成；1m 缓存已入库）。
#   env（与 bench.sh 同款）:
#     REPO / WFUSION / WFGEN   二进制来源（默认 ../../../warp-fusion/target/release）
#     PARSE_PARALLELISM / RULE_PARALLELISM   并行度（默认取 conf/wfusion.toml）
#     DATA_VER   帧缓存版本（默认 v5）
#     SKIP_BIN_CHECK=1 / BIN_CHECK_STRICT=1   二进制新鲜度自检开关
#
# 输出: 每查询一行摘要（stdout + data/verify_daemon_all.txt）；逐查询明细
#   data/verify_daemon_<Q>.txt（文件/指标双口径计数 + oracle 报告）。
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

QUERY="${1:-all}"
TOTAL="${2:-1m}"
DATA_VER="${DATA_VER:-v5}"
PARSE="${PARSE_PARALLELISM:-}"
RULE="${RULE_PARALLELISM:-}"

# 二进制来源（同 bench.sh）：优先本地 warp-fusion release；回退 PATH。
REPO="${REPO:-}"
if [ -z "$REPO" ] && [ -d "../../../warp-fusion" ]; then
  REPO="$(cd ../../../warp-fusion && pwd)"
fi
WFUSION="${WFUSION:-}"
WFGEN="${WFGEN:-}"
if [ -z "$WFUSION" ] && [ -n "$REPO" ] && [ -x "$REPO/target/release/wfusion" ]; then
  WFUSION="$REPO/target/release/wfusion"
fi
if [ -z "$WFGEN" ] && [ -n "$REPO" ] && [ -x "$REPO/target/release/wfgen" ]; then
  WFGEN="$REPO/target/release/wfgen"
fi
if [ -z "$WFUSION" ]; then WFUSION="$(command -v wfusion 2>/dev/null || true)"; fi
if [ -z "$WFGEN" ]; then WFGEN="$(command -v wfgen 2>/dev/null || true)"; fi
if [ -z "$WFUSION" ] || [ -z "$WFGEN" ]; then
  echo "错误: 找不到 wfusion/wfgen 二进制（设置 REPO/WFUSION/WFGEN，或加入 PATH）" >&2
  exit 1
fi

# 二进制新鲜度自检（同 bench.sh）：path 依赖才检查二进制是否早于源码。
if [ "${SKIP_BIN_CHECK:-0}" != "1" ] && [ -n "$REPO" ] && [ -n "$WFUSION" ] \
   && [ -d "$REPO/../wp-reactor/crates" ] \
   && grep -qE '^wf-engine = \{ *path' "$REPO/Cargo.toml" 2>/dev/null; then
  NEWER=$(find "$REPO/../wp-reactor/crates" -name '*.rs' -newer "$WFUSION" -print -quit 2>/dev/null)
  if [ -n "$NEWER" ]; then
    echo "⚠ 二进制新鲜度自检: 二进制早于源码修改: ${NEWER#*crates/} → 需 (cd $REPO && cargo build --release -p wfusion -p wfgen)" >&2
    if [ "${BIN_CHECK_STRICT:-0}" = "1" ]; then
      echo "  错误: BIN_CHECK_STRICT=1 → 拒绝运行（构建后再跑，或 SKIP_BIN_CHECK=1 强制）" >&2
      exit 1
    fi
  fi
fi

PY="${PYTHON:-python3}"
LIB="../scripts/bench_lib.py"        # 哨兵/appended/acked-lag（bench.sh 同款）
VFLIB="scripts/verify_file_lib.py"   # 验证度量/校验库：dirty/counts/cross/content
PORT=9800

TOTAL_N=1000000
case "$TOTAL" in
  1m) TOTAL_N=1000000;; 10m) TOTAL_N=10000000;; 30m) TOTAL_N=30000000;; 100m) TOTAL_N=100000000;;
  *) echo "bad total '$TOTAL' (1m|10m|30m|100m)"; exit 1;;
esac
case "$QUERY" in
  q1|q2|q3|q4|q5|q6|q7|q8|q9|q10|q11|q12|q13|q14|q15|q16|q17|q18|q19|q20|q21|q22)
    QUERIES=("$QUERY");;
  all) QUERIES=(q1 q2 q3 q4 q5 q6 q7 q8 q9 q10 q11 q12 q13 q14 q15 q16 q17 q18 q19 q20 q21 q22);;
  *) echo "bad query '$QUERY' (q1..q22|all)"; exit 1;;
esac

FRAMES="data/bench_${TOTAL}_${DATA_VER}.frames"
if [ ! -s "$FRAMES" ]; then
  echo "错误: 缺少预编码帧 ${FRAMES}（用 bench.sh <query> replay $TOTAL 生成，或先跑一次）" >&2
  exit 1
fi

# 清理上一轮验证产物（文件 sink append 不截断）
rm -f data/alerts/benchmark.ndjson data/metrics.ndjson data/error.ndjson data/perf_sentinel.ndjson
rm -f data/verify_daemon_emit_*.txt data/verify_daemon_cnt_*.txt data/verify_daemon_*.txt data/verify_daemon_all.txt
mkdir -p data/alerts

# ---- daemon 生命周期（bench.sh 同款：端口/收口/宽限/清理） ----
wait_port_free() {
  for i in $(seq 1 50); do
    if ! nc -z 127.0.0.1 "$PORT" 2>/dev/null; then return 0; fi
    sleep 0.2
  done
  echo "    警告: 端口 $PORT 超时未释放" >&2
  return 1
}

# SIGTERM → 轮询最多 300s → SIGKILL（与 bench.sh 对齐：stats/deferred 规则的
# shutdown close flush 需数秒-数十秒；过早 SIGKILL 截断 metrics 导出 → 尾部
# EMIT 计数丢失 → 对拍误报）。
kill_daemon() {
  local PID="$1"
  [ -n "$PID" ] || return 0
  kill "$PID" 2>/dev/null
  local i
  for i in $(seq 1 1500); do
    kill -0 "$PID" 2>/dev/null || { sleep 1; return 0; }
    sleep 0.2
  done
  echo "    警告: daemon $PID SIGTERM 后 300s 未退出, 强制 SIGKILL" >&2
  kill -9 "$PID" 2>/dev/null
  for i in $(seq 1 25); do
    kill -0 "$PID" 2>/dev/null || { sleep 1; return 0; }
    sleep 0.2
  done
  echo "    错误: daemon $PID 连 SIGKILL 都未退出" >&2
  return 1
}

cleanup_daemons() {
  pkill -9 -f "wfusion daemon" 2>/dev/null
  pkill -9 -f "wfgen send-arrow" 2>/dev/null
  sleep 1
  wait_port_free
}
cleanup_daemons
trap cleanup_daemons EXIT INT TERM

# ---- 引擎进度口径（bench.sh 同款） ----
engine_appended() { "$PY" "$LIB" appended data/metrics.ndjson "auction_events,bid_events,person_events"; }
engine_acked_lag() { "$PY" "$LIB" acked-lag data/metrics.ndjson ""; }

# 起 daemon（stdout 输出 PID；本函数经 `$(...)` 在子 shell 执行，错误只能
# return 1——调用方须 `local D; D=$(...) || exit 1` 拆开写）。
# 2026-08-30：不传 --perf-diag——哨兵窗 alert 会经 sinks_file 的 `windows=["*"]`
# 落到 benchmark.ndjson 污染 L2/L3 对拍（WildArray 无排除语法）；完成信号改用
# metrics 追平（appended + acked_lag，bench.sh 的兑底口径，语义等价：规则追平）。
start_daemon() {
  # 防端口残留误连（review 2026-08-30）：上一查询 daemon 若未死透（kill_daemon
  # 失败被忽略），nc -z 会立即成功但连的是**旧进程**（加载上一查询配置）→
  # 数据错乱且对拍误报。启动前先确认端口已释放；等不到则拒绝启动。
  if ! wait_port_free; then
    echo "    错误: 端口 $PORT 未释放（残留 daemon 未退出），拒绝启动——检查并清理后重跑" >&2
    return 1
  fi
  rm -f data/metrics.ndjson data/wfusion.log data/daemon_file.log data/perf_sentinel.ndjson data/error.ndjson
  # 文件 sink append 不截断：每次启动必须清 benchmark.ndjson，否则跨查询/重跑残留累积
  rm -f data/alerts/benchmark.ndjson
  "$WFUSION" daemon --config "$CONF" --work-dir . \
    > data/daemon_file.log 2>&1 &
  local D=$!
  local i
  for i in $(seq 1 40); do
    if ! kill -0 "$D" 2>/dev/null; then
      echo "    错误: daemon 启动失败（进程已退出）——daemon_file.log 尾部：" >&2
      tail -20 data/daemon_file.log >&2
      return 1
    fi
    nc -z 127.0.0.1 "$PORT" 2>/dev/null && break
    sleep 0.2
  done
  if ! nc -z 127.0.0.1 "$PORT" 2>/dev/null; then
    echo "    错误: daemon $D 启动超时（8s 端口 $PORT 未监听）——daemon_file.log 尾部：" >&2
    tail -20 data/daemon_file.log >&2
    return 1
  fi
  echo "$D"
}

# 等引擎消化完：metrics 追平（appended ≥ N 且 acked_lag == 0 = 所有被消费窗口
# 追平，bench.sh 兑底口径；无哨兵窗时这是唯一完成信号——见 start_daemon 注释）。
# 追平后 SIGTERM flush 会把规则尾部 close 收口落盘。
# 超时自适应：100k/s 诚实下限 + 600s 余量。
wait_daemon_drained() {
  local MAX_SEC=$(( TOTAL_N / 100000 + 600 ))
  local j APP=0 DRAINED=1
  for j in $(seq 1 $(( MAX_SEC * 10 ))); do
    APP=$(engine_appended)
    DRAINED=$(engine_acked_lag)
    if [ "${APP:-0}" -ge "$TOTAL_N" ] && [ "${DRAINED:-1}" = "0" ]; then
      return 0
    fi
    if [ $(( j % 100 )) -eq 0 ]; then
      echo "  ingest: $(printf "%d" "${APP:-0}")/$(printf "%d" "$TOTAL_N") ack_lag=${DRAINED:-1}（等待引擎消化，超时上限 ${MAX_SEC}s）"
    fi
    sleep 0.1
  done
  return 1
}

SUMMARY="data/verify_daemon_all.txt"
: > "$SUMMARY"
PASS_ALL=1
BASE_CONF="conf/wfusion.toml"   # daemon 基线（TCP 源 + 哨兵；sinks 逐查询覆盖为 sinks_file）
echo "== verify_daemon: query=$QUERY total=$TOTAL frames=$(basename "$FRAMES") 注入=daemon(TCP+flush) oracle=wfgen verify-nexmark $TOTAL_N =="

# ---- 每查询：临时配置（sinks_file 落盘 + 单查询 rules）→ daemon 注入 → 收口 → 三层对拍 ----
for Q in "${QUERIES[@]}"; do
  CONF="/tmp/verify_daemon_${Q}.toml"
  # 并行度默认取 $BASE_CONF；env 覆盖（单次 sed 完成全部替换）
  PARSE_EFF="${PARSE:-$(sed -n 's/^parse_parallelism = *//p' "$BASE_CONF" | head -1)}"
  RULE_EFF="${RULE:-$(sed -n 's/^rule_shards = *//p' "$BASE_CONF" | head -1)}"
  sed -e "s|^sinks = .*|sinks = \"topology/sinks_file\"|" \
      -e "s|^rules = .*|rules = \"models/queries/${Q}.wfl\"|" \
      -e "s|^parse_parallelism = .*|parse_parallelism = ${PARSE_EFF}|" \
      -e "s|^rule_shards = .*|rule_shards = ${RULE_EFF}|" \
      "$BASE_CONF" > "$CONF"

  # ---- daemon 注入 + 追平 + SIGTERM flush 收口（重跑兜底 2 次）----
  RULE_NAMES=$(grep "^rule " "models/queries/${Q}.wfl" | awk '{print $2}' | sort -u)
  BATCH_OK=0
  for attempt in 1 2; do
    D=$(start_daemon) || { echo "$Q | daemon=FAIL | 启动失败（见 data/daemon_file.log）" >> "$SUMMARY"; PASS_ALL=0; continue 2; }
    # 单连接推完整帧文件 + 哨兵帧（引擎无哨兵窗，帧被丢弃无害；与 bench.sh 客户端一致）
    "$WFGEN" send-arrow --input "$FRAMES" --addr 127.0.0.1:$PORT \
      --sentinel "$TOTAL_N" > /dev/null 2>&1 &
    CLIENT=$!
    if ! wait_daemon_drained; then
      echo "  ⚠ ${Q} 追平超时（${TOTAL_N} 行），重跑第 $(( attempt + 1 )) 次" >&2
      kill "$CLIENT" 2>/dev/null; wait "$CLIENT" 2>/dev/null
      kill_daemon "$D"; wait_port_free
      continue
    fi
    kill "$CLIENT" 2>/dev/null; wait "$CLIENT" 2>/dev/null
    sleep 1
    # SIGTERM flush：把规则尾部 close 输出收口落盘（metrics 导出完成 = kill 返回）。
    # kill_daemon 失败（SIGKILL 都杀不死）→ 端口残留，如实报错走 FAIL 分支。
    if ! kill_daemon "$D"; then
      echo "$Q | FAIL | daemon=无法停止（残留进程占端口 $PORT）" >> "$SUMMARY"
      echo "$Q | FAIL | daemon=无法停止（残留进程占端口 $PORT）"
      PASS_ALL=0
      continue 2
    fi
    wait_port_free
    if [ "$attempt" = "1" ]; then
      DIRTY=$("$PY" "$VFLIB" dirty "$RULE_NAMES")
      if [ -n "$DIRTY" ] && [ "$DIRTY" != "ok" ]; then
        echo "  ⚠ ${Q} 指标口径脏（${DIRTY}），重跑第 2 次" >&2
        continue
      fi
    fi
    BATCH_OK=1
    break
  done
  # 两次尝试均失败（启动失败/追平超时/指标脏）：必须如实报 FAIL，不能静默跳过
  # ——否则汇总可能显示"全部一致"但实际少了该查询（与启动失败分支同款，如实报 FAIL）。
  if [ "$BATCH_OK" != "1" ]; then
    echo "$Q | FAIL | daemon=两次尝试均失败（启动/追平超时/指标脏，见 data/daemon_file.log）" >> "$SUMMARY"
    echo "$Q | FAIL | daemon=两次尝试均失败（启动/追平超时/指标脏）"
    PASS_ALL=0
    continue
  fi

  # ---- 口径 1：metrics.ndjson → EMIT 文件（权威引擎计数）+ 致命计数器 ----
  EMIT="data/verify_daemon_emit_${Q}.txt"
  CNT_FILE="data/verify_daemon_cnt_${Q}.txt"
  FATAL=$("$PY" "$VFLIB" counts "$EMIT" "$CNT_FILE")

  # ---- 交叉检查：文件输出 vs metrics（尾批丢失 → ⚠ 警告不判失败）----
  CROSS=$("$PY" "$VFLIB" cross "$CNT_FILE")

  # ---- oracle 对拍（--detail-diff 字段级对拍；q13/q6 例外见 docs/ORACLE_VERIFY.md §6）----
  ORACLE_LOG="data/verify_daemon_oracle_${Q}.log"
  DETAIL_DIFF="--detail-diff data/alerts/benchmark.ndjson"
  if [ "$Q" = "q13" ] || [ "$Q" = "q6" ]; then
    DETAIL_DIFF=""
  fi
  if [ -n "$DETAIL_DIFF" ]; then
    "$WFGEN" verify-nexmark "$TOTAL_N" --query "$Q" --engine-emit "$EMIT" \
      $DETAIL_DIFF > "$ORACLE_LOG" 2>&1
  else
    "$WFGEN" verify-nexmark "$TOTAL_N" --query "$Q" --engine-emit "$EMIT" > "$ORACLE_LOG" 2>&1
  fi
  VRC=$?

  # ---- 口径 2b：alert 内容断言（L2，verify_file_lib.py CHECKS）----
  CONTENT=$("$PY" "$VFLIB" content "$Q" "$RULE_NAMES")

  VERDICT="FAIL"
  [ "$VRC" = "0" ] && VERDICT="PASS"
  [ "$FATAL" != "clean" ] && VERDICT="DIRTY"
  [ "$CONTENT" != "ok" ] && VERDICT="CONTENT-FAIL"
  [ "$VERDICT" != "PASS" ] && PASS_ALL=0

  LINE="$Q | $VERDICT | daemon=OK | fatal=${FATAL}"
  if [ "$VRC" = "0" ]; then
    LINE="${LINE} | oracle=identical ✅"
  else
    LINE="${LINE} | oracle=diff ❌（见 ${ORACLE_LOG}）"
  fi
  if [ "$CONTENT" = "ok" ]; then
    LINE="${LINE} | 内容断言 ✅"
  else
    LINE="${LINE} | 内容断言 ❌ (${CONTENT})"
  fi
  [ -n "$CROSS" ] && LINE="${LINE} | ⚠尾批丢失: ${CROSS}"
  echo "$LINE" >> "$SUMMARY"
  echo "$LINE"
  {
    echo "-- ${Q}（daemon TCP 注入 + SIGTERM flush，${TOTAL} 数据）--"
    echo "== 指标口径（metrics.emitted_total，权威）=="
    cat "$EMIT"
    echo "== 输出文件口径（data/alerts/benchmark.ndjson）=="
    cat "$CNT_FILE"
    echo "== alert 内容断言 =="
    echo "$CONTENT"
    echo "== oracle 对拍（wfgen verify-nexmark --query $Q --engine-emit）=="
    cat "$ORACLE_LOG"
  } > "data/verify_daemon_${Q}.txt"
done

echo "== done: 结果在 data/verify_daemon_all.txt（逐查询明细 data/verify_daemon_<Q>.txt）=="
if [ "$PASS_ALL" = "1" ]; then
  echo "== verify_daemon: 全部一致 ✅ =="
  exit 0
else
  echo "== verify_daemon: 存在 FAIL/DIRTY ❌（见上方摘要与明细）=="
  exit 1
fi
