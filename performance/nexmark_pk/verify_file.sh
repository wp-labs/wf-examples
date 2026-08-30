#!/usr/bin/env bash
# verify_file.sh — 文件源正确性验证：batch 模式喂 1M 数据 → 输出文件与 oracle 对拍
#
# 背景（NEXMARK.md §5）：bench.sh 走 daemon+TCP 注入验证（--verify）；本脚本验证
# **文件源路径**（conf/wfusion_file.toml batch 配置）：`wfusion batch` 直读预编码
# 帧文件，把规则 EMIT 落到 sinks_file/business.d/benchmark.toml 指定的输出文件
# data/alerts/benchmark.ndjson，逐查询与 `wfgen verify-nexmark` ground truth 对拍。
#
# 用法:
#   ./verify_file.sh [query=q1|..|q22|all] [total=1m|10m|30m|100m]
#     默认 all + 1m（1M 数据快验，~2-4 分钟）。total 对应的预编码帧
#     data/bench_<total>_v5.frames 必须存在（bench.sh 会生成；1m 缓存已入库）。
#   env（与 bench.sh 同款）:
#     REPO / WFUSION / WFGEN   二进制来源（默认 ../../../warp-fusion/target/release）
#     PARSE_PARALLELISM / RULE_PARALLELISM   并行度（默认取 data/verify_file_conf.toml）
#     DATA_VER   帧缓存版本（默认 v5）
#     SKIP_BIN_CHECK=1 / BIN_CHECK_STRICT=1   二进制新鲜度自检开关（同 bench.sh）
#
# 设计要点（历史修复记录已归档 git / docs/ORACLE_VERIFY.md）:
# - **逐查询单跑**（每查询 rules = 该查询 .wfl）：多规则同跑存在规则间交互差异
#   （实测 all 跑：q8 7565→1、q11 17081→118234），单规则保真。
# - **双口径**：metrics.emitted_total（权威引擎计数）+ benchmark.ndjson 文件计数交叉；
#   致命计数器（append_failed/dropped_late/cursor_gap/drain_dropped_records/
#   sink_dispatch_failed/channel_full）非零 → [dirty] 验证作废。
# - **度量/校验逻辑在 scripts/verify_file_lib.py**（dirty/counts/emitted/cross/content
#   子命令）；脚本只做编排。
# - **oracle 定义与边界**（三档验证层级/known 差异/排除规则）见 docs/ORACLE_VERIFY.md。
# - **已修复**（wp-reactor 2026-08-28~30）：文件 sink 关机尾批丢失、q13 中间管道
#   消费竞态、q8/q11/q7 多规则交互、q6/q20 snapshot join 竞态、q3（join 索引与
#   提交前沿竞态）、q5/q7（close_all 尾桶收口语义）。当前 22 查询全 PASS。
# - **剩余已知项**：q12 known（fixed+close 收口，1M 引擎 27446 vs oracle 10240 多
#   ~168%）由 verify-nexmark 内置列表处理（⚠ 不判失败）——q12 是豁免放行而非
#   验证一致，引擎待修项。其余 21 个 L1+L2+L3 真一致。
#
# 输出: 每查询一行摘要（stdout + data/verify_file_all.txt）；逐查询明细
#   data/verify_file_<Q>.txt（文件/指标双口径计数 + oracle 报告）。
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

# 二进制新鲜度自检（同 bench.sh）：warp-fusion 用 **git 依赖**（Cargo.toml 非 path）时
# 本地 wp-reactor 源码不参与编译，二进制即验证真相（oracle/引擎同一二进制），跳过
# mtime 检查避免每次误报；path 依赖才检查二进制是否早于源码修改。
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
  echo "错误: 缺少预编码帧 $FRAMES（用 bench.sh <query> replay $TOTAL 生成，或先跑一次）" >&2
  exit 1
fi

# 清理上一轮验证产物（metrics/输出文件均会跨 run 累积 —— 文件 sink append 不截断）
rm -f data/alerts/benchmark.ndjson data/metrics.ndjson data/error.ndjson
rm -f data/verify_emit_*.txt data/verify_cnt_*.txt data/verify_file_*.txt data/verify_oracle_*.log data/verify_file_all.txt
mkdir -p data/alerts

# 防残留 wfusion 进程污染 metrics.ndjson（2026-08-30 q19 伪 FAIL 根因）：上个
# 会话残留的多规则 daemon 惰性打开 monitor 文件 append，往当前 query 的 metrics
# 写入外来 label（如 q2_mod_123 0）→ 权威口径误读。先清后跑（batch 为同步子
# 进程，此刻无自进程在跑，安全；wfgen 是 oracle 不能杀）。
pkill -9 -f "wfusion daemon" 2>/dev/null
pkill -9 -f "wfusion batch" 2>/dev/null
sleep 1

SUMMARY="data/verify_file_all.txt"
: > "$SUMMARY"
PASS_ALL=1
BASE_CONF="conf/wfusion_file.toml"   # 基线配置（入库）；data/verify_file_conf.toml 是同款本地缓存
[ -f "$BASE_CONF" ] || BASE_CONF="data/verify_file_conf.toml"
[ -f "$BASE_CONF" ] || { echo "错误: 缺少基线配置 $BASE_CONF" >&2; exit 1; }
echo "== verify_file: query=$QUERY total=$TOTAL frames=$(basename "$FRAMES") oracle=wfgen verify-nexmark $TOTAL_N =="

# ---- 每查询：临时配置（$BASE_CONF 改 rules/帧路径）→ batch → 双口径计数 → oracle 对拍 ----
for Q in "${QUERIES[@]}"; do
  CONF="/tmp/verify_file_${Q}.toml"
  # 并行度默认取 $BASE_CONF；env 覆盖（单次 sed 完成全部替换，无 in-place）
  PARSE_EFF="${PARSE:-$(sed -n 's/^parse_parallelism = *//p' "$BASE_CONF" | head -1)}"
  RULE_EFF="${RULE:-$(sed -n 's/^rule_parallelism = *//p' "$BASE_CONF" | head -1)}"
  sed -e "s|^rules = .*|rules = \"models/queries/${Q}.wfl\"|" \
      -e "s|^path = .*|path = \"${FRAMES}\"|" \
      -e "s|^parse_parallelism = .*|parse_parallelism = ${PARSE_EFF}|" \
      -e "s|^rule_parallelism = .*|rule_parallelism = ${RULE_EFF}|" \
      "$BASE_CONF" > "$CONF"

  # ---- batch + 指标口径脏检测（重跑兜底）----
  # 2026-08-30 加固：all 模式偶发 q19 伪 FAIL——残留 wfusion 进程往当前 query 的
  # metrics.ndjson 写入外来 label，权威口径误读。校验 emitted_total label 必须恰为
  # 当前 query 的规则集合；脏则自动重跑一次（最多 2 次）。
  RULE_NAMES=$(grep "^rule " "models/queries/${Q}.wfl" | awk '{print $2}' | sort -u)
  BATCH_OK=0
  for attempt in 1 2; do
    rm -f data/alerts/benchmark.ndjson data/metrics.ndjson data/error.ndjson
    if ! "$WFUSION" batch --config "$CONF" --work-dir . > data/wfusion_file.log 2>&1; then
      echo "$Q  batch=FAIL(退出码非 0) —— data/wfusion_file.log 尾部:" >&2
      tail -15 data/wfusion_file.log >&2
      echo "$Q | batch=FAIL | 退出码非 0（见 data/wfusion_file.log）" >> "$SUMMARY"
      PASS_ALL=0
      continue 2
    fi
    if [ "$attempt" = "1" ]; then
      DIRTY=$("$PY" scripts/verify_file_lib.py dirty "$RULE_NAMES")
      if [ -n "$DIRTY" ] && [ "$DIRTY" != "ok" ]; then
        echo "  ⚠ ${Q} 指标口径脏（${DIRTY}），重跑第 2 次" >&2
        continue
      fi
    fi
    BATCH_OK=1
    break
  done
  [ "$BATCH_OK" = "1" ] || continue

  # ---- 口径 1：metrics.ndjson → EMIT 文件（权威引擎计数）+ 致命计数器 ----
  EMIT="data/verify_emit_${Q}.txt"
  CNT_FILE="data/verify_cnt_${Q}.txt"   # 注意：与明细文件 verify_file_<Q>.txt 不同名，防自读循环
  FATAL=$("$PY" scripts/verify_file_lib.py counts "$EMIT" "$CNT_FILE")

  # ---- 交叉检查：文件输出 vs metrics（尾批丢失 → ⚠ 警告不判失败）----
  CROSS=$("$PY" scripts/verify_file_lib.py cross "$CNT_FILE")

  # ---- oracle 对拍（engine-emit = metrics 口径；q12 known 由 verify-nexmark 处理；
  #      --detail-diff = 字段级明细对拍：oracle 的 yield 字段值 vs benchmark.ndjson）----
  # q13 例外：side_input 是 provider 静态表（knowdb），oracle 不加载 knowdb →
  # join 富化字段无法对拍，由 CHECKS 内容断言 + 计数对拍覆盖。
  # q6 例外：join-then-key 的 join 可见性非确定（引擎 replay append/evict 时序
  # 影响逐 bid 是否计入 avg，oracle 理想值）——计数一致但内容分布不同，
  # 无权威基线（Flink 未实现），明细对拍排除。
  ORACLE_LOG="data/verify_oracle_${Q}.log"
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

  # ---- 口径 2b：alert 内容断言（2026-08-30 新增：计数对拍之上的字段值校验）----
  # 逐规则校验 benchmark.ndjson 行内容：通用（规则归属/字段齐全/request_count）
  # + per-rule 强语义（如 q2 mod(id,123)==0、q9 每 auction 一条 winner）。
  CONTENT=$("$PY" scripts/verify_file_lib.py content "$Q" "$RULE_NAMES")

  VERDICT="FAIL"
  [ "$VRC" = "0" ] && VERDICT="PASS"
  [ "$FATAL" != "clean" ] && VERDICT="DIRTY"
  [ "$CONTENT" != "ok" ] && VERDICT="CONTENT-FAIL"
  [ "$VERDICT" != "PASS" ] && PASS_ALL=0

  LINE="$Q | $VERDICT | batch=OK | fatal=${FATAL}"
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
  # 逐查询明细：EMIT 计数 + 文件计数 + 内容断言 + oracle 报告
  {
    echo "-- ${Q}（文件源 batch，${TOTAL} 数据）--"
    echo "== 指标口径（metrics.emitted_total，权威）=="
    cat "$EMIT"
    echo "== 输出文件口径（data/alerts/benchmark.ndjson）=="
    cat "$CNT_FILE"
    echo "== alert 内容断言 =="
    echo "$CONTENT"
    echo "== oracle 对拍（wfgen verify-nexmark --query $Q --engine-emit）=="
    cat "$ORACLE_LOG"
  } > "data/verify_file_${Q}.txt"
done

echo "== done: 结果在 data/verify_file_all.txt（逐查询明细 data/verify_file_<Q>.txt）=="
if [ "$PASS_ALL" = "1" ]; then
  echo "== verify_file: 全部一致 ✅ =="
  exit 0
else
  echo "== verify_file: 存在 FAIL/DIRTY ❌（见上方摘要与明细）=="
  exit 1
fi
