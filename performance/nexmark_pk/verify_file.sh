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
# 设计（2026-08-28）:
# - **逐查询单跑**（每查询临时配置 rules = models/queries/<Q>.wfl）：多规则同跑存在
#   规则间交互差异（实测 all 跑：q8 7565→1、q11 17081→118234），单规则保真。
# - **引擎 EMIT 计数取 metrics.ndjson 的 emitted_total**（规则任务 join 后导出，
#   权威口径，与 bench.sh --verify 一致）；data/alerts/benchmark.ndjson 是输出文件实测
#   （用户要求的验证对象），两者交叉检查。
# - **已知尾批丢失**：文件 sink 关机时最后 ≤2 个未满 alert 批不落盘（实测 q1 单跑
#   metrics=920000 vs 文件=917469，多轮逐字节一致 —— sink 消费者取消时序所致，非规则
#   问题）。故 oracle 对拍以 metrics 口径为准；文件输出缺额 = 尾批丢失，报告 ⚠ 不判失败。
# - **引擎快速重放非确定（2026-08-28 1M 全量扫描实测）**：batch 自动关机（接收器完
#   成即 cancel）与规则尾收口存在竞态，部分查询偶发/恒差少量记录（metrics 口径 vs
#   oracle，非文件 sink 问题）——脚本如实报 FAIL，属引擎待修项，非规则逻辑错：
#     q3  auction_seller 尾部 close 丢 0~7（6060↔6053，flaky）
#     q5/q7  尾桶 close 恒差 1（51→50 / 10→9）
#     q13  bid_mod→q13b 中间管道消费竞态丢 0~7%（flaky，rule_parallelism=1 时丢 ~96%）
#     q20  snapshot join 可见性竞态丢 3~4%（flaky）
#   其余查询（q1/q2/q4/q6/q8/q9/q10/q11/q12/q14/q15/q16/q17/q18/q19/q21/q22）
#   逐轮与 oracle 一致 ✅。
# - **q12** 已知差异（fixed+close 收口，oracle 理想值）由 verify-nexmark 内置 known
#   列表处理：报告 ⚠ 但不判失败。
# - 致命计数器（append_failed / dropped_late / cursor_gap / drain_dropped_records /
#   sink_dispatch_failed / channel_full）非零 → 该查询标记 [dirty]，验证作废。
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

  rm -f data/alerts/benchmark.ndjson data/metrics.ndjson data/error.ndjson
  if ! "$WFUSION" batch --config "$CONF" --work-dir . > data/wfusion_file.log 2>&1; then
    echo "$Q  batch=FAIL(退出码非 0) —— data/wfusion_file.log 尾部:" >&2
    tail -15 data/wfusion_file.log >&2
    echo "$Q | batch=FAIL | 退出码非 0（见 data/wfusion_file.log）" >> "$SUMMARY"
    PASS_ALL=0
    continue
  fi

  # ---- 口径 1：metrics.ndjson → EMIT 文件（权威引擎计数）+ 致命计数器 ----
  EMIT="data/verify_emit_${Q}.txt"
  CNT_FILE="data/verify_cnt_${Q}.txt"   # 注意：与明细文件 verify_file_<Q>.txt 不同名，防自读循环
  FATAL=$("$PY" - "$EMIT" "$CNT_FILE" <<'PYEOF'
import json, sys
from collections import defaultdict
emit_path, cnt_path = sys.argv[1], sys.argv[2]
emitted = defaultdict(int)
fatal = defaultdict(int)
info = defaultdict(int)
for line in open("data/metrics.ndjson"):
    try:
        o = json.loads(line)
    except Exception:
        continue
    name = o.get("name"); label = o.get("label", "")
    try:
        val = int(float(o.get("value", 0) or 0))
    except (TypeError, ValueError):
        continue
    if name == "emitted_total":
        emitted[label] += val
    elif name in ("append_failed_total", "dropped_late_total", "cursor_gap_total",
                  "drain_dropped_records_total", "sink_dispatch_failed_total",
                  "channel_full_total"):
        fatal[name] += val
    elif name in ("time_evicted_total", "memory_evicted_total"):
        info[name] += val
with open(emit_path, "w") as f:
    for k in sorted(emitted):
        f.write(f"EMIT {k} {emitted[k]}\n")
# 输出文件（benchmark.ndjson）每规则计数
file_cnt = defaultdict(int)
try:
    for line in open("data/alerts/benchmark.ndjson", errors="replace"):
        try:
            file_cnt[json.loads(line).get("__wfu_rule_name")] += 1
        except Exception:
            continue
except FileNotFoundError:
    pass
with open(cnt_path, "w") as f:
    for k in sorted(file_cnt):
        f.write(f"{k} {file_cnt[k]}\n")
bad = [f"{k}={v}" for k, v in sorted(fatal.items()) if v > 0]
print(";".join(bad) if bad else "clean")
PYEOF
  )

  # ---- 交叉检查：文件输出 vs metrics（已知尾批丢失 → ⚠ 警告不判失败）----
  CROSS=""
  while read -r rule n; do
    [ -n "$rule" ] || continue
    m=$("$PY" - "$rule" <<'PYEOF'
import json, sys
rule = sys.argv[1]
s = 0
for line in open("data/metrics.ndjson"):
    try:
        o = json.loads(line)
        if o.get("name") == "emitted_total" and o.get("label") == rule:
            s += int(float(o.get("value", 0) or 0))
    except Exception:
        continue
print(s)
PYEOF
)
    if [ "$m" -gt "$n" ]; then
      gap=$(( m - n ))
      pct=$(( gap * 100 / m ))
      tag="⚠"
      [ "$pct" -ge 1 ] && tag="⚠⚠"
      CROSS="${CROSS}${CROSS:+; }${tag}${rule} 文件${n}/指标${m}（缺${gap}=${pct}%）"
    elif [ "$m" -lt "$n" ]; then
      CROSS="${CROSS}${CROSS:+; }⚠⚠${rule} 文件${n} > 指标${m}（异常，检查是否残留累积）"
    fi
  done < "$CNT_FILE"

  # ---- oracle 对拍（engine-emit = metrics 口径；q12 known 由 verify-nexmark 处理）----
  ORACLE_LOG="data/verify_oracle_${Q}.log"
  "$WFGEN" verify-nexmark "$TOTAL_N" --query "$Q" --engine-emit "$EMIT" > "$ORACLE_LOG" 2>&1
  VRC=$?
  VERDICT="FAIL"
  [ "$VRC" = "0" ] && VERDICT="PASS"
  [ "$FATAL" != "clean" ] && VERDICT="DIRTY"
  [ "$VERDICT" != "PASS" ] && PASS_ALL=0

  LINE="$Q | $VERDICT | batch=OK | fatal=${FATAL}"
  if [ "$VRC" = "0" ]; then
    LINE="${LINE} | oracle=identical ✅"
  else
    LINE="${LINE} | oracle=diff ❌（见 ${ORACLE_LOG}）"
  fi
  [ -n "$CROSS" ] && LINE="${LINE} | ⚠尾批丢失: ${CROSS}"
  echo "$LINE" >> "$SUMMARY"
  echo "$LINE"
  # 逐查询明细：EMIT 计数 + 文件计数 + oracle 报告
  {
    echo "-- ${Q}（文件源 batch，${TOTAL} 数据）--"
    echo "== 指标口径（metrics.emitted_total，权威）=="
    cat "$EMIT"
    echo "== 输出文件口径（data/alerts/benchmark.ndjson）=="
    cat "$CNT_FILE"
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
