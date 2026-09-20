#!/usr/bin/env bash
# verify_wfg.sh — 按查询批量跑「.wfg 语料」的 WFL 验证
#                层次：L0 静态校验 · L0' 规则内联手写用例 · L1 注入断言 · L2 期望文件 ·
#                      可选 --with-engine：L3 引擎级对拍（真引擎输出 vs 期望）
#
# 用法：
#   ./verify_wfg.sh q1                 # 单查询
#   ./verify_wfg.sh q1 q3 q13          # 多查询
#   ./verify_wfg.sh all                # models/queries/q*.wfl 全部（含 q6，验证口径与吞吐口径无关）
#   ./verify_wfg.sh all --lint-only    # 只做 L0 + L0'（快，不落数据）
#   ./verify_wfg.sh q3 --scaffold      # 为缺语料的查询**写出** scenarios/q3_verify.wfg 模板（含 TODO），不执行
#   ./verify_wfg.sh all --duration 10s --out data/wfg_verify   # 压时长 + 指定输出根
#        注：--duration 会覆盖语料自带的 #[duration]；窗口切分敏感的语料（如 q5 的 top-N
#        near_miss「差 1 票」）只在原切分下成立，gen 失败时会提醒这一点。
#   ./verify_wfg.sh q3 --keep          # 保留自动生成的 smoke 场景（默认跑完删除）
#   ./verify_wfg.sh q1 --with-engine   # 额外做引擎级对拍：gen → dump-frames → wfusion batch → wfgen verify
#
# 语料来源（两级）：
#   curated = scenarios/<q>_verify.wfg 已存在（人工写好的 hit/near_miss/miss 用例）→ 直接用；
#             必须是**真语料**：不含注入用例的 curated 文件会被判 NOINJ 失败（否则"跑了但没验证"= 静默失效）；
#   smoke   = 不存在时自动生成背景-only 场景（源流由规则 events 块 ∩ schema 的 stream_tag 推出），
#             跑得出 lint/gen 但**没有注入断言**——只作"规则/ schema 未漂移"的烟枪测试。
#
# 退出码：0 = 全部通过（`--with-engine` 下已登记的「已知差异」不计失败）；
#         1 = 有查询失败（lint / 规则内联用例 / gen / NOINJ / engine）；2 = 用法/环境错误。
#
# L0'：规则内联手写用例（`test` 块）——现成的「手写小表」。
#   只对含 `test` 块的规则文件跑（本仓 10 个规则文件、19 条可跑用例）；跑的是**真引擎的
#   match-engine**（`wf_engine::match_engine::contract::run_test`），不需要生成数据，
#   `--lint-only` 下也跑。能力：`hits cmp N` / `hit[i].{score,origin,entity_type,entity_id,field(名字)}`
#   / `close_trigger {timeout,flush,eos}` / `permutation` / `runs`。
#   为什么需要它：它是唯一能钉住**几何量**的一层（如 q5 的 `hits == size/slide`）——
#   引擎级对拍只证明「同一份规则的两套实现一致」，看不到规则本身偏离权威语义
#   （实测：`hop(10s,2s)`→`hop(20s,2s)` 引擎级 PASS，而内联用例报 `got 7`）。
#   harness **刻意拒绝**的用例（无 WindowLookup 的 join 类，错误文本含 `cannot assert hit counts`）
#   记为「不适用」而不是失败——它们永远红，当失败等于把红灯当信号（需 E2E 覆盖）。
#   缺 wfl 二进制时会在启动时响亮提醒（可用 WFL=/path/to/wfl 指定）。
#
# --with-engine：把该查询的证据从「期望级」升到「引擎级」——
#   gen 产物 JSONL --wfgen dump-frames--> events.arrow_framed
#     → wfusion batch（文件源，跑完输入自动退出）→ alerts
#     → wfgen verify --expected/--actual/--meta 对拍。
#
#   ⚠ 只用对**含 inject 的语料**才有意义：gen 的期望由注入用例驱动，场景里没有
#     `inject` 时 `Expected: 0`（期望文件为空）→ 无可比对，smoke 档一律记 N/A（**不是通过**）。
#     已验证：同一个场景加一条 `inject` 后 Expected 从 0 变 5001。
#   与 --lint-only 同用时不做（没生成数据）。

set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

QUERIES=()
DURATION=""
OUT_ROOT="data/wfg_verify"
LINT_ONLY=0
SCAFFOLD=0
KEEP=0
WITH_ENGINE=0

usage() { awk 'NR>1 && /^# 语料来源/ {exit} NR>1 {sub(/^# ?/, ""); print}' "${BASH_SOURCE[0]}"; exit "${1:-0}"; }

while [ $# -gt 0 ]; do
    case "$1" in
        -h|--help) usage 0 ;;
        --lint-only) LINT_ONLY=1 ;;
        --scaffold) SCAFFOLD=1 ;;
        --keep) KEEP=1 ;;
        --with-engine) WITH_ENGINE=1 ;;
        --duration) shift; DURATION="${1:-}" ;;
        --out) shift; OUT_ROOT="${1:-}" ;;
        -*) echo "unknown option: $1" >&2; usage 2 ;;
        *) QUERIES+=("$1") ;;
    esac
    shift
done
[ "${#QUERIES[@]}" -gt 0 ] || usage 2
[ -n "$OUT_ROOT" ] || { echo "--out 不能为空" >&2; exit 2; }

# ---- wfgen 解析（同 bench.sh 口径：优先本地 warp-fusion release 产物，否则 PATH）----
WFGEN="${WFGEN:-}"
if [ -z "$WFGEN" ] && [ -x "../../../warp-fusion/target/release/wfgen" ]; then
    WFGEN="../../../warp-fusion/target/release/wfgen"
fi
[ -n "$WFGEN" ] || WFGEN="$(command -v wfgen 2>/dev/null || true)"
[ -n "$WFGEN" ] || {
    echo "找不到 wfgen：设置 WFGEN=/path/to/wfgen，或 (cd ../../../warp-fusion && cargo build --release -p wfgen)" >&2
    exit 2
}
# 显式 WFGEN 也要先验证可执行：否则会在 lint 阶段变成一堆看不懂的 FAIL
case "$WFGEN" in
    */*) [ -x "$WFGEN" ] || { echo "WFGEN 不可执行：$WFGEN" >&2; exit 2; } ;;
    *)   wfgen_name="$WFGEN"
         WFGEN="$(command -v "$WFGEN" 2>/dev/null || true)"
         [ -n "$WFGEN" ] || { echo "WFGEN 不在 PATH 中：${wfgen_name}" >&2; exit 2; } ;;
esac

SCHEMA="models/schemas/nexmark.wfs"
[ -f "$SCHEMA" ] || { echo "缺少 $SCHEMA" >&2; exit 2; }

SCHEMA="models/schemas/nexmark.wfs"
[ -f "$SCHEMA" ] || { echo "缺少 $SCHEMA" >&2; exit 2; }

# ---- wfl 解析（规则内联手写用例 `test` 块；同 wfgen 口径）----
WFL="${WFL:-}"
if [ -z "$WFL" ] && [ -x "../../../warp-fusion/target/release/wfl" ]; then
    WFL="../../../warp-fusion/target/release/wfl"
fi
[ -n "$WFL" ] || WFL="$(command -v wfl 2>/dev/null || true)"
[ -z "$WFL" ] && echo "⚠ 未找到 wfl → 规则内联用例（\`test\` 块）**未跑**（手写小表是唯一能钉几何量的一层）；(cd ../../../warp-fusion && cargo build --release -p wfl) 或设 WFL=/path/to/wfl" >&2

# python3：内联用例回执解析 + 引擎对拍报告解析
PY="${PYTHON:-python3}"
HAVE_PY=0
command -v "$PY" >/dev/null 2>&1 && HAVE_PY=1

# 临时目录：内联用例的 json/err 回执（不动数据目录，--lint-only 也不落数据）
OUT_TMP="$(mktemp -d "${TMPDIR:-/tmp}/wfg_verify.XXXXXX")"

# ---- 引擎级对拍（--with-engine）需要的前置文件与工具 ----
# 形态 = **生成文件 → batch 模式**（不起 daemon、不占端口、不发 SIGTERM）：
#   gen 产物 JSONL --wfgen dump-frames--> events.arrow_framed
#     --> wfusion batch（mode=batch + 文件源，跑完自动退出）--> alerts
#     --> wfgen verify 对拍。
# 与仓库自己的 gen↔engine 对拍同形态（crates/wfgen/tests/common、e2e_datagen）。
ENGINE_TIMEOUT="${ENGINE_TIMEOUT:-300}"   # 单查询 batch 上限（秒）；正常情况下 batch 自己退出
if [ "$WITH_ENGINE" = "1" ]; then
    WFUSION="${WFUSION:-}"
    if [ -z "$WFUSION" ] && [ -x "../../../warp-fusion/target/release/wfusion" ]; then
        WFUSION="../../../warp-fusion/target/release/wfusion"
    fi
    [ -n "$WFUSION" ] || WFUSION="$(command -v wfusion 2>/dev/null || true)"
    [ -n "$WFUSION" ] || {
        echo "--with-engine 需要 wfusion：设置 WFUSION=/path/to/wfusion，或 (cd ../../../warp-fusion && cargo build --release -p wfusion)" >&2
        exit 2
    }
    [ -x "$WFUSION" ] || { echo "WFUSION 不可执行：$WFUSION" >&2; exit 2; }
    [ "$HAVE_PY" = "1" ] || { echo "--with-engine 需要 python3（解析对拍报告）" >&2; exit 2; }
    [ -d topology/sinks_file ] || { echo "缺少 topology/sinks_file（batch 的 sink 配置）" >&2; exit 2; }
    [ -f models/schemas/windows.toml ] || { echo "缺少 models/schemas/windows.toml" >&2; exit 2; }
fi

# 生成的临时 smoke 场景：异常中断也要清理（dotfile 被 .gitignore 忽略 → 残留不会在 git status 里暴露）
TMP_FILES=""

# ---- 引擎级对拍：**生成文件 → batch 模式**（不起 daemon、不占端口、不发 SIGTERM）----
# 与仓库自己的 gen↔engine 对拍同形态（crates/wfgen/tests/common、e2e_datagen）。
# `wfusion batch` 跑完输入就自动退出，不再需要“追平启发式 + SIGTERM 收口”。
run_batch_engine() {
    conf="$1"; log="$2"
    "$WFUSION" batch --config "$conf" --work-dir . > "$log" 2>&1 &
    bpid=$!
    i=0
    while [ "$i" -lt $((ENGINE_TIMEOUT * 2)) ]; do
        kill -0 "$bpid" 2>/dev/null || break
        i=$((i + 1)); sleep 0.5
    done
    if kill -0 "$bpid" 2>/dev/null; then
        echo "    错误: wfusion batch 超时（${ENGINE_TIMEOUT}s）——已终止（见 $log）" >&2
        kill -9 "$bpid" 2>/dev/null
        wait "$bpid" 2>/dev/null
        return 1
    fi
    wait "$bpid"
}

# 引擎级对拍：gen 产物 → dump-frames → wfusion batch（跑完自退）→ wfgen verify。
# 报告落 $out_dir/engine_verify.json；裁定词回显 stdout（PASS / FAIL / NA）。
run_engine_verify() {
    q="$1"; scenario="$2"; out_dir="$3"
    stem="$(basename "$scenario" .wfg)"
    abs_out="$(cd "$out_dir" && pwd)"
    frames="$abs_out/events.arrow_framed"
    conf="$abs_out/wfusion_batch.toml"
    alerts="$abs_out/data/alerts/benchmark.ndjson"

    # 1) JSONL → 引擎的 arrow_framed 输入（与 send / bench 同一套帧编码）
    "$WFGEN" dump-frames --scenario "$scenario" --input "$out_dir/$stem.jsonl" \
        --output "$frames" > "$out_dir/engine_dump.log" 2>&1 || {
        echo "    错误: wfgen dump-frames 失败（见 $out_dir/engine_dump.log）" >&2
        return 1
    }

    # 2) batch 配置：mode=batch + 文件源；sinks 复用 topology/sinks_file
    #    （相对 base 落 <work_root>/data/alerts/benchmark.ndjson，即 abs_out 下，
    #    不与 bench/verify_daemon 的共享产物目录相混）。
    cat > "$conf" <<EOF
mode = "batch"
sinks = "$(pwd)/topology/sinks_file"
windows = "$(pwd)/models/schemas/windows.toml"
work_root = "$abs_out"

[[sources]]
type = "file"
name = "ingress"
path = "$frames"
data_format = "arrow_framed"
stream_tag = ""

[runtime]
executor_parallelism = 2
rule_exec_timeout = "30s"
schemas = "models/schemas/*.wfs"
rules = "models/queries/${q}.wfl"

[logging]
level = "info"
format = "plain"
file = "$abs_out/data/wfusion.log"
EOF

    # 3) batch 跑完自动退出（超时兜底见 run_batch_engine）
    rm -rf "$abs_out/data"
    if ! run_batch_engine "$conf" "$out_dir/engine_batch.log"; then
        echo "    错误: wfusion batch 未正常结束（见 $out_dir/engine_batch.log）" >&2
        tail -10 "$out_dir/engine_batch.log" >&2
        return 1
    fi
    # 未产出 alerts 文件不当成错误：建个空文件让 verify 如实报 missing（可能确实是“一条都没输出”）。
    [ -f "$alerts" ] || { mkdir -p "$(dirname "$alerts")"; : > "$alerts"; }

    # 4) 对拍
    "$WFGEN" verify --expected "$out_dir/$stem.except.jsonl" \
        --actual "$alerts" \
        --meta "$out_dir/$stem.except.meta.jsonl" > "$out_dir/engine_verify.json" 2>&1

    # 回显裁定词（stdout）供调用方分流。
    # 1) **不看 wfgen verify 的退出码**：两侧都是 0 条时它照样 exit 0（status=pass），
    #    那是“空对空”而不是证据。
    # 2) **也不信报告里的 status 字段**：直接按 verify 的 pass 定义（missing / unexpected /
    #    field_mismatch 全为 0）从计数重算，标签写错也不会误报通过。
    "$PY" -c '
import json, sys
try:
    d = json.load(open(sys.argv[1])); s = d["summary"]
    expected, actual = s["expected_total"], s["actual_total"]
    bad = s["missing"] + s["unexpected"] + s["field_mismatch"]
except Exception:
    print("FAIL"); sys.exit(0)
if expected == 0 and actual == 0:
    print("NA")
elif bad == 0:
    print("PASS")
else:
    print("FAIL")
' "$out_dir/engine_verify.json" 2>/dev/null
}

cleanup() {
    [ -n "${OUT_TMP:-}" ] && [ -d "$OUT_TMP" ] && rm -rf "$OUT_TMP"
    if [ -n "${TMP_FILES:-}" ] && [ "${KEEP:-0}" != "1" ]; then rm -f $TMP_FILES; fi
}
trap cleanup EXIT INT TERM HUP

# schema 里声明了 stream_tag 的窗口 = 可生成的**源流**
SOURCE_STREAMS="$(awk '/^window /{w=$2} /stream_tag/{print w}' "$SCHEMA" | sort -u)"

# 规则 events 块里的窗口名 = `<alias> : <window>`
rule_event_streams() {
    awk '
      /^[[:space:]]*events[[:space:]]*\{/ { inside = 1 }
      inside && match($0, /:[[:space:]]*[A-Za-z_][A-Za-z0-9_]*/) {
          s = substr($0, RSTART, RLENGTH); sub(/^:[[:space:]]*/, "", s); print s
      }
      inside && /\}/ { inside = 0 }
    ' "$1"
}

# 该查询可生成的源流（events 窗口 ∩ 源流；链式查询的中间窗会被自然过滤掉）
query_source_streams() {
    rule_event_streams "$1" | sort -u | while IFS= read -r s; do
        printf '%s\n' "$SOURCE_STREAMS" | grep -qx "$s" && echo "$s"
    done
}

query_rules() { sed -n 's/^rule \([A-Za-z_][A-Za-z0-9_]*\).*/\1/p' "$1"; }

# 语料自带的 #[duration=...]（脚本的 --duration 会覆盖它）。
scenario_declared_duration() { sed -n 's/^#\[duration=\([^]]*\)\].*/\1/p' "$1" | head -1; }

# curated 语料必须真的含注入用例；只有 background 的文件"跑得通但什么都没验证"。
has_inject_assert() { grep -qE '^[[:space:]]*(hit|near_miss|miss)[[:space:]]*<' "$1"; }

# ---- 已知差异（`--with-engine` 里已定位且已记录的「期望 ↔ 引擎」模型差异）----
# 入库条件（缺一不可）：
#   ① README 语料表里能查到这条差异；② 已定位到具体机制（不是"还没查"）。
# 未登记的查询一旦 engine FAIL 一律判失败 —— 不允许把差异悄悄变成通过。
# 差异被修好后这里会变成 PASS，脚本会提示把它从表里删掉（自清理）。
known_diff_reason() {
    case "$1" in
        q4) printf '%s' "期望把内层 yield 当告警（引擎当中间窗，不落 sink；1 条 missing）+ 1d 桶收口告警（1 条 unexpected）" ;;
        *)  printf '' ;;
    esac
}

# 展开查询列表
expand_queries() {
    local out=() q
    for q in "${QUERIES[@]}"; do
        if [ "$q" = "all" ]; then
            # -t/  -k3.2,3n：按文件名的数字排序（`-t q` 会被 "queries" 里的 q 带偏 → q1,q10,q2…）
            while IFS= read -r f; do out+=("$(basename "$f" .wfl)"); done \
                < <(ls models/queries/q*.wfl 2>/dev/null | sort -t/ -k3.2,3n)
        else
            out+=("$q")
        fi
    done
    # bash 3.2 + set -u：空数组的 "${out[@]}" 会报 unbound variable → 先判长度
    [ "${#out[@]}" -gt 0 ] && printf '%s\n' "${out[@]}"
}

# 规则内联手写用例（`test` 块）——现成的「手写小表」。
# 为什么单独跑它：它是唯一能钉住**几何量**的地方（如 q5 的 `hits == size/slide`）；
# 引擎级对拍比的是「同一份规则的两套实现是否一致」，看不到规则本身偏离权威语义。
#
# 粒度：只在规则文件含 `test` 块时跑（无 test 块 = 无可断言，不输出）。
# 裁定：真断言失败 → FAILED=1；harness **刻意拒绝**的用例（无 WindowLookup 的
# join 类，错误文本含 `cannot assert hit counts`）记为「不适用」——它们永远红，
# 当成失败等于把红灯当信号（需 E2E 覆盖，见 wp-reactor contract.rs 的 P1 guard）。
run_rule_inline_tests() {
    q="$1"; rule="$2"
    grep -qE '^[[:space:]]*test[[:space:]]' "$rule" || return 0
    if [ -z "$WFL" ]; then
        echo "  [${q}] wfl test: N/A（未找到 wfl 二进制 → 规则内联用例未跑；设 WFL=/path/to/wfl）"
        return 0
    fi
    if [ "$HAVE_PY" != "1" ]; then
        echo "  [${q}] wfl test: N/A（未找到 python3 → 无法解析回执）"
        return 0
    fi
    json="$OUT_TMP/${q}_wfl_test.json"
    err="$OUT_TMP/${q}_wfl_test.err"
    "$WFL" test "$rule" -s "$SCHEMA" --format json > "$json" 2>"$err"
    out="$("$PY" - "$json" "$err" <<'PY'
import json, sys
try:
    d = json.load(open(sys.argv[1]))
    tests = d.get("tests", [])
except Exception:
    detail = ""
    try:
        lines = [l.rstrip() for l in open(sys.argv[2]) if l.strip()]
        detail = lines[0] if lines else ""
    except Exception:
        pass
    print(f"FAIL（回执解析失败：{detail or '空回执'}）")
    sys.exit(1)
real, unsup, ok = [], [], 0
for t in tests:
    if t.get("passed"):
        ok += 1; continue
    fs = t.get("failures", [])
    # harness 刻意拒绝（无 WindowLookup → join 必 miss）不是断言失败，见 contract.rs 的 P1 guard
    (unsup if any("cannot assert hit counts" in f for f in fs) else real).append(
        (t.get("name", "?"), fs[0] if fs else ""))
parts = [f"{ok}/{len(tests)} 通过"]
if unsup:
    parts.append(f"{len(unsup)} 条不适用（harness 无 WindowLookup，join 类无法内联断言命中数 → 需 E2E）")
if real:
    print("FAIL（" + " · ".join(parts) + "）")
    for n, f in real[:3]:
        print(f"        | {n}: {f}")
    sys.exit(1)
print("PASS（" + " · ".join(parts) + "）")
sys.exit(0)
PY
)"
    rc=$?
    printf '%s\n' "$out" | sed "1s|^|  [${q}] wfl test: |"
    [ "$rc" -eq 0 ] || FAILED=1
    N_UTEST=$((N_UTEST + 1))
}

# 表格：Q KIND LINT GEN OUT NOTE
# NOTE 放**最后一列且不补位**：它可能含多字节字符/变长文本，printf 按字节补位会串列。
row() { printf '%-5s %-8s %-6s %-5s %-22s %s\n' "$1" "$2" "$3" "$4" "$5" "$6"; }

EXPANDED="$(expand_queries)"
[ -n "$EXPANDED" ] || {
    echo "没有匹配到任何查询：${QUERIES[*]}（models/queries/q*.wfl 是否存在？）" >&2
    exit 2
}

row Q KIND LINT GEN OUT NOTE
printf '%s\n' '---------------------------------------------------------------------------------------------------------'
echo '  GEN 档 NOTE = 注入实体数/期望事件数/实生成事件数；FAIL 档 NOTE = lint/gen 报错。'
if [ "$WITH_ENGINE" = "1" ]; then
    echo '  --with-engine：额外的引擎级对拍（gen → dump-frames → wfusion batch → wfgen verify）。'
    echo '    ⚠ gen 的期望由 inject 驱动：场景无注入用例时 Expected=0，无可比对 → 记 N/A（不是通过）。'
fi

FAILED=0
SCAFFOLDED=0
N_CURATED=0
N_SMOKE=0
N_ENGINE=0
N_ENGINE_NA=0
N_KNOWN=0
N_UTEST=0
for q in $EXPANDED; do
    rule="models/queries/$q.wfl"
    scenario="scenarios/${q}_verify.wfg"
    kind="curated"
    tmp_scenario=""

    if [ ! -f "$rule" ]; then
        row "$q" - - - - "缺少规则 ${rule}"
        FAILED=1
        continue
    fi

    streams="$(query_source_streams "$rule")"
    if [ -z "$streams" ]; then
        row "$q" - - - - "无法从规则推出源流（events 块里没有 schema 的 stream_tag 窗口）"
        FAILED=1
        continue
    fi

    # ---- 语料选择：curated 优先；否则生成 smoke（--scaffold 时写正式模板）----
    if [ ! -f "$scenario" ]; then
        kind="smoke"
        if [ "$SCAFFOLD" = "1" ]; then
            kind="scaffold"
            scenario="scenarios/${q}_verify.wfg"
            tmp_scenario=""   # 正式落盘，不清理
        else
            scenario="scenarios/.${q}_smoke.wfg"   # 与 curated 同目录 → `use "../models/..."` 相对路径一致
            tmp_scenario="$scenario"
            TMP_FILES="$TMP_FILES $scenario"
        fi
        {
            echo "// 自动生成（${kind}）：${q}"
            if [ "$kind" = "smoke" ]; then
                echo "// smoke = 背景-only，**无注入断言**：仅验证规则/schema 未漂移。"
                echo "// 要真正验证规则语义，请参照 scenarios/q1_verify.wfg 写 scenarios/${q}_verify.wfg。"
            else
                echo "// 骨架：请补 inject 用例后再跑（见文件末 TODO）。"
                echo "// 注意：只补 background、不补 inject 的 curated 文件会被本脚本判 NOINJ 失败。"
            fi
            echo "use \"../models/schemas/nexmark.wfs\""
            echo "use \"../models/queries/${q}.wfl\""
            echo
            echo "#[duration=1m]"
            echo "scenario ${q}_verify<seed=1> {"
            echo "  background {"
            while IFS= read -r s; do echo "    stream ${s} gen 1000/s"; done <<< "$streams"
            echo "  }"
            if [ "$kind" = "scaffold" ]; then
                first_stream="$(echo "$streams" | head -1)"
                first_rule="$(query_rules "$rule" | head -1)"
                echo
                echo "  // TODO 按 ${q} 的规则语义补用例（参考 scenarios/q1_verify.wfg）："
                echo "  //   有阈值/窗口条件的规则：hit 构造成立、near_miss 差 1、miss 不成立；"
                echo "  //   \`on each\` 无阈值规则：只有 hit 有意义。"
                echo "  //   条目形态：hit<实体数> for <规则名> ${first_stream} { use(字段=值) x <步骤数> }"
                echo "  //   本文件涉及的规则：$(query_rules "$rule" | paste -sd, -)"
                echo "  // inject {"
                echo "  //   hit<100> for ${first_rule} ${first_stream} { use() x 1 }"
                echo "  // }"
            fi
            echo "}"
        } > "$scenario"
        if [ "$kind" = "scaffold" ]; then
            row "$q" "$kind" - - - "已写出 ${scenario}（补完 TODO 后重跑）"
            SCAFFOLDED=1
            continue
        fi
    fi

    # ---- curated 必须是真语料：没有 hit/near_miss/miss 就等于"没验证" ----
    if [ "$kind" = "curated" ] && ! has_inject_assert "$scenario"; then
        row "$q" "$kind" NOINJ - "$scenario" "curated 语料无注入用例 → 等于未验证；补 inject 或删掉该文件回落 smoke"
        FAILED=1
        continue
    fi

    # ---- L0 静态校验 ----
    lint_out="$("$WFGEN" lint "$scenario" 2>&1)"
    if [ "$lint_out" = "OK" ]; then
        lint_cell="OK"
        lint_msg="-"
    else
        lint_cell="FAIL"
        # 只取前 2 行拼成一行：不用 `cut -c` 截字节，避免把多字节字符从中间劈开
        lint_msg="$(printf '%s' "$lint_out" | sed -n '1,2p' | tr '\n' ' ' | sed 's/ *$//')"
        FAILED=1
    fi

    # ---- L0' 规则内联手写用例（`test` 块）：跑真引擎 match-engine ----
    # 这是现成的「手写小表」：输入行 + `hits` / `hit[i].{entity_id,origin,field(...)}` 硬断言。
    # 不需要生成数据，所以在 --lint-only 下也跑（静态层）。

    if [ "$LINT_ONLY" = "1" ] || [ "$lint_cell" = "FAIL" ]; then
        row "$q" "$kind" "$lint_cell" - - "$lint_msg"
        run_rule_inline_tests "$q" "$rule"
    else
        out_dir="${OUT_ROOT}/${q}"
        mkdir -p "$OUT_ROOT"      # 仅真正落数据时才建（--lint-only 不留空目录）
        rm -rf "$out_dir"
        gen_args=(--scenario "$scenario" --out "$out_dir")
        [ -n "$DURATION" ] && gen_args+=(--duration "$DURATION")
        if gen_out="$("$WFGEN" gen "${gen_args[@]}" 2>&1)"; then
            gen_cell="OK"
        else
            gen_cell="FAIL"
            FAILED=1
        fi
        # 注：hit 必报由 wfgen gen 内部硬断言（INJ1）保证，失败即非零退出 → gen_cell=FAIL
        inj="$(printf '%s' "$gen_out" | sed -n 's/^Inject assert: *//p' | head -1)"
        expected="$(printf '%s' "$gen_out" | sed -n 's/^Expected: *\([0-9,]*\) .*/\1/p' | head -1)"
        events="$(printf '%s' "$gen_out" | sed -n 's/^Generated *\([0-9,]*\) events.*/\1/p' | head -1)"
        note="${inj:-gen 失败}"
        if [ -n "$events" ]; then
            if [ -n "$inj" ]; then
                note="entities ${inj%% *} · expected=${expected} · events=${events}"
            else
                note="无注入断言（smoke）· events=${events}"
            fi
            [ -n "$DURATION" ] && note="$note · dur=$DURATION"
        fi
        row "$q" "$kind" "$lint_cell" "$gen_cell" "$out_dir" "$note"
        run_rule_inline_tests "$q" "$rule"
        [ "$gen_cell" = "FAIL" ] && printf '%s\n' "$(printf '%s' "$gen_out" | sed 's/^/        | /')"
        # --duration 覆盖语料的 #[duration] 会改窗口切分：top-N 类语料的 near_miss（差 1 票）
        # 只在原切分下成立——实话实说，避免把「覆盖参数」当成「语料坏了」。
        if [ "$gen_cell" = "FAIL" ] && [ -n "$DURATION" ]; then
            declared="$(scenario_declared_duration "$scenario")"
            if [ -n "$declared" ] && [ "$declared" != "$DURATION" ]; then
                echo "        | 注：--duration ${DURATION} 覆盖了语料的 #[duration=${declared}]；窗口切分敏感的语料（如 q5 的 top-N near_miss）可能因此不再成立" >&2
            fi
        fi

        # ---- 引擎级对拍（--with-engine）：gen 产物 → wfusion batch → wfgen verify ----
        # ⚠ gen 的期望由 `inject` 驱动：场景里没有注入用例时 `Expected: 0`，
        #   期望文件是空的 → 无可比对。smoke 场景天然如此，不去白跑引擎。
        if [ "$WITH_ENGINE" = "1" ] && [ "$gen_cell" = "OK" ]; then
            if ! has_inject_assert "$scenario"; then
                echo "  [$q] engine: N/A（场景无 inject 用例 → gen 的期望为空，无可比对；要引擎级证据请写 curated 语料）"
                N_ENGINE_NA=$((N_ENGINE_NA + 1))
            else
                echo "  [$q] engine: dump-frames → wfusion batch（文件源，跑完自退）→ wfgen verify"
                verdict="$(run_engine_verify "$q" "$scenario" "$out_dir")"
                known="$(known_diff_reason "$q")"
                case "$verdict" in
                    PASS)
                        echo "  [$q] engine: PASS（报告 $out_dir/engine_verify.json）"
                        N_ENGINE=$((N_ENGINE + 1))
                        [ -n "$known" ] && echo "    注：$q 在已知差异表里却 PASS 了 —— 差异可能已修复，请从 KNOWN_DIFF 与 README 删掉。"
                        ;;
                    NA)
                        echo "  [$q] engine: N/A（期望与引擎实际都是 0 条，空对空不算证据）"
                        N_ENGINE_NA=$((N_ENGINE_NA + 1))
                        ;;
                    *)
                        "$PY" -c '
import json, sys
d = json.load(open(sys.argv[1]))
s = d["summary"]
print("        | " + " ".join(f"{k}={s[k]}" for k in
      ("expected_total", "actual_total", "matched", "missing", "unexpected", "field_mismatch")))
for kind, key in (("missing", "missing_details"), ("unexpected", "unexpected_details"),
                  ("mismatch", "mismatch_details")):
    for a in d.get(key, [])[:3]:
        t = a.get("expected_time") or a.get("time", "")
        name, etype, eid = a["rule_name"], a["entity_type"], a["entity_id"]
        print(f"        | {kind}: {name} {etype}={eid} @{t}")
' "$out_dir/engine_verify.json" >&2 || true
                        if [ -n "$known" ]; then
                            echo "  [$q] engine: FAIL(已知差异，不计失败) —— ${known}（README 语料表）" >&2
                            N_KNOWN=$((N_KNOWN + 1))
                        else
                            echo "  [$q] engine: FAIL（报告 $out_dir/engine_verify.json，引擎日志 $out_dir/engine_batch.log）" >&2
                            FAILED=1
                        fi
                        ;;
                esac
            fi
        fi
    fi

    if [ "$kind" = "curated" ]; then N_CURATED=$((N_CURATED + 1)); else N_SMOKE=$((N_SMOKE + 1)); fi

    [ -n "$tmp_scenario" ] && [ "$KEEP" != "1" ] && rm -f "$tmp_scenario"
done

echo
if [ "$SCAFFOLDED" = "1" ]; then
    echo "已生成容器（scaffold）：补完 TODO 后重跑本脚本即可按 curated 口径执行。"
fi
if [ "$FAILED" = "1" ]; then
    echo "== 结果：有失败 ==" >&2
    exit 1
fi
if [ $((N_CURATED + N_SMOKE)) -eq 0 ]; then
    echo "== 结果：未执行验证（仅写出骨架）=="
    exit 0
fi
if [ "$WITH_ENGINE" = "1" ]; then
    echo "== 结果：全部通过 ==（curated ${N_CURATED} · smoke ${N_SMOKE} · engine PASS ${N_ENGINE} / 已知差异 ${N_KNOWN} / N/A ${N_ENGINE_NA}）"
else
    echo "== 结果：全部通过 ==（curated ${N_CURATED} · smoke ${N_SMOKE}）"
fi
if [ "$N_SMOKE" -gt 0 ]; then
    echo "   注：smoke 档只证明规则/schema 未漂移，**不含语义断言**。"
fi
if [ "$N_KNOWN" -gt 0 ]; then
    echo "   注：已知差异 ${N_KNOWN} 条（已定位 + 已记录，不计失败，见脚本 KNOWN_DIFF 与 README 语料表）。"
fi
if [ "$N_UTEST" -gt 0 ]; then
    echo "   注：规则内联用例（\`test\` 块，真引擎 match-engine）已跑 ${N_UTEST} 个规则文件；断言失败即判失败。"
fi
