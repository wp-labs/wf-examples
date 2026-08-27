#!/usr/bin/env bash
# qradar_pk diag.sh — 性能墙定位（perf-diag 诊断模式驱动）
#
# 回答的问题：**450 规则负载的吞吐墙在哪一段/哪一族**，而不是「吞吐是多少」（那是 run.sh 的事）。
#
#   run.sh   = 基准：全量跑，出 EPS/RSS/#18 门禁（对标 QRadar EP 80k @ 451 规则）
#   diag.sh  = 诊断：六档墙梯逐段切除 + 可选规则家族档，出每段增量成本 + 墙判定
#
# 机制：wfusion daemon --perf-diag conf/perf-diag-wall.toml（哨兵驱动自切换、单 daemon
# 不重启）+ wfgen perf-diag 驱动。设计见 wp-reactor/docs/design/perf-diag-mode-design.md，
# 用法见 wp-reactor/docs/user-guide/perf-diag.md，方法论见 wp-reactor/docs/PERF_BISECTION_METHOD.md。
#
# 度量逻辑（哨兵解析/EPS/CPU-RSS 采样/墙分析）在共享文件 ../scripts/bench_lib.py 与
# ../scripts/diag_analyze.py（与 nexmark_pk 的 bench.sh/diag.sh 共用）——本脚本只做流程编排。
#
# 用法:
#   ./diag.sh [N=200000]
#   FAMILIES=c,g,dist ./diag.sh 200000     # 规则家族档（定位哪一族规则贵）
#
# 环境变量:
#   WARMUP=0            关预热（默认开）。预热 = 墙梯前插一个丢弃的 warmup 档（全链路），
#                       消除首档的窗口冷分配/page fault 偏差（nexmark 实测可低 25%）。
#                       预热档数字一律不显示（与后续档状态不可比）。
#   FAMILIES=c,g,dist   规则家族档：按 rule 名前缀抽子集（data/diag_rules_<fam>.wfl）经
#                       runtime.rules 热 reload 切换。此模式下墙梯 = floor + 各家族档
#                       （家族之间**非叠加**，增量一律相对 floor 计算），不含全量档。
#                       可用前缀见 ./diag.sh --list-families
#   STAGES=recv,decode,floor,rules,emit,full   自定义叠加式墙梯（默认用 conf/perf-diag-wall.toml 的六档）
#   KEEP_RATE=1         保留 conf/wfusion.toml 的 max_ingest_rate（默认**解除**：150k 限速
#                       会把六档全封顶在 150k，墙梯失去区分度——README 测量纪律 §3）
#   PARSE_PARALLELISM= / RULE_PARALLELISM=   并行度（默认取 conf/wfusion.toml）
#   FRAMES=path         直接指定帧文件（默认 data/burst_<N>.frames，与 run.sh 共享）
#   GEN_FRAMES=1        缺帧时自动生成（默认 1：gen_events.py + dump-frames）
#   SAMPLE_MS=100       CPU%/RSS 采样周期（档时长短时决定 CPU 归属可信度）
#   TIMEOUT_SECS=       单次等待超时（默认 N/20000+120；450 规则稳态 ~150k/s，留足余量）
#   WF_DIAG_MAX_TOTAL_BYTES=0|8GB|60%  诊断模式全局窗口内存 cap（引擎侧）：默认 =
#                       物理内存 60%（墙梯重发同一份数据 N 次会放大窗口内存压力，
#                       cap 过小 → commit_append 停车 → 内存墙错报成计算墙，q20 已证伪）；
#                       0 = 沿用配置（测内存约束）；显式字节/百分比可调（引擎启动日志
#                       `perf-diag 内存口径` 打印实际值，报告口径行自动带上）
#
# 输出:
#   data/diag_<N>.txt              诊断报告（墙表 + 增量成本 + 墙判定 + 健康）
#   data/perf_sentinel.ndjson      哨兵四元组（EPS 单一事实源，每档一条）
#   data/perf_diag_wall.txt        wfgen 原始墙表（交叉校验用）
#   data/diag_daemon.log           引擎日志
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

PY="${PYTHON:-python3}"
LIB="../scripts/bench_lib.py"          # 度量工具（comma/parse-n/diag-sampler）
ANALYZER="../scripts/diag_analyze.py"   # 墙表 + 墙判定 + 健康分析（共享）
PORT=9800
RULES_SRC="models/rules/throughput.wfl"

# ---- --list-families：列出可用规则家族前缀 + 规则数 ----
if [ "${1:-}" = "--list-families" ]; then
  echo "可用规则家族（前缀 规则数），共 $(grep -c '^rule ' "$RULES_SRC") 条："
  grep '^rule ' "$RULES_SRC" | sed 's/^rule \([a-z]*\)_.*/\1/' | sort | uniq -c | sort -rn | awk '{printf "  %-6s %s\n", $2, $1}'
  exit 0
fi

N="${1:-200000}"
case "$N" in
  ''|*[!0-9]*) echo "用法: ./diag.sh [事件数=200000]（或 --list-families）" >&2; exit 1;;
esac

WARMUP="${WARMUP:-1}"
FAMILIES="${FAMILIES:-}"
KEEP_RATE="${KEEP_RATE:-0}"
GEN_FRAMES="${GEN_FRAMES:-1}"
SAMPLE_MS="${SAMPLE_MS:-100}"
PARSE="${PARSE_PARALLELISM:-}"
RULE="${RULE_PARALLELISM:-}"
CONF_TMP=/tmp/qdiag_conf.toml
SAMPLES=/tmp/qdiag_samples.txt
DIAG_TOML="${DIAG_TOML:-conf/perf-diag-wall.toml}"
STREAMS="auth_events,conn_events,dns_events,file_events,firewall_events,proxy_events"
DAEMON_PID=""
SAMPLER_PID=""

# ---- 二进制来源（与 run.sh 同款：本地 release 优先，回退 PATH）----
REPO_ROOT="${REPO_ROOT:-}"
if [ -z "$REPO_ROOT" ] && [ -d "../../../warp-fusion" ]; then REPO_ROOT="$(cd ../../../warp-fusion && pwd)"; fi
WFUSION="${WFUSION:-}"; WFGEN="${WFGEN:-}"
if [ -z "$WFUSION" ] && [ -n "$REPO_ROOT" ] && [ -x "$REPO_ROOT/target/release/wfusion" ]; then WFUSION="$REPO_ROOT/target/release/wfusion"; fi
if [ -z "$WFGEN" ] && [ -n "$REPO_ROOT" ] && [ -x "$REPO_ROOT/target/release/wfgen" ]; then WFGEN="$REPO_ROOT/target/release/wfgen"; fi
if [ -z "$WFUSION" ]; then WFUSION="$(command -v wfusion 2>/dev/null || true)"; fi
if [ -z "$WFGEN" ]; then WFGEN="$(command -v wfgen 2>/dev/null || true)"; fi
if [ -z "$WFUSION" ] || [ -z "$WFGEN" ]; then
  echo "错误: 找不到 wfusion/wfgen（设置 REPO_ROOT/WFUSION/WFGEN 或加入 PATH）" >&2; exit 1
fi

comma() { "$PY" "$LIB" comma "$1" 2>/dev/null || echo "$1"; }

mkdir -p data

# ---- 规则家族子集：按 rule 名前缀从 throughput.wfl 抽块（gen_rules.py 产物结构固定）----
write_family_rules() {
  "$PY" - "$RULES_SRC" "$1" "$2" <<'EOF'
import re, sys
src, fam, out = sys.argv[1], sys.argv[2], sys.argv[3]
lines = open(src).read().split('\n')
head = [l for l in lines[:5] if l.startswith('use ')]
keep, cur, name = [], None, None
for l in lines:
    m = re.match(r'^rule\s+(\S+)\s*\{', l)
    if m:
        cur, name = [l], m.group(1)
        continue
    if cur is not None:
        cur.append(l)
        if l == '}':
            if name.split('_')[0] == fam:
                keep.append('\n'.join(cur))
            cur, name = None, None
open(out, 'w').write('\n'.join(head) + '\n\n' + '\n\n'.join(keep) + '\n')
print(len(keep))
EOF
}

# ---- 墙梯配置：默认 committed 六档 + 预热；STAGES/WARMUP 时生成临时档 ----
FAM_COUNTS=""     # "fam:规则数 ..." 供报告用
FAMILIES_LIST=""
if [ -n "$FAMILIES" ] || [ -n "${STAGES:-}" ] || [ "$WARMUP" = "1" ]; then
  DIAG_TOML=data/perf-diag-wall.toml
  : > "$DIAG_TOML"
  echo "# diag.sh 生成（families=${FAMILIES:-none} stages=${STAGES:-default} warmup=${WARMUP}）——勿手改" >> "$DIAG_TOML"
  # 预热档：全链路（不切），把窗口缓冲/规则状态/输出链跑热，分析时丢弃（数字不显示）。
  [ "$WARMUP" = "1" ] && printf '\n[[stages]]\nname = "warmup"\ncut_rules = false\ncut_output = false\nrules = ""\n' >> "$DIAG_TOML"
  if [ -n "$FAMILIES" ]; then
    # 家族档模式：**每家族独立 daemon 会话**（启动即加载子集规则，见主流程）。
    # 不用多档 reload 切规则——子集引用窗口 < 全量 → reload 必 Blocked
    # （hot_reload/topology.rs: 编译后 schema 集合有移除 → requires restart），
    # 实际跑的是全量 450 规则的墙（2026-08-24 实测：fam_c 单跑 reload blocked，
    # 31k 是全量墙；启动加载子集后才测出真实值 1.9µs/条）。
    for fam in $(echo "$FAMILIES" | tr ',' ' '); do
      CNT=$(write_family_rules "$fam" "data/diag_rules_${fam}.wfl")
      [ "${CNT:-0}" -gt 0 ] || { echo "错误: 家族 '${fam}' 无匹配规则（./diag.sh --list-families 看可用前缀）" >&2; exit 1; }
      FAM_COUNTS="${FAM_COUNTS}${FAM_COUNTS:+ }${fam}:${CNT}"
      echo "  家族 ${fam}: ${CNT} 条 → data/diag_rules_${fam}.wfl（每家族独立 daemon）"
    done
    FAMILIES_LIST="$FAMILIES"
  else
    for st in $(echo "${STAGES:-recv,decode,floor,rules,emit,full}" | tr ',' ' '); do
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
fi
if [ -z "$FAMILIES_LIST" ]; then
  [ -f "$DIAG_TOML" ] || { echo "错误: 墙梯配置 ${DIAG_TOML} 不存在" >&2; exit 1; }
  STAGE_NAMES=$(grep '^name = ' "$DIAG_TOML" | sed 's/name = "\(.*\)"/\1/' | tr '\n' ',' | sed 's/,$//')
  STAGE_COUNT=$(echo "$STAGE_NAMES" | tr ',' '\n' | grep -c .)
  [ "$STAGE_COUNT" -ge 2 ] || { echo "错误: ${DIAG_TOML} 至少需 2 档才有墙梯（当前 ${STAGE_COUNT}）" >&2; exit 1; }
fi

CORES=$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 0)
TIMEOUT_SECS="${TIMEOUT_SECS:-$(( N / 20000 + 120 ))}"
echo "== diag: N=$(comma "$N") 档=${STAGE_NAMES:-${FAMILIES_LIST:-default}} cores=${CORES} timeout=${TIMEOUT_SECS}s =="

# ---- daemon 生命周期（与 run.sh/bench.sh 同款纪律）----
wait_port_free() {
  for i in $(seq 1 50); do nc -z 127.0.0.1 "$PORT" 2>/dev/null || return 0; sleep 0.2; done
  echo "    警告: 端口 ${PORT} 超时未释放" >&2
}
kill_daemon() {
  local P="$1"; [ -n "$P" ] || return 0
  kill "$P" 2>/dev/null
  # 宽限 60s：close/stats 规则的 shutdown flush 需数秒~数十秒，提前 SIGKILL 会
  # 截断最终 metrics 导出（尾部 emitted 计数丢失）。
  for i in $(seq 1 300); do kill -0 "$P" 2>/dev/null || { sleep 1; return 0; }; sleep 0.2; done
  echo "    警告: daemon ${P} SIGTERM 后 60s 未退出，强制 SIGKILL" >&2
  kill -9 "$P" 2>/dev/null
}
cleanup() {
  [ -n "$SAMPLER_PID" ] && kill "$SAMPLER_PID" 2>/dev/null
  pkill -9 -f "wfusion daemon" 2>/dev/null
  pkill -9 -f "wfgen send-arrow" 2>/dev/null
  sleep 1; wait_port_free
}
cleanup; trap cleanup EXIT INT TERM

# 诊断 conf：基于 conf/wfusion.toml 覆盖并行度 + **解除入流限速**
# 注意：**不改 report_interval**（与 nexmark_pk 相反）。450 条规则下每区间导出 ~8.7k 行
# 指标，改 100ms 会让 exporter 自己成为负载并**丢样本**（实测 40s 跑批只导出 52 个区间，
# append_total 求和只到 19.5%）。哨兵口径不依赖 metrics 粒度，保持 1s 对 EPS 无影响。
write_conf() {
  # $1 = 启动加载的规则文件（家族档模式传子集文件；默认 conf 的 models/rules/*.wfl）
  local RULES_FILE="${1:-models/rules/*.wfl}"
  PARSE_EFF="${PARSE:-$(sed -n 's/^parse_parallelism = *//p' conf/wfusion.toml | head -1 | tr -d ' ')}"
  RULE_EFF="${RULE:-$(sed -n 's/^rule_parallelism = *//p' conf/wfusion.toml | head -1 | tr -d ' ')}"
  sed -e "s|^rules = .*|rules = \"${RULES_FILE}\"|" \
      -e "s|^parse_parallelism = .*|parse_parallelism = ${PARSE_EFF}|" \
      -e "s|^rule_parallelism = .*|rule_parallelism = ${RULE_EFF}|" \
      conf/wfusion.toml > "$CONF_TMP"
  if [ "$KEEP_RATE" != "1" ]; then
    # 限速会把每一档都封顶在 max_ingest_rate（150k）——墙梯六档全等，定位失效。
    sed -i.bak 's|^max_ingest_rate = |# diag.sh 解除限速（墙梯需要各段真实上限）: max_ingest_rate = |' "$CONF_TMP"
    rm -f "${CONF_TMP}.bak"
    RATE_CTX="unlimited"
  else
    RATE_CTX="$(sed -n 's/^max_ingest_rate = *//p' conf/wfusion.toml | head -1 | tr -d ' ')"
  fi
}

start_daemon() {
  local LOG="$1"
  "$WFUSION" daemon --config "$CONF_TMP" --work-dir . --perf-diag "$DIAG_TOML" > "$LOG" 2>&1 &
  DAEMON_PID=$!
  for i in $(seq 1 60); do
    if ! kill -0 "$DAEMON_PID" 2>/dev/null; then
      echo "    错误: daemon 启动失败（进程已退出）——${LOG} 尾部：" >&2; tail -20 "$LOG" >&2; return 1
    fi
    nc -z 127.0.0.1 "$PORT" 2>/dev/null && break
    sleep 0.2
  done
  nc -z 127.0.0.1 "$PORT" 2>/dev/null || {
    echo "    错误: daemon 启动超时（12s 端口未监听）——${LOG} 尾部：" >&2; tail -20 "$LOG" >&2; return 1; }
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

# ---- 帧文件：与 run.sh 共享 data/burst_<N>.frames ----
ensure_frames() {
  if [ -n "${FRAMES:-}" ]; then
    [ -s "$FRAMES" ] || { echo "错误: FRAMES=${FRAMES} 不存在或为空" >&2; return 1; }
    return 0
  fi
  FRAMES="data/burst_${N}.frames"
  [ -s "$FRAMES" ] && { echo "  frames: ${FRAMES}（复用，$(du -h "$FRAMES" | cut -f1)）"; return 0; }
  if [ "$GEN_FRAMES" != "1" ]; then
    echo "错误: 缺帧文件 ${FRAMES}（GEN_FRAMES=1 生成，或 ./run.sh ${N} 顺带生成）" >&2; return 1
  fi
  echo "==> 生成 $(comma "$N") 事件（源 IP 长尾 + 泊松时间 12min 固定跨度）→ 预编码帧"
  "$PY" scripts/gen_events.py "$N" > data/diag_gen.jsonl || {
    echo "    错误: gen_events.py 失败" >&2; rm -f data/diag_gen.jsonl; return 1; }
  # dump-frames 需要一个在跑的 daemon 做 schema 握手（与 run.sh 同款）
  write_conf
  start_daemon data/diag_daemon_frames.log || return 1
  "$WFGEN" dump-frames --scenario scenarios/throughput.wfg --input data/diag_gen.jsonl \
    --addr 127.0.0.1:$PORT --ws models/schemas/network.wfs --output "$FRAMES" \
    --chunk 10000 --max-frame-bytes 8388608 --max-frame-rows 100000 > /dev/null 2>&1
  kill_daemon "$DAEMON_PID"; DAEMON_PID=""; wait_port_free
  rm -f data/diag_gen.jsonl
  [ -s "$FRAMES" ] || { echo "    错误: dump-frames 产物为空（已删坏缓存）" >&2; rm -f "$FRAMES"; return 1; }
  echo "  frames: ${FRAMES}（$(du -h "$FRAMES" | cut -f1)）"
}

# ---- 单次墙梯 ----
# $2 = 启动加载的规则文件（家族档模式传子集；空 = 全量）。家族模式每家族一套
# 独立 daemon（TOML = warmup?+floor+fam_X，无 reload——避免 schema 移除 Blocked）。
run_ladder() {
  local OUT="$1" RULES_FILE="${2:-}"
  local LOG="data/diag_daemon.log"
  write_conf "$RULES_FILE"
  # 哨兵文件必须在 daemon 启动**前**清空：daemon 启动即写 stage{current=0}，
  # 启动后再删会擦掉这行 → wfgen 首个 wait_for_stage 直接超时（设计文档 §7 坑）。
  : > data/perf_sentinel.ndjson; : > data/perf_diag_wall.txt
  : > data/metrics.ndjson; : > data/wfusion.log; : > "$SAMPLES"
  start_daemon "$LOG" || return 1
  start_sampler "$DAEMON_PID"

  "$WFGEN" perf-diag --diag "$DIAG_TOML" --frames "$FRAMES" --addr "127.0.0.1:$PORT" \
    --n-list "$N" --rounds 1 --timeout-secs "$TIMEOUT_SECS" \
    --sentinels data/perf_sentinel.ndjson --output data/perf_diag_wall.txt
  local RC=$?
  [ "$RC" = 0 ] || echo "    ⚠ wfgen perf-diag 退出码 ${RC}（报告按已落盘哨兵记录尽力分析）" >&2

  kill "$SAMPLER_PID" 2>/dev/null; wait "$SAMPLER_PID" 2>/dev/null; SAMPLER_PID=""
  sleep 3   # 让 metrics 最后一拍导出（report_interval 保持 conf 原值 1s）
  kill_daemon "$DAEMON_PID"; DAEMON_PID=""; wait_port_free

  local LD; LD=$(sysctl -n vm.loadavg 2>/dev/null | awk '{printf "%.1f", $2}')
  local RULES_N; RULES_N=$(grep -c '^rule ' "${RULES_FILE:-$RULES_SRC}" 2>/dev/null || grep -c '^rule ' "$RULES_SRC")
  # 引擎打印的窗口内存口径（诊断模式默认 60% 物理内存 / WF_DIAG_MAX_TOTAL_BYTES 可调）
  # 先剥 ANSI 色码再截到结构字段 ` window_mem_cap=` 前，兼容无括号的来源（如 =0 关闭覆盖）。
  local MEM_CAP; MEM_CAP=$(sed 's/\x1b\[[0-9;]*m//g' "$LOG" | grep -o 'perf-diag 内存口径: max_total_bytes=.* window_mem_cap=' | head -1 | sed -e 's/^perf-diag 内存口径: //' -e 's/ window_mem_cap=$//')
  local CTX="p=${PARSE_EFF} r=${RULE_EFF} ingest=${RATE_CTX} rules=${RULES_N} load=${LD:-n/a}${MEM_CAP:+ · ${MEM_CAP}} · $(date +%m-%d_%H:%M:%S)"
  # 分析：共享 diag_analyze.py（哨兵四元组 × CPU/RSS 采样 × metrics 健康），
  # 输入走环境变量；stdout = 报告，退出码 0=健康 / 1=硬失败。
  QUERY="qradar" N="$N" CTX="$CTX" \
  RULES_COUNT="$RULES_N" \
  STAGE_NAMES="$STAGE_NAMES" CORES="$CORES" \
  SENT_PATH="data/perf_sentinel.ndjson" SAMPLES_PATH="$SAMPLES" \
  METRICS_PATH="data/metrics.ndjson" LOG_PATH="$LOG" \
  STREAMS="$STREAMS" FAM_COUNTS="$FAM_COUNTS" \
    "$PY" "$ANALYZER" | tee -a "$OUT"
  return "${PIPESTATUS[0]}"
}

# ---- 主流程 ----
ensure_frames || exit 1
OUT="data/diag_${N}.txt"
: > "$OUT"
FAILED=0
if [ -n "$FAMILIES_LIST" ]; then
  # 家族档：每家族独立 daemon 会话，启动即加载子集（绕开 reload Blocked）。
  for fam in $(echo "$FAMILIES_LIST" | tr ',' ' '); do
    DIAG_TOML="data/perf-diag-wall-${fam}.toml"
    : > "$DIAG_TOML"
    echo "# diag.sh 家族档 ${fam}（启动即加载子集，无 reload）——勿手改" >> "$DIAG_TOML"
    [ "$WARMUP" = "1" ] && printf '\n[[stages]]\nname = "warmup"\ncut_rules = false\ncut_output = false\nrules = ""\n' >> "$DIAG_TOML"
    printf '\n[[stages]]\nname = "floor"\ncut_rules = true\ncut_output = true\nrules = ""\n' >> "$DIAG_TOML"
    printf '\n[[stages]]\nname = "fam_%s"\ncut_rules = false\ncut_output = true\nrules = ""\n' "$fam" >> "$DIAG_TOML"
    STAGE_NAMES="$( [ "$WARMUP" = "1" ] && echo -n 'warmup,' )floor,fam_${fam}"
    echo "== 家族 ${fam}（独立 daemon，启动加载 data/diag_rules_${fam}.wfl）=="
    run_ladder "$OUT" "data/diag_rules_${fam}.wfl" || FAILED=1
  done
else
  run_ladder "$OUT" || FAILED=1
fi
RC=$FAILED
echo "  → 报告: ${OUT}"
if [ "$RC" = 0 ]; then echo "== diag 完成 =="; else echo "== diag 完成（存在告警/失败项，见报告）==" >&2; exit 1; fi
