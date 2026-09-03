#!/usr/bin/env bash
# nexmark_pk bench — 参数化吞吐/内存测试（send-arrow 连续流 或 wfgen stream 实时生成）
#
# feed:
#   replay = send-arrow 重放预编码帧（旧名 cont）：100M 唯一事件预编码成帧文件，CONNECTIONS 条 TCP
#            连接并发推（默认 1；每条连接推完整帧文件 —— C-UCP 供给并发，配套
#            引擎 source `instances` 消化，见 wp-reactor docs/design/concurrency-scaling.md）
#            （3M+，事件时间固定为预生成数据的 ~30min span）—— 测引擎峰值持续能力
#   stream = wfgen stream 实时生成：事件时间随 slice 推进、按 RATE 目标速率注入
#            （~760k，客户端实时编码受限）—— 测长时实时流稳定性/内存有界
#
# 用法:
#   ./bench.sh [query=q1|..|q22|all|mix] [feed=replay|stream] [total=100m|30m|10m]   (旧名 cont 已移除)
#   query=all  逐个跑：每个查询一个独立 daemon（单规则集），顺序循环全部查询（不含 q6）
#   query=mix  混跑：全部规则同时加载进**一个** daemon（多规则同跑，测合并吞吐，不含 q6）
#              ——与 all 的区别：all 每查询单独跑（规则互不干扰，输出每查询一行）；
#                mix 所有规则争同一引擎（parse/rule 并行度共享，规则间资源竞争真实可见，
#                只输出一行合并吞吐）。
#   ./bench.sh clean [cache|all]   清除生成数据：
#       cache（默认）= 预编码帧/分片缓存 + 日志 + 临时文件（可再生，磁盘大头，
#                     典型 ~10G/100m）；保留结果文件 data/bench_*.txt
#       all          = 连结果文件 data/bench_*.txt、data/verify_*.txt 一起删
#   调优用环境变量（并行度默认取 conf/wfusion.toml）:
#     PARSE_PARALLELISM / RULE_PARALLELISM / MAX_FRAME_BYTES / MAX_FRAME_ROWS
#     MAX_INGEST_RATE（引擎端限速）/ RATE / SLICE_MS（stream）
#     CONNECTIONS（replay 并发连接数，默认 1——2026-08-20 起默认单连接：
#       gen-nexmark 输出已按事件时间排序（v2 数据），单连接整文件推保持时间
#       有序 → over=10m 时间驱逐生效 → 窗口只持 ~10 分钟数据 → 内存/吞吐双赢
#       （q1 100M：RSS 24GB→3GB、EPS 11M→26M，正确性 clean）。多连接
#       （显式 CONNECTIONS>1）会让批次时间乱序（时间驱逐失效、窗口持全量、
#       内存膨胀），仅在有状态负载需要键闭包分片时使用，并配 SHARD_KEYS）
#     SHARD_KEYS（键闭包分片键，默认空=不分片单连接推整文件；CONNECTIONS>1 时
#       配 "bid_events:auction,auction_events:id,person_events:id" 走生成时
#       shard-frames --shard-files，同 key 同连接，有状态负载也安全）
#     WARMUP=1（replay：先跑一轮预热不计结果——stash 重建后首跑系统性偏低，须剔除）
# 示例:
#   PARSE_PARALLELISM=6 RULE_PARALLELISM=6 MAX_FRAME_BYTES=204800 ./bench.sh q1 replay 100m
#   WARMUP=1 ./bench.sh all replay 30m
#   ./bench.sh mix replay 30m     # 全部规则一个 daemon 混跑（合并吞吐，对照 all 逐个均值）
#   CONNECTIONS=4 SHARD_KEYS="bid_events:auction,auction_events:id,person_events:id" ./bench.sh q2 replay 30m  # 有状态:键闭包多连接
#   DATA_VER=old ./bench.sh q1 replay 100m   # 强制用旧乱序数据复现对比
#
# 输出每查询: EPS + RSS 峰值 + 驱逐数
#   + 口径上下文（并行度/帧大小/时间戳）+ 正确性计数器摘要
# 计时终点/完成信号 = 哨兵四元组（data/perf_sentinel.ndjson）：daemon 以
# --perf-diag conf/perf-diag.toml 启动（无档 = 门控全 false，性能零影响，仅注册
# __wf_sentinel 哨兵窗口），send-arrow/stream 以 --sentinel 启用哨兵——**分连接**
# 发送：每条连接 copy 完自己的数据后追加哨兵帧（round=连接号, n=该连接实际行数,
# start_ns=该连接开始），单连接 = 1 条（round=0）；引擎等**数据窗排空**后写
# {round,n,start_ns,emit_ns}，EPS = Σn/(max emit_ns − min start_ns) 精确可算
# （无 metrics 轮询的 ±200ms 误差）。哨兵超时退回 metrics append+acked_lag
# 轮询兑底（TIMEOUT 标注）。
# 结果写 data/bench_<query>_<feed>.txt（含完整 correctness 明细附录）
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

# ---- clean：清除生成数据（缓存/日志/临时 → 结果文件） ----
# 必须在 QUERY 校验之前拦截（clean 不是 query）。
# 缓存按 TOTAL×DATA_VER×帧大小×分片键可再生成（gen-nexmark → dump-frames/
# shard-frames），是磁盘大头（100m 帧缓存 ~7.2G）；结果 txt 是测量记录保留。
# 内联 daemon 清理（不依赖后文函数定义）：删除前确保没有进程在写。
if [ "${1:-}" = "clean" ]; then
  CLEAN_MODE="${2:-cache}"
  case "$CLEAN_MODE" in
    cache|all)
      echo "== bench.sh clean ${CLEAN_MODE}: 清除生成数据 =="
      pkill -9 -f "wfusion daemon" 2>/dev/null
      pkill -9 -f "wfgen send-arrow" 2>/dev/null
      sleep 1
      # 大缓存：预编码帧 + 键闭包分片帧 + mix 规则 symlink 清单（可再生）
      rm -f data/bench_*.frames data/shard_*.frames
      rm -rf data/mix_rules
      # 日志/临时：运行残留（start_daemon 每次 rm -f 重写，可任意删）
      rm -f data/metrics.ndjson data/wfusion.log data/daemon.log data/stream.log \
            data/error.ndjson data/burst_bench.jsonl data/bench_q1q21_100m.log \
            data/daemon_file.log data/perf_sentinel.ndjson \
            data/bench_*_rss.txt \
            /tmp/bench_rss.txt /tmp/bench_conf.toml /tmp/bench_conf.toml.tmp \
            /tmp/bench_gt_verify.json /tmp/bench_warmup_q1.txt
      if [ "$CLEAN_MODE" = "all" ]; then
        # 结果文件 + 旧验证产物（--verify 输出在 /tmp 已清，data/verify_*.txt 是旧命名的残留）
        rm -f data/bench_*_replay.txt data/bench_*_stream.txt data/verify_*.txt \
              data/window_shard_bench_*.txt
        echo "  → 结果文件已删"
      else
        echo "  → 保留结果文件 data/bench_*.txt（要连结果一起删用: ./bench.sh clean all）"
      fi
      echo "  → data/ 剩余 $(du -sh data 2>/dev/null | cut -f1)"
      exit 0
      ;;
    *) echo "bad clean mode '$CLEAN_MODE' (cache|all)"; exit 1;;
  esac
fi

QUERY="${1:-all}"
FEED="${2:-replay}"
# 数据量：默认 100m；PROFILE 模式（插桩性能下降）默认 10m。
if [ "${PROFILE:-0}" = "1" ] && [ -z "${3:-}" ]; then
  TOTAL=10m
else
  TOTAL="${3:-100m}"
fi
# --verify：跑批后用 `wfgen verify-nexmark` ground truth 对拍 EMIT（回归验证）。
# 作为任意位置参数（如 `./bench.sh all replay 30m --verify`）或 VERIFY=1。
VERIFY="${VERIFY:-0}"
for arg in "$@"; do [ "$arg" = "--verify" ] && VERIFY=1; done
# 调优参数：环境变量（并行度不设默认——write_conf 从 conf/wfusion.toml 读取，env 才覆盖）
PARSE="${PARSE_PARALLELISM:-}"
RULE="${RULE_PARALLELISM:-}"
MAX_FRAME_BYTES="${MAX_FRAME_BYTES:-8388608}"
MAX_FRAME_ROWS="${MAX_FRAME_ROWS:-100000}"
MAX_INGEST_RATE="${MAX_INGEST_RATE:-}"
RATE="${RATE:-3000000}"
SLICE_MS="${SLICE_MS:-1000}"
WARMUP="${WARMUP:-0}"
CONNECTIONS="${CONNECTIONS:-1}"
# 数据版本指纹：gen-nexmark 输出（2026-08-20 v2 排序；2026-08-21 v3 严格对齐
# Flink 官方口径：价格对数均匀/热点/引用窗口/字符串随机/extra padding/url 3 段目录；
# v4：bid 增 channel_id 字段（q21 Add channel id 数据侧对齐）。
# v5（2026-08-22）：事件时间固定 100µs/事件（跨度随 count 线性）、auction 有效期 horizon
# 固定 0.1666s、extra ±20% 抖动、字符串 3+rand(max-3)+special、cold 通道均匀随机 +
# abs(Integer.reverse(i))、URL 目录可含 '_'。
# 帧/分片缓存必须带版本号，否则旧口径缓存被静默复用（时间驱逐失效、窗口持全量、
# RSS 20GB+ 的根因）。换 DATA_VER 即强制重新生成对应版本缓存。
DATA_VER="${DATA_VER:-v5}"
# PROFILE=1：instrument-coverage 插桩跑批——真实运行时**行级覆盖数据**。
# 与微基准覆盖（wp-reactor/scripts/profile-cov.sh 的 bench.json）对比，差集 = 需补
# test bench 的路径；指标（EPS/RSS/CPU）与预期值（OSS_VVR_BASELINE.md）对比定位优化。
# 用法：PROFILE=1 ./bench.sh q1 replay 10m（默认 10M 数据量）
PROFILE="${PROFILE:-0}"
PROFILE_DIR="${PROFILE_DIR:-data/profile}"
LLVM_TOOLS="${LLVM_TOOLS:-$HOME/.rustup/toolchains/stable-aarch64-apple-darwin/lib/rustlib/aarch64-apple-darwin/bin}"
# 键闭包分片键：默认空 = 单连接整文件推（时间有序 → 时间驱逐生效）。
# CONNECTIONS>1 时配三流各自按键分，走生成时 shard-frames（同 key 同连接）。
SHARD_KEYS="${SHARD_KEYS:-}"

# 二进制来源：优先本地 warp-fusion 的 target/release 构建（仅当存在时）；否则回退 PATH。
# 不把路径固化为 ../../../warp-fusion —— 脚本可复制到任意目录运行，只要 wfusion/wfgen 在 PATH。
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

# ---- PROFILE 模式：插桩构建 + LLVM_PROFILE_FILE（真实运行行级计数） ----
if [ "$PROFILE" = "1" ]; then
  PROF_COV_DIR="$(pwd)/$PROFILE_DIR/cov-build"
  PROF_PGO="$(pwd)/$PROFILE_DIR/pgo"
  mkdir -p "$PROF_PGO"
  echo "==> profile: instrument-coverage 插桩构建（隔离 ${PROF_COV_DIR}）"
  if ! (cd "$REPO" && RUSTFLAGS="-Cinstrument-coverage -Cdebuginfo=1" \
      CARGO_TARGET_DIR="$PROF_COV_DIR" cargo build --release -p wfusion -p wfgen 2>&1 | tail -1); then
    echo "    ⚠ 插桩构建失败（PROFILE=1 需要 rustc 支持 instrument-coverage）；回退正常二进制" >&2
  elif [ -x "$PROF_COV_DIR/release/wfusion" ]; then
    WFUSION="$PROF_COV_DIR/release/wfusion"
    WFGEN="$PROF_COV_DIR/release/wfgen"
    rm -f "$PROF_PGO"/*.profraw
    export LLVM_PROFILE_FILE="$PROF_PGO/run_%p.profraw"
    echo "==> profile: 使用插桩二进制，profraw → $PROF_PGO"
  fi
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

PY="${PYTHON:-python3}"
LIB="../scripts/bench_lib.py"   # 共享度量工具库（comma/rss-sampler/引擎游标/哨兵/正确性摘要，两 case 共用）
PORT=9800

# 千分位显示：macOS bash 3.2 无 printf %'d；非数字原样返回（如 n/a）。纯逻辑在 bench_lib.py。
comma() { "$PY" "$LIB" comma "$1" 2>/dev/null || echo "$1"; }

# ---- 校验 ----
case "$TOTAL" in
  1m) TOTAL_N=1000000;;
  10m) TOTAL_N=10000000;; 30m) TOTAL_N=30000000;; 100m) TOTAL_N=100000000;;
  *) echo "bad total '$TOTAL' (1m|10m|30m|100m)"; exit 1;;
esac
case "$QUERY" in
  q1|q2|q3|q4|q5|q6|q7|q8|q9|q10|q11|q12|q13|q14|q15|q16|q17|q18|q19|q20|q21|q22) QUERIES=("$QUERY");;
  # q6 排除出 all/mix（2026-08-26）：join-then-key（键 seller 在 join 侧）单线程 +
  # 逐事件 sliding 状态机 + 每事件命中 emit（avg>=200 条件宽松）——30M 仅 634K
  # EPS（1 核 1576ns/evt），架构性慢（非局部可修）。all 里拉低均值且拖长总时长；
  # mix 里更会**门控整个混跑**（完成信号等最慢窗口排空，EPS 被拖到 ~600K 量级）。
  # 单跑研究仍可用 `./bench.sh q6 ...`（保留在上面显式列表）。
  # all=逐个单规则顺序跑；mix=全部规则同跑一个 daemon（多规则混跑，见头注释）。
  all) QUERIES=(q1 q2 q3 q4 q5 q7 q8 q9 q10 q11 q12 q13 q14 q15 q16 q17 q18 q19 q20 q21 q22);;
  mix) QUERIES=(mix);;
  *) echo "bad query '$QUERY' (q1..q22|all|mix；all=逐个单规则，mix=全部规则同跑，均不含 q6)"; exit 1;;
esac
case "$FEED" in
  replay|stream) ;;
  *) echo "bad feed '$FEED' (replay|stream；旧名 cont 已于 2026-08-20 移除，用 replay)"; exit 1;;
esac

# mix 规模注记（2026-08-30 引擎修复后）：全规则同跑曾因 join 索引单 key 独占
# （q8 按 seller / q20 按 id 共窗，后注册者回退全窗扫描）冻结在 ~1.5M——wp-reactor
# 改多 key join 索引后 30M 已正常（EPS ~760K、~105s、clean）。100M 未验证：
# RSS 30M≈9.3GB 线性外推 ~30GB，内存不足的机器请先 clean 或降级 10m/30m。
if [ "$QUERY" = "mix" ] && [ "$TOTAL" = "100m" ]; then
  echo "⚠ mix + 100m：全规则同跑 100M 未验证（30M RSS≈9.3GB 线性外推 ~30GB，需大内存机）" >&2
fi

mkdir -p data
# 核数探测：Linux 用 nproc（sysctl hw.ncpu 是 macOS 专属，Linux 下会报错 → cores=?
# 的假象）；macOS 无 nproc 命令，回落 sysctl。
if command -v nproc >/dev/null 2>&1; then
  CORES=$(nproc 2>/dev/null || echo "?")
else
  CORES=$(sysctl -n hw.ncpu 2>/dev/null || echo "?")
fi
echo "== bench: query=$QUERY feed=$FEED total=$(comma "$TOTAL_N") rate=$(comma "$RATE") slice_ms=$SLICE_MS cores=$CORES =="

# 等 daemon 释放端口（kill 后优雅关闭可能慢，尤其高内存 daemon）。否则下一个
# daemon bind 9800 失败 → accept 任务退出 → source 通道永久关闭（"connection
# channel closed"，后续连接全收不到）。
wait_port_free() {
  for i in $(seq 1 50); do
    if ! nc -z 127.0.0.1 "$PORT" 2>/dev/null; then return 0; fi
    sleep 0.2
  done
  echo "    警告: 端口 $PORT 超时未释放" >&2
}

# 强杀单个 daemon：SIGTERM → 轮询最多 300s → SIGKILL → 确认进程消失。
# 宽限 300s：与 wp-reactor `GROUP_JOIN_TIMEOUT(300s)` 对齐——stats/deferred 规则的
# shutdown close flush 构建尾部输出需数秒-数十秒（q19 30M ≈ 8M 条 ~13s；q18 100M
# spill 流式 drain ≈ 2940 万条分钟级），60s 会在 flush 完成前 SIGKILL → 截断
# `wait()` 的最终 metrics 导出 → 尾部 EMIT 计数丢失（q4/q22 30M 实测 SIGTERM 后
# >10s，--verify 会误报对拍失败）。正常 shutdown 不受影响（无 flush 构建时立即退出）。
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

# 清理所有残留 daemon（含被 kill 的 bench.sh 孤儿化的）：SIGTERM → SIGKILL 兜底。
# 脚本开头与 EXIT/INT/TERM trap 各调用一次，幂等。
cleanup_daemons() {
  # SIGKILL 直接兜底：高内存 daemon 优雅关闭可能 >10s，EXIT 时不留活口
  pkill -9 -f "wfusion daemon" 2>/dev/null
  pkill -9 -f "wfgen send-arrow" 2>/dev/null
  sleep 1
  pkill -9 -f "wfusion daemon" 2>/dev/null
  wait_port_free
}

cleanup_daemons
# INT/TERM 也清理：Ctrl-C 打断脚本时 SIGINT 不保证触发 EXIT trap（bash
# 对被中断命令的 trap 行为），残留 daemon 会继续占端口/烧 CPU。
trap cleanup_daemons EXIT INT TERM

# RSS + 瞬时 CPU% 采样（后台，100ms 周期，调用方 kill 结束）。
# 纯逻辑在 bench_lib.py（rss-sampler）：ps %cpu 是生命周期平均，取 cputime 差分/
# 墙钟差分 = 瞬时核占数%；ps 被权限拒绝时回退 macOS footprint；静默跳过失败样本。
# 每行带 epoch_ns（与哨兵 start_ns/emit_ns 同域），stat_samples 按引擎活跃窗过滤——
# 1s 粗采样对 q2 这类亚秒级突发会漏采/稀释（实测报 CPU 0% 假象）。
start_rss() {
  : > /tmp/bench_rss.txt   # 先截断：wait_sampler_baseline 的 -s 判断只认本轮采样器写出的行
  "$PY" "$LIB" rss-sampler "$1" /tmp/bench_rss.txt 0.1 > /dev/null 2>&1 &
}

# 等采样器产出首个 cputime 差分（文件出现第一行 = 第 2 次 tick 完成）。
# 必须在启动客户端前调用：首 tick 只初始化基线，第一个差分要等第 2 次 ps；
# 若采样器起得晚（python 启动/ps 开销在慢机上 ~0.5-1s），亚秒级突发
# （q2/q8 ≈ 0.4s 活跃窗）会在首个差分前烧完 → 活跃窗内全是空闲样本，
# CPU 恒报 0%（两轮实测复现，与查询无关的确定性失败）。等完基线后突发
# 一定被某个后续差分跨住。超时不阻塞（采样器异常时继续，全样本兑底）。
wait_sampler_baseline() {
  local i
  for i in $(seq 1 40); do   # 最多 ~4s
    [ -s /tmp/bench_rss.txt ] && return 0
    sleep 0.1
  done
}

# 从采样文件提取 PEAK_RSS / CPU_AVG / CPU_MAX（缺样本时给 n/a）。
# 采样行 = "epoch_ns RSS_MB CPU_PCT"。CPU 只统计引擎活跃窗 [CPU_WIN_START,
# CPU_WIN_END]（wall-clock ns，调用方按哨兵 start_ns/emit_ns ± 松弛设置）内的
# 样本——粗采样 + 空闲期（启动/等流/收尾 sleep）稀释会让 CPU% 失真；窗内无样本
# 时回退全样本，宁粗勿空。CPU_PCT 是核占数（多核并行可 >100%）。
stat_samples() {
  local WS="${CPU_WIN_START:-}" WE="${CPU_WIN_END:-}"
  PEAK=$(awk 'NF>=2 && $2>m {m=$2} END {print (m?m:"n/a")}' /tmp/bench_rss.txt)
  local FILT=""
  if [ -n "$WS" ] && [ -n "$WE" ]; then
    FILT="-v ws=$WS -v we=$WE -v filt=1"
  fi
  CPU_AVG=$(awk $FILT 'filt { if (!($1>=ws && $1<=we)) next }
    NF==3 && $3 ~ /^[0-9.]+$/ {s+=$3;n++} END {if(n) printf "%.0f", s/n; else print "n/a"}' /tmp/bench_rss.txt)
  CPU_MAX=$(awk $FILT 'filt { if (!($1>=ws && $1<=we)) next }
    NF==3 && $3 ~ /^[0-9.]+$/ {if($3>m) m=$3; n++} END {if(n) printf "%.0f", m; else print "n/a"}' /tmp/bench_rss.txt)
  # 窗内无样本（采样器起晚/窗太窄/时钟域异常）→ 回退全样本；若连全样本都没有，
  # CPU_MAX 的 n 计数为 0 → n/a，与 CPU_AVG 保持一致（修掉了旧版全 0 时 max 报 n/a 的 bug）
  if [ "$CPU_AVG" = "n/a" ] || [ "$CPU_MAX" = "n/a" ]; then
    CPU_AVG=$(awk 'NF==3 && $3 ~ /^[0-9.]+$/ {s+=$3;n++} END {if(n) printf "%.0f", s/n; else print "n/a"}' /tmp/bench_rss.txt)
    CPU_MAX=$(awk 'NF==3 && $3 ~ /^[0-9.]+$/ {if($3>m) m=$3; n++} END {if(n) printf "%.0f", m; else print "n/a"}' /tmp/bench_rss.txt)
  fi
}

# ---- 写查询 conf：基于 conf/wfusion.toml，覆盖 rules + 并行度（+ 可选限速） ----
# 并行度默认取 conf/wfusion.toml；-p/-r flag 或环境变量覆盖。
write_conf() {
  local Q="$1" M="$2"
  local RULES="models/queries/$Q.wfl"
  if [ "$Q" = "mix" ]; then
    # mix = 全部规则同跑：把 all 的查询集（除 q6，见校验处注释）symlink 进 data/mix_rules/
    # 再 glob——不能直接 models/queries/*.wfl（会把 q6 拉进来门控整个混跑）。每次跑重建
    # （rm -rf）防残留旧文件；symlink 而非 copy，规则文件永不漂移。
    rm -rf data/mix_rules && mkdir -p data/mix_rules
    local QF
    for QF in models/queries/q*.wfl; do
      case "$(basename "$QF")" in q6.wfl) continue;; esac
      ln -sf "../../$QF" "data/mix_rules/$(basename "$QF")"
    done
    RULES="data/mix_rules/*.wfl"
  fi
  PARSE_V_EFF="${PARSE:-$(sed -n 's/^parse_parallelism = *//p' conf/wfusion.toml | head -1)}"
  RULE_V_EFF="${RULE:-$(sed -n 's/^rule_shards = *//p' conf/wfusion.toml | head -1)}"
  sed -e "s|^rules = .*|rules = \"${RULES}\"|" \
      -e "s|^parse_parallelism = .*|parse_parallelism = ${PARSE_V_EFF}|" \
      -e "s|^rule_shards = .*|rule_shards = ${RULE_V_EFF}|" \
      conf/wfusion.toml > /tmp/bench_conf.toml
  # 限速：MAX_INGEST_RATE 设置时在 [runtime] 注入 max_ingest_rate
  if [ -n "${MAX_INGEST_RATE:-}" ]; then
    awk -v r="max_ingest_rate = ${MAX_INGEST_RATE}" '/^rule_exec_timeout = /{print; print r; next} {print}' \
      /tmp/bench_conf.toml > /tmp/bench_conf.toml.tmp
    mv /tmp/bench_conf.toml.tmp /tmp/bench_conf.toml
  fi
}

start_daemon() {
  rm -f data/metrics.ndjson data/wfusion.log data/daemon.log data/stream.log data/perf_sentinel.ndjson
  # --perf-diag conf/perf-diag.toml：无档配置，仅注册 __wf_sentinel 哨兵窗口
  # （初始门控全 false，性能零影响）——bench 用哨兵四元组做精确完成信号/EPS。
  "$WFUSION" daemon --config /tmp/bench_conf.toml --work-dir . \
    --perf-diag conf/perf-diag.toml > data/daemon.log 2>&1 &
  local D=$!
  local i
  for i in $(seq 1 40); do
    # daemon 进程已退出 → 启动失败（配置/规则文件解析错误等）。
    # 不加载坏文件就应明确失败退出，而不是等一个永远不会监听的端口。
    # 注意：本函数经 `$(start_daemon)` 命令替换在子 shell 执行，`exit` 不会
    # 传播到主脚本——必须 `return 1`；且 bash 的 `local D=$(...) || exit 1`
    # 会吞掉命令替换退出码，调用方须拆开写：`local D; D=$(...) || exit 1`。
    if ! kill -0 "$D" 2>/dev/null; then
      echo "    错误: daemon 启动失败（进程已退出）——检查配置/规则文件，daemon.log 尾部：" >&2
      tail -20 data/daemon.log >&2
      return 1
    fi
    nc -z 127.0.0.1 "$PORT" 2>/dev/null && break
    sleep 0.2
  done
  if ! nc -z 127.0.0.1 "$PORT" 2>/dev/null; then
    echo "    错误: daemon $D 启动超时（8s 端口 $PORT 未监听）——daemon.log 尾部：" >&2
    tail -20 data/daemon.log >&2
    return 1
  fi
  echo "$D"
}

# 引擎端到端游标（pull 模型）：
# - append_total：三输入流已 append 的行数累计（EPS 口径 + "数据已全部进入 window"）。
# - acked_lag：每窗口未完全消费批数（0 = 所有规则已消费到最新）。口径 =
#   `WindowProgress::completion_gap`（分组，2026-08-25 review）：key/行号分片
#   （match/stats）窗口看最慢分片（min）；round-robin（whole-batch）窗口看
#   max（每批归属唯一 shard，min 恒停最慢 shard——q13 分片卡尾）。
# pull 下 actor 与 rule 解耦，append 追平 ≠ 规则吃完（曾致 Q3 metrics 漏报尾部 emit）。
# 完成条件 = append 追平 TOTAL 且 acked_lag 归零。
engine_appended() {
  # 三输入流 append_total 全文件求和（counter=区间差值，跨区间求和=累计）
  "$PY" "$LIB" appended data/metrics.ndjson "auction_events,bid_events,person_events"
}

# pull 完成信号：**所有被消费窗口**（三输入流 + 中间管道窗口如 bid_mod /
# auction_finals）最新 acked_lag 之和（0 = 所有规则已消费到最新）。2026-08-23
# q13：中间管道下游（q13b）消费滞后时，只查三输入流会提前 SIGTERM（广播
# seq 用真实批次后，中间窗口的 acked_lag 反映消费进度；nexmark_alerts 等无
# 消费者窗口 gap 恒 0，不受影响）。
engine_acked_lag() {
  # 名单为空 = 所有被消费窗口（三输入流 + 中间管道窗口如 bid_mod /
  # auction_finals）——2026-08-23 q13：中间管道下游（q13b）消费滞后时只查
  # 三输入流会提前 SIGTERM；nexmark_alerts 等无消费者窗口 lag 恒 0 不受影响。
  "$PY" "$LIB" acked-lag data/metrics.ndjson ""
}

# 哨兵汇总 "total_n min_start max_emit count"：读**全部** sentinel 记录——
# 多连接分连接哨兵（round=连接号，4-1..4-4）聚合为批级完成信号：单连接 1 条
# （round=0，兼容旧语义）；多连接 N 条，Σn = 该批总行数，min start = 最先开始
# 的连接，max emit = 最后完成（引擎等全部数据窗排空后写）。
# EPS = Σn / (max_emit − min_start)。空 = 尚未出现。
sentinel_tuple() {
  "$PY" "$LIB" sentinel-tuple data/perf_sentinel.ndjson
}

# ---- 正确性摘要：emitted_total（按规则）+ 致命计数器 ----
# 致命计数器（append_failed/dropped_late/cursor_gap）非零即跑批作废——
# 测量纪律：数字可信的前提。time_evicted 有值属正常窗口关闭；
# memory_evicted（已读/已广播回收）在背压/字节 cap 下是常态，非致命
# （真丢未读信号是 cursor_gap，2026-08-25）。
# 输出两行：SUMMARY 行（进结果行）+ 各规则 emitted（进结果文件）。
correctness_summary() {
  # 输出两行：SUMMARY 行（进结果行）+ 各规则 emitted（进结果文件）。纯逻辑在 bench_lib.py。
  "$PY" "$LIB" correctness data/metrics.ndjson
}

# 轮末报告：单行结果（stdout + 结果文件）+ correctness 附录（结果文件）。
# 上下文字段（帧大小/负载/时间戳/分片键）写进结果行——事后可追溯口径，防 1MiB/8MiB、
# 连接数混淆（曾因口径混杂误判 ±8%）。
report_result() {
  local Q="$1" FEED="$2" OUT="$3" BODY="$4"
  # loadavg（1-min）随结果记录：本机是常载开发机（Zed/VM/WorkBuddy 等后台 ~6-7），
  # 同一配置的 EPS 随后台干扰在 43↔55M 间摆动（见 q1-throughput-bisection.md §9），
  # 结果行不带负载上下文无法解释相位差异。
  local LD
  # loadavg（1-min）：Linux 读 /proc/loadavg（$1=1min）；macOS 走 sysctl vm.loadavg（$2=1min）。
  if [ -r /proc/loadavg ]; then
    LD=$(awk '{printf "%.1f", $1}' /proc/loadavg)
  else
    LD=$(sysctl -n vm.loadavg 2>/dev/null | awk '{printf "%.1f", $2}')
  fi
  local CTX="frame_mb=$((MAX_FRAME_BYTES/1048576)) load=${LD:-n/a}${SHARD_KEYS:+" s=${SHARD_KEYS%%:*}"} · $(date +%m-%d_%H:%M:%S)"
  { echo "$BODY · $CTX"; echo "-- correctness --"; correctness_summary; } >> "$OUT"
  # SUMMARY 行回显到 stdout（EMIT 行只进文件）
  local SUM
  SUM=$(grep '^SUMMARY' "$OUT" | tail -1 | cut -d' ' -f2-)
  echo "$BODY · [$SUM] $CTX"
  case "$SUM" in
    clean) ;;
    *) echo "    ⚠ 正确性计数器非零，本跑批作废: $SUM" >&2 ;;
  esac
}

# ---- feed=replay：send-arrow 重放预编码帧（旧名 cont） ----
# 默认帧大小（8MiB）复用 bench_${TOTAL}_${DATA_VER}.frames；非默认大小用带后缀名（避免覆盖）。
if [ "$MAX_FRAME_BYTES" = "8388608" ]; then
  FRAMES=data/bench_${TOTAL}_${DATA_VER}.frames
else
  FRAMES=data/bench_${TOTAL}_mb${MAX_FRAME_BYTES}_${DATA_VER}.frames
fi
if [ "$FEED" = "replay" ] && [ ! -s "$FRAMES" ]; then
  echo "==> 预编码帧：gen-nexmark $(comma "$TOTAL_N") --check → dump-frames（frame $((MAX_FRAME_BYTES/1048576))MiB）"
  # 帧缓存必须非空（-s）：上次失败/中断可能留下 0 字节或截断文件，存在不等于有效，
  # 否则会跳过生成→ send-arrow 推空帧→空等 MAX_SEC（之前 30m 空等 15 分钟无输出）。
  rm -f "$FRAMES"
  write_conf q1 replay
  local_dummy=$(start_daemon) || exit 1
  # 管道直连 gen→dump-frames（--input - 读 stdin）：不再落 data/burst_bench.jsonl
  # 中间文件——100M 的 JSONL ~30GB，是磁盘峰值大头（os error 28 实测根因）。
  # 注意：sorted 模式（默认，事件时间有序→窗口驱逐生效）仍写 60 桶临时文件到
  # $TMPDIR（同盘），100M ~30GB；想换盘可 TMPDIR=<大数据盘> 指走。
  # gen 的 --check 报告走 stderr 透传终端；pipefail 保证 gen 失败也能报错。
  ( set -o pipefail
    "$WFGEN" gen-nexmark "$TOTAL_N" --check | \
      "$WFGEN" dump-frames --scenario scenarios/nexmark.wfg --input - \
        --ws models/schemas/nexmark.wfs --addr 127.0.0.1:$PORT --output "$FRAMES" --chunk 1000000 \
        --max-frame-bytes "$MAX_FRAME_BYTES" --max-frame-rows "$MAX_FRAME_ROWS" > /dev/null 2>&1
  ) || {
    echo "    错误: gen-nexmark/dump-frames 失败（确认 wfgen 含 --check：$WFGEN）" >&2
    rm -f "$FRAMES"
    kill_daemon "$local_dummy"; wait_port_free
    exit 1
  }
  # kill_daemon（非裸 kill）：只等端口会漏掉"端口已释放但进程未退出"的孤儿，
  # 孤儿继续烧 CPU 会污染本轮首跑测量
  kill_daemon "$local_dummy"; wait_port_free
  rm -f data/burst_bench.jsonl
  # 产出的帧仍为空 → 上一步静默失败（无 set -e），删掉坏缓存并显式退出，
  # 避免带着 0 字节缓存继续跑出"假结果/空等"。
  if [ ! -s "$FRAMES" ]; then
    echo "    错误: dump-frames 产物为空，已删除坏缓存（可重试）" >&2
    rm -f "$FRAMES"
    exit 1
  fi
  # ${FRAMES} 花括号必须：macOS bash 3.2 会把 `$VAR（` 的全角括号字节并入变量名
  # （set -u 下报 `FRAMES�: unbound variable`，实测复现），`${FRAMES}（` 才正确。
  echo "  frames: ${FRAMES}（$(du -h "$FRAMES" | cut -f1)）"
fi

run_replay_one() {
  local Q="$1" OUT_TAG="${2:-replay}"
  local OUT="data/bench_${Q}_${OUT_TAG}.txt"
  local SSTART="" SEMIT=""   # 哨兵窗（wall-clock ns），供 CPU 统计窗使用；函数内必须重置防残留
  write_conf "$Q" replay
  local D
  D=$(start_daemon) || exit 1
  start_rss "$D"; local SP=$!
  wait_sampler_baseline   # 先拿 cputime 差分基线，再启动客户端（防亚秒突发 0% 假象）

  local T0=$("$PY" "$LIB" now)
  # 哨兵 n = 引擎应消化的目标行数：多连接 raw（每连接推完整文件）→ CONNECTIONS×TOTAL；
  # 分片（shard-files/shard-keys，合计 = TOTAL）或单连接 → TOTAL。
  local SHARD_FILES="${SHARD_FILES:-}"
  local SENT_N="$TOTAL_N"
  if [ "$CONNECTIONS" -gt 1 ] && [ -z "$SHARD_KEYS" ] && [ -z "$SHARD_FILES" ]; then
    SENT_N=$(( TOTAL_N * CONNECTIONS ))
  fi
  if [ -n "$SHARD_KEYS" ] && [ "$CONNECTIONS" -gt 1 ]; then
    # 生成时分片(shard-frames)→ 纯 copy 多连接发送(键闭包,零解码):
    # 先检查分片文件缓存(同 TOTAL×CONNECTIONS×shard-keys 复用),缺则 shard-frames
    # 一次生成。缓存 key 必须带 shard-keys 指纹——换键会静默复用旧分片文件(曾踩坑)。
    local SHARD_KEY_FP
    SHARD_KEY_FP=$("$PY" "$LIB" md5 "$SHARD_KEYS")
    local SHARD_PREFIX="data/shard_${TOTAL}_${DATA_VER}_c${CONNECTIONS}_k${SHARD_KEY_FP}"
    local SHARD_FILES=""
    local i
    for i in $(seq 0 $(( CONNECTIONS - 1 ))); do
      [ -s "${SHARD_PREFIX}.s${i}.frames" ] || { SHARD_FILES=""; break; }
      SHARD_FILES="${SHARD_FILES:+$SHARD_FILES,}${SHARD_PREFIX}.s${i}.frames"
    done
    if [ -z "$SHARD_FILES" ]; then
      echo "==> shard-frames(${CONNECTIONS} 分片, key=${SHARD_KEYS%%:*})"
      "$WFGEN" shard-frames --input "$FRAMES" --shards "$CONNECTIONS" \
        --shard-keys "$SHARD_KEYS" --output-prefix "$SHARD_PREFIX" > /dev/null 2>&1 || {
        echo "    ⚠ shard-frames 失败(退出;EXIT trap 清理 daemon)" >&2
        exit 1
      }
      SHARD_FILES=""
      for i in $(seq 0 $(( CONNECTIONS - 1 ))); do
        SHARD_FILES="${SHARD_FILES:+$SHARD_FILES,}${SHARD_PREFIX}.s${i}.frames"
      done
    fi
    "$WFGEN" send-arrow --input "$FRAMES" --addr 127.0.0.1:$PORT --shard-files "$SHARD_FILES" \
      --sentinel "$SENT_N" > /dev/null 2>&1 &
  elif [ -n "$SHARD_KEYS" ]; then
    # 单连接 + shard-keys:无需分区,退化为普通发送
    "$WFGEN" send-arrow --input "$FRAMES" --addr 127.0.0.1:$PORT --sentinel "$SENT_N" > /dev/null 2>&1 &
  else
    "$WFGEN" send-arrow --input "$FRAMES" --addr 127.0.0.1:$PORT --connections "$CONNECTIONS" \
      --sentinel "$SENT_N" > /dev/null 2>&1 &
  fi
  local CLIENT=$!
  # 等引擎真正消化完：完成信号 = 哨兵四元组出现（引擎等数据窗排空后写
  # perf_sentinel.ndjson，无 metrics 轮询粒度误差）。哨兵超时退回 metrics 兑底。
  # 超时自适应：按 100k/s 诚实下限 + 600s 余量（on-each 单线程 ~0.3M/s，
  # 100M 需 ~333s；旧的 300s 上限会在真实负载下提前超时）。
  local MAX_SEC=$(( TOTAL_N / 100000 + 600 ))
  local T2=0 APP=0 TIMEOUT=0 EPS_MODE="sentinel" SENT_TUPLE=""
  for j in $(seq 1 $(( MAX_SEC * 10 ))); do
    SENT_TUPLE=$(sentinel_tuple)
    if [ -n "$SENT_TUPLE" ]; then
      read -r SN SSTART SEMIT SCOUNT <<< "$SENT_TUPLE"
      EPS=$("$PY" "$LIB" eps "$SN" "$SSTART" "$SEMIT")
      T2=$("$PY" "$LIB" now)
      APP=$SN
      break
    fi
    APP=$(engine_appended)
    DRAINED=$(engine_acked_lag)
    if [ "${APP:-0}" -ge "$TOTAL_N" ] && [ "${DRAINED:-1}" = "0" ]; then
      EPS_MODE="metrics-append"
      T2=$("$PY" "$LIB" now)
      break
    fi
    # 长等待期每 10s 报一次进度，避免全程静默看起来像挂死
    if [ $(( j % 100 )) -eq 0 ]; then
      echo "  ingest: $(comma "${APP:-0}")/$(comma "$TOTAL_N") ack_lag=${DRAINED:-1}（等待哨兵/引擎消化，超时上限 ${MAX_SEC}s）"
    fi
    sleep 0.1
  done
  # 追平后 kill 客户端：CONNECTIONS>1 时客户端会推 CONNECTIONS×TOTAL 事件，
  # 引擎只需消化 TOTAL（口径统一）；单连接时客户端已推完，kill 无害。
  kill "$CLIENT" 2>/dev/null; wait "$CLIENT" 2>/dev/null
  if [ "$EPS_MODE" = "sentinel" ] && [ -z "$SENT_TUPLE" ]; then
    # 哨兵未出现（daemon 无 --perf-diag / 引擎卡死）：退回 metrics 口径 + TIMEOUT 标注
    T2=$("$PY" "$LIB" now)
    EPS=$("$PY" "$LIB" eps "$APP" "$T0" "$T2")
    TIMEOUT=1
  elif [ "$EPS_MODE" = "metrics-append" ]; then
    # metrics 追平完成但哨兵未先到：哨兵帧在数据尾几 ms 后才发（send-arrow 推完
    # 数据再追加），等一拍看是否落盘——读到即升级为哨兵口径（更精确）。
    sleep 0.5
    SENT_TUPLE=$(sentinel_tuple)
    if [ -n "$SENT_TUPLE" ]; then
      read -r SN SSTART SEMIT SCOUNT <<< "$SENT_TUPLE"
      EPS=$("$PY" "$LIB" eps "$SN" "$SSTART" "$SEMIT")
      EPS_MODE="sentinel"
    else
      EPS=$("$PY" "$LIB" eps "$APP" "$T0" "$T2")
    fi
  fi
  sleep 3
  # 采样器采到 daemon 退出（2026-08-26 q18 口径修复）: 原实现 sleep 3 后先
  # kill 采样器再 kill_daemon——close flush 期（SIGTERM 后数十秒, q18 100M
  # 2935 万条）完全不在采样窗口, RSS_peak 只反映 ingest 期峰值（23.9G vs
  # 监视器 close 期真实 60G）。kill_daemon 阻塞到 daemon 退出/超时后采样器
  # 才停 → RSS_peak = 全流程真实峰值。
  kill_daemon $D; wait_port_free
  kill $SP 2>/dev/null; wait $SP 2>/dev/null

  # CPU 统计窗 = 哨兵活跃窗 ±0.5s（引擎实际消化时段，剔除启动/等流/收尾空闲稀释）；
  # 哨兵时间缺失（TIMEOUT 兑底/metrics 口径）时退回 [T0, T2]。stat_samples 只统计窗内样本。
  if [ -n "$SSTART" ] && [ -n "$SEMIT" ]; then
    CPU_WIN_START=$(( SSTART - 500000000 ))
    CPU_WIN_END=$(( SEMIT + 500000000 ))
  else
    CPU_WIN_START="$T0"; CPU_WIN_END="$T2"
  fi
  stat_samples
  cp /tmp/bench_rss.txt "data/bench_${Q}_${OUT_TAG}_rss.txt" 2>/dev/null || true   # 留档采样行，供 0%/异常自查
  # grep -c 无匹配时退出码 1 但仍输出 0——`|| echo 0` 会叠出双 0，改为兜底默认
  local EV=$(grep -c 'memory eviction' data/wfusion.log 2>/dev/null || true); EV=${EV:-0}
  : > "$OUT"   # 预清空，防追加残留上一轮
  local TO=""; [ "$TIMEOUT" = 1 ] && TO=" ⚠TIMEOUT(哨兵超时,EPS=metrics-append 兑底)"
  report_result "$Q" replay "$OUT" \
    "$Q/replay: EPS=$(comma "$EPS") · RSS_peak=$(comma "$PEAK")MB · CPU ${CPU_AVG}%avg/${CPU_MAX}%max · evict=$EV · eps_mode=${EPS_MODE}${SCOUNT:+" · conns=$SCOUNT"}$TO"
}

# ---- feed=stream：wfgen stream 实时生成（事件时间推进） ----
run_stream_one() {
  local Q="$1"
  local OUT="data/bench_${Q}_stream.txt"
  local SSTART="" SEMIT=""   # 哨兵窗（wall-clock ns），供 CPU 统计窗使用；函数内必须重置防残留
  write_conf "$Q" stream
  local D
  D=$(start_daemon) || exit 1
  start_rss "$D"; local SP=$!
  wait_sampler_baseline   # 先拿 cputime 差分基线，再启动流（防亚秒突发 0% 假象）

  local T0=$("$PY" "$LIB" now)
  # mix：--wfl 传全部查询文件（脚本内 glob 展开；data/mix_rules/ 由 write_conf 建好）
  local WFL_ARGS="models/queries/$Q.wfl"
  [ "$Q" = "mix" ] && WFL_ARGS="data/mix_rules/*.wfl"
  "$WFGEN" stream --scenario-dir scenarios --ws models/schemas/nexmark.wfs \
    --wfl $WFL_ARGS --addr 127.0.0.1:$PORT \
    --rate "$RATE" --slice-ms "$SLICE_MS" --sentinel "$TOTAL_N" > data/stream.log 2>&1 &
  local S=$!

  # 等引擎消化完：完成信号 = 哨兵四元组（stream 发满 TOTAL 后追加哨兵帧，引擎
  # 等数据窗排空写记录）。哨兵超时退回 metrics append+acked_lag 兑底。
  # 若引擎持续能力 < RATE，backlog 会一直堆积、哨兵永不出现 → 超时退出，此时
  # EPS 按 metrics-append 实际数计算，即"撑不住目标速率"的诚实信号。
  local MAX_SEC=900
  if [ "$RATE" -gt 0 ] 2>/dev/null; then MAX_SEC=$(( TOTAL_N / RATE * 3 + 60 ))
  else MAX_SEC=$(( TOTAL_N / 100000 + 600 )); fi   # 不限速：与 replay 同款自适应
  local T2=0 APP=0 TIMEOUT=0 EPS_MODE="sentinel" SENT_TUPLE=""
  for j in $(seq 1 $(( MAX_SEC * 2 ))); do
    SENT_TUPLE=$(sentinel_tuple)
    if [ -n "$SENT_TUPLE" ]; then
      read -r SN SSTART SEMIT SCOUNT <<< "$SENT_TUPLE"
      EPS=$("$PY" "$LIB" eps "$SN" "$SSTART" "$SEMIT")
      T2=$("$PY" "$LIB" now)
      APP=$SN
      break
    fi
    APP=$(engine_appended)
    DRAINED=$(engine_acked_lag)
    if [ "${APP:-0}" -ge "$TOTAL_N" ] && [ "${DRAINED:-1}" = "0" ]; then
      EPS_MODE="metrics-append"
      T2=$("$PY" "$LIB" now)
      break
    fi
    # 长等待期每 10s 报一次进度，避免全程静默看起来像挂死
    if [ $(( j % 20 )) -eq 0 ]; then
      echo "  ingest: $(comma "${APP:-0}")/$(comma "$TOTAL_N") ack_lag=${DRAINED:-1}（等待哨兵/引擎消化，超时上限 ${MAX_SEC}s）"
    fi
    sleep 0.5
  done
  if [ "$EPS_MODE" = "sentinel" ] && [ -z "$SENT_TUPLE" ]; then
    T2=$("$PY" "$LIB" now)
    EPS=$("$PY" "$LIB" eps "$APP" "$T0" "$T2")
    TIMEOUT=1
  elif [ "$EPS_MODE" = "metrics-append" ]; then
    # 哨兵帧在数据尾几 ms 后才发（stream 发满预算再追加）；等一拍看是否落盘，
    # 读到即升级哨兵口径（更精确）。kill $S 在其后，不影响哨兵帧发送。
    sleep 1
    SENT_TUPLE=$(sentinel_tuple)
    if [ -n "$SENT_TUPLE" ]; then
      read -r SN SSTART SEMIT SCOUNT <<< "$SENT_TUPLE"
      EPS=$("$PY" "$LIB" eps "$SN" "$SSTART" "$SEMIT")
      EPS_MODE="sentinel"
    else
      EPS=$("$PY" "$LIB" eps "$APP" "$T0" "$T2")
    fi
  fi
  kill $S 2>/dev/null; wait $S 2>/dev/null
  sleep 3
  kill_daemon $D; wait_port_free
  kill $SP 2>/dev/null; wait $SP 2>/dev/null

  # CPU 统计窗：哨兵活跃窗 ±0.5s（引擎实际消化时段）；哨兵时间缺失时退回 [T0, T2]。
  if [ -n "$SSTART" ] && [ -n "$SEMIT" ]; then
    CPU_WIN_START=$(( SSTART - 500000000 ))
    CPU_WIN_END=$(( SEMIT + 500000000 ))
  else
    CPU_WIN_START="$T0"; CPU_WIN_END="$T2"
  fi
  stat_samples
  cp /tmp/bench_rss.txt "data/bench_${Q}_stream_rss.txt" 2>/dev/null || true   # 留档采样行，供 0%/异常自查
  local EV=$(grep -c 'memory eviction' data/wfusion.log 2>/dev/null || true); EV=${EV:-0}
  : > "$OUT"
  local TO=""; [ "$TIMEOUT" = 1 ] && TO=" ⚠TIMEOUT(哨兵超时,EPS=metrics-append 兑底)"
  report_result "$Q" stream "$OUT" \
    "$Q/stream: EPS=$(comma "$EPS") · RSS_peak=$(comma "$PEAK")MB · CPU ${CPU_AVG}%avg/${CPU_MAX}%max · evict=$EV · target_rate=$(comma "$RATE") · eps_mode=${EPS_MODE}${SCOUNT:+" · conns=$SCOUNT"}$TO"
}

# ---- 预热轮（WARMUP=1）：stash 重建后首跑系统性偏低（曾三次复现），须剔除 ----
if [ "$WARMUP" = "1" ] && [ "$FEED" = "replay" ]; then
  echo "==> warmup 轮（结果丢弃, 写 /tmp/bench_warmup_q1.txt）"
  run_replay_one q1 warmup_q1
  mv -f data/bench_q1_warmup_q1.txt /tmp/bench_warmup_q1.txt 2>/dev/null || true
fi

for Q in "${QUERIES[@]}"; do
  if [ "$FEED" = "replay" ]; then
    run_replay_one "$Q"
  else
    run_stream_one "$Q"
  fi
done
echo "== done: 结果在 data/bench_*_${FEED}.txt =="

# ---- PROFILE 模式：合并 profraw + 行级覆盖报告（真实运行热路径） ----
if [ "$PROFILE" = "1" ]; then
  PROF_PGO="$(pwd)/$PROFILE_DIR/pgo"
  if ls "$PROF_PGO"/*.profraw >/dev/null 2>&1; then
    "$LLVM_TOOLS/llvm-profdata" merge -o "$PROFILE_DIR/runtime.profdata" "$PROF_PGO"/*.profraw
    "$LLVM_TOOLS/llvm-cov" export --instr-profile="$PROFILE_DIR/runtime.profdata" \
      --object "$WFUSION" > "$PROFILE_DIR/runtime.json" 2>/dev/null
    echo "==> profile: 覆盖数据在 $PROFILE_DIR/{runtime.profdata,runtime.json}"
    echo "    与微基准覆盖对比（wp-reactor/scripts/profile-cov.sh bench.json）："
    echo "    差集 = 真实运行执行但微基准未覆盖 → 需补 test bench 的路径"
  else
    echo "==> profile: 无 profraw（插桩二进制未生效？），跳过合并" >&2
  fi
fi

# ---- --verify：用 wfgen verify-nexmark（真实 WFL 规则引擎 ground truth）对拍 EMIT ----
# verify-nexmark 用 wf_engine 规则引擎处理与引擎同一份数据、同一套 .wfl 规则，
# 产出各规则应 EMIT 计数；--engine-emit 读引擎实际 EMIT，在 wfgen 内用
# git-diff 同款分层方法（L1 哈希快扫判同 → L2 Myers/降级 → L3 明细，similar
# crate）逐规则对拍，退出码 0=一致 / 1=有差异（q21 anti-join 已知差异不判失败）。
# 性能：真实规则引擎逐事件 × 规则数，10m≈80s / 30m≈5-15min（负载相关）；
# 单查询 --verify 传 --query 只验证该查询的规则（26 → 1 个文件）。
if [ "$VERIFY" = "1" ] && [ "$FEED" = "replay" ]; then
  if [ "$QUERY" = "mix" ]; then
    # mix 是多规则同跑：EMIT 计数受规则间交互影响，不能与 oracle 单规则期望对拍
    # （历史 q8/q11 数量级差异；正确性验证走 verify_daemon.sh 逐查询单跑保真）——跳过并明示。
    echo "== verify: query=mix 跳过——混跑 EMIT 不能与 oracle 单规则对拍（正确性请用 verify_daemon.sh 逐查询）=="
  else
    # stderr 保留（进度条走 stderr；stdout 是 JSON + diff 报告）
    VERIFY_SCOPE=""
    [ "$QUERY" != "all" ] && VERIFY_SCOPE="--query $QUERY"
    echo "== verify: wfgen verify-nexmark ${TOTAL_N}（真实规则引擎${VERIFY_SCOPE:+ $VERIFY_SCOPE}）对拍 EMIT =="
    if "$WFGEN" verify-nexmark "$TOTAL_N" $VERIFY_SCOPE --engine-emit data; then
      echo "== verify: 全部一致 ✅ =="
    else
      echo "== verify: 存在差异 ❌（见上方 diff 明细）=="
    fi
  fi
fi
