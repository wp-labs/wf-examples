#!/usr/bin/env bash
# diag_sample.sh — 指定墙梯档 CPU 采样（macOS /usr/bin/sample）
#
# 目的：定位 perf-diag 墙梯某档（默认 full=输出链；可指定 rules=规则求值等）
#       段内热点函数。供 diag.sh 墙表「忙墙」判定（CPU 高占用段）后下一步定位。
#
# 做法：STAGES=<stage> 让墙梯只剩 warmup+<stage> 两档（warmup 全链路预热，
#       丢弃不进结论；目标档保持墙表中同款门控），对 daemon 从目标档开始
#       连续采样（默认 12s @20Hz，覆盖 30M 稳态中段），再聚合 sample 调用图
#       出热点函数 top N。
#
# 用法：./diag_sample.sh [q19] [30m] [stage=full] [secs=12]
#   stage = full|rules|floor|decode|recv（墙表档名；默认 full 保持旧行为）
#   secs  = 采样时长（默认：full=12s，其它档 8s——rules 档 30M @5M EPS ≈ 6s）
set -u
cd "$(dirname "${BASH_SOURCE[0]}")"
Q="${1:-q19}"
TOTAL="${2:-30m}"
STAGE="${3:-full}"
SECS="${4:-}"
[ -z "$SECS" ] && SECS=$([ "$STAGE" = "full" ] && echo 12 || echo 8)
RUN_LOG=/tmp/diag_sample_run.log
SAMPLE_OUT="data/sample_${Q}_${TOTAL}.txt"
[ "$STAGE" != "full" ] && SAMPLE_OUT="data/sample_${Q}_${TOTAL}_${STAGE}.txt"
rm -f "$RUN_LOG" "$SAMPLE_OUT"

echo "== 后台启动 diag（STAGES=${STAGE} → warmup+${STAGE} 双档，采样 ${SECS}s @20Hz）"
STAGES="$STAGE" ./diag.sh "$Q" "$TOTAL" > "$RUN_LOG" 2>&1 &
DIAG=$!

# 等 daemon 起来 + **目标档开始**（warmup 档 ~8s 发送 30M 行，采样应覆盖
# 目标档的稳态——warmup 是预热，数字不进结论；等目标档才采）
DPID=""
for i in $(seq 1 120); do
  DPID=$(pgrep -f "wfusion daemon" 2>/dev/null | head -1 || true)
  if [ -n "$DPID" ] && grep -q "stage 1 \[${STAGE}\] applied\|${STAGE}/1: sent" "$RUN_LOG" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$DIAG" 2>/dev/null; then
    echo "  ✗ diag 提前退出——日志尾部：" >&2; tail -30 "$RUN_LOG" >&2; exit 1
  fi
  sleep 0.5
done
if [ -z "$DPID" ]; then
  echo "  ✗ 未找到 daemon——日志尾部：" >&2; tail -30 "$RUN_LOG" >&2; exit 1
fi
echo "== daemon pid=${DPID}，${STAGE} 档已开始 → sample 采样 ${SECS}s @20Hz"
/usr/bin/sample "$DPID" "$SECS" 20 -file "$SAMPLE_OUT" > /dev/null 2>&1 || true

echo "== 等 diag 收尾"
wait "$DIAG" 2>/dev/null
echo "== diag 完成（完整墙表见 ${RUN_LOG}）"

PY="${PYTHON:-python3}"
# ---- 分析：聚合 sample 调用图（共享分析器 diag_analyze_sample.py：
# inclusive 热点 + 引擎侧热点 + 自耗时 top——本机 sample 旧版格式的解析
# 都收敛在它那里，脚本内不再内嵌易碎的解析逻辑）----
echo ""
echo "==== sample 调用图分析（inclusive + 自耗时；文件: ${SAMPLE_OUT}）===="
"$PY" diag_analyze_sample.py "$SAMPLE_OUT" 40
