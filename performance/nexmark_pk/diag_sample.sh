#!/usr/bin/env bash
# diag_sample.sh — q19 30m full 档 CPU 采样（macOS /usr/bin/sample）
#
# 目的：定位 perf-diag 墙梯「full」档（输出链）段内热点函数。
# 做法：STAGES=full 让墙梯只剩 warmup+full（两档都是全链路，无 cut），
#       对 daemon 连续采样 ~18s（覆盖 warmup 7.6s + full 8.4s 两个全链路档），
#       再聚合 sample 调用图出热点函数 top N。
#
# 用法：./diag_sample.sh [q19] [30m]
set -u
cd "$(dirname "${BASH_SOURCE[0]}")"
Q="${1:-q19}"
TOTAL="${2:-30m}"
RUN_LOG=/tmp/diag_sample_run.log
SAMPLE_OUT="data/sample_${Q}_${TOTAL}.txt"
rm -f "$RUN_LOG" "$SAMPLE_OUT"

echo "== 后台启动 diag（STAGES=full → warmup+full 双全链路档）"
STAGES=full ./diag.sh "$Q" "$TOTAL" > "$RUN_LOG" 2>&1 &
DIAG=$!

# 等 daemon 起来 + **full 档开始**（warmup 档 ~8s 发送 30M 行，采样应覆盖
# full 档的稳态——warmup 是预热，数字不进结论；等 full 档才采）
DPID=""
for i in $(seq 1 120); do
  DPID=$(pgrep -f "wfusion daemon" 2>/dev/null | head -1 || true)
  if [ -n "$DPID" ] && grep -q "full/1: sent\|stage 1 \[full\]" "$RUN_LOG" 2>/dev/null; then
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
# full 档 30M 行 @~3M/s ≈ 10s，采样 12s 覆盖稳态中段
echo "== daemon pid=${DPID}，full 档已开始 → sample 采样 12s @20Hz"
/usr/bin/sample "$DPID" 12 20 -file "$SAMPLE_OUT" > /dev/null 2>&1 || true

echo "== 等 diag 收尾"
wait "$DIAG" 2>/dev/null
echo "== diag 完成（完整墙表见 ${RUN_LOG}）"

# ---- 分析：聚合 sample 调用图 ----
echo ""
echo "==== sample 调用图热点（inclusive 样本数；文件: ${SAMPLE_OUT}）===="
python3 - "$SAMPLE_OUT" <<'PYEOF'
import re, sys
from collections import defaultdict

path = sys.argv[1]
text = open(path, errors="replace").read()

# 采样总数与线程数
m = re.search(r"Total number of samples:\s*(\d+)", text)
total = int(m.group(1)) if m else 0
print(f"总样本: {total}")

# --- inclusive：Call graph 段每帧出现次数（同一函数名聚合）---
incl = defaultdict(int)
in_cg = False
for line in text.splitlines():
    if line.startswith("Call graph:"):
        in_cg = True
        continue
    if in_cg and re.match(r"^\s*$", line):
        in_cg = False
        continue
    if in_cg:
        m = re.match(r"^\s+(\d+)\s+(\S.*)$", line)
        if m:
            n = int(m.group(1))
            name = m.group(2).strip()
            # 去掉尾部花括号计数（部分版本 "name {count}"）
            name = name.split(" {")[0].strip()
            if name and not name.startswith(("Thread_", "_dispatch", "start", "main")):
                incl[name] += n

print("\n── inclusive top 30（样本内出现的总次数，含子树）──")
for name, n in sorted(incl.items(), key=lambda kv: -kv[1])[:30]:
    pct = 100.0 * n / total if total else 0
    print(f"  {n:>7} ({pct:5.1f}%)  {name}")

# --- self：Sort by top of stack 段（栈顶 = 自耗时）---
self_top = defaultdict(int)
in_stos = False
for line in text.splitlines():
    if line.startswith("Sort by top of stack:"):
        in_stos = True
        continue
    if in_stos and (line.startswith("Binary Images") or line.startswith("Total number")):
        in_stos = False
    if in_stos:
        m = re.match(r"^\s*(\d+)\s+(\S.*)$", line)
        if m and not m.group(2).startswith("Thread_"):
            self_top[m.group(2).strip()] += int(m.group(1))

print("\n── self top 15（栈顶自耗时）──")
for name, n in sorted(self_top.items(), key=lambda kv: -kv[1])[:15]:
    pct = 100.0 * n / total if total else 0
    print(f"  {n:>7} ({pct:5.1f}%)  {name}")
PYEOF
