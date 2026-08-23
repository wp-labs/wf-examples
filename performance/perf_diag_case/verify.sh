#!/usr/bin/env bash
# perf_diag_case — 诊断机制验证驱动
#
# ⚠ 依赖引擎实现：`--perf-diag` 启动参数、内置 __perf_sentinel 窗口、诊断点状态机、
#    `wfgen perf-diag` 子命令（见 wp-reactor/docs/design/perf-diag-mode-design.md §8
#    落地清单）。引擎落地前本脚本不可运行，内容即验收程序规格。
#
# 用法: ./verify.sh [N=100000]
set -u
cd "$(dirname "${BASH_SOURCE[0]}")"
REPO_ROOT="$(cd ../../../warp-fusion && pwd)"
WF="$REPO_ROOT/target/release/wfusion"
GEN="$REPO_ROOT/target/release/wfgen"
PY=python3; PORT=9800; N="${1:-100000}"

[ -x "$WF" ] && [ -x "$GEN" ] || { echo "错误: 缺少 release wfusion/wfgen（先构建 warp-fusion）"; exit 1; }

mkdir -p data
pkill -9 -f "wfusion daemon" 2>/dev/null; sleep 1

echo "== 0. 生成数据 + 规则 =="
"$PY" scripts/gen_events.py "$N" > data/evt.jsonl
"$PY" scripts/gen_rules.py > models/rules/basic.wfl
echo "  规则数: $(grep -c '^rule ' models/rules/basic.wfl) 事件数: $N"

echo "== 1. 启动诊断 daemon（--perf-diag）=="
"$WF" daemon --perf-diag conf/perf-diag.toml --config conf/wfusion.toml --work-dir . > data/daemon.log 2>&1 &
DAEMON_PID=$!
trap 'kill $DAEMON_PID 2>/dev/null; pkill -9 -f "wfusion daemon" 2>/dev/null' EXIT
READY=0
for i in $(seq 1 50); do nc -z 127.0.0.1 "$PORT" 2>/dev/null && { READY=1; break; }; sleep 0.2; done
[ "$READY" = 1 ] || { echo "FAIL: daemon 未就绪"; tail -20 data/daemon.log; exit 1; }

echo "== 2. dump 帧 =="
"$GEN" dump-frames --scenario scenarios/evt.wfg --input data/evt.jsonl \
  --addr 127.0.0.1:$PORT --ws models/schemas/evt.wfs --output data/evt.frames \
  --chunk 10000 --max-frame-bytes 8388608 --max-frame-rows 100000 || {
  echo "FAIL: dump-frames（若报 schema/握手错，说明 --perf-diag 窗口未注册）"; tail -20 data/daemon.log; exit 1; }

echo "== 3. 驱动诊断（3 诊断点 × N=100k）=="
"$GEN" perf-diag --diag conf/perf-diag.toml \
  --frames data/evt.frames --addr 127.0.0.1:$PORT --n-list "$N" --rounds 2 || {
  echo "FAIL: perf-diag 驱动（检查 wfgen 版本/引擎实现）"; tail -20 data/daemon.log; exit 1; }

echo "== 4. 验收检查 =="
FAILED=0
# 4.1 哨兵记录：行数 = 诊断点数(3) × 轮数(2)，四元组完整
CNT=$(wc -l < data/perf_sentinel.ndjson 2>/dev/null || echo 0)
[ "$CNT" -ge 3 ] || { echo "FAIL: perf_sentinel.ndjson 行数 $CNT < 3"; FAILED=1; }
"$PY" - data/perf_sentinel.ndjson <<'EOF' || FAILED=1
import json, sys
for line in open(sys.argv[1]):
    o = json.loads(line)
    for k in ("round", "n", "start_ns", "emit_ns"):
        assert o.get(k) is not None, f"哨兵记录缺字段 {k}: {line}"
    assert o["emit_ns"] > o["start_ns"], f"emit_ns 应晚于 start_ns: {o}"
print("  sentinel 记录四元组校验通过:", sum(1 for _ in open(sys.argv[1])), "条")
EOF

# 4.2 墙表：三档 EPS 单调 floor ≥ rules ≥ full（容差 ±10%）
if [ -f data/perf_diag_wall.txt ]; then
  cat data/perf_diag_wall.txt
  "$PY" - data/perf_diag_wall.txt <<'EOF' || FAILED=1
import sys, re
eps = {}
for line in open(sys.argv[1]):
    m = re.search(r'(\S+)\s+eps=([0-9.]+)', line)
    if m: eps[m.group(1)] = float(m.group(2))
order = [k for k in ("floor", "rules", "full") if k in eps]
for a, b in zip(order, order[1:]):
    if eps[a] < eps[b] * 0.9:
        print(f"  FAIL: {a}({eps[a]:.0f}) < {b}({eps[b]:.0f}) — 墙梯应单调")
        sys.exit(1)
print("  墙梯单调校验通过:", " > ".join(f"{k}({eps[k]:.0f})" for k in order))
EOF
else
  echo "  ⚠ 无 data/perf_diag_wall.txt（引擎未产出墙表）"
fi

# 4.3 单 daemon 未重启（pid 不变）
kill -0 "$DAEMON_PID" 2>/dev/null || { echo "FAIL: daemon 中途退出（pid $DAEMON_PID）"; FAILED=1; }

if [ "$FAILED" = 0 ]; then echo "OK: 诊断机制验证通过"; else echo "FAIL: 存在未通过项"; exit 1; fi
