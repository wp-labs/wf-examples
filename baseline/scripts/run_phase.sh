#!/usr/bin/env bash
# ===========================================================================
# baseline 近端 B 相位同窗接线 e2e（conf/loop.phase.wfusion.toml）——
#   producer(15s 收盘) → baseline_out(ndjson) + 相位 judge（共享 BaselineStore
#   按事件时间折相位桶、只与历史同期比较）。相位参数 period=240s/bucket=15s：
#   忙时格(8..15)水平 3000、闲时格(0..7)水平 1000；每轮 +120s = 半周期 → 奇偶轮
#   交替忙/闲、相位位置每 2 轮严格复现（scripts/gen_metrics_phase.py）。
#
# 判定口径（忙/闲双档 + 每轮 1 个 5号线=9000 越界注入）：
#   - judge 每轮**恰 1 条**且只在 5号线、z>3：5 条线在忙/闲切换与正常噪声
#     （±25 有界）下零误报；越界点在相位过滤/滚动两种形态下都检出。
#   - **相位已生效 canary**：round 1 无任何跨轮历史，相位桶内只有越界点自窗
#     → z≈10；若相位配置未生效（滚动单缓冲）→ 基线混合 6 窗 σ≈240 → z≈21。
#     断言首条 3 < z < 15（两形态实测 9.95 vs 20.8，分隔稳定）。
#   - 同相位隔离的精确语义（只与历史同期比、半衰期参照=4×period、事件缺
#     event_time 的全桶回退）由 wf-cep `baseline::` 单测逐条锁定——本脚本防的是
#     全引擎接线回归（config 解析 → install_phased → 收盘分桶 append → judge 读
#     event_time），并顺带验证忙/闲双档长期运行无崩溃/无误报。
#   - 相位关闭行为（默认 conf）不受影响，回归由 scripts/run_loop.sh 覆盖。
#
# 自检/防错位：
#   - 启动即用 gen_metrics_phase.py --constants 交叉校验 conf 相位 period/bucket
#     与生成器常量一致（两处重复无单一事实源，改一忘一 → 忙/闲划分静默错位，FAIL）；
#   - 生成器自身属性（单调/线路均衡/忙闲纯度/spike/周期复现）可独立自检：
#       python3 scripts/gen_metrics_phase.py --selfcheck
#   - 终态另做 detect sanity：若全局基线通道产生告警必须仅 5号线 且 dev>5。
#     相位化供给（2026-09-08）后语义：忙轮 spike(9000) vs 忙桶基线 μ3000 →
#     dev≈2 <5 不告警（相对忙时正常 3× 不算 5× 级越界）；闲轮 spike vs 闲桶 μ1000
#     → dev≈8 告警——detect 只对同相位真越界告警，数量不作硬断言。
#
# 用法: ./scripts/run_phase.sh [rounds]   默认 8 轮 ≈ 1.5 分钟
# 环境: ROUNDS/WFUSION/WFGEN/PYTHON
# ===========================================================================
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

WFUSION=${WFUSION:-wfusion}
WFGEN=${WFGEN:-wfgen}
PY=${PYTHON:-python3}
PORT=9800
ROUNDS="${1:-${ROUNDS:-8}}"
COUNT=3000
SPAN=90
SPIKE_IDX=$(( (COUNT / 2 / 5) * 5 + 4 ))   # 5号线（i%5==4）中段事件
CONF=conf/loop.phase.wfusion.toml
LOG=data/logs/wfusion_phase.log
OUT=data/baseline/baseline.ndjson
JUDGE=data/detect/judge.ndjson
ALERTS=data/detect/alerts.ndjson
CSV=data/detect/baseline_ref.csv

mkdir -p data/logs data/detect data/baseline
rm -f "$LOG" "$OUT" "$JUDGE" "$ALERTS" data/daemon.log data/live.jsonl "$CSV"
# 端口 9800 被各 case 共享：残留 daemon 会抢占——先清场并**等端口真正释放**再
# 启动（避免新 daemon bind 失败或注入打进旧实例的偶发首轮 rx=0）。
lsof -ti:"$PORT" 2>/dev/null | xargs kill 2>/dev/null || true
for _i in $(seq 1 20); do
  lsof -ti:"$PORT" >/dev/null 2>&1 || break
  sleep 0.2
done

# 交叉校验（防配置/生成器错位）：conf 相位常量必须与 gen_metrics_phase.py 一致——
# 两处重复的 period/bucket 没有单一事实源，改其一而忘另一会让忙/闲划分静默错位。
GEN_CONST=$("$PY" scripts/gen_metrics_phase.py --constants)
read -r GEN_PERIOD GEN_BUCKET <<< "$GEN_CONST"
CONF_PERIOD=$(grep -E '^\s*baseline_history_phase_period\s*=' "$CONF" | sed -n 's/.*"\([0-9]*\)s".*/\1/p' | head -1)
CONF_BUCKET=$(grep -E '^\s*baseline_history_phase_bucket\s*=' "$CONF" | sed -n 's/.*"\([0-9]*\)s".*/\1/p' | head -1)
if [ -z "$CONF_PERIOD" ] || [ -z "$CONF_BUCKET" ]; then
  echo "FAIL: 无法从 $CONF 解析 baseline_history_phase_*（需秒为单位字符串）" >&2
  exit 1
fi
if [ "$CONF_PERIOD" != "$GEN_PERIOD" ] || [ "$CONF_BUCKET" != "$GEN_BUCKET" ]; then
  echo "FAIL: conf 相位 period/bucket=${CONF_PERIOD}s/${CONF_BUCKET}s 与生成器 ${GEN_PERIOD}s/${GEN_BUCKET}s 不一致——生成器/配置已错位" >&2
  exit 1
fi
printf '==> 0. 相位常量一致: period=%ss bucket=%ss（conf == gen）\n' "$CONF_PERIOD" "$CONF_BUCKET"
./scripts/check_rules_sync.sh >/dev/null   # rules-loop 组合快照一致性（见 models/README.md）

# 种子 provider CSV（占位 μ≈1000，σ=20；detect 通道沿用 loop 语义，本次不断言）
"$PY" - <<PYEOF
import csv, sys
sys.path.insert(0, "scripts")
import phase_cfg
lines = ["1号线", "2号线", "3号线", "4号线", "5号线"]
with open("$CSV", "w", encoding="utf-8", newline="") as f:
    w = csv.writer(f)
    w.writerow(["entity", "phase_bucket", "n", "sum", "sum_sq", "mu", "sigma"])
    for e in lines:
        for p in range(phase_cfg.BUCKETS):
            w.writerow([e, "p" + str(p), 240, 240000.0, 240096000.0, 1000.0, 20.0])
print("seeded", "$CSV")
PYEOF

echo "==> 0. start phase daemon (conf=$CONF rounds=$ROUNDS)"
"$WFUSION" daemon --config "$CONF" --work-dir . > data/daemon.log 2>&1 &
DAEMON_PID=$!
trap 'kill $DAEMON_PID 2>/dev/null || true' EXIT

for i in $(seq 1 60); do
  nc -z 127.0.0.1 "$PORT" 2>/dev/null && break
  sleep 0.2
done
nc -z 127.0.0.1 "$PORT" || { echo "ERROR: TCP 源未就绪"; tail -30 "$LOG" 2>/dev/null || true; exit 1; }

send_round() {
  local offset=$1
  "$PY" scripts/gen_metrics_phase.py "$COUNT" "$SPAN" "$offset" "$SPIKE_IDX" > data/live.jsonl
  "$WFGEN" send --scenario models/scenarios/metrics_baseline.wfg \
    --input data/live.jsonl --addr 127.0.0.1:$PORT \
    --ws models/schemas/metrics.wfs 2>&1 | tail -1
}

nlines() { local f=$1; [[ -f "$f" ]] && wc -l < "$f" | tr -d ' ' || echo 0; }

prev_rows=0
echo "==> 1. 分轮注入（每轮 offset +120s=半周期；judge 期望累计 = r，即每轮恰 1 条）"
for r in $(seq 1 "$ROUNDS"); do
  offset=$((r * 120))
  echo "-- round $r (offset +${offset}s)"
  send_round "$offset"
  sleep 3                       # 收盘 → baseline_out 落盘 + judge 判定收敛
  "$PY" scripts/export_baseline_ref.py > /dev/null   # detect 通道维持（不断言）
  sleep 2
  ROWS=$(nlines "$OUT"); J=$(nlines "$JUDGE")
  echo "   baseline=$ROWS judge=$J (期望 $r)"
  if (( r >= 2 && ROWS <= prev_rows )); then
    echo "FAIL: round $r 无新收盘（$prev_rows -> ${ROWS}）" >&2
    tail -30 "$LOG" 2>/dev/null || true
    exit 1
  fi
  if (( J != r )); then
    echo "FAIL: round $r judge=$J 期望 ${r}——每轮应恰 1 条 5号线 真越界，其余全静默" >&2
    echo "  judge 前 5 行: $(head -5 "$JUDGE" 2>/dev/null | tr '\n' ' ')" >&2
    tail -30 "$LOG" 2>/dev/null || true
    exit 1
  fi
  prev_rows=$ROWS
done

ROWS=$(nlines "$OUT"); J=$(nlines "$JUDGE"); A=$(nlines "$ALERTS")
echo "==> 2. 终态：baseline=${ROWS} judge=${J} alerts(detect)=${A}（期望 judge=${ROUNDS}）"

# 终态校验（python）：judge 每轮恰 1 条且只含 5号线（z>3）；首条 z 落在
# "相位已生效"区间（3,15）——滚动形态实测 ≈21，作相位接线 canary。
"$PY" - <<PYEOF
import json, os, sys
out = "data/baseline/baseline.ndjson"
judge = "data/detect/judge.ndjson"

def load(p):
    rows = []
    if os.path.exists(p):
        with open(p, encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    rows.append(json.loads(line))
    return rows

bad = []
j = load(judge)
je = sorted({x.get("entity") for x in j})
if je != ["5号线"]:
    bad.append(f"judge 实体 {je} 期望仅 5号线（其余线忙/闲正常事件必须零误报）")
if not all(float(x.get("z", 0)) > 3.0 for x in j):
    bad.append("judge z 应 >3（真越界）")
if len(j) != ${ROUNDS}:
    bad.append(f"judge 总数 {len(j)} 应 = ROUNDS = ${ROUNDS}（每轮恰 1 条）")
first_z = float(j[0].get("z", 0)) if j else 0.0
if not (3.0 < first_z < 15.0):
    bad.append(f"首条 z={first_z} 应 ∈ (3,15)——相位接线 canary（滚动形态实测 ≈21）")
base_rows = 0
with open(out, encoding="utf-8") as f:
    base_rows = sum(1 for line in f if line.strip())
if base_rows < $((ROUNDS * 20)):
    bad.append(f"baseline 收盘过少 {base_rows}")
# detect sanity（忙/闲混合全局基线 μ≈2000 使 9000 偏离钝化 ≈3.5 <5 → 正常应极少告警）：
# 只防误报——若产生告警必须仅 5号线 且为真越界（dev>5），数量不做硬断言。
al = load("data/detect/alerts.ndjson")
ae = sorted({x.get("entity") for x in al})
if ae and ae != ["5号线"]:
    bad.append(f"detect 实体 {ae} 应仅 5号线（忙/闲正常事件不得进入全局基线通道误报）")
if al and not all(float(x.get("deviation", 0)) > 5.0 for x in al):
    bad.append("detect 告警 deviation 应 >5（真越界）")

print(f"  judge {len(j)} 条（全 5号线 z>3，首条 z={first_z:.1f}）；baseline {base_rows} 行；"
      f"detect {len(al)} 条（sanity）")
if bad:
    print("FAIL:")
    for b in bad:
        print("  -", b)
    sys.exit(1)
print(f"PASS: 近端 B 相位同窗接线 e2e——忙/闲双档 ${ROUNDS} 轮无崩溃无误报、每轮恰 1 条真越界，"
      "首条 z 落在相位生效区间；隔离语义由 wf-cep baseline 单测锁定")
PYEOF

kill "$DAEMON_PID" 2>/dev/null || true
trap - EXIT
echo ""
echo "DONE: baseline 相位同窗 e2e $ROUNDS 轮（产物保留在 data/ 下，可复查）"
