#!/usr/bin/env bash
# ===========================================================================
# baseline 长期运行闭环（单 daemon，全链路）——
#   producer(15s 收盘) → baseline_out(ndjson) + 实时滚动基线（共享 BaselineStore
#   → judge(z 越界) ；exporter 周期把收盘聚合导出 CSV → knowdb refresh(1s)
#   → detect(全局周期基线 provider join)。事件时间每轮 +120s 持续驱动收盘；
#   每轮注入 1 个 5号线=9000 越界点，验证 两条判定通道都告警且互不影响。
#
# 产物/断言（data/ 下，随 .gitignore 不入库）：
#   data/baseline/baseline.ndjson   单调增长（每轮新收盘 ≥ 若干键）
#   data/detect/judge.ndjson        实时滚动基线：5号线 z>>3 越界告警
#   data/detect/alerts.ndjson       全局周期基线：5号线 (9000−μ)/μ≈8 > 5 越界告警
#   data/logs/wfusion_loop.log      provider refresh 周期日志锚点
#   内存平台：末段 6s×2 采样 Δ 小于容忍（状态随窗回收）
# 用法: ./scripts/run_loop.sh [rounds]   默认 5 轮 ≈ 1 分钟
# 环境: ROUNDS/GROW_MB/WFUSION/WFGEN/PYTHON
# ===========================================================================
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

WFUSION=${WFUSION:-wfusion}
WFGEN=${WFGEN:-wfgen}
PY=${PYTHON:-python3}
PORT=9800
ROUNDS="${1:-${ROUNDS:-5}}"
GROW_MB="${GROW_MB:-80}"
COUNT=3000
SPAN=90
SPIKE_IDX=$(( (COUNT / 2 / 5) * 5 + 4 ))   # 5号线（i%5==4）中段事件
CONF=conf/loop.wfusion.toml
LOG=data/logs/wfusion_loop.log
OUT=data/baseline/baseline.ndjson
ALERTS=data/detect/alerts.ndjson
JUDGE=data/detect/judge.ndjson
CSV=data/detect/baseline_ref.csv
METRICS=data/metrics.ndjson
SAMPLES=data/loop_samples.tsv

mkdir -p data/logs data/detect data/baseline
rm -f "$LOG" "$OUT" "$ALERTS" "$JUDGE" "$METRICS" "$SAMPLES" data/daemon.log data/live.jsonl "$CSV"
# 端口 9800 被各 case 共享：残留 daemon 会抢占 → 新 daemon bind 失败或注入打进
# 旧实例（偶发首轮 rx=0/judge=0 的根因）。先清场并**等端口真正释放**再启动。
lsof -ti:"$PORT" 2>/dev/null | xargs kill 2>/dev/null || true
for _i in $(seq 1 20); do
  lsof -ti:"$PORT" >/dev/null 2>&1 || break
  sleep 0.2
done

# 种子 provider CSV（占位 μ≈1000，σ=20；每实体×每相位桶一行——detect 按
# (entity, phase_bucket) join，boot 即任意相位可命中；后续每轮由 exporter 用
# 真实收盘聚合原子覆盖）。schema = entity,phase_bucket,n,sum,sum_sq,mu,sigma
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

echo "==> 0. start closed-loop daemon (conf=$CONF rounds=$ROUNDS)"
./scripts/check_rules_sync.sh >/dev/null   # rules-loop 组合快照一致性（见 models/README.md）
"$WFUSION" daemon --config "$CONF" --work-dir . > data/daemon.log 2>&1 &
DAEMON_PID=$!
trap 'kill $DAEMON_PID 2>/dev/null || true' EXIT

for i in $(seq 1 60); do
  nc -z 127.0.0.1 "$PORT" 2>/dev/null && break
  sleep 0.2
done
nc -z 127.0.0.1 "$PORT" || { echo "ERROR: TCP 源未就绪"; tail -30 "$LOG" 2>/dev/null || true; exit 1; }

# 指标采样器（标签 `-` = 无 label，同 run_long）
m() { "$PY" scripts/read_metrics.py "$METRICS" "$1" "$2" "${3:--}"; }
commit_bytes() { m alloc current_commit_bytes; }
arss_bytes()   { m alloc current_rss_bytes; }

echo -e "epoch\tps_rss_kb\talloc_rss\talloc_commit\tbaseline_rows\tjudge\talerts" > "$SAMPLES"
(
  while kill -0 "$DAEMON_PID" 2>/dev/null; do
    R=0; J=0; A=0
    [[ -f "$OUT" ]] && R=$(wc -l < "$OUT" | tr -d ' ')
    [[ -f "$JUDGE" ]] && J=$(wc -l < "$JUDGE" | tr -d ' ')
    [[ -f "$ALERTS" ]] && A=$(wc -l < "$ALERTS" | tr -d ' ')
    printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\n" "$(date +%s)" \
      "$(ps -o rss= -p "$DAEMON_PID" 2>/dev/null | tr -d ' ')" \
      "$(arss_bytes)" "$(commit_bytes)" "$R" "$J" "$A" >> "$SAMPLES"
    sleep 1
  done
) &
SAMPLER_PID=$!

send_round() {
  local offset=$1
  "$PY" scripts/gen_metrics_live.py "$COUNT" "$SPAN" "$offset" "$SPIKE_IDX" > data/live.jsonl
  "$WFGEN" send --scenario models/scenarios/metrics_baseline.wfg \
    --input data/live.jsonl --addr 127.0.0.1:$PORT \
    --ws models/schemas/metrics.wfs 2>&1 | tail -1
}

nlines() { local f=$1; [[ -f "$f" ]] && wc -l < "$f" | tr -d ' ' || echo 0; }

prev_rows=0; prev_judge=0; prev_alerts=0
# 相位化 detect（2026-09-08）：供给按 (entity, phase_bucket) join。spike 相位在
# offset 120/240 交替轮间于 p11/p3 循环 → 同相位第二次出现（r≥3）起每轮告警，
# 首个周期（冷相位）静默（r1/r2 可 0）。judge（相位关滚动）仍每轮 1 条。
echo "==> 1. 分轮注入 + 收盘导出闭环（每轮 offset +120s、1 个 5号线=9000 越界）"
for r in $(seq 1 "$ROUNDS"); do
  offset=$((r * 120))
  echo "-- round $r (offset +${offset}s)"
  send_round "$offset"
  sleep 3                       # 收盘 → baseline_out sink 落盘
  "$PY" scripts/export_baseline_ref.py > /dev/null   # 聚合→CSV（原子替换）
  sleep 2                       # knowdb refresh(1s) 周期内换表
  ROWS=$(nlines "$OUT"); JUDGE_N=$(nlines "$JUDGE"); ALERTS_N=$(nlines "$ALERTS")
  echo "   baseline=$ROWS judge=$JUDGE_N alerts=$ALERTS_N (prev $prev_rows/$prev_judge/$prev_alerts)"
  if (( r >= 2 && ROWS <= prev_rows )); then
    echo "FAIL: round $r no new closes (windows not advancing? $prev_rows -> $ROWS)" >&2
    tail -30 "$LOG" 2>/dev/null || true
    exit 1
  fi
  if (( JUDGE_N < r )); then
    echo "FAIL: round $r judge=$JUDGE_N 期望 ≥$r（5号线 spike 每轮 z 越界）" >&2
    exit 1
  fi
  # detect：同相位冷启动后须单调不回落（累计 ≥ max(0, r-2)）
  det_exp=$(( r >= 3 ? r - 2 : 0 ))
  if (( ALERTS_N < det_exp )); then
    echo "FAIL: round $r detect=$ALERTS_N 期望 ≥$det_exp（相位复现后每轮应告警；" \
      "冷相位首现静默是相位化正确行为）" >&2
    exit 1
  fi
  prev_rows=$ROWS; prev_judge=$JUDGE_N; prev_alerts=$ALERTS_N
done

echo "==> 2. 末段内存平台采样（6s 间隔）"
sleep 2
C1=$(commit_bytes); R1=$(ps -o rss= -p "$DAEMON_PID" 2>/dev/null | tr -d ' ')
sleep 6
C2=$(commit_bytes); R2=$(ps -o rss= -p "$DAEMON_PID" 2>/dev/null | tr -d ' ')
DC=$(( (C2 - C1) / 1024 / 1024 )); DR=$(( (R2 - R1) / 1024 ))
echo "   alloc commit: $C1 -> $C2 (delta $DC MB); ps rss: $R1 -> $R2 (delta $DR MB)"
kill "$SAMPLER_PID" 2>/dev/null || true

ROWS=$(nlines "$OUT"); JUDGE_N=$(nlines "$JUDGE"); ALERTS_N=$(nlines "$ALERTS")
RELOADS=$(grep -c "provider refresh loaded table=baseline_ref" "$LOG" || true)
echo "==> 3. 终态：baseline=$ROWS judge=$JUDGE_N alerts=$ALERTS_N refresh_log=$RELOADS"

# 终态校验（python）：两条判定通道只含 5号线、值≈9000；CSV 5 行且 μ≈1000；
# 数量级与内存平台达标
"$PY" - <<PYEOF
import csv, json, os, sys
out = "data/baseline/baseline.ndjson"
judge = "data/detect/judge.ndjson"
alerts = "data/detect/alerts.ndjson"
csvp = "data/detect/baseline_ref.csv"

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
a = load(alerts)
je = sorted({x.get("entity") for x in j})
ae = sorted({x.get("entity") for x in a})
if je != ["5号线"]:
    bad.append(f"judge 实体 {je} 期望仅 5号线")
if ae != ["5号线"]:
    bad.append(f"detect 实体 {ae} 期望仅 5号线")
if j and not all(float(x.get("z", 0)) > 3.0 for x in j):
    bad.append("judge z 应 >3")
if a and not all(7.0 <= float(x.get("deviation", 0)) <= 9.0 for x in a):
    bad.append("detect deviation 应 ≈8")
base_rows = 0
with open(out, encoding="utf-8") as f:
    base_rows = sum(1 for line in f if line.strip())
if base_rows < 20:
    bad.append(f"baseline 收盘过少 {base_rows}")
ref = []
with open(csvp, encoding="utf-8") as f:
    for r in csv.DictReader(f):
        ref.append((r["entity"], r["phase_bucket"], float(r["mu"])))
# 相位化供给（2026-09-08）：每实体×每相位桶一行；行数随收盘覆盖的桶增长（≥5），
# 且所有行 μ≈1000（5号线 spike 所在桶被 9000 抬到 ≈1013，仍在容差内）。
if len(ref) < 5:
    bad.append(f"provider CSV 行数过少 {len(ref)}")
mu_all = [mu for (_, _, mu) in ref]
if not (900 <= min(mu_all) and max(mu_all) <= 1100):
    bad.append(f"provider 行 μ 应 ≈1000，实际范围 {min(mu_all):.1f}..{max(mu_all):.1f}")
if $RELOADS < 10:
    bad.append("refresh 日志过少")
if $DC > $GROW_MB or $DR > $GROW_MB:
    bad.append(f"内存仍增长 commitΔ=$DC MB rssΔ=$DR MB")

print(f"  judge {len(j)} 条 z>3；detect {len(a)} 条 dev≈8；baseline {base_rows} 行；"
      f"csv {len(ref)} 行 μ={min(mu_all):.1f}..{max(mu_all):.1f}")
if bad:
    print("FAIL:")
    for b in bad:
        print("  -", b)
    sys.exit(1)
print("PASS: 闭环长期运行——producer 持续收盘 → 两条判定通道各按注入告警、"
      "provider 周期刷新、末段内存平台")
PYEOF

kill "$DAEMON_PID" 2>/dev/null || true
trap - EXIT
echo ""
echo "DONE: baseline 长期闭环 $ROUNDS 轮（产物保留在 data/ 下，可复查）"
