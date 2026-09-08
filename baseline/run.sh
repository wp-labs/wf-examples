#!/usr/bin/env bash
# ===========================================================================
# baseline — 持续运行模式（长期闭环：生产 → 收盘 → judge / 导出 → refresh → detect）
# ===========================================================================
# wfusion daemon（TCP :9800）按 conf/loop.wfusion.toml 同时跑三条规则：
#   producer(15s 固定窗收盘) → baseline.ndjson + 实时滚动基线（共享 BaselineStore
#   → judge(z 越界)；另一后台每轮把收盘聚合导出 provider CSV（原子覆盖），
#   knowdb refresh(1s) 周期重载 → detect(全局周期基线 provider join)。
# 事件时间每轮 +120s 持续推进（窗持续收盘），每轮注入 1 个 5号线=9000 越界点，
# 两条判定通道持续出告警。输出实时追加 data/ 下 —— 另开终端 ./view.sh 即可看板。
#
# 用法:
#   ./run.sh              # CSV 数据后端，持续运行（Ctrl-C 停止）
#   ./run.sh 5m           # 运行指定时长后自动停止（验收/演示）
#   ./run.sh --pg         # 全局周期基线供给用 PG 做数据后端（docker postgres）
#   ./run.sh --pg 5m      # 参数可任意组合
# 环境: INJ_INTERVAL（注入轮间隔秒，默认 8）· COUNT / SPAN / PORT / WFUSION / WFGEN / PYTHON
# ===========================================================================
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

PG_MODE=0
DURATION=""
for a in "$@"; do
  case "$a" in
    --pg) PG_MODE=1 ;;                                        # 数据后端：PG
    --help|-h) echo "用法: ./run.sh [--pg] [时长]（例: ./run.sh --pg 5m）"; exit 0 ;;
    *s|*m|*h) DURATION="$a" ;;
    *) echo "错误: 未知参数 '$a'（支持 --pg 与时长，如 30s/5m）" >&2; exit 1 ;;
  esac
done
INJ_INTERVAL="${INJ_INTERVAL:-8}"
COUNT="${COUNT:-3000}"
SPAN="${SPAN:-90}"
PORT="${PORT:-9800}"
WFUSION=${WFUSION:-wfusion}
WFGEN=${WFGEN:-wfgen}
PY=${PYTHON:-python3}
SPIKE_IDX=$(( (COUNT / 2 / 5) * 5 + 4 ))   # 5号线（i%5==4）中段事件

for cmd in "$WFUSION" "$WFGEN"; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "错误: '$cmd' 不在 PATH 中（可用 WFUSION=~/bin/wfusion WFGEN=~/bin/wfgen 覆盖）" >&2
    exit 1
  fi
done

duration_to_seconds() {
  local v="$1"
  case "$v" in
    *s) echo "${v%s}" ;;
    *m) echo "$(( ${v%m} * 60 ))" ;;
    *h) echo "$(( ${v%h} * 3600 ))" ;;
    *) echo "$v" ;;
  esac
}
DURATION_SECONDS=""
if [ -n "$DURATION" ]; then
  DURATION_SECONDS="$(duration_to_seconds "$DURATION")"
  if ! [[ "$DURATION_SECONDS" =~ ^[0-9]+$ ]] || [ "$DURATION_SECONDS" -le 0 ]; then
    echo "错误: 时长参数无效: '$DURATION'（例: 30s / 5m / 省略=持续运行）" >&2
    exit 1
  fi
fi

LOG=data/logs/wfusion_loop.log
OUT=data/baseline/baseline.ndjson
JUDGE=data/detect/judge.ndjson
ALERTS=data/detect/alerts.ndjson
CSV=data/detect/baseline_ref.csv
METRICS=data/metrics.ndjson
SAMPLES=data/loop_samples.tsv
KDB=models/schemas/knowdb.toml
KDB_PG=models/schemas/knowdb.pg.toml
PG_REC_SQL=pg/baseline_records.sql
PG_CID=""
CONF=conf/loop.wfusion.toml

cleanup() {
  [ -n "${INJ_PID:-}" ] && kill "$INJ_PID" 2>/dev/null || true
  [ -n "${SAMP_PID:-}" ] && kill "$SAMP_PID" 2>/dev/null || true
  [ -n "${WFUSION_PID:-}" ] && kill "$WFUSION_PID" 2>/dev/null || true
  if [ -n "${KDB_BAK:-}" ] && [ -f "$KDB_BAK" ]; then
    cp "$KDB_BAK" "$KDB"
    rm -f "$KDB_BAK"
  fi
}
trap cleanup EXIT INT TERM

echo "============================================"
echo "  baseline — 持续长期闭环（daemon + 注入 + 导出刷新）"
if [ "$PG_MODE" = 1 ]; then
  echo "  数据后端: PG（全局周期基线供给 → knowdb NamedSql engine_pg 周期刷新）"
fi
echo "============================================"
echo "  收盘基线: data/baseline/baseline.ndjson（实时追加）"
echo "  judge/detect 告警: data/detect/judge.ndjson / alerts.ndjson"
echo "  查看看板: ./view.sh[ --pg] → http://localhost:8124/view/"
echo "  停止: Ctrl-C${DURATION:+" 或 ${DURATION} 后自动停止"}"
echo "============================================"

mkdir -p data/logs data/detect data/baseline
rm -f "$LOG" "$OUT" "$JUDGE" "$ALERTS" "$METRICS" "$SAMPLES" data/daemon.log data/live.jsonl "$CSV"

# 0) 数据后端：CSV（默认）或 PG（--pg）
if [ "$PG_MODE" = 1 ]; then
  command -v docker >/dev/null 2>&1 || { echo "错误: --pg 需要 docker（docker-compose.yml 起 postgres）" >&2; exit 1; }
  echo "0> PG 数据后端：起 postgres + 种子表"
  docker compose up -d postgres >/dev/null
  PG_CID=$(docker compose ps -q postgres)
  for i in $(seq 1 40); do
    docker exec "$PG_CID" pg_isready -U postgres -d postgres >/dev/null 2>&1 && break
    sleep 0.5
  done
  docker exec "$PG_CID" pg_isready -U postgres -d postgres >/dev/null 2>&1 || { echo "ERROR: postgres 未就绪" >&2; exit 1; }
  # 事实库（PG sink 落点）建表/清空；供给由引擎每次装载/刷新直接聚合本表
  docker exec -i "$PG_CID" psql -U postgres -d postgres -v ON_ERROR_STOP=1 < "$PG_REC_SQL" >/dev/null
  # 启动占位种子（win_start='seed' 标记）：boot 即有 5 行可注册 provider；
  # 首轮收盘后由下方清理，避免污染事实聚合。
  docker exec "$PG_CID" psql -U postgres -d postgres -v ON_ERROR_STOP=1 -c "
INSERT INTO baseline_records (entity, metric, win_start, win_end, n, sum, sum_sq) VALUES
  ('1号线','flow','seed','seed',1,1000.0,1000400.0),
  ('2号线','flow','seed','seed',1,1000.0,1000400.0),
  ('3号线','flow','seed','seed',1,1000.0,1000400.0),
  ('4号线','flow','seed','seed',1,1000.0,1000400.0),
  ('5号线','flow','seed','seed',1,1000.0,1000400.0);" >/dev/null
  # 引擎 knowdb 配置切 PG 变体（退出恢复 CSV 变体）；sink 树切 sinks-pg（双写 PG 事实库）
  KDB_BAK=$(mktemp)
  cp "$KDB" "$KDB_BAK"
  cp "$KDB_PG" "$KDB"
  CONF=conf/loop.pg.wfusion.toml
else
  # 种子 provider CSV（占位 μ≈1000 σ=20；首轮 detect 即有基线；随后 exporter 原子覆盖）
  "$PY" - <<PYEOF
import csv
lines = ["1号线", "2号线", "3号线", "4号线", "5号线"]
with open("$CSV", "w", encoding="utf-8", newline="") as f:
    w = csv.writer(f)
    w.writerow(["entity", "n", "sum", "sum_sq", "mu", "sigma"])
    for e in lines:
        w.writerow([e, 240, 240000.0, 240096000.0, 1000.0, 20.0])
PYEOF
fi

# 1) wfusion daemon（避免占用 9800 的残留进程）
lsof -ti:$PORT 2>/dev/null | xargs kill 2>/dev/null || true
sleep 1
echo "1> 启动 wfusion daemon (log=$LOG)"
"$WFUSION" daemon --config "$CONF" --work-dir . >data/daemon.log 2>&1 &
WFUSION_PID=$!
sleep 2
if ! kill -0 "$WFUSION_PID" 2>/dev/null; then
  echo "错误: wfusion 启动失败" >&2
  tail -n 40 data/daemon.log >&2 || true
  exit 1
fi
echo "   wfusion PID=$WFUSION_PID"

echo "2> 启动注入 + 导出刷新 + 采样后台任务"

# 注入/导出 worker：事件时间每轮 +120s，每轮导出一次（闭环刷新）
(
  r=0
  while kill -0 "$WFUSION_PID" 2>/dev/null; do
    r=$((r + 1))
    offset=$((r * 120))
    "$PY" scripts/gen_metrics_live.py "$COUNT" "$SPAN" "$offset" "$SPIKE_IDX" > data/live.jsonl
    "$WFGEN" send --scenario models/scenarios/metrics_baseline.wfg --input data/live.jsonl \
      --addr 127.0.0.1:$PORT --ws models/schemas/metrics.wfs >/dev/null 2>&1 || true
    sleep 3                      # 收盘 → sink 落盘
    "$PY" scripts/export_baseline_ref.py > /dev/null 2>&1 || true   # 聚合 → CSV（看板/审计，两模式都写）
    sleep "$INJ_INTERVAL"
  done
) &
INJ_PID=$!

# 采样 worker：RSS/commit/行数 → loop_samples.tsv（内存平台曲线）
m() { "$PY" scripts/read_metrics.py "$METRICS" "$1" "$2" "${3:--}"; }
(
  echo -e "epoch\tps_rss_kb\talloc_rss\talloc_commit\tbaseline_rows\tjudge\talerts" > "$SAMPLES"
  while kill -0 "$WFUSION_PID" 2>/dev/null; do
    R=0; J=0; A=0
    [[ -f "$OUT" ]] && R=$(wc -l < "$OUT" | tr -d ' ')
    [[ -f "$JUDGE" ]] && J=$(wc -l < "$JUDGE" | tr -d ' ')
    [[ -f "$ALERTS" ]] && A=$(wc -l < "$ALERTS" | tr -d ' ')
    printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\n" "$(date +%s)" \
      "$(ps -o rss= -p "$WFUSION_PID" 2>/dev/null | tr -d ' ')" \
      "$(m alloc current_rss_bytes)" "$(m alloc current_commit_bytes)" \
      "$R" "$J" "$A" >> "$SAMPLES"
    sleep 1
  done
) &
SAMP_PID=$!

echo "3> 健康自检：等待首轮收盘 + 双通道首条告警（≤45s）"
elapsed=0
healthy=0
while [ "$elapsed" -lt 45 ]; do
  R=0; J=0; A=0
  [[ -f "$OUT" ]] && R=$(wc -l < "$OUT" | tr -d ' ')
  [[ -f "$JUDGE" ]] && J=$(wc -l < "$JUDGE" | tr -d ' ')
  [[ -f "$ALERTS" ]] && A=$(wc -l < "$ALERTS" | tr -d ' ')
  if [ "$R" -gt 0 ] && [ "$A" -gt 0 ] && [ "$J" -gt 0 ]; then
    healthy=1; break
  fi
  if ! kill -0 "$WFUSION_PID" 2>/dev/null; then break; fi
  sleep 2
  elapsed=$((elapsed + 2))
done
if [ "$healthy" -ne 1 ]; then
  echo "ERROR: loop not healthy in ${elapsed}s (baseline=$R judge=$J alerts=$A)" >&2
  tail -n 40 "$LOG" 2>/dev/null || tail -n 40 data/daemon.log >&2 || true
  exit 1
fi
echo "   healthy: round-1 closed loop OK (baseline=$R judge=$J alerts=$A)"
if [ "$PG_MODE" = 1 ]; then
  # 首轮收盘已把真实聚合写入 provider——清掉启动占位种子（等 2s 覆盖 ≥1 个刷新周期）
  sleep 2
  docker exec "$PG_CID" psql -U postgres -d postgres -c "DELETE FROM baseline_records WHERE win_start = 'seed';" >/dev/null || true
fi

echo "4> 运行中…（每 5s 打印一次）"
watch_elapsed=0
while true; do
  if ! kill -0 "$WFUSION_PID" 2>/dev/null; then
    echo "ERROR: daemon exited early (see data/daemon.log / $LOG)" >&2
    tail -n 30 data/daemon.log >&2 || true
    exit 1
  fi
  if [ $((watch_elapsed % 5)) -eq 0 ]; then
    R=0; J=0; A=0
    [[ -f "$OUT" ]] && R=$(wc -l < "$OUT" | tr -d ' ')
    [[ -f "$JUDGE" ]] && J=$(wc -l < "$JUDGE" | tr -d ' ')
    [[ -f "$ALERTS" ]] && A=$(wc -l < "$ALERTS" | tr -d ' ')
    printf "  [%ss] baseline=%s judge=%s alerts=%s rss=%sMB\n" \
      "$watch_elapsed" "$R" "$J" "$A" \
      "$(ps -o rss= -p "$WFUSION_PID" 2>/dev/null | tr -d ' ' | awk '{print int($1/1024)}')"
  fi
  sleep 1
  watch_elapsed=$((watch_elapsed + 1))
  if [ -n "$DURATION_SECONDS" ] && [ "$watch_elapsed" -ge "$DURATION_SECONDS" ]; then
    break
  fi
done

echo "5> 停止进程…"
cleanup
sleep 1
R=0; J=0; A=0
[[ -f "$OUT" ]] && R=$(wc -l < "$OUT" | tr -d ' ')
[[ -f "$JUDGE" ]] && J=$(wc -l < "$JUDGE" | tr -d ' ')
[[ -f "$ALERTS" ]] && A=$(wc -l < "$ALERTS" | tr -d ' ')
echo "6> 运行结束: baseline=$R 行 judge=$J 条 alerts=$A 条（./view.sh 回放产物）"
