#!/usr/bin/env bash
# ===========================================================================
# PG 事实源 → 引擎供给聚合刷新生效实证（供给=records 直接聚合，无中转表）
#
#   1. docker compose 起 postgres → 建 baseline_records（清空）
#   2. 种子事实行：5 条线路各 1 行（n=1, sum=1000, sum_sq=1000400 → μ≈1000, σ=20）
#   3. 引擎 daemon（conf/refresh.wfusion.toml + knowdb.pg.toml 替换 knowdb.toml）
#      —— boot/每 1s NamedSql 刷新都对 baseline_records 执行聚合 SQL
#      （SELECT entity, sum(n)…GROUP BY entity），结果直接进 ProviderWindow
#   4. phase1 正常客流注入 → 无告警（μ≈1000）
#   5. UPDATE baseline_records 5号线 行（n=1, sum=5, sum_sq=25 → μ=5）
#   6. phase2 同批注入 → 仅 5号线 告警（mu=5, dev≈199）
#      —— 证明引擎读到的供给是"运行时对事实源现算"，改明细即改判定
#
# 用法: ./scripts/run_pg_refresh.sh
# ===========================================================================
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

WFUSION=${WFUSION:-wfusion}
WFGEN=${WFGEN:-wfgen}
PY=${PYTHON:-python3}
PORT=9800
LOG=data/logs/wfusion_refresh.log
ALERTS=data/detect/alerts.ndjson
CONF=conf/refresh.wfusion.toml
KDB=models/schemas/knowdb.toml
KDB_PG=models/schemas/knowdb.pg.toml
CSV_BACKUP=$(mktemp)

echo "==> 0. 起 postgres（docker compose）"
docker compose up -d postgres >/dev/null
CID=$(docker compose ps -q postgres)
for i in $(seq 1 40); do
  docker exec "$CID" pg_isready -U postgres -d postgres >/dev/null 2>&1 && break
  sleep 0.5
done
docker exec "$CID" pg_isready -U postgres -d postgres >/dev/null 2>&1 || { echo "ERROR: postgres 未就绪"; exit 1; }

echo "==> 1. 建事实表 baseline_records + 种子（全相位桶占位 μ≈1000，改明细即改供给）"
docker exec -i "$CID" psql -U postgres -d postgres -v ON_ERROR_STOP=1 < pg/baseline_records.sql >/dev/null
docker exec "$CID" psql -U postgres -d postgres -v ON_ERROR_STOP=1 -c "
INSERT INTO baseline_records (entity, metric, win_start, win_end, n, sum, sum_sq)
SELECT e, 'flow',
       to_char(TIMESTAMP '1970-01-01' + p * INTERVAL '15 second', 'YYYY-MM-DD HH24:MI:SS'),
       to_char(TIMESTAMP '1970-01-01' + (p + 1) * INTERVAL '15 second', 'YYYY-MM-DD HH24:MI:SS'),
       1, 1000.0, 1000400.0
FROM unnest(ARRAY['1号线','2号线','3号线','4号线','5号线']) AS e,
     generate_series(0, 15) AS p;" >/dev/null

# 备份并替换 knowdb.toml（PG 变体），退出恢复
cp "$KDB" "$CSV_BACKUP"
cp "$KDB_PG" "$KDB"
restore() {
  kill "${DAEMON_PID:-0}" 2>/dev/null || true
  cp "$CSV_BACKUP" "$KDB"; rm -f "$CSV_BACKUP"
}
trap restore EXIT

mkdir -p data/logs data/detect
rm -f "$LOG" "$ALERTS" data/daemon.log data/live.jsonl

echo "==> 2. 启动 daemon（供给 = records 聚合查询，1s NamedSql 刷新）"
"$WFUSION" daemon --config "$CONF" --work-dir . > data/daemon.log 2>&1 &
DAEMON_PID=$!
sleep 2
if ! kill -0 "$DAEMON_PID" 2>/dev/null; then
  echo "ERROR: daemon 启动失败" >&2
  tail -n 40 data/daemon.log >&2 || tail -n 40 "$LOG" >&2 || true
  exit 1
fi
for i in $(seq 1 60); do
  nc -z 127.0.0.1 "$PORT" 2>/dev/null && break
  sleep 0.2
done
nc -z 127.0.0.1 "$PORT" || { echo "ERROR: daemon 未就绪"; tail -30 "$LOG" 2>/dev/null || true; exit 1; }

send_batch() {
  local offset=$1
  "$PY" scripts/gen_metrics_live.py 1500 90 "$offset" > data/live.jsonl
  "$WFGEN" send --scenario models/scenarios/metrics_baseline.wfg \
    --input data/live.jsonl --addr 127.0.0.1:$PORT \
    --ws models/schemas/metrics.wfs 2>&1 | tail -1
}

alert_count() { [[ -f "$ALERTS" ]] && wc -l < "$ALERTS" | tr -d ' ' || echo 0; }

echo "==> 3. phase1：正常客流注入（μ≈1000，预期无告警）"
send_batch 60
sleep 4
N1=$(alert_count)
echo "   alerts = $N1 (expect 0)"
if [ "$N1" != "0" ]; then
  echo "FAIL: phase1 不应告警 (μ≈1000 时 dev<5%)" >&2
  exit 1
fi

echo "==> 4. UPDATE baseline_records 5号线（sum/sum_sq → μ=5）"
docker exec "$CID" psql -U postgres -d postgres -c \
  "UPDATE baseline_records SET n = 1, sum = 5.0, sum_sq = 25.0 WHERE entity = '5号线';" >/dev/null
sleep 4   # >=3 个 1s 聚合刷新周期

echo "==> 5. phase2：同批客流注入（预期仅 5号线 告警，dev≈199）"
send_batch 60
sleep 4

RELOADS=$(grep -c "provider refresh loaded table=baseline_ref" "$LOG" || true)
echo "   refresh log count = $RELOADS (expect >=2)"
if [ "$RELOADS" -lt 2 ]; then
  echo "FAIL: 未见供给周期重载日志" >&2
  exit 1
fi
N2=$(alert_count)
echo "   alerts = $N2"
if [ "$N2" -lt 1 ]; then
  echo "FAIL: phase2 应有告警 (μ=5 后正常客流即越界)" >&2
  exit 1
fi

"$PY" - <<PYEOF
import json, os, sys
alerts = []
p = "data/detect/alerts.ndjson"
if os.path.exists(p):
    with open(p, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                alerts.append(json.loads(line))
bad = []
by = {}
for a in alerts:
    by.setdefault(a.get("entity"), []).append(a)
if set(by) != {"5号线"}:
    bad.append(f"告警实体集合不符: {sorted(by)} 期望仅 5号线")
for a in alerts:
    if float(a.get("mu", 0)) != 5.0:
        bad.append(f"mu 应来自对 records 现算的聚合(5.0)，实际 {a.get('mu')}")
    d = float(a.get("deviation", 0))
    if not (150 <= d <= 250):
        bad.append(f"deviation 期望≈199，实际 {d}")
if bad:
    print("FAIL:")
    for b in bad:
        print("  -", b)
    sys.exit(1)
print(f"PASS: 引擎对事实源现算聚合——UPDATE baseline_records → 1s 刷新 → join 判定用新 μ "
      f"({len(alerts)} 条，仅 5号线，mu=5.0，dev≈199)")
PYEOF

echo ""
echo "PASS: records 直聚合供给刷新闭环（日志 $RELOADS 次重载）"
