#!/usr/bin/env bash
# ===========================================================================
# 全局周期基线 · PG 供给刷新实证（S2-M3c-PG）
#
#   1. docker compose 起 postgres（docker-compose.yml）
#   2. 建表 + 种子 5 条线路基线（mu≈1000, sigma=20）→ pg/baseline_ref.sql
#   3. 引擎 daemon（conf/refresh.wfusion.toml + knowdb.pg.toml 替换 knowdb.toml）
#      —— boot 从 PG 命名 provider(engine_pg) SELECT 装载 baseline_ref，
#         NamedSql 规格 1s 周期刷新
#   4. phase1 正常客流注入 → 无告警（mu=1000）
#   5. UPDATE 5号线 mu→5（等效 CSV 覆盖）→ 等刷新
#   6. phase2 同批再注入 → 仅 5号线 告警（mu=5, dev≈199）
#      —— 证明 PG 供给的周期刷新真实生效（判定读到的是更新后的数据库行）
#
# 用法: ./scripts/run_pg_refresh.sh    （退出后自动恢复 knowdb.toml 并重种表）
# 环境: WFUSION/WFGEN/PYTHON/COMPOSE_DIR(默认本目录)
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
SQL=pg/baseline_ref.sql
CSV_BACKUP=$(mktemp)

echo "==> 0. 起 postgres（docker compose）"
docker compose up -d postgres >/dev/null
CID=$(docker compose ps -q postgres)
for i in $(seq 1 40); do
  docker exec "$CID" pg_isready -U postgres -d postgres >/dev/null 2>&1 && break
  sleep 0.5
done
docker exec "$CID" pg_isready -U postgres -d postgres >/dev/null 2>&1 || { echo "ERROR: postgres 未就绪"; exit 1; }

echo "==> 1. 建表 + 种子基线（pg/baseline_ref.sql）"
docker exec -i "$CID" psql -U postgres -d postgres -v ON_ERROR_STOP=1 < "$SQL" >/dev/null

# 备份并替换 knowdb.toml（PG 变体），退出恢复
cp "$KDB" "$CSV_BACKUP"
cp "$KDB_PG" "$KDB"
restore() {
  kill "${DAEMON_PID:-0}" 2>/dev/null || true
  cp "$CSV_BACKUP" "$KDB"; rm -f "$CSV_BACKUP"
  # 表重种为初始基线（demo 后清场）
  docker exec -i "$CID" psql -U postgres -d postgres -v ON_ERROR_STOP=1 < "$SQL" >/dev/null 2>&1 || true
}
trap restore EXIT

mkdir -p data/logs data/detect
rm -f "$LOG" "$ALERTS" data/daemon.log data/live.jsonl

# 避免占用 9800 的残留 daemon 干扰（本 case 脚本都用 9800）
lsof -ti:$PORT 2>/dev/null | xargs kill 2>/dev/null || true
sleep 1

echo "==> 2. 启动 daemon（PG 供给 + 1s NamedSql 刷新）"
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

echo "==> 3. phase1：正常客流注入（mu=1000，预期无告警）"
send_batch 60
sleep 4
N1=$(alert_count)
echo "   alerts = $N1 (expect 0)"
if [ "$N1" != "0" ]; then
  echo "FAIL: phase1 不应告警 (mu=1000 时 dev<5%)" >&2
  exit 1
fi

echo "==> 4. UPDATE 5号线 mu 1000 -> 5"
docker exec "$CID" psql -U postgres -d postgres -c \
  "UPDATE baseline_ref SET mu = 5.0, n = 1, sum = 5.0, sum_sq = 25.0, sigma = 0.0 WHERE entity = '5号线';" >/dev/null
sleep 4   # >=3 个 1s NamedSql 刷新周期

echo "==> 5. phase2：同批客流注入（预期仅 5号线 告警，dev≈199）"
send_batch 60
sleep 4

RELOADS=$(grep -c "provider refresh loaded table=baseline_ref" "$LOG" || true)
echo "   refresh log count = $RELOADS (expect >=2)"
if [ "$RELOADS" -lt 2 ]; then
  echo "FAIL: 未见 PG 周期重载日志" >&2
  exit 1
fi
N2=$(alert_count)
echo "   alerts = $N2"
if [ "$N2" -lt 1 ]; then
  echo "FAIL: phase2 应有告警 (mu=5 后正常客流即越界)" >&2
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
        bad.append(f"mu 应来自 PG 更新后值(5.0)，实际 {a.get('mu')}")
    d = float(a.get("deviation", 0))
    if not (150 <= d <= 250):
        bad.append(f"deviation 期望≈199，实际 {d}")
if bad:
    print("FAIL:")
    for b in bad:
        print("  -", b)
    sys.exit(1)
print(f"PASS: PG 供给刷新生效——UPDATE mu=5 → NamedSql 周期重载 → join 判定用新值 "
      f"({len(alerts)} 条，仅 5号线，mu=5.0，dev≈199)")
PYEOF

echo ""
echo "PASS: PG 供给刷新闭环（日志 $RELOADS 次重载）"
