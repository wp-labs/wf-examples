#!/usr/bin/env bash
# common_rules_100 — 100 条常见 SOC 检测规则 · 性能/回归运行
#
# 测量"真实语义规则"负载（爆破/扫描/外传/C2/DGA/Web 攻击/被控主机）下：
#   1. 吞吐 EPS（事件注入 + 引擎消化口径）
#   2. 规则触发（emitted_total 按规则 label 累计 + 触发规则数）
#   3. #18 门禁（wp-reactor#18）：内存驱逐告警 = 0（object 大批次未被丢）
#   4. RSS 平台期峰值（送达后采样）
#
# 用法:
#   ./run.sh                 # 默认 200000 事件
#   ./run.sh 1000000         # 长跑
#   N=20000 ./run.sh         # 小规模快速冒烟
# 环境: WFUSION/WFGEN（缺省 PATH）；PROFILE=release|debug
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

PROFILE="${PROFILE:-release}"
REPO_ROOT="${REPO_ROOT:-}"
if [ -z "$REPO_ROOT" ] && [ -d "../../../warp-fusion" ]; then
  REPO_ROOT="$(cd ../../../warp-fusion && pwd)"
fi
WFUSION="${WFUSION:-}"
WFGEN="${WFGEN:-}"
if [ -z "$WFUSION" ] && [ -n "$REPO_ROOT" ] && [ -x "$REPO_ROOT/target/$PROFILE/wfusion" ]; then
  WFUSION="$REPO_ROOT/target/$PROFILE/wfusion"
fi
if [ -z "$WFGEN" ] && [ -n "$REPO_ROOT" ] && [ -x "$REPO_ROOT/target/$PROFILE/wfgen" ]; then
  WFGEN="$REPO_ROOT/target/$PROFILE/wfgen"
fi
WFUSION="${WFUSION:-$(command -v wfusion 2>/dev/null || true)}"
WFGEN="${WFGEN:-$(command -v wfgen 2>/dev/null || true)}"
if [ -z "$WFUSION" ] || [ -z "$WFGEN" ]; then
  echo "错误: 找不到 wfusion/wfgen（设置 REPO_ROOT/WFUSION/WFGEN 或加入 PATH）" >&2
  exit 1
fi
PY=${PYTHON:-python3}
N="${1:-${N:-200000}}"
PORT=9800
METRICS=data/metrics.ndjson

mkdir -p data
rm -f "$METRICS" data/*.ndjson data/wfusion.log data/daemon.log data/*.jsonl

echo "==> 0. 启动 daemon（TCP 源 + 指标，report_interval=1s） profile=$PROFILE"
"$WFUSION" daemon --config conf/wfusion.toml --work-dir . > data/daemon.log 2>&1 &
DAEMON_PID=$!
trap 'kill $DAEMON_PID 2>/dev/null || true' EXIT

# ---- 指标读取（label `-` = 无 label 指标）----
m() { "$PY" scripts/read_metrics.py "$METRICS" "$1" "$2" "${3:--}"; }
# receiver.rows_total 是 1s 区间 delta → 累加；acked_lag=0 表示引擎消化完
received() { "$PY" - "$METRICS" <<'EOF'
import json, sys
s = 0
try:
    for line in open(sys.argv[1]):
        try: o = json.loads(line)
        except Exception: continue
        if o.get("stage") == "receiver" and o.get("name") == "rows_total" and o.get("label") == "ingress":
            s += int(o.get("value", 0))
except FileNotFoundError:
    pass
print(s)
EOF
}
acked_lag() { m window acked_lag conn_events; }
# alert.emitted_total 按规则 label 累计；emitted = 全 label 和，rules = label 数(>0)
emitted() { "$PY" - "$METRICS" <<'EOF'
import json, sys
s = 0
seen = set()
try:
    for line in open(sys.argv[1]):
        try: o = json.loads(line)
        except Exception: continue
        if o.get("stage") == "alert" and o.get("name") == "emitted_total":
            v = int(o.get("value", 0))
            if o.get("label"):
                seen.add(o["label"])
                s += v
            elif s == 0:
                s = v
except FileNotFoundError:
    pass
print(s)
EOF
}
emit_rules() { "$PY" - "$METRICS" <<'EOF'
import json, sys
seen = set()
try:
    for line in open(sys.argv[1]):
        try: o = json.loads(line)
        except Exception: continue
        if o.get("stage") == "alert" and o.get("name") == "emitted_total" \
           and o.get("label") and int(o.get("value", 0)) > 0:
            seen.add(o["label"])
except FileNotFoundError:
    pass
print(f"{len(seen)}: " + ",".join(sorted(seen)))
EOF
}
evict() { grep -c "in memory eviction" data/wfusion.log 2>/dev/null || true; }
rss_kb() { ps -o rss= -p "$DAEMON_PID" 2>/dev/null | tr -d ' '; }

echo "==> 1. 等待 TCP 源就绪 (port $PORT)"
READY=0
for i in $(seq 1 50); do
  if nc -z 127.0.0.1 "$PORT" 2>/dev/null; then READY=1; break; fi
  sleep 0.2
done
[ "$READY" = 1 ] || { echo "ERROR: TCP 源未就绪"; tail -20 data/daemon.log; exit 1; }

echo "==> 2. 生成 $N 事件（正常底噪 + 攻击会话，seed=42）"
"$PY" scripts/gen_events.py "$N" > data/burst.jsonl
LINES=$(wc -l < data/burst.jsonl)
echo "    生成 $LINES 行（jsonl）"

# EPS 口径 = 引擎消化口径：从 send 开始计时，到 conn_events acked_lag=0（全部消化）
echo "==> 3. 全速发送并计时（引擎消化完 = acked_lag 归 0）"
START=$($PY -c 'import time; print(time.time())')
"$WFGEN" send --scenario scenarios/common.wfg --input data/burst.jsonl \
  --addr 127.0.0.1:$PORT --ws models/schemas/network.wfs > /dev/null 2>&1
for i in $(seq 1 300); do
  RCV=$(received)
  LAG=$(acked_lag)
  if [ "${RCV:-0}" -ge "$LINES" ] && [ "${LAG:-999}" = "0" ]; then break; fi
  sleep 0.2
done
END=$($PY -c 'import time; print(time.time())')
ELAPSED=$($PY -c "print($END - $START)")
RCV=$(received)
EPS=$($PY -c "print(int($LINES / $ELAPSED))" 2>/dev/null || echo 0)
echo "    接收 $RCV / $LINES 事件，引擎消化墙钟 ${ELAPSED}s"
echo "    EPS = $EPS events/sec（引擎消化口径）"

echo "==> 4. 送达后平台期采样（RSS 峰值 + 告警落盘，8s）"
RSS_PEAK=0
for i in $(seq 1 8); do
  sleep 1
  R=$(rss_kb)
  [ "${R:-0}" -gt "$RSS_PEAK" ] && RSS_PEAK=$R
done
RSS_MB=$(( RSS_PEAK * 1024 / 1048576 ))

echo ""
EV=$(evict)
EM=$(emitted)
RULE_TOTAL=$(grep -c '^rule ' models/rules/*.wfl | awk -F: '{s+=$NF} END{print s}')
RULE_HIT=$(emit_rules)   # 输出 "N: 规则1,规则2,..."（N = 触发规则数）
RULE_N=${RULE_HIT%%:*}
echo "==> 结果与回归门禁（wp-reactor#18：object 大批次不被窗口内存驱逐）"
echo "    EPS = $EPS events/sec（引擎消化口径）   RSS_peak = ${RSS_MB}MB（送达后 8s 平台期峰值）"
echo "    规则发射 emitted_total = ${EM}（引擎侧全量告警计数，按规则 label 累计）"
echo "    触发规则 = ${RULE_N}/${RULE_TOTAL}（emitted_total>0 的规则；列表见 data/metrics.ndjson 排查）"
echo "    内存驱逐告警 = ${EV}（窗口有损驱逐次数；0 = object 未被丢）"
[ "${EM}" -ge 1000 ] || { echo "FAIL: 规则未触发（emitted_total=${EM} < 1000），检查数据/规则" >&2; tail -15 data/wfusion.log >&2; exit 1; }
[ "${EV}" = "0" ] || { echo "FAIL: 回归失败 — 内存驱逐告警 ${EV} 次（object 大批次被窗口内存驱逐丢弃，wp-reactor#18）" >&2; exit 1; }
echo "OK: 回归通过 — 驱逐=0 且规则正常触发（emitted_total=${EM} ≥ 1000）"
[ "${EPS}" -ge 10000 ] && echo "OK: 吞吐达标（EPS=${EPS} ≥ 10000）" || echo "注: EPS=${EPS} < 10000（小规模/本机负载）"
echo "==> 完成。"
