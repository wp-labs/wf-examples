#!/usr/bin/env bash
# 持续注入拐点扫描：376规则 单IP 300万，多档注入速率测引擎跟上情况
set -u
cd "$(dirname "$0")"

# 二进制来源：优先本地 warp-fusion release 构建；否则回退 PATH（与 run.sh 同款探测）。
REPO_ROOT="${REPO_ROOT:-}"
if [ -z "$REPO_ROOT" ] && [ -d "../../../warp-fusion" ]; then
  REPO_ROOT="$(cd ../../../warp-fusion && pwd)"
fi
WF="${WFUSION:-}"; GEN="${WFGEN:-}"
if [ -z "$WF" ] && [ -n "$REPO_ROOT" ] && [ -x "$REPO_ROOT/target/release/wfusion" ]; then
  WF="$REPO_ROOT/target/release/wfusion"
fi
if [ -z "$GEN" ] && [ -n "$REPO_ROOT" ] && [ -x "$REPO_ROOT/target/release/wfgen" ]; then
  GEN="$REPO_ROOT/target/release/wfgen"
fi
[ -n "$WF" ] || WF="$(command -v wfusion 2>/dev/null || true)"
[ -n "$GEN" ] || GEN="$(command -v wfgen 2>/dev/null || true)"
if [ -z "$WF" ] || [ -z "$GEN" ]; then
  echo "错误: 找不到 wfusion/wfgen 二进制（设置 REPO_ROOT/WFUSION/WFGEN，或加入 PATH）" >&2
  exit 1
fi

PORT=9800; PY="${PYTHON:-python3}"
N="${1:-3000000}"
CONF=/tmp/sw.toml; FRAMES=/tmp/sw.frames; METRICS=data/metrics.ndjson
CR=/tmp/sw_rules.wfl
python3 - <<'EOF'
src=open('models/rules/throughput.wfl').read()
out=[]; r=0
for ln in src.split('\n'):
    if ln.startswith('rule '):
        r+=1
    if r<=450 or not ln.startswith('rule '): out.append(ln)
open('/tmp/sw_rules.wfl','w').write('\n'.join(out)+'\n')
EOF
sed -e 's|rules = "models/rules/\*.wfl"|rules = "/tmp/sw_rules.wfl"|' -e '/max_ingest_rate/d' conf/wfusion.toml > "$CONF"
pkill -9 -f "wfusion daemon" 2>/dev/null; sleep 1
echo "== gen+dump ${N} (once, reuse across sweeps) =="
GEN_SINGLE_IP=10.0.0.1 "$PY" scripts/gen_events.py "$N" > data/burst.jsonl 2>/dev/null
"$WF" daemon --config "$CONF" --work-dir . > /tmp/qd_sw.log 2>&1 &
D0=$!
for i in $(seq 1 40); do nc -z 127.0.0.1 "$PORT" 2>/dev/null && break; sleep 0.2; done
"$GEN" dump-frames --scenario scenarios/throughput.wfg --input data/burst.jsonl \
  --ws models/schemas/network.wfs --output "$FRAMES" --chunk 10000 \
  --max-frame-bytes 8388608 --max-frame-rows 100000 >/dev/null 2>&1
rm -f data/burst.jsonl
FW=$(ls -la "$FRAMES" | awk '{print $5}'); EV=$(( FW / N ))
echo "frames=$FW B 每事件=$EV B"
kill $D0 2>/dev/null; sleep 1

emit() {
  "$PY" <<'EOF'
import json
path='data/metrics.ndjson'; lab={}
try:
    for line in open(path, errors='replace'):
        try: o=json.loads(line)
        except Exception: continue
        if o.get('name')=='emitted_total':
            try: v=int(o.get('value',0))
            except (TypeError,ValueError): continue
            lab[o.get('label','?')]=max(lab.get(o.get('label','?'),0), v)
except FileNotFoundError: pass
print(sum(lab.values()))
EOF
}

run() {
  local EPS="$1"; local RATE=$((EPS*EV))
  : > "$METRICS"
  "$WF" daemon --config "$CONF" --work-dir . > /tmp/qd_r.log 2>&1 &
  local D=$!
  for i in $(seq 1 40); do nc -z 127.0.0.1 "$PORT" 2>/dev/null && break; sleep 0.2; done
  local T0=$($PY -c 'import time;print(time.time())')
  "$GEN" send-arrow --input "$FRAMES" --addr 127.0.0.1:$PORT --rate-bytes "$RATE" >/dev/null 2>&1
  local TINJ=$($PY -c 'import time;print(time.time())')
  local PE=-1 ST=0 E=0 i=0
  while [ $i -lt 6000 ]; do
    E=$(emit)
    if [ "$E" = "$PE" ] && [ "$E" -gt 0 ]; then ST=$((ST+1)); else ST=0; PE=$E; fi
    if [ "$ST" -ge 4 ]; then break; fi
    sleep 0.5; i=$((i+1))
  done
  local T1=$($PY -c 'import time;print(time.time())')
  local INJ=$($PY -c "print(f'{$TINJ-$T0:.1f}')"); local TOT=$($PY -c "print(f'{$T1-$T0:.1f}')")
  local DIG=$(( N / $($PY -c "print(max(1,int($T1-$T0)))") ))
  echo "rate=${EPS}kEPS inj=${INJ}s full=${TOT}s  全墙钟/注入=$($PY -c "print(f'{$T1-$T0}/{$TINJ-$T0:.1f}')")  engine_digest≈${DIG}"
  kill $D 2>/dev/null; sleep 1
}

for E in 100000 150000 250000 400000 700000; do
  echo "--- 注入 $((E/1000))K EPS ---"
  run "$E"
done
rm -f "$METRICS" "$FRAMES" /tmp/qd_sw.log /tmp/qd_r.log "$CONF" "$CR"
