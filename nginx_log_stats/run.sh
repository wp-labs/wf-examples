#!/usr/bin/env bash
# ===========================================================================
# nginx_log_stats — 持续运行模式（实时检测 + 统计）
# ===========================================================================
# wfusion daemon（TCP :9800 接收）+ wfgen stream（按场景持续注入 Nginx 流量）。
# 引擎输出实时追加到 data/alerts/nginx.ndjson —— 另开终端 ./view.sh 即可
# 看到每 3 秒自动刷新的统计/检测看板。
#
# 用法:
#   ./run.sh            # 持续运行，Ctrl-C 停止
#   ./run.sh 30s        # 运行指定时长后自动停止（验收/演示用）
# 环境变量: INTERVAL / RATE 注入参数——
#   INTERVAL=60  每个场景完整跑完再切换（勿设小，否则场景被截断循环、事件时间回绕，
#                 stats 固定桶不再关闭——这是“累计请求不变”的根因）
#   RATE=0       0=用场景声明的 gen 100/s；可 RATE=1500 加速注入
# 注入场景: models/scenarios/live/nginx_access_live.wfg（虚拟时长 2h）——wfgen 每轮
# 场景循环会重置事件时间，虚拟时长越长，事件时间单调推进越久（2h 虚拟≈50min 真实），
# 避免演示窗口内出现统计冻结。
# ===========================================================================
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

DURATION="${1:-}"
INTERVAL="${INTERVAL:-60}"
RATE="${RATE:-0}"

for cmd in wfgen wfusion; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "错误: 命令 '$cmd' 不在 PATH 中" >&2
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
    echo "错误: 时长参数无效: '$DURATION'（例: 30s / 5m / 或省略=持续运行）" >&2
    exit 1
  fi
fi

# ── 清理 ──
cleanup() {
  [ -n "${WFGEN_PID:-}" ] && kill "$WFGEN_PID" 2>/dev/null || true
  [ -n "${WFUSION_PID:-}" ] && kill "$WFUSION_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "============================================"
echo "  nginx_log_stats — 持续运行（daemon + stream）"
echo "============================================"
echo "  输出实时追加: data/alerts/nginx.ndjson"
echo "  查看实时数据: ./view.sh  →  http://localhost:8123/view/"
echo "  停止: Ctrl-C${DURATION:+"　或 ${DURATION} 后自动停止"}"
echo "============================================"

mkdir -p data/alerts data/logs
rm -f data/alerts/*.ndjson data/logs/wfusion.log data/logs/wfgen.log data/wfusion.log

# 1) wfusion daemon（避免占用 9800 的残留进程）
lsof -ti:9800 2>/dev/null | xargs kill 2>/dev/null || true
sleep 1
echo "1> 启动 wfusion daemon (log=data/logs/wfusion.log)"
wfusion daemon --config conf/wfusion.toml --work-dir . >data/logs/wfusion.log 2>&1 &
WFUSION_PID=$!
sleep 2
if ! kill -0 "$WFUSION_PID" 2>/dev/null; then
  echo "错误: wfusion 启动失败" >&2
  tail -n 40 data/logs/wfusion.log >&2 || true
  exit 1
fi
echo "   wfusion PID=$WFUSION_PID"

# 2) wfgen stream — 持续注入 Nginx access 流量（含 5xx 突发）
WFL_ARGS=()
for f in models/rules/*/*.wfl; do WFL_ARGS+=(--wfl "$f"); done
echo "2> 启动 wfgen stream → 127.0.0.1:9800 (log=data/logs/wfgen.log)"
wfgen stream \
  --scenario-dir models/scenarios/live/ \
  --ws models/schemas/nginx.wfs \
  "${WFL_ARGS[@]}" \
  --addr 127.0.0.1:9800 \
  --interval "$INTERVAL" \
  --rate "$RATE" >data/logs/wfgen.log 2>&1 &
WFGEN_PID=$!
sleep 2
if ! kill -0 "$WFGEN_PID" 2>/dev/null; then
  echo "错误: wfgen stream 启动失败" >&2
  tail -n 40 data/logs/wfgen.log >&2 || true
  exit 1
fi
echo "   wfgen PID=$WFGEN_PID"
echo ""
echo "  运行中…（每 5s 打印一次输出行数）"

# 3) 主循环
elapsed=0
while true; do
  if ! kill -0 "$WFGEN_PID" 2>/dev/null || ! kill -0 "$WFUSION_PID" 2>/dev/null; then
    echo "错误: 进程提前退出（查看 data/logs/wfusion.log / wfgen.log）" >&2
    exit 1
  fi
  if [ $((elapsed % 5)) -eq 0 ]; then
    printf "  [%ss] 输出行数=%s\n" "$elapsed" "$(wc -l < data/alerts/nginx.ndjson 2>/dev/null | tr -d ' ' || echo 0)"
  fi
  sleep 1
  elapsed=$((elapsed + 1))
  if [ -n "$DURATION_SECONDS" ] && [ "$elapsed" -ge "$DURATION_SECONDS" ]; then
    break
  fi
done

echo "3> 停止进程…"
cleanup
sleep 1
echo "4> 运行结束: data/alerts/nginx.ndjson 共 $(wc -l < data/alerts/nginx.ndjson | tr -d ' ') 行"
