#!/usr/bin/env bash
# ===========================================================================
# baseline 长期闭环 — 结果查看页（直接读取 data/ 下产物）
# ===========================================================================
# 用 python3 起一个只读静态服务（默认 8124 端口），然后打开浏览器：
#   http://localhost:8124/view/
# 页面每 2s 自动 fetch：
#   data/baseline/baseline.ndjson    收盘基线（每 15s 窗 × entity 一行）
#   data/detect/judge.ndjson         近端 B z 越界告警
#   data/detect/alerts.ndjson        远端 A provider 偏离告警
#   data/detect/baseline_ref.csv     远端 A 供给表（mu/sigma）
#   data/loop_samples.tsv            内存/RSS/行数采样（内存平台曲线）
#   data/logs/wfusion_loop.log       provider refresh 次数（log 锚点）
# 未跑 run_loop.sh 时各区块显示"暂无"，运行后自动出现。
#
# 用法: ./view.sh [port]
# ===========================================================================
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

PORT="${1:-8124}"

if ! command -v python3 >/dev/null 2>&1; then
  echo "错误: 需要 python3 提供静态服务" >&2
  exit 1
fi

echo "baseline 闭环查看: http://localhost:${PORT}/view/"
echo "（先 ./run.sh 持续运行生成 data/ 产物，或 ./scripts/run_loop.sh 做有界校验；Ctrl-C 停止服务）"
python3 -m http.server "$PORT" --bind 127.0.0.1
