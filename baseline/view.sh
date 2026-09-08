#!/usr/bin/env bash
# ===========================================================================
# baseline 长期闭环 — 结果查看页（直接读取 data/ 下产物）
# ===========================================================================
# 用 python3 起一个只读静态服务（默认 8124 端口），然后打开浏览器：
#   http://localhost:8124/view/
# 页面每 2s 自动 fetch：
#   data/baseline/baseline.ndjson    收盘基线（每 15s 窗 × 线路 一行）
#   data/detect/judge.ndjson         实时滚动基线(judge) z 越界告警
#   data/detect/alerts.ndjson        全局周期基线(detect) 偏离告警
#   data/detect/baseline_ref.csv     全局周期基线供给表（mu/sigma；--pg 模式下仍每轮写出供本看板）
#   data/loop_samples.tsv            内存/RSS/行数采样（内存平台曲线）
#   data/logs/wfusion_loop.log       provider refresh 次数（log 锚点）
# 未跑 run.sh 时各区块显示"暂无"，运行后自动出现。
#
# 用法:
#   ./view.sh            # 默认（run.sh：CSV 数据后端）
#   ./view.sh --pg       # run.sh --pg（PG 数据后端）——本看板仍读引擎产物文件，
#                        # 供给表由 run.sh --pg 每轮原子写出 baseline_ref.csv + 写 PG
#   ./view.sh 8125       # 自定义端口；--pg 与端口可同用
# ===========================================================================
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

PG_MODE=0
PORT=8124
for a in "$@"; do
  case "$a" in
    --pg) PG_MODE=1 ;;
    --help|-h) echo "用法: ./view.sh [--pg] [port]（默认 8124）"; exit 0 ;;
    *) PORT="$a" ;;
  esac
done

if ! command -v python3 >/dev/null 2>&1; then
  echo "错误: 需要 python3 提供静态服务" >&2
  exit 1
fi

echo "baseline 闭环查看: http://localhost:${PORT}/view/"
if [ "$PG_MODE" = 1 ]; then
  echo "（数据后端: PG —— 先 ./run.sh --pg 持续运行；供给表另经 docker postgres 验证）"
else
  echo "（先 ./run.sh 持续运行生成 data/ 产物，或 ./scripts/run_loop.sh 做有界校验；Ctrl-C 停止服务）"
fi
python3 -m http.server "$PORT" --bind 127.0.0.1
