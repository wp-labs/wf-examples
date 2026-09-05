#!/usr/bin/env bash
# ===========================================================================
# nginx_log_stats — 输出查看页（直接读取 data/alerts/*.ndjson）
# ===========================================================================
# 用 python3 起一个只读静态服务（默认 8123 端口），然后打开浏览器。
# 页面会 fetch data/alerts/nginx.ndjson 展示；未跑 run.sh 时可先用
# 页面上的「选择文件」直接打开任意 ndjson。
#
# 用法: ./view.sh [port]
# ===========================================================================
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

PORT="${1:-8123}"

if ! command -v python3 >/dev/null 2>&1; then
  echo "错误: 需要 python3 提供静态服务" >&2
  exit 1
fi

echo "nginx_log_stats 输出查看: http://localhost:${PORT}/view/"
echo "（先跑 ./run.sh 生成 data/alerts/nginx.ndjson；Ctrl-C 停止服务）"
python3 -m http.server "$PORT" --bind 127.0.0.1
