#!/usr/bin/env bash
# ===========================================================================
# nginx_log_stats — batch 冒烟验证（确定性一次性回放）
# 持续运行模式请用 ./run.sh；本脚本为 CI 式单跑验证。
# ===========================================================================
# 流程:
#   1. wfgen lint           场景语法校验
#   2. wfgen gen            生成 NDJSON 演示数据 (2m × 100/s)
#   3. wfusion batch        回放数据 → 统计行 + 5xx 突发告警落 data/alerts/nginx.ndjson
#   4. 断言输出非空并打印摘要
#
# 前置条件:
#   - wfgen / wfusion 在 PATH 中 (可用 WFGEN=/path WFUSION=/path 覆盖)
# ===========================================================================
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

WFGEN_BIN="${WFGEN:-wfgen}"
WFUSION_BIN="${WFUSION:-wfusion}"

SCENARIO="models/scenarios/nginx_access_quick.wfg"
CASE_NAME="$(basename "$SCENARIO" .wfg)"
GENERATED_DIR="data/generated"
ALERT_DIR="data/alerts"
ALERTS_FILE="$ALERT_DIR/nginx.ndjson"

for cmd in "$WFGEN_BIN" "$WFUSION_BIN"; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "错误: 命令 '$cmd' 不在 PATH 中" >&2
    exit 1
  fi
done

mkdir -p "$GENERATED_DIR" "$ALERT_DIR" data/logs
rm -f "$ALERT_DIR"/*.ndjson data/wfusion.log

echo "1> lint scenario: $SCENARIO"
"$WFGEN_BIN" lint "$SCENARIO"

echo "2> generate events ($CASE_NAME)"
"$WFGEN_BIN" gen --scenario "$SCENARIO" --out "$GENERATED_DIR" --format jsonl

echo "3> run wfusion batch replay"
"$WFUSION_BIN" batch --config test/wfusion.batch.toml --work-dir .

if [[ ! -s "$ALERTS_FILE" ]]; then
  echo "错误: 期望非空输出: $ALERTS_FILE" >&2
  exit 1
fi

echo "4> output summary"
echo "  total rows : $(wc -l < "$ALERTS_FILE" | tr -d ' ')"
echo "  stats rows : $(grep -c '"alert_type":"nginx_status_stats"' "$ALERTS_FILE" || true)"
echo "  alert rows : $(grep -c '"alert_type":"http_5xx_surge"' "$ALERTS_FILE" || true)"

echo "5> sample rows"
head -5 "$ALERTS_FILE"

echo ""
echo "=============================================="
echo "  通过: nginx_log_stats batch 验证成功"
echo "=============================================="
echo "输出: $ALERTS_FILE"
echo "查看: ./view.sh  (浏览器打开 data/alerts/nginx.ndjson 展示页)"
