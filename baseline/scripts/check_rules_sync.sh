#!/usr/bin/env bash
# ===========================================================================
# rules-loop 组合快照一致性守卫（见 models/README.md）：
#   闭环 conf 的 rules 是单 glob → judge/detect/producer_long 必须复制在同一目录；
#   改动 canonical 源后忘同步副本会让闭环形态与最小验证形态行为漂移。
# 逐字节 diff 三对文件，任一不一致即 FAIL。
#
# 用法: ./scripts/check_rules_sync.sh        （挂在 run_loop.sh / run_phase.sh 启动）
# ===========================================================================
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

check() {
  if ! diff -q "$1" "$2" >/dev/null 2>&1; then
    echo "DRIFT: $1 与 $2 不一致——副本须与 canonical 源逐字同步（见 models/README.md）" >&2
    return 1
  fi
}

fail=0
check models/rules/baseline_judge.wfl models/rules-loop/baseline_judge.wfl || fail=1
check models/rules/baseline_detect.wfl models/rules-loop/baseline_detect.wfl || fail=1
check models/rules-long/baseline_producer_long.wfl models/rules-loop/baseline_producer_long.wfl || fail=1

if [ "$fail" = 1 ]; then
  exit 1
fi
echo "PASS: rules-loop 组合快照与 canonical 源一致"
