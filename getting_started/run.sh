#!/usr/bin/env bash
# ===========================================================================
# getting_started — wfusion 最简可用用例
# ===========================================================================
# 验证 wfadm init → smoke.sh 全流程:
#   1. wfadm init --mode normal  初始化完整项目 (规则+窗口+拓扑+测试)
#   2. smoke.sh                  lint → wfgen 生成数据 → batch replay → 验证告警
#
# 前置条件:
#   - wfadm / wfusion / wfgen / wfl 在 PATH 中 (通过 gx run build 安装到 $HOME/bin)
# ===========================================================================
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

# 检查前置命令
for cmd in wfadm wfusion wfgen; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "错误: 命令 '$cmd' 不在 PATH 中,请先执行 gx run build"
    exit 1
  fi
done

echo "=============================================="
echo "  wfusion getting_started — 最简可用用例"
echo "=============================================="

# 步骤 1: wfadm init 初始化项目
echo ""
echo "步骤 1> wfadm init 初始化项目"
rm -rf .tmp-work
wfadm init --dir .tmp-work --mode normal
echo "   ✓ 项目已创建: .tmp-work"

# 步骤 2: 进入项目目录,运行 smoke 测试
echo ""
echo "步骤 2> smoke 测试 (lint + 生成数据 + batch replay + 验证)"
cd .tmp-work
./smoke.sh

echo ""
echo "=============================================="
echo "  通过: getting_started 全流程验证成功"
echo "=============================================="
echo ""
echo "生成的项目位于: .tmp-work"
echo "查看: ls -la .tmp-work"
