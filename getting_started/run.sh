#!/usr/bin/env bash
# ===========================================================================
# getting_started — wfusion 最简可用用例
# ===========================================================================
# 验证 wfadm init → smoke.sh + test_run.sh 全流程:
#   1. wfadm init --mode normal  初始化完整项目 (规则+窗口+拓扑+测试)
#   2. smoke.sh                  lint → wfgen 生成数据 → batch replay → 验证告警
#   3. test_run.sh 30s           TCP daemon + wfgen stream 联调
#
# 前置条件:
#   - wfadm / wfusion / wfgen 在 PATH 中 (通过 gx run build 安装到 $HOME/bin)
#   - 端口 9800 可用 (TCP daemon source)
# ===========================================================================
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

assert_stream_tag_config() {
  local root="$1"
  local legacy

  legacy=$(
    find "$root/conf" "$root/models" "$root/topology" "$root/test" \
      -type f \( -name '*.toml' -o -name '*.wfs' \) -print0 |
      xargs -0 grep -nE '^[[:space:]]*stream[[:space:]]*=' 2>/dev/null || true
  )
  if [[ -n "$legacy" ]]; then
    echo "错误: 生成的 case 仍包含旧配置字段 stream =,请更新为 stream_tag ="
    echo "$legacy"
    exit 1
  fi

  if ! grep -R "stream_tag" "$root/conf" "$root/models" "$root/topology" "$root/test" >/dev/null 2>&1; then
    echo "错误: 生成的 case 未包含 stream_tag 配置"
    exit 1
  fi
}

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
rm -rf tmp-work
wfadm init --dir tmp-work --mode normal
assert_stream_tag_config "tmp-work"
echo "   ✓ 项目已创建: tmp-work"
echo "   ✓ stream_tag 配置契约已验证"
echo "   (可用 ls tmp-work 查看生成的文件)"

# 步骤 2: smoke 测试 (batch replay + 验证)
echo ""
echo "步骤 2> smoke 测试 (lint + wfgen 生成数据 + batch replay + 验证)"
cd tmp-work
./smoke.sh
echo "   ✓ smoke 测试通过"

# 步骤 3: TCP daemon 联调 (wfgen stream → wfusion daemon)
echo ""
echo "步骤 3> TCP daemon 联调 (wfgen stream → wfusion daemon, 10s)"
# 杀掉前次运行残留的 daemon (占用 9800 端口)
lsof -ti:9800 2>/dev/null | xargs kill 2>/dev/null || true
sleep 1  # 等待端口释放
./test_run.sh 10s
echo "   ✓ TCP daemon 联调通过"

cd ..
echo ""
echo "=============================================="
echo "  通过: getting_started 全流程验证成功"
echo "=============================================="
echo ""
echo "生成的项目位于: tmp-work/"
echo "探索: cd tmp-work && ls -la"
