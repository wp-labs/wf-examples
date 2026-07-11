#!/usr/bin/env bash
# ===========================================================================
# remote_ctrl — wfusion 全链路验证脚本
# ===========================================================================
# 验证范围:
#   远端运维 → 本地 bootstrap → 规则版本切换 → daemon 启动 →
#   admin_api 在线热重载/发布 (L1/L2/blocked)
#
# 对应 wparse 的 wp-examples/core/remote_ctrl,在 wfusion 中补齐了
# admin_api reload/publish(在线热切换/发布)部分。
#
# 前置条件:
#   - wfusion / wfadm / curl 在 PATH 中(通过 gx run build 安装到 $HOME/bin)
#   - 可访问 GitHub(wf-conf-example 和 wf-rules 仓库)
#   - 端口 9800 可用(daemon TCP source)
#
# 验证的 reload 能力分层(对应设计文档 docs/design/admin_api_reload_design.md §11):
#   L1: 规则热替换(改 score,不改 window/schema) → 200
#   L2: admin_api update=true + group/version 在线发布 → 200
#   blocked: 阻断变更 → 409 + reload_failed
#   (L3 修改现有 window schema 由单元/集成测试覆盖,未在本脚本中验证)
#
# 流程:
#   步骤 1-4.  wfadm init --repo + conf update 版本切换(离线 sync)
#   步骤 5.    启动 daemon + admin_api
#   步骤 6.    L1: 修改已有规则(score 70.0→99.0)→ POST reload → 200
#   步骤 7.    L2: POST update=true group=models version=v0.1.6 → 200
#   步骤 8.    status 端点:引擎健康检查
#   步骤 9.    鉴权:无 token → 401
#   步骤 10.   blocked: 修改 over_cap → 409 + reload_failed
# ===========================================================================
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"
DAEMON_PID=""

cleanup() {
  if [[ -n "${DAEMON_PID:-}" ]]; then
    kill "$DAEMON_PID" 2>/dev/null || true
    wait "$DAEMON_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

assert_stream_tag_config() {
  local root="$1"
  local legacy

  legacy=$(
    find "$root/conf" "$root/models" "$root/topology" "$root/test" \
      -type f \( -name '*.toml' -o -name '*.wfs' \) -print0 |
      xargs -0 grep -nE '^[[:space:]]*stream[[:space:]]*=' 2>/dev/null || true
  )
  if [[ -n "$legacy" ]]; then
    echo "错误: 同步后的 case 仍包含旧配置字段 stream =,请更新远端版本为 stream_tag ="
    echo "$legacy"
    exit 1
  fi

  if ! grep -R "stream_tag" "$root/conf" "$root/models" "$root/topology" "$root/test" >/dev/null 2>&1; then
    echo "错误: 同步后的 case 未包含 stream_tag 配置"
    exit 1
  fi
}

assert_models_version() {
  local expected="$1"
  local normalized="${expected#v}"
  local tag="v$normalized"

  if grep -Eq "\"version\"[[:space:]]*:[[:space:]]*\"($expected|$normalized)\"" "$STATE_FILE" ||
     grep -Eq "\"tag\"[[:space:]]*:[[:space:]]*\"($expected|$tag)\"" "$STATE_FILE"; then
    return 0
  fi

  echo "错误: models 组未同步到 $expected"
  cat "$STATE_FILE"
  exit 1
}

# -- 远端仓库/版本配置(可通过环境变量覆盖) --------------------------------
CONF_REPO="${CONF_REPO:-https://github.com/wp-labs/wf-conf-example.git}"
# wf-conf-example 中携带 [project_remote] 配置的 tag(bootstrap 目标)
CONF_INIT_VERSION="${CONF_INIT_VERSION:-v0.1.3}"
# wf-rules models 组的版本(first sync → switch)
INIT_VERSION="${INIT_VERSION:-v0.1.5}"
TARGET_VERSION="${TARGET_VERSION:-v0.1.6}"
L2_VERSION="${L2_VERSION:-v0.1.6}"
WORK_ROOT="${WORK_ROOT:-$PWD/.tmp-work}"

STATE_FILE="$WORK_ROOT/.run/project_remote_state.json"

# 检查前置命令(需通过 gx run build 安装到 PATH)
for cmd in wfadm wfusion curl; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "错误: 命令 '$cmd' 不在 PATH 中,请先执行 gx run build"
    exit 1
  fi
done

# -- 步骤 1-4: wfadm 离线 sync ---------------------------------------------
# 从 wf-conf-example 远端仓库 bootstrap 工作根目录,
# 再通过 `conf update` 切换 wf-rules 的 models 组版本。

echo "步骤 1> 从远端仓库 bootstrap: $CONF_REPO @ $CONF_INIT_VERSION"
rm -rf "$WORK_ROOT"
wfadm init --dir "$WORK_ROOT" --repo "$CONF_REPO" --version "$CONF_INIT_VERSION" >/dev/null

echo "步骤 2> 同步 models 组到 $INIT_VERSION (从 wf-rules 拉取规则)"
wfadm conf update --work-root "$WORK_ROOT" --group models --version "$INIT_VERSION" --json

if [[ ! -f "$STATE_FILE" ]]; then
  echo "错误: 状态文件未创建: $STATE_FILE"
  exit 1
fi
assert_models_version "$INIT_VERSION"
echo "   ✓ models 组已同步到 $INIT_VERSION"

echo "步骤 3> 切换 models 组到 $TARGET_VERSION (版本切换)"
wfadm conf update --work-root "$WORK_ROOT" --group models --version "$TARGET_VERSION" --json

assert_models_version "$TARGET_VERSION"
echo "   ✓ models 组已切换到 $TARGET_VERSION"

echo "步骤 4> 验证 models 目录已从 wf-rules 同步"
# wf-rules 使用嵌套目录布局(如 models/rules/01-recon/...),
# wf-conf-example 使用扁平布局(如 models/rules/port_scan.wfl)。
# 检查 01-recon/ 路径是否存在来验证 models 组已被正确替换。
if [[ ! -f "$WORK_ROOT/models/rules/01-recon/port_scan.wfl" ]]; then
  echo "错误: 同步后缺少 wf-rules 标志性规则布局"
  exit 1
fi
echo "   ✓ models/rules 目录存在"
assert_stream_tag_config "$WORK_ROOT"
echo "   ✓ stream_tag 配置契约已验证"

# wf-rules 使用嵌套目录布局,但早期 wf-conf-example 的配置使用扁平 glob。
# 新版本已自带递归 glob；这里保留兼容替换。
sed -i.bak2 's|models/rules/\*.wfl|models/rules/**/*.wfl|' "$WORK_ROOT/conf/wfusion.toml"

# -- 步骤 5: 启动 daemon + admin_api ----------------------------------------
echo "步骤 5> 配置 admin_api 并启动 wfusion daemon"
# 拉取的配置已有 [admin_api] 段(默认值),
# 覆盖 bind 地址(临时端口)和 token 文件路径。
sed -i.bak \
  -e 's/bind = "127.0.0.1:19080"/bind = "127.0.0.1:0"/' \
  -e 's|token_file = "\${HOME}/.warp_fusion/admin_api.token"|token_file = "runtime/admin_api.token"|' \
  "$WORK_ROOT/conf/wfusion.toml"

# 创建 token 文件(必须 600 权限)
mkdir -p "$WORK_ROOT/runtime"
echo "reload-test-token" > "$WORK_ROOT/runtime/admin_api.token"
chmod 600 "$WORK_ROOT/runtime/admin_api.token"

# 后台启动 daemon,日志写入 work_root/logs/
mkdir -p "$WORK_ROOT/logs"
wfadmlog="$WORK_ROOT/logs/wf-engine.log"
# 杀掉前次运行残留的 daemon(占用 9800 端口)
lsof -ti:9800 2>/dev/null | xargs kill 2>/dev/null || true
wfusion daemon --config "$WORK_ROOT/conf/wfusion.toml" --work-dir "$WORK_ROOT" >> "$wfadmlog" 2>&1 &
DAEMON_PID=$!

# 等待 admin_api 打印监听地址(日志行缓冲,5s 后读取)
sleep 5
ADMIN_ADDR=$(grep -o 'http://127\.0\.0\.1:[0-9]*' "$wfadmlog" | head -1 | sed 's|http://||')
if [[ -z "$ADMIN_ADDR" ]]; then
  echo "错误: admin_api 未在 5 秒内启动"
  exit 1
fi
echo "   ✓ admin_api 已监听 http://$ADMIN_ADDR"

# -- 步骤 6: L1 验证 (rule-only 热替换) -----------------------------------
echo "步骤 6> [L1] 规则热替换: 修改 ssh_brute_force 的 score (70.0→99.0)"
RULE_FILE="$WORK_ROOT/models/rules/02-initial_access/ssh_brute_force.wfl"
sed -i.bak 's/score(70.0)/score(99.0)/' "$RULE_FILE"

RELOAD_AUTH="Bearer reload-test-token"
RESP=$(curl -s -w "\n%{http_code}" -X POST "http://$ADMIN_ADDR/admin/v1/reloads/model" \
  -H "Authorization: $RELOAD_AUTH" \
  -H "Content-Type: application/json" \
  -d '{}')
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
if [[ "$HTTP_CODE" != "200" ]]; then
  echo "错误: L1 规则热替换应返回 200,实际返回 $HTTP_CODE"
  echo "$BODY"
  exit 1
fi
if ! echo "$BODY" | grep -q '"result".*"reload_done"'; then
  echo "错误: L1 热替换结果不是 'reload_done'"
  echo "$BODY"
  exit 1
fi
echo "   ✓ L1 热替换成功 (score 70.0 → 99.0)"

# -- 步骤 7: L2 验证 (增量新增) --------------------------------------------
echo "步骤 7> [L2] 在线发布: admin_api update=true group=models version=$L2_VERSION"
RESP=$(curl -s -w "\n%{http_code}" -X POST "http://$ADMIN_ADDR/admin/v1/reloads/model" \
  -H "Authorization: $RELOAD_AUTH" \
  -H "Content-Type: application/json" \
  -d "{\"update\":true,\"group\":\"models\",\"version\":\"$L2_VERSION\",\"reason\":\"remote_ctrl-l2\"}")
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
if [[ "$HTTP_CODE" != "200" ]]; then
  echo "错误: L2 在线发布应返回 200,实际返回 $HTTP_CODE"
  echo "$BODY"
  exit 1
fi
assert_models_version "$L2_VERSION"
if ! echo "$BODY" | grep -q '"result".*"reload_done"'; then
  echo "错误: L2 在线发布结果不是 'reload_done'"
  echo "$BODY"
  exit 1
fi
L2_NORMALIZED="${L2_VERSION#v}"
if ! echo "$BODY" | grep -Eq "\"current_version\"[[:space:]]*:[[:space:]]*\"($L2_VERSION|$L2_NORMALIZED)\""; then
  echo "错误: L2 响应未返回 current_version=$L2_VERSION"
  echo "$BODY"
  exit 1
fi
echo "   ✓ L2 在线发布成功 (models=$L2_VERSION, reload-test 规则已应用)"

# -- 步骤 8: status 端点验证 ------------------------------------------------
echo "步骤 8> 验证 status 端点 (L1+L2 之后引擎健康)"
STATUS=$(curl -s -w "\n%{http_code}" -X GET "http://$ADMIN_ADDR/admin/v1/runtime/status" \
  -H "Authorization: $RELOAD_AUTH")
HTTP_CODE=$(echo "$STATUS" | tail -1)
BODY=$(echo "$STATUS" | sed '$d')
if [[ "$HTTP_CODE" != "200" ]] || ! echo "$BODY" | grep -q '"accepting_commands".*true'; then
  echo "错误: status 端点异常"
  echo "$BODY"
  exit 1
fi
echo "   ✓ status: accepting_commands=true (引擎正常运行)"

# -- 步骤 9: 鉴权验证 -------------------------------------------------------
echo "步骤 9> 验证 bearer 鉴权: 无 token → 401"
RESP=$(curl -s -w "\n%{http_code}" -X POST "http://$ADMIN_ADDR/admin/v1/reloads/model" \
  -H "Content-Type: application/json" \
  -d '{}')
HTTP_CODE=$(echo "$RESP" | tail -1)
if [[ "$HTTP_CODE" != "401" ]]; then
  echo "错误: 无 token 应返回 401,实际返回 $HTTP_CODE"
  exit 1
fi
echo "   ✓ 无 token 正确返回 401"

# -- 步骤 10: blocked 验证 --------------------------------------------------
echo "步骤 10> [blocked] 阻断变更 (改 over_cap 30m→1h) → 409 + reload_failed"
# 修改 over_cap 触发 raw-diff 阻断(类型=Windows,需重启)
sed -i.bak2 's/over_cap = "30m"/over_cap = "1h"/' "$WORK_ROOT/models/schemas/windows.toml"
RESP=$(curl -s -w "\n%{http_code}" -X POST "http://$ADMIN_ADDR/admin/v1/reloads/model" \
  -H "Authorization: $RELOAD_AUTH" \
  -H "Content-Type: application/json" \
  -d '{"reason":"remote_ctrl-blocked"}')
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
if [[ "$HTTP_CODE" != "409" ]]; then
  echo "错误: 阻断变更应返回 409,实际返回 $HTTP_CODE"
  echo "$BODY"
  exit 1
fi
if ! echo "$BODY" | grep -q '"result".*"reload_failed"'; then
  echo "错误: 阻断变更结果不是 'reload_failed'"
  echo "$BODY"
  exit 1
fi
echo "   ✓ 阻断变更被正确拒绝 (daemon 继续运行)"

echo "=============================================="
echo "  通过: 全链路验证成功 (L1+L2+blocked)"
echo "=============================================="
