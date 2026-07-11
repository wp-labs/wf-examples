# remote_ctrl

本用例演示"从远端规则仓库同步规则,并切换规则版本"的场景(对齐 wparse 的 `wp-examples/core/remote_ctrl`)。

## 目的

验证以下能力:

- 使用 `wfadm conf update` 从远端 git 仓库同步 managed 目录(models/conf/topology/connectors)
- 在 dual-repo 模式下,对 models 组(wf-rules)做版本切换(v0.1.5 → v0.1.6)
- 版本状态持久化到 `.run/project_remote_state.json`
- 校验远端同步产物已使用 `stream_tag` / `stream_tag = ""`，不再包含旧的 `stream =` 配置字段
- 通过 admin_api `POST /admin/v1/reloads/model` 执行在线 reload / 发布
- 验证阻断变更返回 `409 reload_failed`,不会触发进程重启

## 远端仓库

- **wf-conf-example**:`https://github.com/wp-labs/wf-conf-example.git` — 提供 infra 组(conf/topology/connectors),也用作工作目录的初始项目来源;其 `conf/wfusion.toml` 自带 `[project_remote]` dual-repo 配置
  - 引导版本:`v0.1.3`(含 `[project_remote]` 配置、外置 `windows.toml` 和 `stream_tag` source/schema 配置)
- **wf-rules**:`https://github.com/wp-labs/wf-rules.git` — 提供 models 组(models/),本用例对它做版本切换
  - 初始版本:`v0.1.5`
  - 切换目标:`v0.1.6`

## 快速开始

```bash
cd core/remote_ctrl
./run.sh
```

可选覆盖参数:

```bash
CONF_REPO=https://github.com/wp-labs/wf-conf-example.git \
CONF_INIT_VERSION=v0.1.3 \
INIT_VERSION=v0.1.5 \
TARGET_VERSION=v0.1.6 \
L2_VERSION=v0.1.6 \
WORK_ROOT="$PWD/.tmp-work" \
./run.sh
```

## 脚本流程

1. `wfadm init --repo <wf-conf-example> --version v0.1.3` 从远端引导工作目录(建骨架 + 同步 managed dirs;拉下的 `conf/wfusion.toml` 已含 `[project_remote]` dual-repo 配置和 `stream_tag` source/schema 配置)。
2. `wfadm conf update --group models --version v0.1.5` 首次同步 models 组到 wf-rules v0.1.5。
3. `wfadm conf update --group models --version v0.1.6` 切换 models 组到 v0.1.6。
4. 校验 `.run/project_remote_state.json` 的 `models.version` 已切换,且 `models/rules/` 规则文件就位；同时检查 conf/models/topology/test 中的 `.toml/.wfs` 已对齐 `stream_tag` 配置。
5. 启动 `wfusion daemon` 和 admin_api。
6. 修改本地规则 score,调用 `POST /admin/v1/reloads/model` 发送 `{}` 验证 L1 热替换返回 `200 reload_done`。
7. 调用 admin_api 发送 `{"update":true,"group":"models","version":"v0.1.6"}` 验证在线发布返回 `200 reload_done`,响应包含 `current_version`。
8. 调用 status 端点验证 `accepting_commands=true`。
9. 无 token 调用 reload 验证 `401`。
10. 修改需重启的 window 配置后 reload,验证返回 `409 reload_failed`。

## 说明

- 本用例依赖网络访问(`wfadm init --repo` / `conf update` 会 clone 远端 git 仓库)。
- 工作目录保留在 `.tmp-work` 下,便于失败后排查。
- 覆盖 `wfadm init --repo`(远端引导)+ `wfadm conf update`(离线 sync + 校验)+ admin_api reload/update(在线切换/发布)。
- wf-rules v0.1.6 中 `ssh_brute_force.wfl` 的 `detail` 文案带有 `rules-reload-test-v0.1.6` 标记,用于观察 reload 后新规则内容是否生效。
