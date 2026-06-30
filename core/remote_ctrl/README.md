# remote_ctrl

本用例演示"从远端规则仓库同步规则,并切换规则版本"的场景(对齐 wparse 的 `wp-examples/core/remote_ctrl`)。

## 目的

验证以下能力:

- 使用 `wfadm conf update` 从远端 git 仓库同步 managed 目录(models/conf/topology/connectors)
- 在 dual-repo 模式下,对 models 组(wf-rules)做版本切换(v0.1.0 → v0.1.1)
- 版本状态持久化到 `.run/project_remote_state.json`

## 远端仓库

- **wf-conf-example**:`https://github.com/wp-labs/wf-conf-example.git` — 提供 infra 组(conf/topology/connectors),也用作工作目录的初始项目来源;其 `conf/wfusion.toml` 自带 `[project_remote]` dual-repo 配置
  - 引导版本:`v0.1.1`(含 `[project_remote]` 配置)
- **wf-rules**:`https://github.com/wp-labs/wf-rules.git` — 提供 models 组(models/),本用例对它做版本切换
  - 初始版本:`v0.1.0`
  - 切换目标:`v0.1.1`

## 快速开始

```bash
cd core/remote_ctrl
./run.sh
```

可选覆盖参数:

```bash
CONF_REPO=https://github.com/wp-labs/wf-conf-example.git \
CONF_INIT_VERSION=0.1.1 \
INIT_VERSION=0.1.0 \
TARGET_VERSION=0.1.1 \
WORK_ROOT="$PWD/.tmp-work" \
./run.sh
```

## 脚本流程

1. `wfadm init --repo <wf-conf-example> --version 0.1.1` 从远端引导工作目录(建骨架 + 同步 managed dirs;拉下的 `conf/wfusion.toml` 已含 `[project_remote]` dual-repo 配置)。
2. `wfadm conf update --group models --version 0.1.0` 首次同步 models 组到 wf-rules v0.1.0。
3. `wfadm conf update --group models --version 0.1.1` 切换 models 组到 v0.1.1。
4. 校验 `.run/project_remote_state.json` 的 `models.version` 已切换,且 `models/rules/` 规则文件就位。

## 说明

- 本用例依赖网络访问(`wfadm init --repo` / `conf update` 会 clone 远端 git 仓库)。
- 工作目录保留在 `.tmp-work` 下,便于失败后排查。
- 覆盖 `wfadm init --repo`(远端引导)+ `wfadm conf update`(离线 sync + 校验)。admin_api reload(在线切换)尚未实现,待后续补齐后将扩展为完整的 init → daemon → reload 链路(对齐 wparse remote_ctrl)。
- wf-rules 的 `models/` 在 v0.1.0 与 v0.1.1 之间内容相同(仅根 `version.txt` 变化),因此版本切换体现在 state 文件的 `models.version`,而非 `models/` 目录内容变化。
