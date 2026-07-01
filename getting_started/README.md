# getting_started

wfusion 最简可用用例 — 一条命令验证完整 CEP 管道。

## 目的

验证 `wfadm init` → smoke test 全流程:

- `wfadm init --mode normal`: 从内置模板初始化完整项目（规则 + 窗口 + 拓扑 + 测试配置）
- `./smoke.sh`: lint 场景 → wfgen 生成演示数据 → wfusion batch replay → 验证告警输出

## 快速开始

```bash
cd getting_started
./run.sh
```

## 脚本流程

1. `wfadm init --dir .tmp-work --mode normal` — 初始化完整 wf-rules 项目
2. 进入 `.tmp-work/`
3. `./smoke.sh` — 三步验证:
   - `wfgen lint` — 场景语法校验
   - `wfgen gen` — 从场景生成 NDJSON 演示数据
   - `wfusion batch` — batch 模式回放数据 + 产出告警
4. 验证 `data/alerts/network.ndjson` 包含 `port_scan` 告警

## 生成的项目结构

```
.tmp-work/
├── conf/wfusion.toml               # 项目配置
├── models/
│   ├── rules/                      # 17 条检测规则
│   ├── schemas/                    # 6 个窗口定义
│   └── scenarios/                  # 5 个测试场景
├── topology/
│   ├── sources/                    # 输入源
│   └── sinks/                      # 输出 sink
├── test/wfusion.batch.toml         # batch 测试配置
├── smoke.sh                        # 烟雾测试脚本
└── test_run.sh                     # TCP 联调脚本
```

## 前置条件

- `wfadm` / `wfusion` / `wfgen` 在 PATH 中 (通过 `gx run build` 安装到 `$HOME/bin`)
