# wf-examples — wfusion 场景 / 基准用例集

wfusion（warp-fusion + wp-reactor）的**可运行验证场景与性能基准**仓库：从最简管道、
运维控制面，到 NEXMark / QRadar 对标与内存稳定性验证，每个 case 自带规则、场景、
配置与一键脚本，可直接复跑并产出数据。

配套仓库（构建产物使用方）：

| 仓库 | 提供 | 消费方式 |
|---|---|---|
| `warp-fusion` | `wfusion`（daemon/CLI）与 `wfgen`（编译/注入/诊断）二进制 | case 脚本默认取 PATH 中的 `wfusion`/`wfgen`（可用 `WFUSION=/path` `WFGEN=/path` 覆盖） |
| `wp-reactor` | `wf-engine` / `wf-runtime` / `wf-lang` 引擎（warp-fusion 以 path 依赖引用） | 引擎行为修复会改变 case 结论——case README 会注明所需引擎版本（如 memory_stability 依赖墙钟信用累计修复 `727cbfe`） |

## 目录导航

| 目录 | 定位 | 入口 |
|---|---|---|
| [`getting_started/`](getting_started/README.md) | 最简可用：一条命令验证完整 CEP 管道（batch + daemon 联调） | `./run.sh` |
| [`core/remote_ctrl/`](core/remote_ctrl/README.md) | 远端规则仓库同步 + 规则版本切换 + admin_api 热重载（L1/L2/blocked）全链路 | `./run.sh` |
| [`core/meta_disable/`](core/meta_disable/) | meta/spill 关闭路径冒烟 | `./smoke.sh` |
| [`connectors/`](connectors/) | sink/source 连接器配置样例（blackhole/file/doris/syslog/tcp…） | 供 case `topology/` 引用 |
| [`performance/nexmark_pk/`](performance/nexmark_pk/README.md) | NEXMark Q1–Q22：吞吐 PK（bench）+ 性能墙定位（diag）+ 正确性对拍（verify） | `./bench.sh` `./diag.sh` `./verify_daemon.sh` |
| [`performance/qradar_pk/`](performance/qradar_pk/README.md) | 376 规则高压吞吐 + IBM QRadar EP 对标（run/diag/sweep/规则集二分；run.sh 内置 #18 object 驱逐门禁） | `./run.sh` `./diag.sh` `./sweep.sh` |
| [`performance/common_rules_100/`](performance/common_rules_100/README.md) | 100 条常见 SOC 检测规则（爆破/扫描/C2/DGA/Web 攻击…）真实语义负载性能 case | `./run.sh` |
| [`performance/perf_diag_case/`](performance/perf_diag_case/README.md) | perf-diag 诊断**机制**本身的独立验证（有区分度的墙梯） | `./verify.sh` |
| [`performance/memory_stability/`](performance/memory_stability/README.md) | 长时间运行内存稳定性：idle 实例按 TTL 释放 + allocator 口径泄漏检测 | `./run.sh [smoke|demo|leak]` |
| [`performance/scripts/`](performance/scripts/) | 共享 python 库：`bench_lib.py`（度量/注入）、`diag_analyze.py`（墙表/墙判定）、`diag_mem_analyze.py`、`rule_phase_profile.py` | 被各 case 的 shell 脚本调用 |

## 快速开始

```bash
# 1) 构建/准备工具（warp-fusion），并放入 PATH：
#    cargo build --release -p wfusion -p wfgen
#    ln -s .../warp-fusion/target/release/{wfusion,wfgen} ~/bin/

# 2) 最简管道验证（~1 分钟）
cd getting_started && ./run.sh

# 3) 按需进入各 case（各自 README 为准）
cd ../performance/nexmark_pk && ./bench.sh q1 replay 10m      # 示例：NEXMark q1 吞吐
cd ../performance/memory_stability && ./run.sh --leak         # 示例：内存稳定性泄漏检测
```

## 通用约定

- **模式**：性能/验证类 case 均以 `wfusion daemon`（TCP 实时注入 + 指标上报）为主路径；
  正确性类（nexmark verify、perf_diag_case）走 wfgen 驱动的对拍/哨兵机制。
- **产物**：运行产物统一落在各 case 的 `data/`（已被 `.gitignore` 忽略）；指标流
  `metrics.ndjson`、日志 `daemon.log`/`wfusion.log` 可事后分析。
- **口径**：EPS/内存等判定的准确定义见各 case README 与 `performance/scripts/` 实现；
  基准数据页会注明机器与引擎版本（跨机/跨版本对比需同口径）。
- **端口**：各 case 使用自己的 TCP 注入端口（多数为 9800），同机并发跑多个 case 前先
  确认端口空闲。

## 变更记录

见 [`CHANGELOG.md`](CHANGELOG.md)（performance / verification 场景的逐项改动、实测数据
与结论修正均按日期归档）。
