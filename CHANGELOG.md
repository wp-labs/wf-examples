# Changelog

All notable changes to the wf-examples performance / verification scenarios will be documented in this file.

## [Unreleased]

### Changed

- **场景重命名**：`performance/eps_throughput_rules100` → **`performance/qradar_pk`**（对标 QRadar Event Processor PK 负载）；`performance/flink_pk` 删除（其对比数据已并入 qradar_pk README 的行业定位表）。
- **qradar_pk — 规则 300 → 450**（对标 IBM QRadar Event Processor 认证负载 **451 条规则 / 80k EPS**）：`scripts/gen_rules.py` 目标从 300 改为 450，新增约 150 条真实规则——conn action 过滤计数、更高阈值、聚合/guard/distinct 网格扩展，auth/dns/proxy/firewall/file 各源深化，6 条多事件 + 2 条序列；`count` 补齐降至 74。
- **qradar_pk — 450 规则实测**（release，200k 事件，M3 Max，连续 3 轮）：**EPS ~96.7k（95.8-98.0k）、RSS ~2.24GB、驱逐告警 0、emitted ~242.8 万（~293/450 规则触发）**——超出 QRadar 认证上限 80k，单进程 @ 有效并行 6-9 核，每核效率约 QRadar 虚拟版（56-80 核）的 **8-13×**。
- **qradar_pk 配置**：`parse_parallelism` / `rule_parallelism` 10；`conn_events max_window_bytes` 256MB → **1GB**（200k 下窗口深内容含 parsed-event 足迹约 490MB，防止 #18 有损驱逐使 close 路径归零）；默认 sink 改 **blackhole**（对齐 Flink Nexmark discarding 口径，只测处理吞吐不落盘）。
- **qradar_pk README**：记录 450 规则类别表、实测结果、行业定位（与 QRadar 80k @ 451 规则、Flink、自研 nexmark 的同维度对比），300 规则历史数据标注保留。
- **nexmark_pk `bench.sh`**：查询参数扩展至 q3/q9，新增 `WARMUP=1` 首跑剔除（stash 重建后首跑系统性偏低，须剔除）。

### Removed

- **qradar_pk `validate.sh`**：旧 300 规则指标汇总脚本，被 `run.sh` 内置的 #18 门禁（驱逐告警 + `emitted_total` 计数）取代。

### Chore

- **`.gitignore`**：忽略 `qradar_pk/ab_results/`（benchmark 告警/日志产物）与 `topology_1/`（旧拓扑副本）。
