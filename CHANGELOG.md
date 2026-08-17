# Changelog

All notable changes to the wf-examples performance / verification scenarios will be documented in this file.

## [2026-08-17]

### Changed

- **场景重命名**：`performance/eps_throughput_rules100` → **`performance/qradar_pk`**（对标 QRadar Event Processor PK 负载）；`performance/flink_pk` 删除（其对比数据已并入 qradar_pk README 的行业定位表）。
- **qradar_pk — 规则 300 → 450**（对标 IBM QRadar Event Processor 认证负载 **451 条规则 / 80k EPS**）：`scripts/gen_rules.py` 目标从 300 改为 450，新增约 150 条真实规则——conn action 过滤计数、更高阈值、聚合/guard/distinct 网格扩展，auth/dns/proxy/firewall/file 各源深化，6 条多事件 + 2 条序列；`count` 补齐降至 74。
- **qradar_pk — 450 规则实测**（release，**200k 事件口径**，M3 Max，连续 3 轮）：**EPS ~96.7k（95.8-98.0k）、RSS ~2.24GB、驱逐告警 0、emitted ~242.8 万（~293/450 规则触发）**——超出 QRadar 认证上限 80k，单进程 @ 有效并行 6-9 核，每核效率约 QRadar 虚拟版（56-80 核）的 **8-13×**（1M 稳态见下 Added）。
- **qradar_pk 配置**：`parse_parallelism` / `rule_parallelism` 10；`conn_events max_window_bytes` 256MB → **1GB**（200k 下窗口深内容含 parsed-event 足迹约 490MB，防止 #18 有损驱逐使 close 路径归零）；默认 sink 改 **blackhole**（对齐 Flink Nexmark discarding 口径，只测处理吞吐不落盘）。
- **qradar_pk README**：记录 450 规则类别表、实测结果、行业定位（与 QRadar 80k @ 451 规则、Flink、自研 nexmark 的同维度对比），300 规则历史数据标注保留。
- **nexmark_pk `bench.sh`**：查询参数扩展至 q3/q9，新增 `WARMUP=1` 首跑剔除（stash 重建后首跑系统性偏低，须剔除）。
- **nexmark_pk 基准配置（2026-08-17，P0-②）**：`parse_buffer_bytes` 4GB → **2GB**——记账单位从 `get_array_memory_size`（IPC 解码结构性高估 ~10×）改 `content_bytes`（≈ wire）后，2GB ≈ 288 槽为甜点（q1 7.58M / q2 7.23M），4GB 过度缓冲退化（~5.9M）；默认配置 128MB ≈ 18 槽（旧默认之上小幅提升，RSS 仍 ~6GB）。
- **nexmark_pk 报告（2026-08-17 全量重跑）**：`PK_REPORT_MAC.md`/`.html` 更新为 100m / 4 连接 / 2GB content 口径——**7/7 全胜**（vs OSS 3.5-261× / vs VVR 1.0-30×）：Q1 7.55M、Q3 由落后 0.72× 反超 2.4×、Q9 261×/30×、q2 EMIT=747,816 逐位一致；标注 append_total 口径与默认配置（128MB）RSS 对照。
- **nexmark_pk README**：测量纪律新增 RSS 口径条目（引用 RSS 须标注 `parse_buffer_bytes`，旧“100M 6.8GB”数字不直接对等）；feed=cont 改 `shard-frames` 4 分片 + `send-arrow --shard-files` 注入；正确性侧证（100M：q2=747,816、q9=6,000,000）；清除历史（q5 排查工具、stash 首跑记录）。
- **qradar_pk 长跑配置（2026-08-17）**：`conn_events max_window_bytes` 1GB → **4GB**、`max_total_bytes` 2GB → **8GB**——1M 长跑下 conn 窗口深内容 ~2.5GB+（200k 的 5×），1GB 触发 38 条有损驱逐（#18 门禁失败），4GB 后驱逐 0（nexmark 经验：窗口 cap 须与数据量匹配）。

### Added

- **qradar_pk 1M 稳态实测（2026-08-17 晚，当前二进制）**：**EPS 150-162k（三轮 150.4/156.0/162.4k）/ RSS ~6.7GB / 驱逐 0 / emitted 1000 万+ / 371 规则触发**——1M 事件 + 窗口 4GB + CHUNK=10000 的稳态口径（总量 ×5 消除固定开销稀释），超出 QRadar EP 认证上限（80k @ 451 规则）约 2×，每核效率 17-27k EPS/核约为 QRadar 虚拟版的 **12-27×**。
- **qradar_pk 测试报告**：新增 `PK_REPORT_MAC.md` + `PK_REPORT_MAC.html`（1M 稳态参数矩阵、窗口 4GB 驱逐修复、行业定位、诚实边界）；README 顶部引用报告（与 nexmark_pk 的报告结构对称）。

### Removed

- **qradar_pk `validate.sh`**：旧 300 规则指标汇总脚本，被 `run.sh` 内置的 #18 门禁（驱逐告警 + `emitted_total` 计数）取代。

### Chore

- **`.gitignore`**：忽略 `qradar_pk/ab_results/`（benchmark 告警/日志产物）与 `topology_1/`（旧拓扑副本）。
