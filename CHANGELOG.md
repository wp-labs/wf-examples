# Changelog

All notable changes to the wf-examples performance / verification scenarios will be documented in this file.

## [2026-08-24]

### Added

- **nexmark_pk `diag.sh` — 性能墙定位脚本**：把引擎内置的性能诊断模式（perf-diag，见 `wp-reactor/docs/design/perf-diag-mode-design.md`）接进 nexmark_pk。`./diag.sh [q1..q22|all] [1m|10m|30m]` 驱动 `floor → rules → full` 三档墙梯（单 daemon 不重启，哨兵驱动自切换），输出 `data/diag_<q>_<total>.txt`：每档 EPS/耗时/每事件 ns/**增量成本**/**CPU%**/RSS + 墙判定 + 健康校验。与 `bench.sh` 分工：bench 回答「吞吐是多少」，diag 回答「墙在哪一段」。
- **nexmark_pk `conf/perf-diag-wall.toml`**：三档墙梯配置（与 `conf/perf-diag.toml` 的无档配置分工：后者仅供 bench.sh 拿哨兵精确 EPS 口径）。
- **CPU%/RSS 按档归属**：采样带 epoch 纳秒时间戳，与哨兵四元组的 `[start_ns, emit_ns]` 区间对齐切分——补上了 `wfgen perf-diag` 墙表尚未提供的 CPU%/RSS 列，使方法论 §2.4 的**忙墙/等墙判别**（CPU 占核比 >50% = 忙墙、<15% = 等/供给墙）可自动化。
- **`WARMUP=1` 预热档**：墙梯在单 daemon 内顺序跑，首档独自承担窗口冷分配/page fault。实测 q1 10m 不预热时 `floor`(21.2M) 反而慢于 `rules`(26.6M) 25%（偏差大于信号）；预热后 `floor` 升到 31.8M、墙梯恢复单调。脚本在出现负增量时报警并提示该选项。
- **首批实测墙表（M3 Max 12 核，N=10M，`WARMUP=1`）**：q1（无状态投影）floor 31.8M → rules 33.0M（−1.2ns，噪声内 = 规则成本≈0）→ full 18.8M（**+22.9ns = 输出墙 43%**，CPU 53% 占用 = 忙墙）；q5（滑窗 top-N）floor 31.7M → rules 939k（**+1032.8ns = 规则墙 95%**，CPU 仅 9% = 等/供给墙）→ full 918k（输出墙 2.3%）。两次独立运行的 `floor` 一致（31.7/31.8M），可作口径自校验。

### Fixed

- **诊断口径守卫（由实测假象驱动）**：初版 `diag.sh` 在缺 `data/bench_10m_v5.frames` 时自动复用 `bench_10m_v6.frames`，旧版本帧的 Arrow schema 与当前 `nexmark.wfs` 不符（person 缺 `creditCard`、bid 缺 `channel_id`、多 `wp_src_ip`）→ window actor `schema mismatch` **整批丢弃**、只剩哨兵被处理 → 三档全报 ~50M EPS 假象。修正：**不再跨 `DATA_VER` 自动复用帧**（只列出候选，需显式 `FRAMES=`），并强制校验 `appended = N × 档数`，不追平即判失败并给出根因（含日志中的 schema mismatch 计数）。
- **脚本内嵌 Python 全部抽离（脚本里生成脚本问题）**：nexmark_pk `bench.sh`/`diag.sh` 与 qradar_pk `run.sh`/`diag.sh` 曾把 10 余处度量逻辑用 heredoc 内嵌在 bash（comma/采样器/引擎游标/哨兵汇总/正确性摘要/分析器）——无法单独测试、改一行要重跑整个基准。现已抽成**共享库** `performance/scripts/bench_lib.py`（子命令分派，可单独验证）+ `performance/scripts/diag_analyze.py`（墙表/墙判定/健康分析，输入走环境变量），四个 shell 脚本只做流程编排、零内嵌。顺带修掉两个抽离时引入的回归：CSV 流名 `split()` 不拆逗号导致 `append_total` 恒为 0；`now` 秒/纳秒口径不统一导致 EPS 数量级错误（统一为 epoch 纳秒，`diff-ns`/`eps` 配套）。
- **qradar_pk 家族档修复（reload Blocked 污染，结论修正）**：家族档原用「全量启动 + `runtime.rules` 热 reload 切子集」——但子集引用窗口 < 全量时 reload 必 Blocked（`hot_reload/topology.rs`：编译后 schema 集合有移除 → requires restart），实际跑全量 450 规则，**所有家族测出同一个 ~31k 的「全量墙」**（早期「所有 match 家族等成本、规则数无关、chain 每规则最贵」的错误结论即由此而来）。修复：**每家族独立 daemon 会话启动即加载子集**（`data/diag_rules_<fam>.wfl`，无 reload）。修正后的真实结论：**`c`（conn count）家族 125 条 +8.8µs/事件是绝对主墙，`g`（guard）45 条 +3.9µs 第二**，前 4 大家族（c/g/dist/avg）占 450 规则总成本一半；**删规则有效**（删 c 家族省 ~27%）；`chain` 并不贵（强 action 绑定过滤触发率低）。
- **qradar_pk 删 74 条 `c_pad_*` 凑数补齐规则（450 → 376，规则墙 −18%）**：修复后的家族档
  定位出 `c`（conn count）家族 125 条里 74 条是无 guard 的冗余 conn count（阈值 5/7/9/11…
  与正式 `c_sip_3/8/20` 重叠，纯凑数、贡献 c 家族近半成本）。已从 `gen_rules.py` 移除，规则集
  450 → **376**、c 家族 125 → **51** 条——全量规则墙 31,800 → **26,049 ns/事件（−18%）**，
  EPS 31.4k → **38.4k（+22%）**，语义零损失；run.sh #18 门禁（200k）EPS 25.1k → 30.6k，OK。
  删除后的规则集变更为：300（历史基线）+ 76 新增 − 74 补齐。
- **输出墙消失（055d330 引擎更新后）**：qradar 三档墙梯 `full` 档从 43.6µs（输出墙 +11.8k = 27%）降到 31.2µs（输出墙 ≈0）——输出链成本已被引擎 emit 路径优化吸收，墙梯现为纯规则墙（100%）。

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
