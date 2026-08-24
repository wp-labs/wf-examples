# OSS Flink vs VVR Nexmark 性能基线（阿里白皮书）

> 独立存档阿里云 Nexmark 性能白皮书的 **OSS（开源 Flink）与 VVR（实时计算 Flink）逐查询吞吐数据**，
> 以及 wfusion 的对照情况。供 PK 对比、报告引用、查询能力评估使用。
> 与 `../README.md`（套件结构）、`NEXMARK.md`（基准背景/数据/正确性）、`CAPABILITY_GAP_MATRIX.md`
> （查询能力/语义判定）、`SEMANTIC_ALIGNMENT.md`（各查询语义对齐状态）互为配套。

## 1. 数据来源与口径

| 项 | 值 |
|---|---|
| 来源 | 阿里云《性能白皮书（Nexmark 性能测试）》 |
| 开源 Flink | Version 1.20.4（部署于 ECS） |
| 实时计算 Flink（VVR） | vvr-11.5-jdk11-flink-1.20 |
| 计算资源 | **8 CU**（VVR 口径；OSS 实际跑在 3 × ecs.g6a.xlarge = 12 vCPU / 48GiB） |
| 输入 | 每条查询 **1 亿条**（100,000,000）记录 |
| 指标 | **RPS = 输入数据量 ÷ 用时**（Duration ms） |
| 结果表 | **Blackhole**（排除外部存储干扰，专注引擎自身） |
| 总体结论 | **VVR = OSS 的 3.24×**；简单查询（q0/q1/q2）RPS 400万~650万，复杂聚合/窗口（q4/q5/q16）15万~63万 |
| 完整性 | 白皮书发布 q0~q22（23 条）；**q6、q13 未发布** |

## 2. 全表（q0 ~ q22）

| Query | OSS 用时(ms) | OSS RPS | VVR 用时(ms) | VVR RPS | VVR/OSS (×) |
|---|---|---|---|---|---|
| q0 | 58,848 | 1,699,293 | 23,450 | 4,264,392 | 2.51 |
| q1 | 57,045 | 1,753,002 | 22,824 | 4,381,353 | 2.50 |
| q2 | 51,890 | 1,927,154 | 15,224 | 6,568,576 | 3.41 |
| q3 | 84,986 | 1,176,664 | 21,558 | 4,638,649 | 3.94 |
| q4 | 553,426 | 180,693 | 157,117 | 636,468 | 3.52 |
| q5 | 365,636 | 273,496 | 357,547 | 279,684 | 1.02 |
| q7 | 1,257,452 | 79,526 | 333,837 | 299,547 | 3.77 |
| q8 | 79,788 | 1,253,321 | 29,939 | 3,340,125 | 2.67 |
| q9 | 2,324,518 | 43,020 | 266,563 | 375,146 | 8.72 |
| q10 | 189,985 | 526,357 | 51,202 | 1,953,049 | 3.71 |
| q11 | 408,384 | 244,868 | 145,983 | 685,011 | 2.80 |
| q12 | 121,554 | 822,680 | 36,991 | 2,703,360 | 3.29 |
| q14 | 68,903 | 1,451,316 | 20,012 | 4,997,002 | 3.44 |
| q15 | 183,709 | 544,339 | 42,734 | 2,340,057 | 4.30 |
| q16 | 917,597 | 108,980 | 337,293 | 296,478 | 2.72 |
| q17 | 102,847 | 972,318 | 27,076 | 3,693,308 | 3.80 |
| q18 | 574,949 | 173,928 | 96,335 | 1,038,044 | 5.97 |
| q19 | 586,287 | 170,565 | 95,121 | 1,051,293 | 6.16 |
| q20 | 1,340,638 | 74,591 | 231,482 | 431,999 | 5.79 |
| q21 | 127,089 | 786,850 | 39,693 | 2,519,336 | 3.20 |
| q22 | 94,830 | 1,054,519 | 31,228 | 3,202,254 | 3.04 |

> 各查询语义（NEXMark 标准）：q0 计数投影、q1 无状态投影、q2 过滤、q3 person⋈auction join、
> q4 bid⋈auction 均价、q5 窗口计数、q7 最高出价、q8 监控用户、q9 胜出出价、q10 任意选择、
> q11 用户会话、q12/14 Top-N 类、q15+ 扩展场景（去重/session/多级聚合等）。
> 白皮书未发布 **q6、q13**（表内无这两行）。

## 3. 与 wfusion 的对照（最新跑批为准）

wfusion 对照数字随引擎版本演进，**最新结果以当次跑批 `../data/bench_*_replay.txt` 为准**。

**2026-08-24 三次全量 30M replay（hop conv 分片 + review 后，工作区未提交；22/22 `[clean]`）**：
vs OSS **7.25×~221.21× 全面领先**；vs VVR **1.89×~58.73×，20/20 有基线查询全部达 VVR**。
逐查询明细用 `scripts/compare-metrics.sh` 生成（读当次 `data/bench_*_replay.txt` 对拍本表）。

> **本轮（q5 hop conv 分片，P2c 延伸）**：conv top_ties 是窗口级聚合（跨 auction），
> 原 inline 单核；给 hop 生成 conv_window（桶对齐 = slide、封口 = size）+ conv stage
> 分片后：
>
> | 查询 | 12:27 全量 | 本次 | vs VVR | vs OSS |
> |------|----------|------|--------|--------|
> | **q5** | 916,732 | **6,628,512** | **23.70×** | **24.24×** |
> | q7 | 16,195,264 | **17,591,601** | 58.73× | 221.21× |
> | q10 | 6,661,395 | **19,978,996** | 10.23× | 37.96× |
>
> q5 从全量最慢梯队（~1M）跳到 6.6M（CPU 98%→580% 多核）；q10 提升 3× 为环境因素
> （12:27 那次 sink 写盘被并行跑干扰，CPU 882% 但吞吐低）。其余查询在负载噪声内
> 波动（q1 −9%、q19 −12% 等，load 2-5 波动）。**q6/q13 白皮书无基线**；q13 仍最慢
> （410k，双规则链 + 中间窗物化，RSS 15.5GB）。

**2026-08-24 二次全量 30M replay（D4 保留 pin 落地后，工作区未提交；22/22 `[clean]`）**：
vs OSS **3.35×~203.65× 全面领先**；vs VVR **1.91×~54.07×，20/20 有基线查询全部达 VVR**。
逐查询明细用 `scripts/compare-metrics.sh` 生成（读当次 `data/bench_*_replay.txt` 对拍本表）。

> **本轮关键变化**：q9/q4 的 deferred join 正确性修复（D4 保留 pin——`bid_events` 字节上限
> 驱逐不再静默丢 join 目标行，见 wp-reactor 接力手记）兼得大幅提速：
>
> | 查询 | 修前 | 本次 30M | vs VVR | vs OSS |
> |------|----------|----------|--------|--------|
> | q9 | 939,682（−62% 错）| **4,632,016** | 12.35× | 107.67× |
> | q4 | 3,887,727（8GB 上限绕过时）| **4,054,046** | 6.37× | 22.44× |
> | q20 | 4,565,019 | **4,633,270** | 10.73× | 62.12× |
>
> **2026-08-24 傍晚：q5 hop conv 分片（P2c 延伸）**——conv top_ties 是窗口级聚合
> （跨 auction），原 inline 单核 998k；给 hop 生成 conv_window（桶对齐 = slide、
> 封口 = size）+ conv stage 分片后 **6,479,375（6.5×）**、CPU 602%、oracle identical。
> 全量对照见 `scripts/compare-metrics.sh`（q5 现在 vs VVR 23.2×、vs OSS 23.7×）。
>
> 机制：驱逐清扫原本在 append 热路径的 `log.write()` 写锁内 + 摘 1718 万行 join 索引，
> 与 lookup 读锁死磕——这是此前 q9 「80% 无法归因」的性能黑洞（微基准测不到的部分），
> 消除后 q9 不再是最慢查询。**q6/q13 白皮书无基线**；q13 仍是全量最慢（412,853 EPS，
> 双规则链 + 中间窗物化，RSS 15.4GB，见 ⚠️ 观察 1）。

**2026-08-24 首次全量 30M replay（wp-reactor `584852e`，merge 列式 List-Index guard 后；22/22 `[clean]`）**：
vs OSS **3.05×~197.78× 全面领先**；vs VVR **1.44×~52.51×，20/20 有基线查询全部达 VVR**（此前 8-23 唯一落后的 q14 已从 0.9× 回升到 2.15×）。
逐查询明细用 `scripts/compare-metrics.sh` 生成（读当次 `data/bench_*_replay.txt` 对拍本表）。

> 本轮提升归因：wp-reactor merge 的列式代码（List-Index guard 及列式门控扩展），非日志级别（
> `conf/wfusion.toml` 已由调试遗留的 debug 恢复为 info；debug 日志为节流输出，非每事件热路径）。
> 口径差异（见 §4）：基线 1 亿条/8CU(VVR)/12vCPU(OSS)/Blackhole，本次 30M/16 核 Mac/本地文件 sink。
> 查询形态与能力面见 `CAPABILITY_GAP_MATRIX.md`。

历史对照（2026-08-14~21 旧构建，Q1~Q22 × 30M / 100M 快照）已随文档清理删除（记录见 git
历史）；8-21 起的旧数字在缓存热度 / 输出链瓶颈修复前测得，**不可直接复现**（对比时必须带当次
注入方式、`parse_buffer_bytes` 与机器负载）。

## 4. 引用注意

- 白皮书数字基于特定硬件与引擎版本，官方声明仅供参考；硬件升级/版本更新后可能变化。
- 引用 wfusion RSS 数字须标注 `parse_buffer_bytes` 配置（默认 128MB；吞吐优先 2GB）。

## 5. 来源

- 阿里云《性能白皮书（Nexmark 性能测试）》：
  - 中文：[help.aliyun.com/zh/flink/realtime-flink/support/nexmark-performance-testing](https://help.aliyun.com/zh/flink/realtime-flink/support/nexmark-performance-testing)
  - English：[help.aliyun.com/en/flink/realtime-flink/support/nexmark-performance-testing](https://help.aliyun.com/en/flink/realtime-flink/support/nexmark-performance-testing)
  - 繁体：[www.alibabacloud.com/help/tc/flink/realtime-flink/support/nexmark-performance-testing](https://www.alibabacloud.com/help/tc/flink/realtime-flink/support/nexmark-performance-testing)
