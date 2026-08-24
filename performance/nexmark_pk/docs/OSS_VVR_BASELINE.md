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

## 3. 与 wfusion 的对照（以当次跑批为准）

wfusion 对照数字随引擎版本演进，**最新结果以当次跑批 `../data/bench_*_replay.txt` 为准**；
逐查询 PK 明细用 `scripts/compare-metrics.sh` 生成（读当次 `data/bench_*_replay.txt` 对拍本表）。
跑批归档（逐查询 PK 表 + 环境/口径标注）见 `BENCH_RESULTS.md`；历史轮次（2026-08-14~24
各次 30M/10M 跑批及优化过程）已清理，见 git 历史。

**当前对照（2026-08-25 Linux 30M · 22/22 `[clean]` · 哨兵 EPS 口径）**：
vs OSS **3.79×~190.62× 全面领先**；vs VVR **1.10×~32.91×，20/20 有基线查询全部达 VVR**。

- 边缘项：**q14 vs VVR 1.10×**（Top-N，两轮 1.07/1.10× 稳定确认，为 vs VVR 最弱项）、
  q17 1.67×、q22 1.75×。
- ⚠ **q19 规模退化**（10M→30M：12.4M→4.1M，驱逐触发，待查）；q6/q13 白皮书无基线。
- 对拍：Q4/Q9 30M identical（D4 保留 pin 修复）；Q12 fixed+close 尾桶、Q19 stats 为 known-diff。

> **口径说明**：本表起 EPS 用**哨兵四元组**（`eps_mode=sentinel`，Σn/(max emit − min
> start)，剔除 ingest 等待与启动开销）；此前轮次为 metrics-append 口径（含部分等待）——
> **两口径不可直接逐位比较**。白皮书基线为 100M 条/8CU(VVR)/12vCPU(OSS)/Blackhole，
> wfusion 跑批为 10M/30M 条 × 本机核数 × 本地文件 sink，**作量级参照，非逐位比较**。
> 查询形态与能力面见 `CAPABILITY_GAP_MATRIX.md`。

## 4. 引用注意

- 白皮书数字基于特定硬件与引擎版本，官方声明仅供参考；硬件升级/版本更新后可能变化。
- 引用 wfusion RSS 数字须标注 `parse_buffer_bytes` 配置（默认 128MB；吞吐优先 2GB）。

## 5. 来源

- 阿里云《性能白皮书（Nexmark 性能测试）》：
  - 中文：[help.aliyun.com/zh/flink/realtime-flink/support/nexmark-performance-testing](https://help.aliyun.com/zh/flink/realtime-flink/support/nexmark-performance-testing)
  - English：[help.aliyun.com/en/flink/realtime-flink/support/nexmark-performance-testing](https://help.aliyun.com/en/flink/realtime-flink/support/nexmark-performance-testing)
  - 繁体：[www.alibabacloud.com/help/tc/flink/realtime-flink/support/nexmark-performance-testing](https://www.alibabacloud.com/help/tc/flink/realtime-flink/support/nexmark-performance-testing)
