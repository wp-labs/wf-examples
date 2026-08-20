# wfusion vs Flink — NexMark PK 性能报告（Linux 8 CU 对等口径）
> 注：feed 模式旧名 `cont` 已于 2026-08-20 改名 **`replay`**（重放预编码帧），
> `cont` 别名已移除——现需 `./bench.sh <q> replay 100m`。本文为历史记录，
> 正文中的 `cont` 均指现 `replay`。

> 测试日期：2026-08-17 晚（**wfusion 0.3.1** / wp-reactor 1.0.2，P0-② content 记账 2GB，与 Mac 报告同一构建/同一查询集）
> 对齐口径：NexMark 100M/查询 + discard 输出（blackhole）+ send-arrow `cont`（shard-files 唯一数据，4 连接）
> **本报告为 Linux 8 CU 云主机对等复测**——硬件与阿里 VVR（8 CU）同量级，消除
> `PK_REPORT_MAC.md`（16 核 M3 Max）的硬件红利，是对外对比的**公平口径**。

---

## 1. 摘要

在 **Linux 8 核（8 CU）** 云主机上完成 7 个 NexMark 查询（Q1-Q5/Q7/Q9）的 100M 事件
长稳测试，全部 `appended=100M/100M` + SUMMARY clean：

| 结论 | 结果 |
|---|---|
| 7/19 查询（对等口径） | **4/7 领先 VVR**：复杂窗口（Q4/Q5/Q7）3.7-4.4×、join（Q9）8.6×；**简单查询（Q1/Q2/Q3）落后 0.46-0.81×** |
| vs OSS（12 vCPU） | 全部领先：1.6-75× |
| vs Mac M3 Max | 0.29-0.47×（约 1/3）：8 核 vs 16 核 + 云 CPU 单核弱 |
| 内存 | RSS 2.5-10.9GB，随吞吐降，峰值后回落 |
| 稳定性 | 100M 全部处理，无丢失，正确性 clean |

---

## 2. 测试环境

| 项 | 值 |
|---|---|
| 芯片 | **AMD EPYC 9T95**（KVM 虚拟化，8 vCPU / 8 核 / 每核 1 线程，x86_64） |
| 内存 | **30 GiB**（29 GiB available，Swap 0） |
| 系统 | **Ubuntu 24.04.4 LTS**（Noble），内核 6.8.0-137-generic |
| 引擎 | **wfusion 0.3.1** / wp-reactor 1.0.2（release，P0-② content 记账） |
| 注入 | 100m / 4 连接 / shard-files 唯一数据 / `parse_buffer_bytes=2GB` / p=10 r=10 |
| 对照 | Flink OSS 12 vCPU（3 × ecs.g6a.xlarge）/ 阿里 VVR **8 CU**——VVR 与本次硬件同量级 |

---

## 3. 结果（100M，send-arrow cont）

| 查询 | Linux 8CU EPS | RSS | CPU 均/峰 | vs Mac M3 Max | **vs VVR（8 CU 对等）** | vs OSS（12 vCPU） |
|---|---|---|---|---|---|---|
| Q1 | 3.54M | 10.9GB | 469/790% | 0.47× | **0.81×** | 2.0× |
| Q2 | 3.01M | 5.1GB | 314/497% | 0.45× | **0.46×** | 1.6× |
| Q3 | 3.24M | 5.8GB | 644/890% | 0.29× | **0.70×** | 2.8× |
| Q4 | 2.36M | 8.6GB | 403/791% | 0.45× | **3.7×** | 13× |
| Q5 | 1.24M | 8.0GB | 404/692% | 0.36× | **4.4×** | 4.5× |
| Q7 | 1.22M | 10.7GB | 418/788% | 0.39× | **4.1×** | 15× |
| Q9 | 3.24M | 2.5GB | 587/793% | 0.29× | **8.6×** | 75× |

正确性：全部 SUMMARY clean（serialize_failed / dropped_late / memory_evicted /
cursor_gap = 0），q2 EMIT 与单连接基线逐位一致（747,816）、q9=6,000,000、q1=92,000,000。

---

## 4. 对等分析（硬件与 VVR 8 CU 同量级）

1. **4/7 领先 VVR**：复杂窗口（Q4/Q5/Q7，3.7-4.4×）与 join（Q9，8.6×）是对等硬件下
   的**架构优势区**——单进程内存引擎 vs 分布式算子链（KeyBy 重排 + 序列化 + state 访问 +
   checkpoint/屏障），复杂查询的分布式代价放大仍是主因；
2. **简单查询（Q1/Q2/Q3）落后 VVR（0.46-0.81×）**：VVR 对简单过滤/投影/轻 join 查询的
   优化占优（Mac 的“7/7 全胜”含 16 核 M3 Max 硬件红利，不可作为对等结论）；
3. **vs Mac 为 0.29-0.47×（约 1/3）**：8 核 vs 16 核 + 云 CPU 单核弱于 M3 性能核；
   RSS 同步下降（吞吐低 → 窗口/输出驻留少）；
4. **每核效率**（EPS ÷ CPU 均核数）：
   - q1：Linux 755k/核 vs Mac 799k/核——**相当**，核数主导瓶颈，单核能力接近；
   - q2：958k vs 1.66M/核（0.58×）；q5/q7：292-307k vs 448-503k/核（0.61-0.65×）；
   - **join（q3/q9）：503-552k vs 2.13-2.25M/核（0.22-0.26×）差距最大**——join 是
     内存带宽敏感路径，云 CPU 带宽/单核弱放大差距。

---

## 5. 结论

- **对等硬件（8 CU）下 wfusion 复杂窗口与 join 查询仍显著领先 VVR**（3.7-8.6×），
  简单查询落后（0.46-0.81×）——优势区与 Mac 报告一致，但幅度收敛，且简单查询
  需要引擎侧优化（如规则输出列式化、简单投影路径减成本）才能对等反超；
- **吞吐约为 Mac 的 1/3**：8 核 vs 16 核 + 云 CPU 单核弱；每核效率 q1 相当
  （核数主导），join 带宽敏感差距最大；
- **对外引用建议**：涉及与 VVR/OSS 的公平对比时使用本报告（8 CU 对等）；
  Mac 报告（7/7 全胜）作为硬件上限参考，两者口径分开引用。

---

## 6. 诚实边界

1. **环境已记录（2026-08-17 补录）**：AMD EPYC 9T95 KVM 虚拟机，8 vCPU / 8 核，30GiB
   内存，Ubuntu 24.04.4（内核 6.8.0-137）；CPU 均值为 `os.wait4` rusage 口径；
   vs Mac M3 Max 的差距含 EPYC 单核（~2.4GHz 量级）与 M3 性能核的频率/带宽差；
2. **查询/数据近似与 Mac 报告一致**：Q4 为 bid⋈auction join 近似（非 category 均价）、
   Q5/Q7 阈值版；
3. **Flink 数字来源单一**（白皮书，不可复现）；OSS 为 12 vCPU（比本次 8 CU 更多），
   vs OSS 倍率含核数差异；
4. **sink 为 blackhole**（discarding 口径），落盘 file sink 慢 ~4-5%。

---

> 完整技术细节、优化链、瓶颈分析见 `TASK_PK_FLINK.md` §8；基准工具见本目录 `bench.sh`
> （`./bench.sh all cont 100m`）；Mac 口径对照见 `PK_REPORT_MAC.md`。
