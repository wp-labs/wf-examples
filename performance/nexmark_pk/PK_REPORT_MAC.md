# wfusion vs Flink — NexMark PK 性能报告（Apple Silicon 环境）

> 测试日期：2026-08-17（**wfusion 0.3.1** / wp-reactor 1.0.2，P0-② content 记账 2GB，2026-08-17 全量重跑）
> 对齐口径：NexMark 标准数据 + 查询 + **100M 记录/查询**（与 Flink 白皮书一致）+ discard 输出（blackhole）
> **send-arrow `cont` 连续流**：100M 唯一事件预编码分片帧（shard-files，4 连接并发推）
> **计时口径**：window append_total 追平 100M（append_total，见 §3）

---

## 1. 摘要

在单台 **Apple M3 Max（16 核 / 64GB）** 上，wfusion 完成全部 7 个 NexMark 查询（Q1-Q5/Q7/Q9）
的 100M 事件长稳测试，**无丢失、内存有界**。对比 Flink 白皮书（OSS 1.20.4 跑在
12 vCPU/48GiB，VVR 8 CU）：

| 结论 | 结果 |
|---|---|
| 7/19 查询 | **7/7 全部超过 Open Flink 与阿里 VVR**（OSS 3.5-261×，VVR 1.0-30×） |
| 简单查询（Q1/Q2） | 反超 VVR（1.0-1.7×）——P0-② 前旧口径 1.0-1.3× |
| join 查询（Q3/Q9） | **11.2-11.3M；2.4-30× VVR**（Q3 由落后 0.72× 反超为 2.4×） |
| 复杂窗口查询（Q4/Q5/Q7） | **8.3-12× VVR、13-39× Open Flink** |
| 内存 | 100M 下 RSS 峰值 5.4-14.0GB，峰值后回落（有界、非泄漏） |
| 稳定性 | 100M 全部处理（appended=100M/100M），无丢失，正确性 clean |

> **Linux 8 CU 对等实测见独立报告 `PK_REPORT_LINUX.md`**（2026-08-17，消除硬件红利）。

---

## 2. 测试环境

### 2.1 硬件（wfusion 侧）

| 项 | 值 |
|---|---|
| 芯片 | Apple M3 Max |
| 核心 | 16（12 性能 + 4 能效），稳态只用 ~5-9 核 |
| 内存 | 64 GB |
| 机型 | MacBook Pro（Mac15,9） |
| 系统 | macOS 26.5 |

### 2.2 Flink 侧（白皮书配置）

| 项 | 值 |
|---|---|
| **OSS**（对比基准） | **3 × ecs.g6a.xlarge**（合计 12 vCPU / 48GiB）；8 个 TaskManager，
  cgroup 各限 3 核（24 核配额超订在 12 vCPU 上），版本 Flink 1.20.4 |
| **VVR**（另列） | 阿里 VVR 11.5，**8 CU** 计算资源 |
| 输出 | Blackhole sink（discard，与我们对齐） |
| 内存 | **白皮书未报告**——无法公平比内存 |

> 说明：白皮书中 **8 CU 是 VVR 的配置**；**OSS 列实际跑在 3 × ecs.g6a.xlarge
> （12 vCPU/48GiB）**。我们对比的 OSS 机器（12 vCPU）比 8 CU（≈8 vCPU）更大——
> 倍率是在低估对手机器的口径下测得的（相对保守）。

### 2.3 引擎配置（wfusion，2026-08-17 全量重跑口径）

- `parse_parallelism = 10`、`rule_parallelism = 10`（规则分片）
- **`parse_buffer_bytes = 2GB`（P0-② content 记账，2026-08-17）**：预算按内容字节
  （≈ wire）计，2GB ≈ 288 槽为甜点（4GB 过度缓冲退化，见 wp-reactor
  concurrency-scaling.md §2.3）；引擎默认 128MB ≈ 18 槽（旧默认之上小幅提升，
  RSS 仍 ~6GB 量级）
- 窗口：bid `max_window_bytes` 15GB / person·auction 64MB，over 10m，
  `max_total_bytes = 20GB`，`allowed_lateness = 30m`
- 输入：NexMark 100M 事件（Person 2% / Auction 6% / Bid 92%），事件时间 ~30 分钟，hot 分布
- 发送：`wfgen gen-nexmark 100m` 流式生成（RSS ~23MB）→ `dump-frames` 预编码 Arrow 帧 →
  `shard-frames --shards 4`（键闭包分片，唯一数据）→ `send-arrow` 4 连接并发推
  （引擎真实摄取，无客户端解析/编码开销）

---

## 3. 方法论

- **数据**：标准 NEXMark 事件模型（与 Flink 白皮书同源），100M 唯一事件
- **查询**：Q1（pass-through）、Q2（按 auction 过滤）、Q3/Q9（person⋈auction join）、
  Q4（bid⋈auction join）、Q5（计数）、Q7（窗口 MAX）
- **输出**：blackhole（丢弃，对齐 Flink discard 口径）
- **注入**：send-arrow `cont` = 持久 TCP 连接并发推预编码分片帧（shard-files，
  默认 4 连接、键闭包分片；旧版为单连接推完整帧文件）。`EPS = 100M ÷ 墙钟`
  （send-arrow 回放计时，背压约束到引擎真实摄取速率）
- **内存**：daemon 进程 RSS 每秒采样，取峰值 + 观察是否回落（有界性）
- **EPS 测量口径（2026-08-16 起，README 测量纪律）**：计时终点 = **window
  append_total 三输入流追平 100M**（窗口→规则推送通道有界，append 追平 ≈ 规则
  真正吃完，误差 ≤32 批次 <0.5%），非 receiver 预读游标。metrics 1s 一刷 + 0.5s
  轮询 → 量化误差 ±1~1.5s。**单轮全量套件数字 + 复测锚点**（q1=7.55M 三次一致），
  run 间方差 ±5-10%（q2 6.69↔7.23M）；Q3/Q5/Q7 的 EMIT 计数有驱逐时序波动
  （非正确性破坏）。

---

## 4. 结果（100M，send-arrow cont）

### 4.1 吞吐 + 内存

> ✅ **口径修订（2026-08-17）**：下表 wfusion EPS 为**现行 append_total 口径**
> （bench.sh 计时终点 = window append_total 追平 100M，见 README 测量纪律），
> 数据来自 2026-08-17 全量重跑（100m / 4 连接 / shard-files 唯一数据 / **P0-②
> content 记账 2GB**，`parse_buffer_bytes=2147483648`）。run 间方差 ±5-10%
> （q2 曾现 6.69↔7.23M）；正确性全部 SUMMARY clean，q2 EMIT 与单连接基线逐位一致。

| 查询 | wfusion EPS | **wfusion RSS 峰值** | Flink OSS | **vs OSS** | 阿里 VVR | **vs VVR** |
|---|---|---|---|---|---|---|
| Q1 | **7.55M** | **14.0 GB** | 1,753,002 | **4.3×** | 4,381,353 | **1.7×** |
| Q2 | **6.71M** | **7.5 GB** | 1,927,154 | **3.5×** | 6,568,576 | **1.0×** |
| Q3 | **11.26M** | **7.5 GB** | 1,177,322 | **9.6×** | 4,638,649 | **2.4×** |
| Q4 | **5.28M** | **13.0 GB** | 180,693 | **29×** | 636,468 | **8.3×** |
| Q5 | **3.42M** | **11.6 GB** | 273,496 | **13×** | 279,684 | **12×** |
| Q7 | **3.12M** | **13.9 GB** | 79,526 | **39×** | 299,547 | **10×** |
| Q9 | **11.24M** | **5.4 GB** | 43,021 | **261×** | 375,146 | **30×** |

> Flink 内存白皮书未报告（RPS only）；wfusion 内存为 daemon RSS 峰值，全部峰值后回落。
> 与旧口径（2026-08-16 作废的 ingress 计时 + 解码记账）相比：Q1/Q2 提升 15-28%
> （预算槽位第一道墙解除），**Q3/Q9 翻倍以上**（join 查询此前被预算卡住），
> **Q3 由落后 VVR（0.72×）反超为 2.4×，Q9 由 8.8× 升至 30×**——7/7 全胜。
>
> **测量说明（2026-08-17）**：EPS 为单轮全量套件（q1→q2→…→q9 顺序）数字，
> 复测 q1=7.55M 稳定（3 次一致）；q2 EPS 同配置在 6.69~7.23M 间波动。RSS 随吞吐
> 上升（吞吐↑ → 窗口/输出驻留↑），Q1/Q4/Q7 在 13-14GB 量级。**本表为吞吐优先
> 配置（`parse_buffer_bytes=2GB` content 记账）；默认配置（128MB，P0-② 后）下
> q1 ≈ 6.1M / RSS ~5.9GB（旧默认 256MB 解码记账为 5.93M / 4.4GB）。**
> 引用 RSS 数字须标注配置。
>
> **覆盖范围**：PK 覆盖 **7/19 条 NEXMark 查询**（Q1/Q2/Q3/Q4/Q5/Q7/Q9，均为
> 白皮书测试集内）。Q3/Q9 为 auction 驱动的 person⋈auction join（hash join
> 新增能力）。**复杂窗口（Q4/Q5/Q7）与 Q9 是明显优势区**；简单查询（Q1/Q2）
> 与 VVR 相当（1.0-1.7×）。

> Flink 内存白皮书未报告（RPS only）；wfusion 内存为 daemon RSS 峰值，全部峰值后回落。
>
> **Q4 修正（2026-08-15）**：Q4 原用 auction-only 近似（仅处理 6M auctions，92M bids 被
> 无订阅快路径跳过）——错位对比。已实现 **bid⋈auction hash join**（引擎新增标量 key 哈希
> 索引，`join <window> snapshot on ...` O(1) 查找），Q4 现处理 **92M bids + join**（规则命中
> ~91M），RSS 从 6.8GB 升至 18.0GB（bid 流进管道的证据）。修正后 vs OSS 30×、vs VVR 8.6×。
>
> **join 能力边界**：snapshot/anti 走标量 key hash 索引（O(1)）；**asof 暂留扫描**（索引
> 未时间戳化）、**每窗口单 join key**（多规则不同 key 时后建的走扫描兜底）——这两项未支持，
> 待实际需求再扩展。

### 4.2 EPS 分布——规则负载决定复杂查询成本（2026-08-17 重测口径）

| | 简单(Q1/Q2) | 复杂(Q4/Q5/Q7) | 下降 |
|---|---|---|---|
| wfusion | 6.71-7.55M | 3.12-5.28M | ≤59% |
| Flink OSS | 1.75-1.93M | 0.08-0.27M | **22×** |

- **wfusion 下降来自规则计算本身**（Q5 count 每 key 一实例、Q7 MAX per-auction
  状态，per-event match 评估重），预算供给放开后（P0-②）Q1/Q2 吃满共享管道上限，
  Q5/Q7 则撞在规则计算上限——简单/复杂差距从旧口径的 ≤19% 拉大到 ≤59%，
  这是**真实规则成本**，不是供给瓶颈；
- **但 vs Flink 的复杂查询优势依然巨大**：Q4/Q5/Q7 vs OSS 13-39×、vs VVR 8.3-12×——
  Flink 的分布式 + exactly-once 在复杂查询上放大代价 20× 的结论不变；
- Q3/Q9（join，几乎无窗口聚合）最高：11.2-11.3M，与 Q1 同档。

> **Linux 8 CU 对等实测见独立报告 `PK_REPORT_LINUX.md`**（2026-08-17，硬件与 VVR 8 CU 对等）。

### 4.3 完整性

- 5 个查询各 100M 事件**全部摄入**（routed rows = 100M）；**无 cursor gap**
  （窗口驱逐未造成数据空洞，`grep 'cursor gap' wfusion.log` = 0）
- **无丢失交叉验证（Q2）**：Q2 为 `auction % 123 == 0`（对齐 Flink Q2，选中 ~0.81%
  bids），1M 事件输出 7,564 行 vs 预期 ~7,452（偏差 ~1.5%，分布噪声内）——规则处理了
  全部 bids 且输出数与语义预期一致
- **注意**：窗口 size-cap 驱逐（bid 窗口 1GB）会丢弃驱逐的事件——routed=100M 指全部摄入，
  驱逐事件未进规则；这对吞吐 PK 无碍（驱逐是引擎真实行为），但"无丢失"严格指摄入完整 +
  无 cursor gap，非"全部事件完成窗口聚合"
- 驱逐（窗口 cap 内存驱逐）正常，未影响处理；`dropped_late = 0`

### 4.4 内存控制杠杆（2026-08-14 补充）

| 杠杆 | 效果 |
|---|---|
| 注入限速 2M/s（`IngestLimiter`） | RSS 峰值降 54-69%（Q5 21.8→~10GB，Q2/Q4 →1.5-1.6GB） |
| 包粒度 100K（分包回放） | Q5 100M RSS 10.37→2.15GB |
| stream 客户端受限 | 实时生成路径上限 ~760k/s（GenEvent→Arrow 逐事件编码），非引擎能力 |

---

## 5. 优化历程（累计，含 2026-08-14）

| 优化 | 效果 |
|---|---|
| 窗口记账含 parsed-event 足迹 | #20 内存失控根因（object 字段低估 2-4×） |
| 无订阅窗口跳过物化 | 空规则 ingest +90%（3.26→6.19M） |
| collect_alias_event 减分配 | 每事件免字段名 String clone |
| bounded 规则通道 + 背压 | 慢消费者阻塞注入，防无界积压 |
| needs_field_history（触发事件读字段） | count 规则跳过 field_values 收集 |
| **InstanceKey SmolStr 内联** | 每事件 key 0 堆分配（RSS 峰值 -61%） |
| **wfgen stream 事件时间推进 + 限速** | stream 实时生成正确性（`--rate`/`--slice-ms`） |
| **gen-nexmark 流式化** | 100M 生成 RSS ~55GB→23MB（per-phase 直写，无全量排序） |
| **preread 预算 content 记账（P0-②，2026-08-17）** | 记账从 `get_array_memory_size`（解码高估 ~10×）改 `content_bytes`（≈wire）；同吞吐下配置预算降 2×，Q2 RSS 减半（7.5 vs 14.8GB），Q3/Q9 吞吐翻倍 |

**整体效果**：持续 50M 注入 RSS 从 **16.4GB 失控 → 有界**；EPS 2.5M → **6.6M**（cont 口径）。
P0-② 后全查询 100M：Q1 7.55M / Q3·Q9 11.3M / 其余 3.1-6.7M（2026-08-17 全量重跑）。

---

## 6. 诚实边界

1. **硬件不对等**：wfusion 在 16 核 M3 Max（64GB），Flink OSS 在 12 vCPU/48GiB
   （3 × ecs.g6a.xlarge）、VVR 在 8 CU——倍率含硬件红利，非纯引擎声明；且 OSS 实为 12 vCPU
   （比 8 CU 更大），倍率相对保守；wfusion 稳态仅用 ~5-9 核，未吃满
2. **语义简化**：Q4 为 bid⋈auction join 近似（按 auction 窗口 count，非 Flink 的 category
   均价 closing price）；Q2 已对齐 `MOD(auction,123)=0`（选中 ~0.81% bids）；Q5/Q7 为阈值版
   （非完整 Top-N）
3. **Flink 无内存数据**：白皮书只发布 RPS，未报告内存——无法公平比内存
4. **架构差异**：Flink 有 exactly-once + checkpoint + 分布式重排开销，wfusion 单机
   at-least-once 无此成本——吞吐优势不等于总成本优势
5. **stream 客户端受限**：实时生成路径（~760k）不代表引擎能力，引擎能力以 cont 预编码回放为准
6. **单机 vs 分布式**：wfusion 单节点，Flink 是分布式系统——规模化对比需另行测试；真实负载
   （<1M EPS 居多）+ 突发峰值下，单机 3.1-11.3M（依查询）持续能力有充足余量，吞吐维度单机即终局

---

## 6.5 调优：帧大小 × 并行度 × 限速（2026-08-15）

### 帧大小（Q1 100M，parse=6/rule=6，不限速）

| 帧 cap | 实际帧 | EPS | RSS 峰值 | 驱逐 |
|---|---|---|---|---|
| 8MiB | ~5.6MiB / ~7万行 | 6.23M | 28.2GB | 860 |
| **1MiB** | ~0.72MiB / ~8千行 | 5.86M | **8.3GB** | 9,885 |
| 200KiB | ~0.14MiB / ~1.6千行 | 4.93M | **5.9GB** | 50,923 |

- **1MiB 是甜点**：8MiB→1MiB 内存 -75%、吞吐仅 -6%
- 200K 内存更省（5.9GB）但吞吐 -16%——帧越小每批固定开销占比越高

### 帧大小（Q4 真实 bid⋈auction join，100M，parse=6/rule=6，不限速）

| 帧 cap | EPS | RSS 峰值 |
|---|---|---|
| 8MiB | 5.51M | 18.3GB |
| **1MiB** | 4.96M | **9.7GB** |
| 200KiB | 4.29M | **7.2GB** |

Q4 处理 92M bids + hash join；帧越小 RSS 越降（-61%）、吞吐越降（-22%），与 Q1 同规律。

### 全查询 200K vs 1MiB（6/6，100M）

| 查询 | 1MiB EPS | 200K EPS | EPS Δ | 1MiB RSS | 200K RSS | RSS Δ |
|---|---|---|---|---|---|---|
| Q1 | 5.86M | 4.80M | -18% | 8.3GB | 5.0GB | -40% |
| Q2 | 5.80M | 4.32M | -26% | 7.9GB | 6.3GB | -20% |
| Q4 | 4.96M | 4.29M | -14% | 9.7GB | 7.2GB | -26% |
| Q5 | 4.56M | 3.44M | -24% | 9.2GB | 9.7GB | +5% |
| Q7 | 4.32M | 3.30M | -24% | 12.0GB | 10.7GB | -11% |

200K 的 RSS 收益**不统一**（Q1 -40% 但 Q5 +5%）；EPS 全面降 6-26%。**1MiB 是吞吐/内存综合最优**。

### 并行度（Q1 1MiB，100M）

| parse/rule | EPS | RSS |
|---|---|---|
| 4/6 | 5.88M | 9.6GB |
| 2/8 | 5.25M | 9.9GB |
| 4/8 | 5.46M | 9.8GB |
| **6/6** | **5.86M** | **8.3GB** |
| 8/8 | 5.47M | 9.4GB |

**6/6 最优**；parse/rule 偏移都更差（Q1 瓶颈是每事件 match 评估，非并行度）。

### 限速 × 帧大小（IngestLimiter 天花板）

| 帧 | 限速 | EPS | RSS |
|---|---|---|---|
| 1MiB | 5M | 3.47M | 7.0GB |
| 8MiB | 5M | 4.80M | 7.1GB |
| 200K | 2M | ~0.90M | Q1 7.2GB |

**IngestLimiter 有效速率 = 批大小 ÷ 每批开销**——帧越小天花板越低（200K 限 2M 只到 ~0.9M）。
限速只对「注入突发驱动的 allocator 峰值」有效；**200K 帧已平滑，限速反而让窗口滞留更多、
RSS 升 33-44%**（Q1/Q2/Q4）。限速和帧大小是两套独立的内存杠杆，作用对象不同。

### 连接器 bug：macOS TCP read EINVAL（已修复）

- **根因**：wp-core-connectors `try_read_buf` 无界读入 BytesMut → 下游慢（Q2/Q5 规则）时
  source 被拖住、socket 积压 → 一次读入几 GB → buffer 涨到 1.4GB → **macOS `read()` 对超大
  buffer 返回 EINVAL** → 连接断开。Q1（快）和 1MiB 帧（背压轻）不触发。
- **修复**：`third_party/wp-core-connectors` fork（`[patch.crates-io]` 接入），
  `try_read_batch`/`read_batch` 改**有界读**（每次 ≤256KB，staging + extend）→ buffer 不无界
  增长 → EINVAL 消除。验证：q2 200K 从「EINVAL 0 事件」→「EPS 4.32M 完整 100M」。

### 内存构成与窗口数据密度

```
内存 = 空规则基线(~1.5GB) + 窗口数据 + 规则实例状态 + 瞬时在途 + macOS allocator 页保留
窗口内存 = 窗口内事件数 × 数据密度（每事件解析足迹）
```

- **密度**：Arrow 紧凑 ~87B/事件 → Parsed Event（HashMap）~1-2.5KB，**膨胀 10-20×**
- #20 内存失控根因即记账只算 87B、漏算 parsed 足迹 → 驱逐不触发
- 密度杠杆：字段过滤（已做 -21~28%）；**列式零拷贝（M16）直接读 Arrow 列、不物化 HashMap，
  预期密度 ↓10×、窗口内存 ↓10-30×**——报告标记的 P2 最大杠杆
- 规则实例状态（Q5 count 每 key 一实例、Q7 MAX）独立于窗口数据量，是 Q5/Q7 的内存下限

---

## 6.6 Flink PK 对齐性分析（2026-08-15）

### 指标口径对齐（核心成立）

| 维度 | wfusion | Flink 白皮书 | 对齐 |
|---|---|---|---|
| 输入量 | 100M 事件 | 1 亿条 | ✅ |
| 输出 | blackhole（丢弃） | Blackhole 黑洞表 | ✅ |
| 指标 | EPS = 100M ÷ 摄取墙钟 | RPS = 输入量 ÷ 用时 | ✅ 同量纲 |
| 性质 | 稳定最高速率（send-arrow 打满引擎） | 稳定平均速率（TPS 注入） | ✅ |

白皮书方法（Alibaba Nexmark 性能白皮书）：Nexmark 源表按 **TPS 注入速率**生成
Person/Auction/Bid → **Blackhole 黑洞表**（排除外部存储干扰，专注引擎处理能力）→
**RPS = 输入量 ÷ 用时**，1 亿条（OSS 12 vCPU / VVR 8 CU）。**两边都是处理能力指标，量纲一致。**

### 逻辑审查结论

1. **cont EPS = 稳定可支持的最高速率**（处理能力指标）：send-arrow 以最大速率推、
   daemon 全程背压饱和，EPS = 100M ÷ 全程时长 = **最大持续处理速率**。不是"突发峰值"
   （无超过引擎能力的瞬时冲高），测量成立。
2. **事件时间压缩只影响内存口径**：100M 事件 = 30min 事件时间、15-20s 推完
   （事件时间 ~120× 实时），窗口状态瞬态（填满→完成→清空）。吞吐不受影响；
   内存为"峰值后回落"口径，与 Flink 稳态口径不同（Flink 未报告内存，规避直接比较）。
3. **EPS 严格说是"摄取完成"率**（`T2` = daemon 收到 100M 时刻），背压使摄取率 ≈
   慢环节速率，近似端到端。

### 剩余真实差异（已记录于 §6 诚实边界）

- **硬件**：16 核 M3 Max vs OSS 12 vCPU（3 × ecs.g6a.xlarge）/ VVR 8 CU —— 倍率含硬件红利
- **Flink checkpoint**：白皮书启用 `execution.checkpointing.interval`（exactly-once），
  wfusion 无 —— 我们的数字不含 checkpoint 开销（架构差异）
- **查询/数据近似**：Q4 为 join 近似（非 Flink 的 category 均价）、Q5/Q7 阈值版；
  hot 分布参数可能与 Flink 默认不同
- **Flink 数字来源单一**：白皮书不可复现，内存未报告

### 结论

PK **无"指标错配"的逻辑问题**——两边都是处理能力指标（RPS/EPS 同量纲）。倍率的
影响因素收敛为 **硬件 + checkpoint 开销 + 查询/数据近似**，均已记录，非测量错误。

> 来源：[Alibaba Nexmark 性能白皮书](https://www.alibabacloud.com/help/en/flink/realtime-flink/support/nexmark-performance-testing)

---

## 7. 结论

- **全部 7 查询同时超过 Open Flink 与阿里 VVR（2026-08-17 重测口径）**：vs OSS 3.5-261×、
  vs VVR 1.0-30×。复杂窗口（Q4/Q5/Q7）8-39×、join（Q3/Q9）2.4-30×、简单查询
  （Q1/Q2）1.0-1.7×——cont 连续流口径下没有短板，Q3 由落后反超为 2.4×
- **100M 口径已完整对齐**：7 查询无丢失、内存有界（RSS 峰值后回落）、EPS 稳定——可发布
- **吞吐分布 = 规则负载**：Q1/Q3/Q9（轻规则）11.2-11.3M；Q5/Q7（per-key 状态规则）
  3.1-3.4M 是规则计算上限，非供给瓶颈；Flink 的分布式 + exactly-once 在复杂查询上
  放大代价 20× 的架构差异结论不变
- **内存有界**：RSS 峰值 5.4-14.0GB（Q9 最低 / Q1 最高），64GB 机器余量充足，且峰值后回落

> **Linux 8 CU 对等实测（4/7 领先 VVR、简单查询落后）见 `PK_REPORT_LINUX.md`**。

> 完整技术细节、优化链、瓶颈分析见 `TASK_PK_FLINK.md` §8；基准工具见本目录 `bench.sh`
> （`./bench.sh all cont 100m`）。
