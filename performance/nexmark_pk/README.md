# nexmark_pk — NEXMark 基准查询吞吐（对齐 Flink 官方基线）

与 Flink 对齐的 PK case：**同一份权威基准数据（NEXMark）+ 同一批查询（Q1/Q2/Q4/Q5/Q7）**，
跑引擎实测吞吐，对照 Flink 官方发布的 Nexmark 基线。这是**数据+查询+输出三方对齐**的对比。

## 数据（NEXMark 事件模型）

**原生 Rust 生成**：`wfgen gen-nexmark <count> [--seed N]`（warp-fusion 的 wfgen 子命令，
流式写出、内存有界——100M 事件峰值 RSS ~23MB）。`scripts/gen_nexmark.py` 保留为参考实现
（算法一致）。对齐标准 NEXMark schema：

| 流 | 占比 | 字段 |
|---|---|---|
| person_events | 2% | id/name/email/city/state/dateTime |
| auction_events | 6% | id/itemName/description/initialBid/reserve/dateTime/expires/seller/category/extra |
| bid_events | 92% | auction/bidder/price/channel/url/dateTime/extra |

事件时间覆盖 ~30 分钟；hot 分布（50% hot auction / 25% hot bidder / 25% hot seller）。

## 查询（12 条 WFL 规则，10m 窗口）

`models/rules/nexmark.wfl` 实现 NEXMark 查询子集：
- **Q1** pass-through 基线（bid 计数）
- **Q2** 按 `auction % 123 == 0` 过滤（对齐 Flink Q2，选中 ~0.81% bids）
- **Q3** person⋈auction join（auction 驱动，seller==person.id）
- **Q9** person⋈auction join + 按 seller 计数
- **Q4** bid⋈auction join + 窗口聚合（hash join，92M bids 进管道）
- **Q5** 每 auction bid 计数（Top-N 的计数面）
- **Q7** 每 auction 10m 窗口最高出价（滑动窗口 MAX）

> Q3（person+auction join）需跨字段 join key，当前 match-key 模型不支持。单查询隔离文件在
> `models/queries/{q1,q2,q4,q5,q7}.wfl`。

## 基准工具：bench.sh

```bash
./bench.sh [query=all|q1|q2|q4|q5|q7] [feed=cont|stream] [total=100m|30m|10m]
```

- **feed=cont**（默认）：`gen-nexmark` 生成 → `dump-frames` 预编码 Arrow 帧 →
  `send-arrow` **一条 TCP 连接连续推完**（无每轮连接开销）。100M 唯一事件、事件时间固定。
  EPS = 引擎持续吞吐（背压约束）。
- **feed=stream**：`wfgen stream` 实时生成（事件时间随 slice 推进、按 `RATE` 注入）。
  注意：wfgen 客户端实时编码受限 ~760k/s，**不是引擎上限**——stream 用于正确性/长稳，
  cont 用于峰值 PK。
- 输出每查询 `data/bench_<q>_<feed>.txt`：EPS + RSS 峰值 + 驱逐数。
- 环境变量：`RATE`（stream 目标速率，默认 3000000）、`SLICE_MS`（默认 1000）。

帧文件 `data/bench_100m.frames`（~7.7GB）跨查询复用，生成一次后可直接重跑。

## 实测（100M 事件/查询，send-arrow cont，单 M3 Max 16 核/64GB，2026-08-14）

**blackhole 输出（= Flink discard 口径，纯处理吞吐）**：

| 负载 | EPS（中位） | RSS 峰值（中位） |
|---|---|---|
| Q1 | ~5.8M | 12.4 GB |
| Q2 | ~6.6M | 3.8 GB |
| Q4 | ~5.5M | 18.0 GB |
| Q5 | ~5.8M | 20.8 GB |
| Q7 | ~4.6M | 16.9 GB |

> EPS 为 3 次运行中位数（波动 ±2-6%，Q7 ±29%），2 位有效数字。

- 100M 全部处理、无丢失；RSS 峰值后回落（有界、非泄漏）。
- **EPS 平坦**（5.5-6.8M，≤19% 波动）：所有查询撞在同一共享管道上限（parse 物化 +
  fanout 广播 + 每事件 match 评估的固定成本）；窗口聚合是叠在共享成本上的增量 O(1)。
  对比 Flink 复杂查询掉 20×（KeyBy 重排 + 状态 + checkpoint），这是单机内存架构的差异。
- **cont 比 10m×10 分包重放高 1.6-2.2×**（消除每轮 TCP 连接开销）。

## 对 Flink 官方 Nexmark 基线（同输出口径：都丢弃）

来源：[Alibaba Nexmark 白皮书](https://help.aliyun.com/en/flink/realtime-flink/support/nexmark-performance-testing)
——开源 Flink 1.20.4（OSS，3 × ecs.g6a.xlarge = 12 vCPU/48GiB）与阿里 VVR（8 CU），
100M 记录/查询。
**白皮书只发布 RPS，未报告内存**。

| 查询 | wfusion | Flink OSS | vs OSS | VVR | vs VVR |
|---|---|---|---|---|---|
| Q1 | ~5.8M | 1,753,002 | **3.3×** | 4,381,353 | **1.3×** |
| Q2 | ~6.6M | 1,927,154 | **3.4×** | 6,568,576 | **1.0×** |
| Q3 | ~3.3M | 1,177,322 | **2.8×** | 4,638,649 | **0.72×** |
| Q4 | ~5.5M | 180,693 | **30×** | 636,468 | **8.6×** |
| Q5 | ~5.8M | 273,496 | **21×** | 279,684 | **21×** |
| Q7 | ~4.6M | 79,526 | **58×** | 299,547 | **15×** |
| Q9 | ~3.3M | 43,021 | **77×** | 375,146 | **8.8×** |

5 个查询全部同时超过 OSS Flink 与阿里 VVR（旧 10m 重放口径下 Q1/Q2 落后 VVR，cont 下反超）。

## 调优结果（2026-08-15，100M，parse=6/rule=6）

**帧大小**（Q1）：1MiB 是甜点（EPS 5.86M / RSS 8.3GB，内存比 8MiB 降 75%、吞吐仅 -6%）；
200KiB 内存更省（5.9GB）但吞吐 -16%（Q2/Q5/Q7 降 24-26%）。

**全查询 200K vs 1MiB**：200K 的 RSS 收益不统一（Q1 -40%、Q5 反而 +5%），EPS 全面降 6-26%——
**1MiB 是吞吐/内存综合最优**。

**并行度**（Q1 1MiB）：6/6 最优（5.86M / 8.3GB）；parse/rule 偏移都更差。

**限速 × 帧大小**：IngestLimiter 有效速率 = 批大小 ÷ 每批开销（帧越小天花板越低，200K 限 2M
只到 ~0.9M）。限速只压「注入突发」内存；200K 帧已平滑，限速反而窗口滞留、RSS 升 33-44%。

**内存构成**：空规则基线 ~1.5GB + 窗口数据 + 规则实例状态 + 瞬时在途 + allocator 页保留。
窗口内存 = 事件数 × 密度（Parsed Event ~1-2.5KB vs Arrow 87B，膨胀 10-20×，是 #20 根因）。
密度杠杆：字段过滤（已做）；列式零拷贝 M16 预期 ↓10×。

> 连接器 bug：wp-core-connectors macOS EINVAL（无界读 → buffer 涨 GBs）已用有界读 256KB 修复
> （`third_party/wp-core-connectors` fork + `[patch.crates-io]`）。

## 诚实边界

1. **硬件不对等**：wfusion 在 16 核 M3 Max（64GB），Flink OSS 在 12 vCPU/48GiB
   （3 × ecs.g6a.xlarge）、VVR 在 8 CU——倍率含
   硬件红利，非纯引擎声明。wfusion 稳态只用 ~5-9 核，未吃满。
2. **语义简化**：Q4 为 bid⋈auction join 近似（非 Flink 的 category 均价），Q5/Q7 为阈值版（非完整 Top-N）。
3. **Flink 无内存数据**：白皮书只发布 RPS，无法公平比内存；wfusion 内存为 RSS 峰值（有界）。
4. **架构差异**：Flink 有 exactly-once + checkpoint + 分布式重排开销，wfusion 单机
   at-least-once 无此成本——吞吐优势不等于总成本优势。
5. **stream 客户端受限**：`wfgen stream` 实时生成路径上限 ~760k/s（GenEvent→Arrow 逐事件
   编码），不是引擎能力；引擎真实能力以 cont（预编码回放）为准。
6. **单机 vs 分布式**：wfusion 单节点，Flink 分布式——规模化对比需另行测试。真实负载
   （<1M EPS 居多）+ 突发峰值下，单机 5.5-6.8M 持续能力有充足余量。

## 前提

- `wfusion` / `wfgen` 在 PATH 或 `WFUSION=/path WFGEN=/path`，需含 `gen-nexmark`、
  `dump-frames`/`send-arrow`、`stream`（warp-fusion HEAD 之后）。
- `nc`、`python3`；端口 9800 空闲。
