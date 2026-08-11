# nexmark_pk — NEXMark 基准查询吞吐（对齐 Flink 官方基线）

与 Flink 对齐的 PK case：**同一份权威基准数据（NEXMark）+ 同一批查询（Q1/Q2/Q4/Q5/Q7）**，
跑我们引擎的实测吞吐，对照 Flink 官方发布的 Nexmark 基线。这是**数据+查询**对齐的对比
（区别于 flink_pk 用的 PatternStudio 锚——后者数据集未公开）。

## 数据（NEXMark 事件模型）

`scripts/gen_nexmark.py` 确定性生成（seed 可调），对齐标准 NEXMark schema：

| 流 | 占比 | 字段 |
|---|---|---|
| person_events | 2% | id/name/email/city/state/dateTime |
| auction_events | 6% | id/itemName/description/initialBid/reserve/dateTime/expires/seller/category/extra |
| bid_events | 92% | auction/bidder/price/channel/url/dateTime/extra |

事件时间覆盖 ~30 分钟（3 个 10m 窗口）；hot 分布（50% hot auction / 25% hot bidder /
25% hot seller）。重新生成：`python3 scripts/gen_nexmark.py 200000 > data/burst.jsonl`。

## 查询（12 条 WFL 规则，10m 窗口）

`models/rules/nexmark.wfl` 实现 NEXMark 查询子集：
- **Q1** pass-through 基线（bid 计数）
- **Q2** 按 auction id 过滤（stateless filter）
- **Q4** 每 category 10m 窗口均价（窗口聚合）
- **Q5** 每 auction bid 计数（Top-N 的计数面）
- **Q7** 每 auction 10m 窗口最高出价（滑动窗口 MAX）

> Q3（person+auction 按 seller=id join）需跨字段 join key，当前 match-key 模型不支持，
> 列为后续项。单查询隔离文件在 `models/queries/{q1,q2,q4,q5,q7}.wfl`。

## 实测（200000 事件，release，单 Mac，2026-08-11）

EPS 用 send 墙钟计时（单连接流式）。组合（12 规则）与单查询隔离各跑：

| 负载 | 送达 | EPS | 驱逐 | 告警 |
|---|---|---|---|---|
| 组合 Q1+Q2+Q4+Q5+Q7 | 200000 | **~453k** | 0 | ~271k |
| 仅 Q1 | 200000 | ~435k | 0 | 38k |
| 仅 Q2 | 200000 | ~499k | 0 | 31 |
| 仅 Q4 | 200000 | **~469k** | 0 | 12k |
| 仅 Q5 | 200000 | **~481k** | 0 | 3.6k |
| 仅 Q7 | 200000 | **~455k** | 0 | 34k |

## 对 Flink 官方 Nexmark 基线

来源：[Alibaba Nexmark 白皮书](https://help.aliyun.com/en/flink/realtime-flink/support/nexmark-performance-testing)
——开源 Flink 1.20.4，8 CU，100M 记录/查询，RPS=records/sec（输入事件/秒，与我们 EPS 同单位）。

| 查询 | 我们（1 Mac） | Flink（8 CU） | 倍率 |
|---|---|---|---|
| Q4 窗口均价 | **~469k** | 180,693 | ~2.6× |
| Q5 计数 | **~481k** | 273,496 | ~1.8× |
| Q7 窗口 MAX | **~455k** | 79,526 | ~5.7× |

## 诚实边界

1. **数据/查询已对齐**（同一 NEXMark 事件模型 + 同一查询，units 均为输入事件/秒）——这是
   与 flink_pk（PatternStudio 锚，数据集未公开）的本质区别。
2. **硬件/规模不同**：我们 1 台 M 系 Mac、200k 事件；Flink 基线是 8 CU 云主机、100M 记录。
   单机对 8 CU 仍领先 ~1.8–5.7×，说明差距主要在引擎架构（#19 共享解析 + 单遍规则评估）
   而非资源。
3. **Q4 用 initialBid 代 closing price**；Q5/Q7 为阈值版（非完整 Top-N 输出）。
4. **Q3 join 未实现**——引擎跨字段 join key 是当前模型缺口。

## 前提

- `wfusion` / `wfgen` 在 PATH（或用 `WFUSION=/path WFGEN=/path`），wfgen 需含
  `--chunk/--rate-ms` 单连接流式支持（warp-fusion 84333b5）。
- `nc`、`python3`；端口 9800 空闲。
