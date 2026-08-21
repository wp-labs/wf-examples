# nexmark_pk — NEXMark 基准：吞吐 PK + 正确性验证（对齐 Flink 官方基线）

与 Flink 对齐的 PK case：**同一份权威基准数据（NEXMark）+ 同一批查询（Q1/Q2/Q3/Q4/Q5/Q7/Q9）
+ 同输出口径（blackhole 丢弃）**，跑引擎实测吞吐与输出正确性，对照阿里 Nexmark 白皮书发布的
OSS Flink / VVR 基线。性能结论与诚实边界见 `PK_REPORT_MAC.md`（姊妹文档）。

## 目录结构

```
bench.sh                 # 基准驱动（生成/复用帧 → 起 daemon → send-arrow 回放 → 采样）
conf/wfusion.toml        # daemon 配置（parse/rule 并行度等）
models/queries/qN.wfl    # 查询定义（唯一权威来源，每文件一组同族规则）
models/schemas/          # 事件 schema（nexmark.wfs）
topology/                # source/sink 拓扑（send-arrow 源、blackhole 汇）
scripts/                 # 数据生成 + 正确性验证工具（见下文）
data/                    # 运行产物（gitignore）：帧文件、bench 结果、metrics、ground truth
```

## 数据（NEXMark 事件模型）

`wfgen gen-nexmark <count> [--seed N]`（默认 seed=1，StdRng，确定性、流式、内存有界）：

| 流 | 占比 | 说明 |
|---|---|---|
| person_events | 2% | 30M 总量 = 600k person |
| auction_events | 6% | 30M 总量 = 1.8M auction |
| bid_events | 92% | 30M 总量 = 27.6M bid |

事件时间 ~30 分钟（线性映射，严格递增，等价 Flink `outOfOrderGroupSize=1`）；热点：
50% hot auction（出价 [100,500]）/ 50% cold（[10,150]），seller 与 bidder 各自
50% 走最近 15s 热窗、50% 走最近 60s 冷窗。
**同一 count + seed 的生成结果字节级确定**（`wfgen gen-nexmark`，确定性已验证）。
与 Flink 官方定义（`nexmark/nexmark`）的**逐项符合性对照（含刻意偏离项及影响）**
见 [`NEXMARK_CONFORMANCE.md`](./NEXMARK_CONFORMANCE.md)；`gen-nexmark --check` 与
`verify-nexmark` 会在报告尾部自动输出符合性摘要。

## 查询：与 Flink Nexmark 测试集的逻辑匹配度

Flink 参照系 = [Alibaba Nexmark 白皮书](https://help.aliyun.com/en/flink/realtime-flink/support/nexmark-performance-testing)
测试集内的 7 条查询（Q1/Q2/Q3/Q4/Q5/Q7/Q9；**Q6、Q8 等其余 12 条未实现**）。
每条按「语义对齐程度」分三档：

| Query | Flink 语义 | wfusion 实现（`models/queries/`） | 匹配度 |
|---|---|---|---|
| Q1 | 无状态投影：每 bid 输出一行 | `on each` 每 bid 一条告警（无 match 状态机——keyed 版会让 6M 实例表击穿 CPU 缓存，非对等工作负载） | **精确**（工作负载等价，输出 schema 不同） |
| Q2 | `WHERE MOD(auction,123)=0` 过滤 | `events { b && b.auction % 123 == 0 }` + count≥1 fire | **精确**（谓词同构，选中 ~0.81% bids） |
| Q3 | person⋈auction（seller=id）投影卖家信息 | auction 驱动 + `join person_events snapshot`，count≥1 每 auction 一条 | **精确**（join 语义同构；输出 seller id 而非 name/city/state 投影） |
| Q4 | bid⋈auction 后按 category 均价（两层：每 auction max → category avg） | bid 驱动 + `match<auction:10m:fixed>` + `and close { b.price \| avg }`（每 auction 窗口均价） | **部分精确**（avg 聚合面；外层 category avg 平台不可表达，见 SEMANTIC_ALIGNMENT §5.3） |
| Q5 | 滑窗计数面（Top-N 的 counting 面） | `match<auction:10m>` count≥{10,50,100} 三阈值 | **状态语义精确**（滑动计数 + reset-on-fire；非完整 Top-N 输出） |
| Q7 | 每 auction 滑窗最高出价 | `match<auction:10m>` max(price)≥{200,500,1000} 三阈值 | **状态语义精确**（滑窗 MAX；输出为阈值告警非 max 值投影） |
| Q9 | person⋈auction 按 seller 分组计数 | auction 驱动 + snapshot join + `match<seller:10m>` count≥1 | **精确**（join + 分组计数同构） |

**匹配度小结**：Q1/Q2/Q3/Q9 精确；Q5/Q7 窗口状态语义精确（输出为告警阈值面）；Q4 部分精确
（avg 聚合面对齐，外层 category avg 平台不可表达，见 SEMANTIC_ALIGNMENT §5.3）。全部 7 条
的白皮书测试集内对齐已覆盖（7/19 条 NEXMark 全集）。

### 正确性验证（30M replay，seed=1）

期望值由 `wfgen verify-nexmark`（真实 WFL 规则引擎）对同一份确定性数据逐规则算出：
`bench.sh <q> replay 30m --verify` 在 wfgen 内与引擎实际 EMIT 计数对拍
（git-diff 同款分层：L1 哈希 → L2 Myers → L3 明细，退出码 0=一致 / 1=有差异）。

| 规则 | 期望 | 引擎实测 | 结果 |
|---|---|---|---|
| q2_mod_123 | 224,289 | 224,289 | ✅ |
| q3_auction_seller | 1,800,000 | 1,800,000 | ✅ |
| q4_avg_price_by_category | 5,254,483（oracle 理想） | 30M 4,228,230 | ⚠（fixed+close 收口非确定，丢尾部，见 SEMANTIC_ALIGNMENT §6.1） |
| q5_bidcount_10 | 1,712,532 | 1,712,470 | ✅（差 62 = 0.0036%，scan_timeouts 墙钟非确定性） |
| q5_bidcount_50 / 100 | 0 / 0 | 0 / 0 | ✅ |
| q6_avg_price_200 | 9,794,325 | 10m 3,263,324 | ✅（10m 对拍 ±1 墙钟摆动） |
| q7_maxbid_200/500/1000 | 10,350,961 / 34,578 / 0 | 同左 | ✅ |
| q8_monitor_new_user | 600,000 | 10m 200,000 | ✅（10m 对拍精确） |
| q9_winning_bid | 5,254,483（oracle 理想） | 30M 4,183,632 | ⚠（fixed+close 收口非确定，丢尾部，见 SEMANTIC_ALIGNMENT §6.1） |
| q10_arbitrary_selection | 3,944,636 | 10m 1,314,285 | ✅（10m 对拍精确） |
| q13_bid_person_join | 27,600,000 | 10m 9,200,000 | ✅（10m 对拍精确） |

> **新查询语义诚实标注**：q6 按 auction 聚合均价（非标准按卖家——卖家来自 join，窗口键须取原始
> 事件）；q8/q11 会话窗口（`session(gap)`）。**q11 的会话在 bench 按 auction 分片下是 per-shard**
> （同一 bidder 的 bid 跨 shard，会话被切碎；要全局会话语义须 `CONNECTIONS=1` 或按 bidder 分片）；
> **q12/q14 的 conv top-N 是全局的**（conv 阶段跨分片合并后做 top-N，`CONNECTIONS=4` 即全局）。
> `wfgen verify-nexmark` 覆盖全部规则；**已知差异 q21（anti join）**——oracle 不评估 join 窗口
> 状态，标 ⚠ 不判失败；q11 全局语义以 `CONNECTIONS=1` 验证，q12/q14 以 30M
> 端到端确定性 + `[clean]` 为准。

28k 事件探针逐 alert 对拍 **2,679/2,679 全量精确吻合**（含 fire 时刻）。q1 为无状态路径
（输出=输入 bid 数，无状态机语义）。全部跑批 appended 30M/30M，
serialize_failed/dropped_late/cursor_gap/memory_evicted = 0。

**100M 吞吐跑批的正确性侧证（2026-08-17，P0-② content 记账 2GB）**：全量 Q1-Q9
（q1/q2/q3/q4/q5/q7/q9）SUMMARY 全 clean（serialize_failed / dropped_late /
memory_evicted / cursor_gap = 0），appended=100M/100M。输出计数：q1=92,000,000
（=输入 bid 数）、**q2=747,816（占比 0.8129%，与 30M 的 0.8127% 精确吻合）**、
q9=6,000,000（=30M 期望 1.8M × 100/30）。q3/q5/q7 的 EMIT 在 run 间有驱逐时序
波动（非正确性破坏，见 PK_REPORT_MAC 测量说明）。

## 基准工具：bench.sh

```bash
./bench.sh [query=all|q1|q2|q3|q4|q5|q7|q9] [feed=replay|stream] [total=100m|30m|10m]
MAX_FRAME_BYTES=1048576 ./bench.sh all replay 30m    # 指定帧 cap（默认 8MiB）
```

- **feed=replay**（默认，PK 口径）：gen-nexmark → dump-frames 预编码 Arrow 帧 →
  send-arrow 重放（默认 **单连接**整文件推，`CONNECTIONS=1`、`SHARD_KEYS` 空；
  多连接仅在有状态负载需要键闭包分片时用）。事件按 30s 桶序（v2 排序数据），
  帧/分片缓存带 `DATA_VER`（默认 v2）指纹，`data/bench_<total>_v2.frames`
  跨查询复用，存在即不重生成。**测引擎峰值持续吞吐（性能基准口径）**。
- **feed=stream**：wfgen 实时生成按 RATE 注入（事件时间随墙钟推进，客户端编码
  上限 ~760k/s——**非引擎能力**，EPS 不可比）。**使用场景**：
  ① 真实时间窗口语义（`over=10m` 时间驱逐/watermark 按真实时间实时发生，而非
  replay 的追赶式）；② 长时稳定性/内存有界（中低速持续跑几十分钟~几小时，看
  RSS 是否有界、不泄漏）；③ 生产形态模拟（事件时间=现在，验证 late/`allowed_lateness`
  行为）。**不用于**吞吐对比与短时跑批。
- 输出每查询 `data/bench_<q>_<feed>.txt`：EPS + RSS 峰值 + 驱逐数；
  `data/metrics.ndjson` 为计数器流，`data/{wfusion,daemon}.log` 为引擎日志。

### 测量纪律（违反会得出假结论）

1. **计时口径 = append_total**：metrics 三输入流 append 计数器求和追平 TOTAL 的时刻
   （旧 ingress 预读游标口径已作废；PK_REPORT_MAC §4.1 已按新口径更新）。
2. **RSS 口径（2026-08-17 起）**：`parse_buffer_bytes` 默认值已改为 128MB
   （P0-② content 记账，18 槽）——q1 100M ≈ 6.1M / RSS ~5.9GB，旧默认
   （256MB 解码记账）为 5.93M / 4.4GB。吞吐优先场景显式调大预算（bench 默认
   2GB：q1 7.5M+，RSS 随吞吐升至 12-14GB）。引用 RSS 数字时必须标注所用
   `parse_buffer_bytes`，旧口径数字（“100M 6.8GB”等）与现默认不直接对等。
3. **A/B 必须不限速**：`RATE=10000000`（限速会把 EPS 封顶在 RATE）。
4. **同时段交错对比**：bench 机 EPS 与 RSS_peak 呈双峰相位强相关（同配置差 ±8%），
   结论必须按 RSS 相位配对；单轮数字只能作量级参考。
5. 消费侧计数器提取：`python3 scripts/extract_emitted.py data/metrics.ndjson`
   （counter 跨 1s 区间求和；gauge 取峰值，不可混用）。

## scripts/ 工具清单

| 脚本 | 用途 |
|---|---|
| `wfgen verify-nexmark [--query qN] [--engine-emit data]` | 真实规则引擎 ground truth + 引擎 EMIT 对拍（git-diff 同款分层） |
| `extract_emitted.py` | metrics.ndjson → emitted/append/正确性计数器汇总 |
| `read_metrics.py` | metrics NDJSON 指定 stage/name/label 最新值查询 |

## 前提

- `wfusion` / `wfgen` 在 PATH 或 `WFUSION=/path WFGEN=/path`，需含 `gen-nexmark`、
  `dump-frames`/`send-arrow`、`stream` 子命令。
- `nc`、`python3`；端口 9800 空闲。
- 正确性验证另需 sink 输出（file_json_sink 的 alerts.ndjson），吞吐 PK 用 blackhole。
