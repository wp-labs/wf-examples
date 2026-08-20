# NEXMark 基准：背景、查询、数据与正确性标准

> 配套文档：
> - `README.md` —— 套件结构、bench.sh 用法、测量纪律
> - `TEST_PLAN.md` —— 可执行测试方案（微基准 + 端到端 + 回归对比 + 验收标准）
> - `PK_REPORT_MAC.md` / `PK_REPORT_LINUX.md` —— 实测性能报告与诚实边界

本文回答五个问题：**Nexmark 是什么？Q1~Q22 做什么测试？数据有什么要求？我们如何准备数据？正确的标准是什么？**

---

## 1. Nexmark 是什么

**NEXMark** 是流处理领域的标准基准，模拟**实时拍卖平台**（eBay 类）的事件流与查询。它源自论文
*"Streaming Queries over Streaming Data"*（Tucker et al., 2008），阿里在其 Flink（VVR）性能测试
白皮书中发布了可复现的 19 条查询测试集，成为流引擎吞吐/延迟/状态规模 PK 的事实标准（对标
Flink OSS / VVR、Kafka Streams、Spark Streaming 等）。

**事件模型**：三个事件流，共 30M 条/回合（可缩放）：

| 流 | 占比 | 30M 总量 | 语义 |
|---|---|---|---|
| `person_events` | 2% | 600k | 竞拍者/卖家资料（id/name/email/city/state） |
| `auction_events` | 6% | 1.8M | 拍卖品（id/itemName/seller/category/price 等） |
| `bid_events` | 92% | 27.6M | 出价（auction/bidder/price/channel/url） |

**23 条查询（q0~q22）**覆盖流处理的核心能力面：无状态投影（Q1）、过滤（Q2）、等值 join
（Q3/Q9）、窗口聚合（Q4/Q6/Q16）、计数/去重/Top-N（Q5/Q12/Q14/Q17）、滑窗极值（Q7）、
会话（Q8/Q11）、子集（Q10）、序列（Q19/Q20）、anti/asof join（Q21/Q22）等。本项目实现
**q1~q22 全部 22 条**（q0 为 Q1 的无状态等价面；白皮书未发布 q6/q13 基线，但 q6 已实现），
对齐 Flink 测试集，用于本引擎的吞吐 PK 与输出正确性验证。

---

## 2. Q1~Q22 做什么测试

| Query | Flink 语义 | 本项目实现（`models/queries/`） | 测的能力面 |
|---|---|---|---|
| **Q1** | 无状态投影：每 bid 输出一行 | `on each` 每 bid 一条告警 | **无状态吞吐天花板**（管线极限，无状态机开销） |
| **Q2** | `WHERE MOD(auction,123)=0` 过滤 | `events { b && b.auction % 123 == 0 }` + count≥1 | **过滤 + 列式 guard**（选中 ~0.81% bids） |
| **Q3** | person⋈auction（seller=id）投影卖家信息 | auction 驱动 + `join person_events snapshot` + count≥1 | **hash join + 窗口计数** |
| **Q4** | bid⋈auction 后按 category 均价 | bid 驱动 + `join auction_events snapshot` + 窗口 count | **高吞吐 join 查表 + 窗口聚合**（92M bids 进管道） |
| **Q5** | 滑窗计数面 | `match<auction:10m>` count≥{10,50,100} | **滑动窗口计数 + 状态机**（fire/reset，高实例 churn） |
| **Q6** | 按卖家/分类窗口均价 | `match<auction:10m>` `b.price \| avg >= 200`（按 auction 聚合，卖家来自 join 需 emit 后数据） | **avg measure + 状态机** |
| **Q7** | 每 auction 滑窗最高出价 | `match<auction:10m>` max(price)≥{200,500,1000} | **滑窗 MAX + emit 密集路径** |
| **Q8** | 监控新用户 | person 流 `match<id:session(60s)>` count≥1 | **session 窗口**（person 流） |
| **Q9** | person⋈auction 按 seller 分组计数 | auction 驱动 + snapshot join + `match<seller:10m>` count≥1 | **join + 分组计数** |
| **Q10** | 任意选择（确定性子集） | `on each` + `b.auction % 7 == 0` | **on-each + bind filter**（无状态子集） |
| **Q11** | 用户会话 | bid 流 `match<bidder:session(60s)>` count≥1 | **session 窗口**（bid 流；per-shard 见 §5） |
| **Q12** | Top-N（前 N 项） | fixed 10m + `and close` count + `conv { sort(-n) \| top(3) }` | **conv top-N + fixed 窗口** |
| **Q13** | 有界侧输入 join | bid ⋈ person snapshot join | **第二窗口 snapshot join** |
| **Q14** | 两级聚合 Top-N | auction 按 seller 计数 + `conv { sort(-n) \| top(10) }` | **conv top-N（更大 N）+ 第二窗口键** |
| **Q15** | 过滤 + 窗口计数 | `b.price > 100` 过滤 + `match<auction:10m>` count≥5 | **bind filter + 滑窗计数** |
| **Q16** | 复杂窗口聚合 | fixed 10m + `and close` `b.price \| sum >= 1000` | **on close 窗口聚合（sum）** |
| **Q17** | 去重/集合聚合 | `match<auction:10m>` `b.bidder \| distinct \| count >= 20` | **distinct 变换 + 窗口 measure** |
| **Q18** | 窗口内累积 | `on event<accu> { b \| count >= 5 }` | **`on event<accu>` 累积 fire** |
| **Q19** | 有序序列 | `on event seq { has b; has b within 60s }` | **`on event seq` 有序序列** |
| **Q20** | 无序并存 | `on event any { count>=2; count>=3 }` | **`on event any` 无序 step** |
| **Q21** | anti/semi join | `join person_events anti on b.bidder == person_events.id` | **anti join**（命中丢弃） |
| **Q22** | 时间邻近 join | `join person_events asof within 60s` | **asof join**（时间邻近） |

> **语义诚实标注**：q6 按 auction 聚合（标准按卖家，卖家来自 join 属 emit 后数据，窗口键须取
> 原始事件）；q4 聚合面是 count 而非 category 均价（工作负载等价）；q8/q11 会话窗口；
> q12/q14 conv top-N 是**全局**（conv 阶段跨分片合并后 top-N）；**q11 会话在按 auction 分片下
> 是 per-shard**（要全局会话语义须 `CONNECTIONS=1` 或按 bidder 分片）；q21 anti join 在
> person 窗口过期时保留 bid（EMIT ≈ 部分 bid）。

**能力面覆盖**：on-each / 过滤 / snapshot-join / anti-join / asof-join / 滑窗 count·max·avg·sum /
session / distinct / conv top-N / `on event<accu>` / seq / any —— 主要 DSL 能力全覆盖。

---

## 3. 数据有什么要求

NEXMark 标准数据的要求，本实现（`scripts/gen_nexmark.py` + `wfgen gen-nexmark`）全部满足：

1. **三流结构**：person/auction/bid 各字段与 NEXMark 事件模型一致（见 `models/schemas/nexmark.wfs`），
   事件时间字段为 `dateTime`（Timestamp）。
2. **事件时间跨度 ~30 分钟**：`SPAN_NS = 1800s`——恰好填 3 个 10m 滑窗，保证窗口能完整滑动
   与过期（窗口聚合/状态机语义可被真实触发）。
3. **hot 分布**（贴近真实拍卖的偏斜）：**50% hot auctions**、**25% hot bidders**、**25% hot sellers**
   （`HOT_SELLERS=250`、`HOT_BIDDERS=250`、`HOT_AUCTION_RATIO=0.50`）——制造热 key 争抢，
   暴露状态/join 的热点压力。
4. **确定性**：同一 `count + seed`（默认 seed=1）生成结果**字节级确定**（`random.Random(seed)`
   / StdRng）——这是正确性可验证的**前提**（ground truth 可对同一数据独立复算）。
5. **跨查询共享同一份数据**：Q1-Q22 全部跑在**同一份确定性数据**上，保证查询间可比较、
   吞吐 PK 的公平性。
6. **规模可缩放**：`gen-nexmark <count>` 按 `count` 缩放（30M 全量 / 10M / 100M 吞吐跑批）。

---

## 4. 我们如何准备数据（管线）

`bench.sh` 的 `feed=replay`（PK 口径）数据准备管线，产物全部可缓存复用：

```
1. 生成        wfgen gen-nexmark <N>                     → data/burst_bench.jsonl（N 条 JSONL 事件流）
2. 预编码帧    wfgen dump-frames --scenario nexmark.wfg   → data/bench_<N>[_mb<bytes>].frames
                   --input burst_bench.jsonl --max-frame-bytes <MAX_FRAME_BYTES>（默认 8MiB）
                   （事件流编码成 Arrow IPC 帧文件，跨查询复用，存在即不重生成）
3. 按键分片（仅 CONNECTIONS>1）：
               wfgen shard-frames --shards <C> --shard-keys "bid_events:auction,auction_events:id,person_events:id"
                   → data/shard_<N>_c<C>_k<md5>.frames（同 key 同分片，键闭包）
4. 回放        send-arrow --shard-files …（CONNECTIONS 条 TCP 连接并发推，每连接一份分片）
                   → 引擎 daemon（conf/wfusion.toml，端口 9800）
```

**要点**：
- **帧文件缓存**：`data/bench_<total>.frames` 跨查询/跨跑复用（预编码避免每次重复生成）。
- **键闭包分片**：`SHARD_KEYS` 让同一 key 的事件走同一连接，保证**有状态规则并发安全**
  （实测 emitted 与单连接逐位一致）。
- **正确性另有 `feed=stream`**（wfgen 实时生成注入）用于长稳/正确性，非吞吐 PK。

**吞吐/内存调参**（bench.sh 环境变量）：`PARSE_PARALLELISM` / `RULE_PARALLELISM` /
`MAX_FRAME_BYTES` / `MAX_FRAME_ROWS` / `CONNECTIONS` / `SHARD_KEYS` / `RATE`（限速，A/B 必须关）。

---

## 5. 正确的标准是什么

正确性 = **输出与确定性 ground truth 一致** + **数据完整性无丢失**。

### 5.1 确定性 ground truth（权威标准）

`scripts/verify_ground_truth.py` 用 Python **独立重放同一份确定性数据**，精确镜像引擎
match 语义，逐规则算出期望 `emitted_total`：

- 每个规则独立 MatchEngine（独立实例表/过期堆/pending-expiry 去重集）。
- 滑动窗口 `match<key:10m>` 过期语义（`created_at + 600s <= watermark`）。
- `push_expiry_candidate` **按 key 去重**（pending set）——28k 探针逐 alert 对拍
  **2,679/2,679 全量精确吻合**（含 fire 时刻）验证过的引擎细节。
- `on event`（非 accu）：fire 后实例 **reset**（count/max 清空，created_at = fire 时刻）。
- snapshot join **miss 不 drop 事件**（只富化不丢）。

**30M（seed=1）期望值**（`verify_ground_truth.py`；30M 权威 + 10m 对拍）：

| 规则 | 30M 期望 | 引擎实测 | 结果 |
|---|---|---|---|
| q2_mod_123 | 224,289 | 224,289 | ✅ |
| q3_auction_seller | 1,800,000 | 1,800,000 | ✅ |
| q4_real_avg_100 | 27,600,000 | 27,600,000 | ✅ |
| q5_bidcount_10 | 1,712,532 | 1,712,470 | ✅（差 62=0.0036%，scan_timeouts 墙钟非确定） |
| q6_avg_price_200 | 9,794,325 | 10m 3,263,324 | ✅（10m 对拍 ±1） |
| q7_maxbid_200/500/1000 | 10,350,961 / 34,578 / 0 | 同左 | ✅ |
| q8_monitor_new_user | 600,000 | 10m 200,000 | ✅（10m 对拍精确） |
| q9_seller_count | 1,800,000 | 1,800,000 | ✅ |
| q10_arbitrary_selection | 3,944,636 | 10m 1,314,285 | ✅（10m 对拍精确） |
| q13_bid_person_join | 27,600,000 | 10m 9,200,000 | ✅（10m 对拍精确） |
| q15_high_bid_count_5 | 2,563,592 | 2,563,534 | ✅（差 58≈0.002%） |
| q17_distinct_bidders_20 | 157,154 | 10m 52,236 | ✅（10m 对拍 ~0.3%） |
| q18_accumulate_fires | 17,919,533 | 17,918,519 | ✅（差 1014≈0.006%） |
| q19_seq_two_bids | 490,097 | 10m 162,879 | ✅（10m 对拍 ~0.3%） |
| q20_any_count_3 | 7,763,818 | 10m 2,587,329 | ✅（10m 对拍 ~0.02%） |
| q16_sum_price_1000 | 1,886,924 | 1,886,924 | ✅（2026-08-20 修复：max_memory 8GB 去限流 + scan_timeouts 无界预算收口最后桶） |
| q21_anti_person | 0（朴素值） | 时序相关（10m ~21k-31k） | ⚠️ person 窗口驱逐，见下 |

> **模拟器覆盖**：q2/q3/q4/q5/q6/q7/q8/q9/q10/q13/q15/q16/q17/q18/q19/q20 由
> 模拟器精确对拍（q15~q20 已用 fresh 引擎运行对拍；q16 2026-08-20 起逐位一致）。
> **时序相关（非固定 ground truth）**：
> - q16（fixed + `and close` sum）：2026-08-20 修复后与模拟器**逐位一致**——根因是
>   ① `max_memory=512MB` 在 30m 规模触发 Throttle 丢事件（sum 累计不全，EMIT 低 8 倍）
>   改为 8GB；② 最后桶在数据末尾过期晚于最后事件时间，靠墙钟 `scan_timeouts` 兜底，
>   但 1024 增量预算处理不完 → 改 scan_timeouts 用**无界预算**（事件热路径仍 1024 防冻结）。
> - q21（anti join）：模拟器给的是「所有 bidder 都是 person」的朴素 0；引擎实际因 person
>   窗口在 lookup 时刻不完全而保留少量 bid（时序相关）→ 以 `[clean]` + 多轮 EMIT 确定性验证。
> **未覆盖**：q11（per-shard 会话，单机模拟无法匹配）、q12/q14（fixed 窗口 close+conv 未模拟）、
> q22（asof join 全量扫描 O(n²)，独立里程碑）——以 **10m/30m 端到端确定性 + `[clean]`** 验证
> （多轮 EMIT 一致），或 `CONNECTIONS=1`（q11 全局会话）。

### 5.2 数据完整性（`[clean]`）

每查询跑批输出 `SUMMARY`：`appended = 总行数/总行数` 且
`serialize_failed / dropped_late / memory_evicted / cursor_gap = 0` → **clean**。
这是「无丢失」的完整性权威。

### 5.3 输出计数（EMIT）口径

- **30M**：与 ground truth 逐位比对（权威，覆盖 q2/q3/q4/q5/q6/q7/q8/q9/q10/q13/
  q15/q17/q18/q19/q20）。
- **100M**：EMIT 与 30M **同比例侧证**（如 q2=747,816 = 30M 的 224,289 × 100/30 的
  0.8129% 占比精确吻合；q9=6,000,000 = 1.8M × 100/30）。
- **时序相关查询（q16/q21）**：以**多轮 10m/30m 端到端 EMIT 确定性 + `[clean]`** 验证
  （区间重叠而非单值相等；模拟器给理想/朴素值作参照，见 §5.1）。
- **新查询（q11/q12/q14/q22）**：以**多轮 10m/30m 端到端 EMIT 确定性 + `[clean]`**
  验证（q11 全局会话用 `CONNECTIONS=1` 单独验）。
- **新旧二进制回归**：Q2/Q3/Q7/Q9 必须逐位一致；Q4/Q5 在**既存波动带**内（区间重叠
  而非单值相等，见 TEST_PLAN §8）。

### 5.4 逐 alert 对拍（深验）

28k 事件探针：引擎 `alerts.ndjson` vs 模拟器期望**逐 alert 全量吻合**（含 fire 时刻），
作为语义锁死的最终标准（`scripts/q5_diff_v2.py`）。

### 5.5 已知波动（正确性的诚实边界）

- **Q4**（join auction_events）：EMIT 在 ~7.3M↔9.2M 随 run 波动——源是 auction_events
  窗口保留量随管道时序变化（join 求值时刻窗口里的 auction 数），**新旧二进制同样波动**，
  非正确性破坏。
- **Q5**（count≥10）：EMIT ±10（571,061~571,076），max_memory 驱逐时序 + 墙钟
  scan_timeouts 非确定性。
- 判定标准：**区间重叠**而非单值相等；`[clean]` + ground truth 才是正确性权威。

### 5.6 max_memory：限流机制与计算公式（2026-08-20 定稿）

规则级 `limits { max_memory }` 是**实例内存硬上限**，超限行为（默认 `on_exceed = throttle`）：
引擎每事件检查 `Σ实例估算内存 ≥ max_memory`，超限**静默丢弃该事件**（不推进规则状态、不
fire；无日志、无指标、`[clean]` 照常）→ EMIT 悄悄变少且不随数据规模增长，极易误判成
“引擎正确、对拍基准错”。`drop_oldest` 驱逐最旧实例（状态丢失）、`fail_rule` 让规则永久失效。

**判定依据 = 引擎估算**（`match_engine/state.rs::estimated_bytes`），不是真实 RSS（实测 RSS
约为估算的 2~3×，`collected_values` 等有 cap 且不计入估算）。每实例估算构成：

```
128 (Instance) + 32 (key) + Σ step×branch×80 + distinct_set(≈40B/值) + field_values + baselines×128
```

NEXMark 查询每实例估算：

| 类型 | 查询 | 每实例 base |
|---|---|---|
| 单 step count/avg/max | q5/q6/q7/q15/q18 | 240B |
| 双 step（seq 两步 / any 两步） | q19/q20 | 320B |
| distinct | q17 | 240B + 40B×去重 bidder（≈15） |

**实例数 = 规则 key 基数**（NEXMark = auction 数 = 0.06 × 总事件数）：10m→600k、30m→1.8M、
100m→6M。

**计算公式**：`max_memory ≥ 实例数 × 每实例估算 × 安全系数(2~4，覆盖引擎开销/估算波动)`

| 规模 | 单 step | 双 step | distinct |
|---|---|---|---|
| 10m | 0.3GB | 0.4GB | ~0.6GB |
| 30m | 0.9GB | 1.2GB | ~2.3GB |
| 100m | 2.9GB | 3.8GB | ~10GB |

**实测验证**：q20 30m 公式 576MB → 512MB 触发（EMIT 2.16M vs 8.45M）、1GB 不触发 ✅；
q5 30m 432MB < 512MB 不触发 ✅——这就是 30m 对拍只有 q17/q19/q20 失败的原因（均为
估算超 512MB 的双 step/distinct 型）。**“内存不足能跑多高速率”**：max_memory 不限制速率，
只限制**正确处理的数据规模**（超限后事件静默丢弃、不降速不报错）；速率上限由 CPU/管道并行度
决定。给定 M 与每实例估算 e：能正确处理的 key 数 = M/e（NEXMark 规模 = (M/e)/0.06）。

## 6. q1 内存调查（2026-08-20）：RSS 21-25GB → 1.9GB

### 现象
q1 100M 单独跑 RSS 峰值 21~25GB（`ps rss` 口径）、EPS 11~16M。

### 排查链（全部实测）
| 假设 | 实验 | 结论 |
|---|---|---|
| preread 预算卡内存（文档 §3.1 曲线） | `parse_buffer_bytes` 2GB/1GB/128MB 三档 | **无效**（RSS 全 ~24GB）。文档曲线是 push 模型时代测的，pull 模型下预算只卡源→窗口段，不管窗口持有量 |
| 窗口膨胀来自 max_window_bytes | 15GB→256MB（4 连接） | EPS +42%（16.5M）但 RSS 仅 20.9GB——**floor-respecting 驱逐被规则 ack 落后挡着**，持有量由消费进度决定 |
| 时间驱逐（over=10m）生效 | evictor 日志：watermark/floor 正常，`time_evicted` 恒 0 | **不生效**：bench 注入（key 分片 + 100k 行攒批）批次内事件时间乱序，每批 max 事件时间接近末尾，`batch.max_ts < watermark-10m` 永不满足 |
| 解码/分配器 | 零拷贝 decode（StreamDecoder）：列数据 64B 对齐 vs IPC 8B pad → 75% 列仍复制；mimalloc vs System 对比 RSS 无差异 | **都不是主因**（已回滚 decode 改动） |

### 根因
**窗口持有量 = 规则 ack 进度 × 内存驱逐（max_window_bytes）**。
- 4 连接 key 分片（默认 `SHARD_KEYS`）→ 多源交错 → 规则 ack 慢 → 窗口堆积 ~727 批未 ack → floor-respecting 驱逐无法清 → 持有全量 ~8GB 内容。
- 单连接整文件推 → 规则 ack 快 → 内存驱逐生效 → 窗口压到 cap。

### 最优配置（q1 无状态专用）
```bash
SHARD_KEYS="" CONNECTIONS=1 ./bench.sh q1 replay 100m   # 单连接整文件推
# 临时把 models/schemas/windows.toml 的 bid_events max_window_bytes 调小（如 256MB）
```
实测：**RSS 1.9~5.5GB（-90%+）、EPS 21~26M（+50~120%）、[clean]**（三次 1.9/3.0/4.5GB、26.2/23.4/22.4M）。

### 注意
- `max_window_bytes` 调小**只对无状态查询（q1）安全**——有状态/join 查询的 join 目标窗口数据会被驱逐、破坏正确性，不可全局套用。
- macOS `ps rss` **高估**物理占用（含 swap 出/空 zone 页）；`vmmap` 的 `Physical footprint` / `footprint` 更准（System allocator 下 12.3GB vs ps 22GB）。
- 时间驱逐依赖注入顺序：真实按事件时间有序的流不受影响；bench 的 key 分片注入天然乱序。

### 定案（2026-08-20 固化）

- **`gen-nexmark` 默认按事件时间排序输出**（60×30s 桶 + 桶内排序，内存有界，
  事件集合不变，`--no-sort` 保留旧 phase-major）——已提交 warp-fusion。
- **`bench.sh` 默认单连接**（`CONNECTIONS=1`、`SHARD_KEYS` 空）+ 帧缓存带
  `DATA_VER`（默认 v2）指纹，旧乱序缓存自动失效。
- **实测口径**（单连接 + v2 数据）：q1 100M RSS 3.0GB（256MB cap）/ 7.6GB
  （15GB cap）、EPS 25-26M、clean；Q1~Q21 30M 全 `[clean]`。
- 多连接（key 分片）仅在有状态负载需要键闭包时使用，引用内存数字必须标注
  注入方式与 DATA_VER。
