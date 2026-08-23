# NEXMark 基准：背景、查询、数据与正确性标准

> 配套文档：
> - `README.md` —— 套件结构、bench.sh 用法、测量纪律
> - `CAPABILITY_GAP_MATRIX.md` —— 22 查询逐条能力/语义判定（当前权威）
> - `archive/TEST_PLAN.md` —— 可执行测试方案（历史；测量纪律已并入 README）
> - `archive/PK_REPORT_MAC.md` / `archive/PK_REPORT_LINUX.md` —— 实测性能报告历史快照

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

22 条查询覆盖 Flink 官方 NEXMark 测试集的全部查询（`qN.sql`，权威原文见
`NEXMARK_AUTHORITATIVE_SEMANTICS.md`）。各查询的**官方意图 / 本地 `.wfl` 实现 / 能力档位**
见 `CAPABILITY_GAP_MATRIX.md` §一（18 已有 / Q12 待补强 / Q6·Q11·Q13 特殊口径），
执行器应用与语义偏差详见 `SEMANTIC_SUPPORT_MATRIX.md` / `SEMANTIC_ALIGNMENT.md`。

**能力面覆盖**：on-each / 过滤 / snapshot·deferred·asof join / fixed·sliding·session·hop
窗口 / distinct / conv top-N·top_ties / stats<>（count/sum/avg/min/max/distinct/last/top）/ `1d:fixed` 日历天桶。

---

## 3. 数据有什么要求

NEXMark 标准数据的要求，本实现（`wfgen gen-nexmark`）全部满足：

1. **三流结构**：person/auction/bid 各字段与 NEXMark 事件模型一致（见 `models/schemas/nexmark.wfs`），
   事件时间字段为 `dateTime`（Timestamp）。
2. **事件时间跨度 ∝ count**：`dateTime = BASE_NS + event_id × 100µs`（官方 `interEventDelayUs=100`
   固定速率）——30M → 3000s ≈ 50min，10m 窗口可完整滑动与过期（窗口聚合/状态机语义可被真实触发）。
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

`wfgen verify-nexmark` 用**真实 WFL 规则引擎**（wf_engine，经 oracle 管线）处理与
引擎同一份确定性数据、同一套 .wfl 规则，逐规则算出期望 `emitted_total`（规则名即
引擎 EMIT 名，identity 对拍）：

- 与 `gen-nexmark` 相同的 30s 桶序喂入——与引擎 daemon 收到的帧序一致，窗口过期
  语义对拍才成立（每规则独立 CepStateMachine + RuleExecutor）。
- 覆盖全部规则（含 q1 on-each / q11/q12/q14/q22 等早期模拟器未建模项）。
- **已知差异**：Q19（stats 规则 oracle 未接入，标 ⚠ 不判失败）；q21 已随数据侧
  `channel_id` 对齐（官方 95% 输出量）由 verify 覆盖。其余规则与引擎 EMIT 精确相等。
- 对拍在 wfgen 内完成（`--engine-emit data`）：git-diff 同款分层（L1 哈希 →
  L2 Myers/降级 → L3 明细），退出码 0=一致 / 1=有差异。

**各查询 30M 期望 / 实测 EMIT 状态**（Q1~Q22 全量 30M replay 全部 `[clean]`；
Q8/Q9 已与 oracle 对拍一致，Q19 待 stats oracle 接入）：见
`CAPABILITY_GAP_MATRIX.md` §一·§二（含已解决历史与 known-diff 登记）。

### 5.2 数据完整性（`[clean]`）

每查询跑批输出 `SUMMARY`：`appended = 总行数/总行数` 且
`serialize_failed / dropped_late / memory_evicted / cursor_gap = 0` → **clean**。
这是「无丢失」的完整性权威。

### 5.3 输出计数（EMIT）口径

- **30M**：与 ground truth 逐位比对（权威；`wfgen verify-nexmark` 覆盖全部规则）。
- **100M**：EMIT 与 30M **同比例侧证**（历史例：q2=747,816 ≈ 30M 占比 0.8129%；
  q9=6,000,000 = 1.8M × 100/30）。
- **特殊口径查询**（q11 per-shard 会话 / q12 处理时间近似 / q13 形状对齐 / Q19 stats
  oracle 未接入）：以多轮端到端 EMIT 确定性 + `[clean]` 验证，已知差异见
  `CAPABILITY_GAP_MATRIX.md` §一·§二（q21 已随 `channel_id` 对齐由 verify 覆盖）。
- **新旧二进制回归**：Q2/Q3/Q7/Q9 必须逐位一致；Q4/Q5 在**既存波动带**内（区间重叠
  而非单值相等）。

### 5.4 深验（当前）

`bench.sh <q> replay 30m --verify`：`wfgen verify-nexmark --engine-emit data` 用真实
规则引擎逐规则对拍引擎 EMIT（git-diff 同款分层：L1 哈希 → L2 Myers/降级 → L3 明细），
退出码 0=一致 / 1=有差异（Q19 stats oracle 未接入 = known-diff ⚠ 不判失败）。逐 alert 明细对拍
（旧 28k 探针 `alerts.ndjson` 方案）随引擎 sink 改造已不再产生该文件，由计数级对拍替代。

### 5.5 已知波动（正确性的诚实边界）

- **Q4**（avg-of-max 双规则链）：内层 deferred reduce maxrow（Q9 同款）确定性；外层
  stats avg 的 oracle 对拍待接入（known-diff，见 `CAPABILITY_GAP_MATRIX.md` §一 Q4）。
- **Q5**（HOP + conv top_ties）：窗口形状/基数与权威一致（30M 1500 窗 vs 旧 fixed 300 桶，
  5× 修正）；EMIT 除 scan_timeouts 墙钟级微差外无已知波动带。
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

## 6. q1 内存调查结论（2026-08-20 定案；排查链细节已入 git 历史）

**根因**：窗口持有量 = 规则 ack 进度 × 内存驱逐（`max_window_bytes`）。多连接 key 分片注入
使规则 ack 慢 → 窗口堆积 → floor-respecting 驱逐无法清；单连接整文件推 → ack 快 → 窗口压到 cap。

**定案**：
- **`gen-nexmark` 默认按事件时间排序输出**（30s 桶 + 桶内排序，内存有界，事件集合不变，
  `--no-sort` 保留旧 phase-major）。
- **`bench.sh` 默认单连接**（`CONNECTIONS=1`、`SHARD_KEYS` 空）+ 帧缓存带 `DATA_VER`（默认
  v2）指纹，旧乱序缓存自动失效。
- **窗口字节预算按查询显式配置**（`models/schemas/windows.toml`：bid_events 256MB，
  2026-08-23，远端 `over_cap` 保留）——**有状态/join 查询的 join 目标窗口数据不可调小**，
  否则被驱逐破坏正确性。
- 多连接（key 分片）仅在有状态负载需要键闭包时使用；引用内存数字必须标注注入方式与 DATA_VER。
- macOS `ps rss` **高估**物理占用（含 swap 出/空 zone 页），`vmmap` 的 `Physical footprint` 更准。
