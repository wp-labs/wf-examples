# NEXMark 基准：背景、查询、数据与正确性标准

> 配套文档：
> - `../README.md` —— 套件结构、bench.sh 用法、测量纪律
> - `BENCH_RESULTS.md` —— 基准实测结果归档（按跑批日期分节）
> - `CAPABILITY_GAP_MATRIX.md` —— 22 查询逐条能力/语义判定（当前权威）
>
> 度量口径（EPS/RSS/CPU/正确性）已并入本文 §7（原 TEST_PLAN.md）。

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
见 `CAPABILITY_GAP_MATRIX.md` §一（21 已有 / Q12 待补强 / Q6 特殊口径·Flink 官方未实现），
执行器应用与语义偏差详见 `SEMANTIC_ALIGNMENT.md`（§4 对齐状态表 / §8 执行器矩阵）。

**能力面覆盖**：on-each / 过滤 / snapshot·deferred·asof join / fixed·sliding·session·hop
窗口 / distinct / conv top-N·top_ties / stats<>（count/sum/avg/min/max/distinct/last/top）/ `1d:fixed` 日历天桶。

---

## 3. 数据有什么要求

NEXMark 标准数据的要求，本实现（`wfgen gen-nexmark`）全部满足：

1. **三流结构**：person/auction/bid 各字段与 NEXMark 事件模型一致（见 `../models/schemas/nexmark.wfs`），
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
1. 生成        wfgen gen-nexmark <N>                     → ../data/burst_bench.jsonl（N 条 JSONL 事件流）
2. 预编码帧    wfgen dump-frames --scenario nexmark.wfg   → ../data/bench_<N>[_mb<bytes>].frames
                   --input burst_bench.jsonl --max-frame-bytes <MAX_FRAME_BYTES>（默认 8MiB）
                   （事件流编码成 Arrow IPC 帧文件，跨查询复用，存在即不重生成）
3. 按键分片（仅 CONNECTIONS>1）：
               wfgen shard-frames --shards <C> --shard-keys "bid_events:auction,auction_events:id,person_events:id"
                   → ../data/shard_<N>_c<C>_k<md5>.frames（同 key 同分片，键闭包）
4. 回放        send-arrow --shard-files …（CONNECTIONS 条 TCP 连接并发推，每连接一份分片）
                   → 引擎 daemon（../conf/wfusion.toml，端口 9800）
```

**要点**：
- **帧文件缓存**：`../data/bench_<total>.frames` 跨查询/跨跑复用（预编码避免每次重复生成）。
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
- oracle 的完整定义（处理流程 / 计数·内容·字段级三档验证 / 排除与边界 / known 差异）
  见 `ORACLE_VERIFY.md`。
- 覆盖全部规则（含 q1 on-each / q11/q12/q14/q22 等早期模拟器未建模项）。
- **已知差异**：Q12（fixed+close 尾桶收口，引擎实现面非确定，标 ⚠ 不判失败）；q21 已随
  数据侧 `channel_id` 对齐（官方 95% 输出量）由 verify 覆盖。其余规则与引擎 EMIT 精确相等
  （stats 规则 2026-08-27 起接入 oracle：q4b/q15~q19 10M 对拍逐条 identical）。
- 对拍在 wfgen 内完成（`--engine-emit data`）：git-diff 同款分层（L1 哈希 →
  L2 Myers/降级 → L3 明细），退出码 0=一致 / 1=有差异。

**各查询 30M 期望 / 实测 EMIT 状态**（Q1~Q22 全量 30M replay 全部 `[clean]`；
Q8/Q9 已与 oracle 对拍一致，Q19 等 stats 规则已随 oracle stats 接入（2026-08-27）覆盖）：
见 `CAPABILITY_GAP_MATRIX.md` §一·§二（含已解决历史与 known-diff 登记）。

### 5.2 数据完整性（`[clean]`）

每查询跑批输出 `SUMMARY`：`appended = 总行数/总行数` 且
`append_failed / dropped_late / memory_evicted / cursor_gap = 0` → **clean**。
这是「无丢失」的完整性权威。

### 5.3 输出计数（EMIT）口径

- **30M**：与 ground truth 逐位比对（权威；`wfgen verify-nexmark` 覆盖全部规则）。
- **100M**：EMIT 与 30M **同比例侧证**（历史例：q2=747,816 ≈ 30M 占比 0.8129%；
  q9=6,000,000 = 1.8M × 100/30）。
- **特殊口径查询**（q11 per-shard 会话 / q12 处理时间近似 / q13 形状对齐）：以多轮
  端到端 EMIT 确定性 + `[clean]` 验证，已知差异见 `CAPABILITY_GAP_MATRIX.md`
  §一·§二（q21 已随 `channel_id` 对齐由 verify 覆盖；stats 规则已由 oracle 覆盖）。
- **新旧二进制回归**：Q2/Q3/Q7/Q9 必须逐位一致；Q4/Q5 在**既存波动带**内（区间重叠
  而非单值相等）。

### 5.4 深验（当前）

`bench.sh <q> replay 30m --verify`：`wfgen verify-nexmark --engine-emit data` 用真实
规则引擎逐规则对拍引擎 EMIT（git-diff 同款分层：L1 哈希 → L2 Myers/降级 → L3 明细），
退出码 0=一致 / 1=有差异（Q12 fixed+close 尾桶 known-diff ⚠ 不判失败；stats 规则
2026-08-27 起 oracle 已覆盖）。逐 alert 明细对拍
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
- **窗口字节预算按查询显式配置**（`../models/schemas/windows.toml`：bid_events 256MB，
  2026-08-23，远端 `over_cap` 保留）——**有状态/join 查询的 join 目标窗口数据不可调小**，
  否则被驱逐破坏正确性。
- 多连接（key 分片）仅在有状态负载需要键闭包时使用；引用内存数字必须标注注入方式与 DATA_VER。
- macOS `ps rss` **高估**物理占用（含 swap 出/空 zone 页），`vmmap` 的 `Physical footprint` 更准。

---

## 7. 度量口径（原 TEST_PLAN.md）

> 本文回答「bench 结果每一列是怎么测出来的、口径是什么、什么情况下不可信」。
> 代码事实源：`../bench.sh`（驱动）、`../scripts/bench_lib.py`（度量工具库）、
> `wfgen` 的 `cmd_frames`/`cmd_stream`/`cmd_perf_diag`（哨兵帧生成）、
> wf-runtime `perf_diag.rs`（哨兵落盘）。改口径先改这里，再同步本文。

### 7.0 口径一览

| 指标 | 主口径 | 备用口径 | 说明 |
|---|---|---|---|
| **EPS** | 哨兵四元组 `Σn/(max_emit−min_start)` | metrics-append / TIMEOUT 兑底 | 引擎侧精确墙钟窗，无轮询粒度误差 |
| **RSS_peak** | 采样峰值（100ms，全生命周期） | — | `ps rss`，macOS footprint 回退 |
| **CPU avg/max** | 哨兵活跃窗内样本（核占数，可 >100%） | 无哨兵时 [T0,T2] / 全样本 | 100ms cputime 差分 |
| **正确性** | `wfgen verify-nexmark` oracle 对拍 | — | 逐规则 EMIT 计数一致 + known-diff 清单 |

**读数第一条**：先看结果行的 `eps_mode=`——`sentinel` = 精确口径；`metrics-append` /
`⚠TIMEOUT` = 兑底值，只作量级参考。

### 7.1 EPS 统计口径（主：哨兵四元组）

链路：客户端推事件流末尾追加哨兵帧 `{round, n, start_ns}`（`start_ns`=该连接开始发送的
墙钟 epoch ns，`n`=实际发送行数）→ daemon 以 `--perf-diag conf/perf-diag.toml` 启动，注册
保留窗口 `__wf_sentinel`（门控全 false 零开销）→ 数据窗排空后补 `emit_ns`，四元组经 alert
链落盘 `data/perf_sentinel.ndjson` → `bench_lib.py sentinel_tuple()` 聚合
`Σn/(max_emit−min_start)`（多连接取 min_start/max_emit 覆盖整批）。

- **关键性质**：start/emit 都是墙钟（与事件时间无关）；EPS = 引擎**消化速率**（整轮均值）；
  replay **不限速**（`rate=3M/s` 只作用于 stream）；短跑可信（引擎写盘即完成信号，无 metrics
  1s 轮询粒度误差）；哨兵窗口零性能影响。
- **SENT_N 与哨兵 n**：单连接 = TOTAL_N；多连接 raw = TOTAL_N × CONNECTIONS；分片 =
  TOTAL_N。哨兵 n 是客户端实际发送行数，Σn 天然 = SENT_N；`appended`（`append_total` 求和）作旁证。
- **兑底口径**（哨兵缺失时）：① metrics-append：`engine_appended`（三输入流 `append_total` 求和）
  ≥ TOTAL_N 且 `engine_acked_lag` = 0（含 q13 中间管道窗口 bid_mod/auction_finals）→
  `EPS = APP/(T2−T0)`；② TIMEOUT：超过 `MAX_SEC`（replay：`TOTAL_N/100000 + 600`；stream：
  `TOTAL_N/RATE×3 + 60`）→ 结果行打 `⚠TIMEOUT`。哨兵缺失最常见根因：daemon 未带
  `--perf-diag`、引擎卡死、持续能力 < 目标速率。

### 7.2 RSS_peak 口径

`rss-sampler`（`bench_lib.py`）每 100ms 读 `ps -o rss=,cputime=`（macOS 拒读时回退
`footprint`）。`RSS_peak` = 全部样本峰值。快查询的短暂峰值可能落在采样网格之间被低估
（保守方向）。

### 7.3 CPU avg/max 口径

- 瞬时 CPU = cputime 差分 / 墙钟差分 × 100；单位是**核占数**（多核可 >100%）。
- 只统计引擎活跃窗 `[sentinel start_ns − 0.5s, sentinel emit_ns + 0.5s]` 内样本：剔除 daemon
  启动/等流/收尾空闲稀释。无哨兵退回 `[T0, T2]`，窗内无样本再回退全样本。
- **基线前置**：采样器首 tick 只初始化基线，bench 在启动客户端前等首个差分
  （`wait_sampler_baseline`）——否则亚秒级突发（q2/q8 ≈ 0.4s）在首个差分前烧完，CPU 恒报
  0%（实测复现，确定性失败）。
- 短跑（<2s 活跃窗）读数只宜作量级参考；全 0 样本时 max 报 `0` 而非 `n/a`（历史 bug 已修）。

### 7.4 正确性验证口径（verify-nexmark）

- **oracle = 真实 WFL 规则引擎**逐事件求值（非性能引擎）：规则按 yield-bind 依赖并查集分组，
  每组一线程独立吃完整事件流（非分片，慢是预期的）。
- 对拍：归一化两侧为 `规则名 计数` 文本行（Myers 对齐），git-diff 式逐规则报告；退出码
  0=一致 / 1=有差异。`bench.sh --verify` 串接同一对拍。
- **known-diff（对拍时已知，非回归）**：Q12 fixed+close 尾桶收口（10M oracle=102,400 vs
  引擎=282,514）；30M 按 5% 容差；stats 规则 fixed 窗口（q4b/q15~q19）2026-08-27 起接入
  oracle，session/sliding stats 仍不接入；all-10m verify 的 oracle 工作集 ~19GB，stats 规则
  建议单查询 10m 对拍 + all 用 1m 端到端。
- 引擎侧取 oracle 覆盖的规则，单查询验证时其它查询残留 EMIT 是历史噪音，不计入。

### 7.5 测量纪律（执行版）

1. **先看 `eps_mode=` 再读数字**：非 sentinel 的 EPS/CPU 只作量级参考。
2. **预热轮**：stash 重建后首跑系统性偏低（曾三次复现），`WARMUP=1` 剔除。
3. **A/B 必须不限速**：`RATE` 会把 EPS 封顶（限速 = 测供给不是引擎）。
4. **同时段交错对比**：bench 机 EPS 与 RSS_peak 双峰相位强相关（同配置差 ±8%），结论按
   RSS 相位配对；单轮数字只作量级参考。
5. **引用 RSS 标注 `parse_buffer_bytes`**：默认 128MB 与 2GB 预算的 EPS/RSS 不可直接对等。
6. **Linux 探测**：核数走 `nproc`、loadavg 走 `/proc/loadavg`；采样走 `ps`（权限被拒时
   Linux 无 footprint 回退 → 样本为空，报 n/a）。

### 7.6 复现命令

```sh
./bench.sh all replay 10m          # 22 查询全量吞吐 + RSS + CPU（哨兵 EPS 口径）
./bench.sh all replay 10m --verify # 同上 + 每查询 oracle 对拍（~40min 量级）
./bench.sh q2 replay 10m           # 单查询
./diag.sh q5 10m                   # 性能墙定位（六档墙梯 + 每段 CPU/RSS）
python3 scripts/extract_emitted.py data/metrics.ndjson  # 消费侧计数器
```
