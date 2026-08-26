# NEXMark 100M 内存问题清单（RSS > 10GB 判定）

> 判定标准（2026-08-25 用户定调）：**RSS_peak > 10GB 即为内存问题**。
> 基线：`./bench.sh all replay 100m`（over=30m 配置、哨兵 EPS、conns=1、p=10 r=10）。
> 每项解决后更新状态与实测数据。逐项解决（不要并行改配置，避免数据交叉污染）。

## 实测（2026-08-26 17:25-17:27 全量单独确认，采样器已修复覆盖 close flush）

> 批内超红线项全部单独跑确认完毕（铁律：批内 RSS 受前序 allocator 残留污染，必须单独确认）。
> **真实超标清单 = q4 / q16 / q17 / q18**，其余为批内污染。

| 查询 | 批内 100M RSS | 单独 100M RSS | 判定 | 状态 |
|------|------|------|------|------|
| q4 | 18,574MB | **14,045MB** | 🔴 真实超标 | ⚠ M-04 新增 |
| q5 | 20,294MB | 7,918MB（10:54） | ✅ 批内污染 | — |
| q13 | 14,488MB | 3,792MB（10:43） | ✅ 批内污染 | — |
| q14 | 11,613MB | **3,992MB** | ✅ 批内污染 | — |
| q16 | 18,880MB | **16,187MB** | 🔴 真实超标 | ⚠ M-16 |
| q17 | 27,202MB | **26,185MB** | 🔴 真实超标 | ⚠ M-17 |
| q18 | 39,730MB | **40,787/37,755/40,566MB** | 🔴 真实超标 | ⚠ M-18 |
| q19 | 23,492MB | 7,066MB / **8,002MB** | ✅ 达标（批内污染） | — |
| q22 | 27,048MB | 3,950/24,490/8,938 → **4,177MB**（修后） | ✅ **已解决** | — |
| q11 | EPS 2.5M | 16.2M / 4,024MB | ✅ 批内负载干扰 | — |

**批内污染量级参考**：q18 39.7G 之后 q19 报 23.5G、q22 报 27.0G（单独均 ~4-7G）——
前序 close flush 期 allocator 残留可污染后续 2-3 个查询。

---

## 实测（2026-08-26 17:11 全量批内 + 17:2x 单独确认，采样器已修复覆盖 close flush）

> ⚠ **批内污染警示**：批内 100M 全量跑批的 RSS_peak 受前序查询 allocator 残留 + close flush
> 期叠加影响，**超红线项必须单独跑确认**（铁律，q19 23.5G→单独 7.1G 即为此类）。

| 查询 | 批内 100M RSS | 单独 100M RSS | 状态 |
|------|------|------|------|
| q1 | 4,812MB | — | ✅ 达标 |
| q2 | 4,254MB | — | ✅ 达标 |
| q3 | 5,721MB | — | ✅ 达标 |
| q4 | **18,574MB** | 待跑 | ⚠ 待确认 |
| q5 | **20,294MB** | **7,918MB**（08-26 10:54） | ✅ 达标（批内污染） |
| q7 | 4,061MB | — | ✅ 达标 |
| q8 | 5,210MB | — | ✅ 达标 |
| q9 | 7,609MB | — | ✅ 达标 |
| q10 | 4,347MB | — | ✅ 达标 |
| q11 | 4,245MB（EPS 2.5M 异常） | **16.2M EPS / 4,024MB** | ✅ 批内负载干扰 |
| q12 | 4,040MB | — | ✅ 达标 |
| q13 | **14,488MB** | **3,792MB**（08-26 10:43） | ✅ 达标（批内污染） |
| q14 | **11,613MB** | 待跑 | ⚠ 待确认 |
| q15 | 5,604MB | — | ✅ 达标 |
| q16 | **18,880MB** | 待跑 | ⚠ 待确认 |
| q17 | **27,202MB** | 待跑 | ⚠ 待确认 |
| **q18** | **39,730MB** | **40,787/37,755MB** | 🔴 **M-18 真实超标 ~38-41G** |
| q19 | **23,492MB** | **7,066MB** | ✅ 达标（批内污染） |
| q20 | 7,794MB | — | ✅ 达标 |
| q21 | 4,083MB | — | ✅ 达标 |
| q22 | **27,048MB** | 9,986MB（08-26 11:09） | ⚠ 边缘待确认 |

---

## 实测（2026-08-25 12:29-12:42，修复后 over=30m 全量）

| 查询 | EPS | RSS_peak | evict | 状态 |
|------|-----|----------|-------|------|
| q1 | 22.0M | 5,411MB | 2272 | ✅ 达标 |
| q2 | 25.8M | 5,140MB | 2272 | ✅ 达标 |
| q3 | 23.7M | 6,027MB | 2272 | ✅ 达标 |
| q4 | 10.3M | 8,680MB | 2272 | ✅ 达标（本次修复后） |
| **q5** | 5.5M | **17,472MB** | 226 | ⚠ M-05 |
| q6 | 3.0M | 3,586MB | 415 | ✅ 达标（metrics-append 口径） |
| q7 | 15.5M | 5,084MB | 2272 | ✅ 达标 |
| q8 | 25.1M | 6,224MB | 2272 | ✅ 达标 |
| q9 | 10.7M | 8,348MB | 2272 | ✅ 达标（本次修复后） |
| q10 | 21.3M | 5,615MB | 2272 | ✅ 达标 |
| q11 | 19.9M | 4,947MB | 2272 | ✅ 达标 |
| q12 | 21.1M | 5,058MB | 2272 | ✅ 达标 |
| **q13** | 390k | **27,100MB** | 441 | 🔴 M-13（**丢数据**：memory_evicted_total=1479，跑批作废） |
| **q14** | 17.7M | **18,047MB** | 1232 | ⚠ M-14 |
| q15 | 16.1M | 9,606MB | 1690 | ✅ 达标（边缘） |
| **q16** | 8.3M | **22,923MB** | 239 | ⚠ M-16 |
| **q17** | 14.3M | **14,856MB** | 2272 | ⚠ M-17 |
| **q18** | 9.4M | **24,182MB** | 2272 | ⚠ M-18 |
| **q19** | 4.3M | **32,865MB** | 448 | ⚠ M-19 |
| q20 | 22.0M | 8,460MB | 2272 | ✅ 达标 |
| q21 | 18.9M | 5,243MB | 2272 | ✅ 达标 |
| **q22** | 6.2M | **29,979MB** | 125 | ⚠ M-22 |

---

## 🔴 M-13 — q13 中间窗积压 + 驱逐丢数据（最严重，优先）

- **RSS 27.1GB + `memory_evicted_total=1479`（正确性被破坏，跑批作废）**
- 机制：双规则链。q13a `on each b` → 中间窗 `bid_mod`（92M 行）；q13b 读 bid_mod + `join side_input snapshot`（EPS 仅 390k，消费慢）→ 中间窗积压超 2GB → 内存驱逐丢**未读**数据。
- 疑点：中间窗（pipe 机制）的内存驱逐应尊重消费者 ack floor（未读不驱逐）——为什么丢？需查 bid_mod 窗的驱逐语义 + q13b 消费滞后。
- 修复方向：① 引擎级——中间窗驱逐尊重未读（若未生效）；② 规则级——q13b 加速（snapshot join 静态表每行查，是否可批量/索引化）。
- 状态：未解决。

### ✅ 2026-08-25 修复进展（本 session 落地，分 4 步）

1. **丢数据定案：1479 次驱逐是误报**（`memory_evicted_total` 全部是已读回收，未读保护 `min_acked` 契约正常——双链测试钉死）。q13 真问题是慢 + 内存，不是丢。
2. **q13b 分片（push round-robin）**：2026-08-23 的保守规则「bind 中间窗的 each 强制单 worker」在 push 模式下没有 pull 游标竞态——广播带真实窗口 seq、每批恰一次投递到唯一 shard。`spawn.rs` 放开 `consumes_intermediate`（要求非 deferred 且目标不被 Match 消费）。30M：EPS 400k→642k，RSS 15.4GB→9.1GB；100M：EPS 390k→630k，RSS 27.1GB→14.5GB。输出完整（27.6M/30M = oracle）。
3. **卡尾 9 批根因 = ack 语义**（`max_acked` 修复）：round-robin 分片下每个 shard 只 ack 自己的批次（`seq % N == shard`），`min_acked` 恒停在最慢 shard 的最后一批 → 哨兵排空判定（`min_acked >= next_seq`）永不成立 → 哨兵永不触发 → bench 挂死等超时（10M/30M/100M 都复现）。修复：`WindowProgress::max_acked()`（完成信号）+ 哨兵 `wait_for_data_drain` 判定 + `acked_lag` 指标全部改 max；驱逐保护**仍用 min_acked**（未读不驱逐）。
4. **q13a 分片（生产者分片放开）**：yield 中间窗的 each 规则当 target 不被 Match 消费时允许分片（q13b 已 stateless 分片，保序不再必要）；`process_push` ack 改 `fetch_max`（乱序广播下单调）。100M：**EPS 1.04M（2.7×）、CPU 10.9 核全并行**。

### ⚠ 遗留（2026-08-25 未解决）

- **RSS 41.5GB（100M，q13a 分片后）**：`window_bytes` 峰值 20.5GB —— ingest 期间（~33s）事件时间跨度仅 ~10min < over=30m，`bid_events` 时间驱逐不触发 → 100M 行全量驻留（~20GB），drain 后才回落到 30m 稳态 3.8GB。**这是「over 语义 × ingest 速度」的固有窗口内容，不是泄漏**——但 RSS_peak 超 10GB 目标。候选：ingest 阶段放宽（时间驱逐按 wall-clock？No——事件时间语义）或接受 peak（稳态有界）。
- **`memory_evicted_total=188` 仍非零**（100M q13a 分片后）：bench 作废判定触发。需确认驱逐全部是已读回收（预期是）→ bench 判定对该场景是否应豁免。
- **q13a 分片 pull 的 ack 语义隐患**：`pull_and_advance` 分片下 ack 的是**读位置**（`new_cursor`=全部批次）而非处理位置——`min_acked` 追平 → `bid_events` 驱逐无未读保护 → cap 驱逐可能删「其他 shard 还没处理」的批次。q13a 分片后消费快（未实测触发），但语义上存在竞态；修复方向：分片 pull 的 ack 改为只推进「自己份额处理完的连续位置」或驱逐 floor 按归属计算。**（已修：`q13_sharded_pull_acks_processed_not_read_position` 钉死——分片 pull ack 改处理位置，2026-08-25）**

### ✅ 2026-08-25 追加：内存控制达成（100M RSS 20.3→6.7GB）

**根因定案（非 mimalloc）**：100M window_bytes 峰值 21.5GB ≈ RSS 20.3GB——**窗口本身**。
ingest 3M/s 33s 内事件时间跨度仅 ~10min < over=30m（时间驱逐不触发）；q13a 消费
630k/s 跟不上 ingest → `min_acked` 保护挡住 cap 驱逐 → bid_events 全量驻留 20GB。
30M 全量仅 6GB 所以不炸（RSS 9.2GB）。

**控制组合（windows.toml 配置，全部正确性无损——驱逐受未读保护，EMIT 完整）**：
- `max_total_bytes = "2GB"`（全局窗口 cap，20GB→8GB→6GB→4GB→2GB 逐档验证）
- `evict_interval = "200ms"`（1s→200ms，背压滞后大降：窗口峰值 8.4→~3GB）
- `bid_mod max_window_bytes = "512MB"`（2GB→512MB，round-robin 分片下 min_acked
  保守 floor 允许的已读滞留从 2GB 降到 512MB）
- **memory_evicted 判定修复**（bench_lib.py）：`memory_evicted_total` 从致命计数器
  移除——min_acked/retention-pin 保护下驱逐只回收已读/已广播批次，真丢未读信号
  是 `cursor_gap`（保留致命）。背压/字节 cap 下驱逐是常态（2000+ 次），非零不
  表示正确性受损。

**实测（哨兵 EPS，本地 mac）**：
| 规模 | 修前（20GB cap） | 修后（2GB cap + 200ms） |
|------|------|------|
| 30M | 400k / 15.4GB | 648k / **6.05GB** ✓ |
| 100M | 390k / 27.1GB | 643k / **6.73GB** ✓ |
| 100M EMIT | — | q13a 92M / q13b 92M（= oracle）✓ clean |

**性能瓶颈（已解，见下）**：EPS ~640k = q13a 单 worker row path（1.6µs/行，
已接近 executor 1.19µs）。q13a 生产 630k/s < ingest 3M/s → bid_events 驻留靠
背压控制。**追平 ingest 需 q13a 列式化/批量 staging**（bench 数据 3.4×，仍不够
追平——需 mod BinOp 列式 10×）。q13a 分片曾试过（EPS 1.04M）但 10 核 row path
分配让 RSS 40GB——**回退**（内存优先）。

**⚠ 全局配置影响**：`max_total_bytes=2GB` + `evict_interval=200ms` 是全局的——
q5/q16/q18 等 stats 大窗口查询会被背压（变慢，RSS 降低），需全量验证后定案。

### ✅ 2026-08-25 追加：q13a 列式化（row path 1229ns → pipe 列式 203ns/行，6.1×）

q13a 中间窗生产路径列式化（详见 wp-reactor notes「q13a 列式化」节）：
- `each_batch_prepare` 编译范围扩到任意 `expr_is_columnar`（`%` BinOp → 批级 cvec）；
- 新 `each_pipe_columnar_safe` 门控（保守形状）+ `execute_each_pipe_batch_columnar`
  （零 Event/OutputRecord 物化）+ `PipeBatchStager::new_columnar/push_row`（列来源
  计划，免每列名字查找）；
- **对拍测试**钉死与行式路径字节一致（含 `__wfu_meta_*` 与 `__wf_pipe_ts` 列）。

**bench（cargo test --release -p wf-runtime q13a_pipe_columnar_bench -- --ignored
--nocapture）**：pipe 列式 203.4ns/行 = 4.92M/s 单 worker > ingest 3M/s →
q13a 消费可追平摄入，bid_events 不再靠背压驻留。为「生产者分片放开」二次评估
提供依据（列式化分配量级大降，mimalloc arena 膨胀风险缓解）。

### ✅ 2026-08-25 追加（本 session 已修）：分片 pull ack 改处理位置 + q13 性能 bench 定位

- **分片 pull ack 改处理位置（已修 + 测试）**：`pull_and_advance` 对 whole-batch round-robin 分片改 ack
  **最后处理批次 + 1**（非分片/key-partitioned 保持读位置）。`fetch_max` 单调。测试
  `q13_sharded_pull_acks_processed_not_read_position`：shard0 处理批 0,2 → ack=3（非 4）；
  min_acked=3（未处理份额受保护）、max_acked=4（完成信号=next_seq）。
  ⚠ **代价**：驱逐 floor 变保守 → q13a 分片下 `bid_events` 驻留增大（30M window_bytes 峰值 8.7GB，
  有界但 RSS 峰值问题加剧）。
- **q13 性能瓶颈数据（cargo bench，非猜测）**：`crates/wf-runtime/src/engine_task/rule_task_bench.rs`
  （`cargo test --release -p wf-runtime q13a_pipe_bench -- --ignored --nocapture`）：
  - q13a 生产路径（row path）：物化 236ns + per-record 500ns + stage 452ns = **1.19µs/行**（单核 0.84M/s）；
    批量路径（execute_each_direct_batch）348ns → **3.4×**。
  - q13b 生产路径（row path：Event clone + join 命中 + fmt）：**1.18µs/行**（单核 0.85M/s）。
  - **生产峰值速率**：q13a 3.3M/s、q13b 2.7M/s（10 核）——q13a 被 q13b 反压，**最终瓶颈 = q13b
    row path**（理论 8.5M/s vs 峰值 2.7M/s，~3× 并发损耗）。
  - q13b_join_bench 只测了列式（462ns）——**生产 q13b 走 row path**（`fmt("{}", side_input.value)`
    在 live join 下 columnar gate 拒绝）→ bench 未覆盖生产路径，本段补齐。
  - **RSS 40GB（30M/100M）主因 = mimalloc arena 膨胀**：window_bytes 峰值仅 8.7GB（有界），窗外
    ~31GB 是 10 核 row path 高频分配（Event clone/OutputRecord/String）后 mimalloc 不归还。
    q1/q2 列式路径分配少 → RSS 正常（2.5-4GB），支持该假设。
- **优化方向（数据支撑）**：q13a/q13b 列式化（mod BinOp 列式 + intermediate 批量 staging + 列式
  join 支持 fmt 右窗字段）→ 3.4× 且分配量级大降（RSS 回落）。或编译器层 `fmt("{}", x)` 恒等消除
  （q13b 直接走 q13b_join_bench 的 462ns 列式路径）。
- **q1 回归验证**（ack 改处理位置后）：10M 21.2M/s、RSS 2.5GB、clean ✓（分片 pull 完成判定不受影响）。

### 测试（本 session 新增，全部通过）

- `q13_dual_chain_sharded_push_consumption_complete`：3 shard push round-robin 消费完整 + 未读保护 + 内存有界
- `q13_dual_chain_sharded_push_high_slope_repro`：10 shard 紧通道（cap=2）背压下 70 批全量输出（复现斜率场景）
- `q13_dual_chain_sharded_producer_and_consumer`：2 生产者 shard 乱序广播 + 3 消费者 shard，60/60 完整
- `max_acked_tracks_completion_across_shards`（wf-engine）：min 停滞 / max 追平 next_seq 的语义锁
- 回归：wf-runtime 540 / wf-engine 1137 / clippy 0

### 代码改动（未提交）

- `wf-engine/src/window/progress.rs`：+`max_acked()`
- `wf-engine/src/window/buffer/cursor.rs`：无（ack 语义在 runtime）
- `wf-runtime/src/perf_diag.rs`：哨兵排空判定 min→max
- `wf-runtime/src/metrics/sampling.rs`：`acked_lag` 指标 min→max（无消费者窗口保持 trivially drained）
- `wf-runtime/src/engine_task/rule_task.rs`：`process_push` ack 改 `fetch_max`
- `wf-runtime/src/lifecycle/spawn.rs`：q13b 消费中间窗分片 + q13a 生产中间窗分片（均要求非 deferred 且 target 不被 Match 消费）
- `wf-runtime/src/engine_task/deferred_integration_tests.rs`：+3 测试

## ⚠ M-04 — q4 双规则链中间窗（新增 2026-08-26）

- RSS 14.0GB（单独 100M 确认，批内 18.6G）；EPS 5.5M；evict=339。
- 机制：q4a deferred reduce maxrow → 中间窗 `auction_finals`（over=30m，每 auction 一条
  最终价）→ q4b stats avg。100M 下 bid_events over=30m 语义保留 + auction_finals 中间窗
  + q4a/q4b 双规则链各自状态。
- 对照：q9（同类 deferred reduce）100M 7.6G 达标——q4 多出的是 q4b stats 链 + 中间窗。
- 状态：未解决。

## ⚠ M-05 — q5 hop 重叠窗状态累积

- RSS 17.5GB（30M 6.6GB，2.6×）；evict=226（驱逐少）。
- 机制：`match<auction:hop(10s,2s)>` 5 窗重叠 + `conv { sort(-n) | top_ties(1) }`；`limits { max_memory = "512MB" }` **未生效**（hop 窗状态是否时间驱逐待查）。
- 修复方向：hop/conv 窗的 over/驱逐配置；limits 为什么没拦住。
- 状态：未解决。

## ✅ M-14 — q14 输出积压（已解决 2026-08-26：批内污染）

- 单独 100M = **3,992MB** 达标；批内 11.6G 为前序 q13 残留污染。
- 之前 08-25 批内 18.0G 亦同因（前序 q13 27.1G 残留）。无状态 on each 本身无内存问题。

## ⚠ M-16 — q16 distinct_count 精确集合

- RSS 16.2GB（**单独 100M 确认**，批内 18.9G；08-25 批内 22.9G）；EPS 8.0M；evict=360。
- 机制：stats 1d group by channel + **12 个 `distinct_count` 度量**（total/r1/r2/r3 × bidder/auction）→ 每 channel 维护精确元素集合（100M 下每集合 ~30 万元素）。
- 修复方向：distinct_count 近似化（HLL）或确认 limits 语义。
- 状态：未解决。

## ⚠ M-17 — q17 stats 1d 大键基数

- RSS 26.2GB（**单独 100M 确认**，批内 27.2G）；EPS 12.7M；evict=2272。
- 机制：stats 1d group by auction（~30 万键）× 每键状态，1d 桶覆盖全流累积到 EOS。
  100M 键数虽远少于 q18（30 万 vs 2935 万），但每键保留量/度量类型不同——待量化。
- 修复方向：规则级（桶更小/减少每键保留）。
- 状态：未解决。

## ⚠ M-18 — q18 stats 1d 最大键基数

- RSS ~38-41GB（**单独 100M 确认**：40.8G / 37.8G / 40.6G，批内 39.7G）；EPS 12.6-15.9M；evict=2272。
- **内存归因（2026-08-26 metrics + diag 墙梯，100M 全量）**：

  | 构成 | 峰值 | 性质 |
  |---|---|---|
  | stats 状态（2935 万键 × 336B） | ~9.8GB | **语义必然**（键数=数据特征，不可减） |
  | bid_events 窗口（字节 cap 生效） | 2.15GB | 受控（evict=283） |
  | auction_events 窗口 | 2.14GB | 180k 行却 2.1G？行均 ~530B 待查 |
  | person_events 窗口 | 0.59GB | 240 万行 |
  | 未归因（帧/parse/allocator） | ~25GB | **待定位** |

  - **alloc 自报 peak_rss = 22.1GB**（引擎 malloc 记账）vs **采样 RSS 40.5G**——差
    ~18.4G 非 malloc（macOS RSS 含页表/映射开销 + alloc 采样可能漏 close 峰值）。
  - 30M 墙梯（MEMORY=1 不预热）：recv/decode 0G + floor 4.4G（窗口）+ rules 5.6G
    （状态 ~3G + 工作态）+ full 0.6G = 10.6G——30M 未归因仅 ~3G。
  - ⚠ **「30M recv 档 DIRTY 9.1G = 帧滞留」是误判**：那是 warmup 残留（非 MEMORY
    墙梯 6 档同数据累积）；MEMORY=1 不预热时 recv=0G。帧读到即丢，不滞留。
  - 100M 未归因 ~25G vs 30M ~3G 呈 **~8× 超线性**（非 3.33×）——q13 文档
    `issues/q13-memory-peak-scales-with-volume.md` 同款规律（内存 ∝ 分配速率 ×
    存活时间，峰值在 close flush 期）。
  - RSS 采样曲线：ingest 期爬升至 ~35G，**峰值 40.5G 在 close flush 期**（非 ingest）。
- **已做**：StatsAccum 紧凑化（208B→24B）、列式直写 close、分块 flush、guard 预算修正。
- **状态：未解决**（语义 9.8G 不可减；~25G 帧/parse 部分待归因）。
- 修复方向：规则级。
- 状态：未解决。

## ✅ M-19 — q19 stats 30m 桶（已解决 2026-08-26：批内污染）

- 单独 100M = **7,066MB** 达标；批内 23.5G 为 q18 残留污染（q18 在批内紧邻 q19 之前）。
- stats 30m 桶滚动释放语义正常。

## ✅ M-22 — q22 纯投影输出链慢 → 输入窗全量驻留（**已解决 2026-08-26 17:58**）

- **根因（cargo bench 量化）**：q22 列式全链 851 ns/evt = q1（212）的 4×。两处热点：
  ① `split_index_vec` 每行全分割 `text.split(sep).collect::<Vec<_>>()` 建 Vec——q22 的
  `concat(mvindex(parts,3/4/5))` 内联后是 **3 个独立 SplitIndex 节点**，同一 url 列被
  分割 3 次（纯 split 就 3×116=348 ns）；② `concat_vec` 每行 `String::new()` 无预分配
  + 逐参 `cscalar_to_value` 的 Value 克隆中转。
- **修复（2 处，字节一致对拍通过）**：
  ① `split_index_vec` 改**惰性 nth 取段**——正索引不建 Vec、只扫描到第 k 段
  （裸 split 116→33 ns，-72%）；负索引 clone 迭代器 count 拿 len 保语义。
  ② `concat_vec` 预分配（按参长度预算 cap）+ Str 参数零拷贝 push（省 Value 克隆中转）。
- **效果（100M 单独跑 2 次）**：EPS 10.0-11.0M → **16.9-17.7M（+60%）**；RSS
  4.0/24.5/8.9G 波动 → **4.0-5.4G 稳定达标**；evict 2272/220/2235 → 2272/2272（驱逐正常）。
  列式全链 851.3 → 461.2 ns/evt（-46%），单 worker 1.17→2.17M evt/s。
- **bench 用例**：`q22_each_split` 新增 split/concat 内部拆解（collect vs nth、
  无预分配 vs 预分配）。

---

## 修复顺序（计划）

1. **M-13** ✅ 已解决（q13 列式化 + 分片 + 内存控制，2026-08-25/26）
2. **M-22** ✅ 已解决（2026-08-26：split 惰性 nth + concat 预分配，EPS +60%，RSS 稳定 4-5.4G）
3. **M-04 / M-16**（中间窗/精确集合 → 先分析成本，再定方案）
4. **M-17 / M-18**（stats 1d 大键基数——键数=数据特征，先定位每键成本，参考 q18 路径）
5. ~~M-05 / M-14 / M-19~~ 已关闭（批内污染，单独达标）

## 验证方式

每项修复后：`./bench.sh qN replay 100m` 单独跑 → RSS < 10GB 且 EMIT 对拍（verify 或 oracle 口径）无误 → 更新本表。

> ⚠ 批内全量跑批的 RSS 不可作为超标判定依据（前序 allocator 残留污染），只作初筛；超标项一律单独跑确认。
