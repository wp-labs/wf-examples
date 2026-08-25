# NEXMark 100M 内存问题清单（RSS > 10GB 判定）

> 判定标准（2026-08-25 用户定调）：**RSS_peak > 10GB 即为内存问题**。
> 基线：`./bench.sh all replay 100m`（over=30m 配置、哨兵 EPS、conns=1、p=10 r=10）。
> 每项解决后更新状态与实测数据。逐项解决（不要并行改配置，避免数据交叉污染）。

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
- **q13a 分片 pull 的 ack 语义隐患**：`pull_and_advance` 分片下 ack 的是**读位置**（`new_cursor`=全部批次）而非处理位置——`min_acked` 追平 → `bid_events` 驱逐无未读保护 → cap 驱逐可能删「其他 shard 还没处理」的批次。q13a 分片后消费快（未实测触发），但语义上存在竞态；修复方向：分片 pull 的 ack 改为只推进「自己份额处理完的连续位置」或驱逐 floor 按归属计算。**下一步优先处理。**

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

## ⚠ M-05 — q5 hop 重叠窗状态累积

- RSS 17.5GB（30M 6.6GB，2.6×）；evict=226（驱逐少）。
- 机制：`match<auction:hop(10s,2s)>` 5 窗重叠 + `conv { sort(-n) | top_ties(1) }`；`limits { max_memory = "512MB" }` **未生效**（hop 窗状态是否时间驱逐待查）。
- 修复方向：hop/conv 窗的 over/驱逐配置；limits 为什么没拦住。
- 状态：未解决。

## ⚠ M-14 — q14 输出积压（无状态 on each）

- RSS 18.0GB。机制：价格过滤后 ~92M 条 alert 输出，detail 长字符串（fmt + count_char），sink 跟不上 → 输出链积压无界。
- 修复方向：引擎级输出背压（emit→sink 通道限流）。
- 状态：未解决。

## ⚠ M-16 — q16 distinct_count 精确集合

- RSS 22.9GB（`limits 8GB` 未拦住）。机制：stats 1d group by channel + **12 个 `distinct_count` 度量**（total/r1/r2/r3 × bidder/auction）→ 每 channel 维护精确元素集合（100M 下每集合 ~30 万元素）。
- 修复方向：distinct_count 近似化（HLL）或确认 limits 语义。
- 状态：未解决。

## ⚠ M-17 — q17 stats 1d 大键基数

- RSS 14.9GB。机制：stats 1d group by auction（~30 万键）× 每键状态，1d 桶覆盖全流累积到 EOS。
- 修复方向：规则级（桶更小/减少每键保留）。
- 状态：未解决。

## ⚠ M-18 — q18 stats 1d 最大键基数

- RSS 24.2GB。机制：stats 1d group by (bidder,auction)（键基数最大）× 4 个 last 度量保留整行。
- 修复方向：规则级。
- 状态：未解决。

## ⚠ M-19 — q19 stats 30m 桶

- RSS 32.9GB（全批最高）。机制：stats 30m group by auction；**fixed 桶到期是否释放待查**（若 5-6 桶全累积 → 键 × 桶数）。
- 修复方向：确认桶滚动释放语义；规则级。
- 状态：未解决。

## ⚠ M-22 — q22 输出积压（纯投影）

- RSS 30.0GB。机制：纯投影输出 92M 条 alert，detail = url 三段字符串（~100B/条）；`limits 512MB` 只管状态不管输出。
- 修复方向：引擎级输出背压（与 M-14 同根）。
- 状态：未解决。

---

## 修复顺序（计划）

1. **M-13**（丢数据，正确性）→ 引擎级中间窗驱逐 + 规则级 q13b 加速
2. **M-05 / M-16**（limits 未生效 → 先查 limits 语义，可能同根）
3. **M-14 / M-22**（输出背压，同根）
4. **M-17 / M-18 / M-19**（规则固有，最后）

## 验证方式

每项修复后：`./bench.sh qN replay 100m` 单独跑 → RSS < 10GB 且 EMIT 对拍（verify 或 oracle 口径）无误 → 更新本表。
