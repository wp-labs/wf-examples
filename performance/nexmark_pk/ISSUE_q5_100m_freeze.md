# Issue: q5 100M 冻结（~69M appends 后 pipeline 停滞）

**状态：** 已定位根因；两项修复已实现（cheap-skip + recalibrate-skip），把冻结点从
~69M 推进到 ~99M+（100M 从"必冻"变成"多数跑通"）；剩余 ~1M 尾部 evict 偶发，见"剩余问题"。
**涉及代码：** `wp-reactor/crates/wf-engine/src/match_engine/match_engine/mod.rs`、
`wp-reactor/crates/wf-runtime/src/engine_task/rule_task.rs`
**benchmark：** `./bench.sh q5 cont 100m`（`wf-examples/performance/nexmark_pk`）

---

## 1. 现象

- q5（`match<auction:10m>` 固定窗口 count 规则，3 条规则 × 10 分片 = 30 个 rule task）
  在 100M 输入下于 **~69M appends** 处冻结：daemon CPU 从 ~116% 掉到 ~25%（几乎全闲），
  append_total 不再增长，bench 等到超时（1600s）仍不完成。
- 此前墙钟封顶修复已把冻结点从 ~22M 推到 ~72M；30M 偶发正常（负载低时 ~8s 完成，
  EPS ~3.4M），负载高时也会冻结在 ~27-29M。
- 100M 下正确性计数器显示 `matches=0, alerts=0`（仅个别早期 match），`sm_delta=+6.7M`
  （每规则 ~2.2M 实例）。

## 2. 根因（修正后的完整图景）

初始判断（scan_expired_at 的 CPU 成本）经实测**不成立**：

| 观测 | 结论 |
|---|---|
| 规则 profile：advance ~610ns/event、scan ~41ns/event，规则 task **99.9% 空闲** | 规则不是 CPU 瓶颈；"rule task 被扫描占住" 不成立 |
| 冻结时所有规则 task 停在 `rx.recv()`（push 通道空），window actor `mailbox=0 pending=0` | pipeline 不是死锁，是"投递停下来 + 数据被丢" |
| `acquire_window_budget` 频繁 `avail=0`（单次 100M 运行 633-713 次） | **window 字节预算（64MB）是硬节流点**：parse worker 拿不到许可就阻塞投递 |
| window 缓冲满 → evict 丢弃；`append_total` 是单调计数器 → 停在天生追不平的某个值 | **丢数据 → 完成条件不可达** |

**链条：**
1. receiver 以 ~5.9M/s 摄入，但 window→规则投递/append 速率是瓶颈（~1M/s 量级）。
2. window 缓冲（`max_window_bytes=64MB` ≈ 800k 行）在规则**短暂停顿**期间填满。
3. 规则停顿的来源之一是 `scan_timeouts` 里 `recalibrate_memory`：每 1s 遍历 ~2.2M
   实例做 `estimated_bytes`（O(实例)），期间规则不消费 push → 30 个 rule channel 填满 →
   window broadcast 阻塞 → 字节预算耗尽 → parse worker 阻塞 → receiver 停发。
4. window 对超预算的旧批次做 evict（drop），被丢弃的 bid 事件**从未 append** →
   `append_total` 永远 < TOTAL → bench 的完成条件（append ≥ 100M）不可满足 → 表现为冻结。

**注意：** `append_total` 是 `fetch_add` 单调计数器，evict 不减。冻结值 < TOTAL 说明那部分
事件是**投递阶段就被丢弃**（receiver 卡预算，未到达 window），而非 append 后被 evict。

## 3. 已实现的修复

### 3.1 cheap-skip 非告警 close（主修复）
`scan_expired_at_impl(skip_non_alerting)`：对**无 close steps** 的规则，实例能否产出告警
可以由 `event_ok`（一个 bool）直接判定，无需构建 `CloseOutput`：

- `And` 模式：qualify = `event_ok && close_ok`，`close_ok` 恒真 → 只需看 `event_ok`
- `Or` 模式：`close_step_data` 为空 → 永不 qualify

q5 这类 count 规则在 100M 下绝大多数过期实例从未达标（`event_ok=false`），`evaluate_close`
（close-steps 求值 + bind 快照 + completed-steps 移动）对它们纯属浪费。跳过时**实例照常
从 `instances`/`estimated_memory_bytes` 移除**，不延迟、不占内存，只省掉昂贵的 close 构建。

新增公开 API（`scan_expired_at` 原契约不变，oracle/测试全兼容）：
- `scan_expired_at_skip_non_alerting`
- `scan_expired_at_with_conv_skip_non_alerting`
rule_task 的 4 个热路径调用点（per-row ×2、scan_timeouts ×2）切到 skip 版本。

### 3.2 recalibrate_memory 超预算跳过
`estimated_memory_bytes >= max_memory_bytes` 时跳过 O(实例) 的每 1s 校准：精确值只会更
大（实例状态只增不减），不可能把节流决策从"超预算"翻回"未超"，跳过语义安全。消除
scan_timeouts 里 ~1s 级的阻塞窗口（这正是把 30 个 rule channel 填满的源头之一）。

### 修复效果（100M，本机实测）
| 版本 | 结果 |
|---|---|
| 修复前 | 冻结在 ~69M（必冻，无法完成） |
| 仅 cheap-skip | 冻结推进到 ~82M，偶发完成（EPS 6.31M） |
| cheap-skip + recalibrate-skip | 100M 完成 2/3 次（EPS 5.46M / 6.31M，SUMMARY clean），1 次卡 99.07M |

正确性：`wf-engine` 503 tests + `wf-runtime` 169 tests 全过；30M 无回归（正常完成时
EPS ~3.4M、EMIT q5_bidcount_10 ~1.7M，与修复前一致）。

## 4. 剩余问题 / 待办

1. **尾部 evict 仍偶发（~1M 事件）**：100M 有 1/3 概率卡在 ~99M，30M 负载高时也可能卡
   ~29M。根因仍是 window 提交（append + broadcast 到 30 个规则）速率在吞吐余量边缘。
   建议下一步从 **window→规则投递路径**入手（`window/fanout.rs` 的 30 路广播、字节预算
   与 evict 的交互），而非 scan_expired_at。
2. **q5 100M 告警被 memory 节流压制**：`max_memory=512MB`（共享预算）在 ~2.1M 实例处
   触发 `Throttle`，后续事件被 drop → count 无法累积 → 100M 下 `q5_bidcount_10` 仅 EMIT
   43（30M 时 1.71M）。这是**配置语义**（内存预算确实触顶），不是丢数，但会让 100M 的
   告警口径与 30M 差异巨大。若期望 100M 告警数与 Flink 对齐，需评估预算/实例基数。
3. **`sm_delta=+6.7M` 与 `estimated_bytes` 口径**：`Instance::estimated_bytes` 未纳入
   部分状态增长时，`recalibrate_memory` 的精确值可能略高于/低于镜像，属已知近似，超预算
   跳过只在"已是上限"时生效，安全。

### 4.1 已实施缓解：rule channel 深度 32 → 256（针对尾部冻结）

**定位（2026-08-19）**：window actor 是单写者，`commit_appended_batch` 内联
`await fanout.broadcast_batch_only` → `join_sends` 对 **30 个 rule channel** 做阻塞广播。
任一条 rule 瞬时停顿（GC / 锁竞争 / 残留 `recalibrate_memory` 扫描，通常 <1s ≈ 73 批）
使其 channel 填满 → actor 的广播卡在该 channel → **所有 window 提交停滞** → mailbox(16)
堆积 → `dispatch_parsed` 阻塞 → 字节预算(64MB) 耗尽 → receiver 停收 → `append_total`
永远追不平 TOTAL → 1600s 超时冻结。冻结呈间发性（~1/3，与机器 RSS/负载双峰相位相关），
因 actor 吞吐偶发跟不上。

**修复**：`wf-runtime/src/lifecycle/spawn.rs` 的 `RULE_CHANNEL_CAPACITY` 32 → 256。
让 actor 在瞬时停顿期间把广播缓冲进较深的 channel，而非被卡死。FIFO 与 window-lookup
一致性完全保留（channel 仍是按序交付）。

**内存权衡**：`RulePush` 持 `Arc<RecordBatch>`，深 channel 会把未消费的批列数据一直保活
（原作者据此把深度封顶在 32，无界 channel 曾致 RSS ~13GB）。256 约吸收 ~3.5s 停顿，最坏
单条滞后 rule 瞬时保活 ~5GB（基线已 ~13.6GB，可接受）；**不处理持续单分片倾斜**（那种需
走 pull 模型 / per-rule 序列器解耦，见下）。

**验证待办**：`cargo check` 已通过；需用户跑 `./bench.sh q5 cont 100m` 多次（或 30M 高负载
复现）确认冻结率从 1/3 下降。建议至少 3 次 100M 取冻结发生频次。

### 4.2 根因级修复方向（未做，需评估语义）

彻底消除"actor 被最慢 rule 卡死"需把广播从 actor 单写者关键路径解耦：rule task 改为从
window log 按游标**拉取**（生产装配的 pull 模式 `run_pull_loop`/`events_since` 目前仅测试
在用），或 per-rule 序列器 + actor fire-and-forget。难点：须保住"rule 处理 N 时窗口含 ≤N"
的 window-lookup 一致性（nexmark 定长窗口 + 按 key 状态实际对此不敏感，但需 EMIT 口径回归
验证）。属更大改动，建议先在 4.1 缓解上验证冻结率，再决定是否推进。

## 5. 复现与诊断工具（供后续使用）

- 复现：`REPO=<warp-fusion 路径> ./bench.sh q5 cont 100m`
- 规则空闲判定：临时在 `scan_timeouts` 加 `last_activity_wall.elapsed() > 5s` 的 warn，
  大量出现即规则停在 `rx.recv()`。
- window 提交判定：临时在 `acquire_window_budget` 打 `available_permits()`，`avail=0` 即
  parse worker 被字节预算卡住。
- 每事件成本：`dump_profiling` 的 `advance_nanos/scan_nanos` 累计值 ÷ 批处理事件数。
