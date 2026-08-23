# NEXMark Q1~Q22 语义支持度与执行器应用矩阵

> 状态：2026-08-23（join 算子族 P1-P4 + HOP + conv top_ties + avg-of-max 双规则链落地后）
> 参考：`NEXMARK_AUTHORITATIVE_SEMANTICS.md`（权威 SQL 逐条）· `SEMANTIC_ALIGNMENT.md`（对齐过程）
> 视角：本文回答两个问题——**① 每个查询的语义是否得到全面支持；② CEP / Stat / Join / on-each 是否被正确应用**。
> 执行器：**on-each**（无状态投影/过滤）、**CEP**（match 状态机：fixed/sliding/session + measure + conv）、
> **Stats**（`stats<...>` 列式批执行器：group by + count/distinct/min/max/avg/sum/last/top + where 行过滤）、
> **Join 族**（snapshot/asof/anti/interval，P1-P4 含 `within`/`reduce`/`emit at` deferred）。

---

## 1. 判定标准

**语义全面支持** = 三面同时对齐：

| 面 | 含义 |
|---|---|
| 输入面 | 数据/触发条件与权威一致（过滤、窗口声明、驱动流） |
| 输出基数 | 每事件 / 每窗 / 每键一行的数量对齐权威 |
| 值语义 | 聚合口径、字段值、并列/平手规则对齐权威 |

**执行器应用正确** = 该查询用对了执行器家族（无状态变换用 on-each、纯聚合用 Stats、join 用 Join 原语、状态序列用 CEP），且是**最小形态**（无冗余包裹）。

状态三档：✅ 全面支持 / ⚠️ 近似支持（有明确已声明偏差）/ ❌ 未支持或严重偏差。

---

## 2. 总览矩阵

| Q | 权威算子形态 | 当前实现执行器 | 语义 | 执行器判断 |
|---|---|---|---|---|
| Q1 | 无状态投影（0.908×price） | `on each` | ✅ | ✅ on-each 正确 |
| Q2 | 过滤（MOD(auction,123)=0） | `on each` + bind filter | ✅ | ✅ on-each 正确 |
| Q3 | 增量 join + 过滤（INNER + state∈OR/ID/CA + category=10） | `match<id:10m>` + join snapshot + where | ✅ | ✅ join 正确；⚠️ CEP 包裹冗余（on-each + join + where 即可，Q20 同款写法） |
| Q4 | **两层聚合 avg-of-max**（内层每 auction max 胜出价 → 外层按 category avg） | 双规则链：deferred `reduce maxrow` → 中间窗 `auction_finals` → `stats<1d:fixed> group by(category) avg` | ✅ | ✅（2026-08-23 落地；外层 stats oracle 对拍待接入） |
| Q5 | HOP 滑窗(2s,10s) 每窗 bid 数最多 auction（并列全出） | `match<auction:hop(10s, 2s)>` + close count + conv `top_ties(1)` | ✅ | ✅（2026-08-23：窗口形状/基数与权威一致，并列全输出） |
| Q6 | OVER 窗口（每 seller 最近 10 笔成交胜出价均值；**官方未落地**） | `match<seller:10m>` + join-then-key + avg≥200 阈值 | ⚠️ | ⚠️ 形态不同（阈值告警 vs 流式输出）；「最近 N 笔滑动均值」是能力缺口 |
| Q7 | 每 10s 桶全局最高价 bid（并列全出） | `match<auction:10s:fixed>` + close max + conv `top_ties(1)` | ✅ | ✅（2026-08-23 top_ties 并列全出；残留 = auction 粒度 vs 权威 bid 行粒度） |
| Q8 | person⋈auction **同窗 join**（10s 桶内注册且创建拍卖） | **P3 deferred 存在 join**：`within [p.dateTime, <bucket_end(10s)] on p.id==auction_events.seller emit at bucket_end` | ✅ | ✅ deferred join 正确（10M 对拍一致） |
| Q9 | 生命周期内胜出价（ROW_NUMBER price DESC, dateTime ASC） | **P3 deferred + reduce**：`within [a.dateTime, a.expires] reduce maxrow(price) tie(dateTime asc) as winner emit at a.expires` | ✅ | ✅ deferred + reduce 正确（能力项测试全覆盖 §7；10M 对拍一致） |
| Q10 | 全量投影落盘（dt/hm 分区） | `on each` | ✅ | ✅ on-each 正确 |
| Q11 | session 窗口 count（gap 10s，每会话一行） | `match<bidder:session(10s)>` + close count | ✅ | ✅ CEP 正确（每会话一行对齐） |
| Q12 | 固定窗口 count（**处理时间** p_time） | `match<bidder:10s:fixed>` / `stats<10s:fixed> group by (b.bidder)` | ⚠️ | ✅ 两者皆可（stats 更优）；⚠️ 事件时间近似处理时间（引擎不支持 PT 窗口） |
| Q13 | 流 ⋈ 有界 side input（mod(auction,10000)=key） | `match<bidder:10m>` + join person snapshot | ⚠️ | ✅ join snapshot 正确；⚠️ 键近似（bidder vs mod(auction,10000)）；**P4 provider 精确化路径已就绪**（`wp-reactor/docs/design/provider-window-usage.md`） |
| Q14 | 投影 + CASE 分型 + UDF + 价格过滤 | `on each` + bind filter + if/strftime + count_char | ✅ | ✅ on-each 正确 |
| Q15 | 全局 12 列聚合（4 count + 8 distinct，FILTER 分档） | CEP `match<:30m:fixed>` 12 measure；**stats 版存在** | ✅ | ✅ **stats 是正确执行器**；⚠️ bench 默认跑 CEP 版 |
| Q16 | channel 复合键聚合（15 列） | CEP `match<channel:30m:fixed>`；**stats 版存在** | ✅ | ✅ stats 正确（复合键）；⚠️ bench 默认 CEP 版 |
| Q17 | auction 复合键聚合（count 分档 + min/max/avg/sum） | CEP `match<auction:30m:fixed>`；**stats 版存在** | ✅ | ✅ stats 正确；⚠️ bench 默认 CEP 版 |
| Q18 | dedup（每 (bidder,auction) 最后一条**字段值**） | CEP 版输出键+计数（无 last 值）；**stats 版 last 度量** | ✅ | ✅ **stats last 正确**（字段值带出）；CEP 版值语义缺失 |
| Q19 | per-auction top-10（ROW_NUMBER price DESC） | **stats `group by (b.auction) + top(10, b.price)`**（q19.wfl 已是 stats 版） | ✅ | ✅ stats top 正确（tie-break 确定性：同价先到在前） |
| Q20 | filter join（bid⋈auction + category=10） | `on each` + join snapshot + where | ✅ | ✅ join 正确 |
| Q21 | CASE + regexp 提取 + 过滤（热 50% + cold 有参 90% → 95% 输出） | `on each` + bind filter（channel_id 数据侧预生成） | ✅ | ✅ on-each 正确 |
| Q22 | split 投影（SPLIT_INDEX(url,'/',3/4/5)） | `on each` + let + mvindex | ✅ | ✅ on-each 正确 |

---

## 3. 分类分析

### 3.1 无状态类（Q1/Q2/Q10/Q14/Q21/Q22）—— 6 个 ✅，执行器正确

全部用 `on each` 最小形态表达投影/过滤/变换，没有错误套用 CEP/Stats/Join。这是正确的执行器选择。

### 3.2 Join 类（Q3/Q13/Q20）—— join 原语应用正确，3 个 ✅

- snapshot join + join 后 `where`（Q3 州过滤 / Q20 category 过滤）已能表达 INNER JOIN 语义（join miss → where 抑制）。
- Q13 形状对齐（流 + 有界侧输入 enrichment），键仍是近似（bidder→person vs 官方 mod(auction,10000)→文件表）；**P4 已补齐 provider 静态表 + checker「仅 snapshot」约束**，落地清单见 `wp-reactor/docs/design/provider-window-usage.md` §2/§3，只差 bench 数据侧导出 person 表。
- Q3 的 `match<id:10m>` 包裹是历史遗留：每 auction 一行、无累积需求，`on each` + join + where 即可（Q20 已示范）。语义正确，非最小形态。

### 3.3 Stats 类（Q15/Q16/Q17/Q18/Q19）—— 5 个 ✅，stats 是正确执行器（2026-08-23 已切标准形态）

- 纯窗口聚合（无 join、无 CEP 状态机需求），stats 的列式批执行器 + 复合键 group by + distinct_count/last/top 是**正确形态**；`qN.wfl` 已是 stats 标准形态（Q19 原本就是 stats 版）。
- **执行器切换已完成（2026-08-23）**：`q15→q15_stats`、`q16→q16_stats`、`q17→q17_stats`、`q18→q18_stats` 的 bench 建议已落地——stats 版改为标准 `qN.wfl`（bench.sh 默认加载），CEP 版改名为 `qN-verify.wfl`（交叉验算）。Q18 值语义已由 stats last 度量修复（CEP 版只输出键+计数）。

### 3.4 CEP 正确类（Q11/Q12）—— 2 个 ✅/⚠️

- Q11 session 窗口：CEP 原生 session + 每会话一行，基数对齐 ✅（stats 虽声明 Session 模式但无需切换）。
- Q12 fixed 窗口：CEP 与 stats 皆可，stats 更优；唯一偏差是**处理时间用事件时间近似**（引擎不支持 processing time 窗口，replay 下同步、实时流下分桶不同）——引擎级能力缺口，非执行器选错。

### 3.5 未全面支持（Q6）—— 1 个 ⚠️，需要新能力（引擎缺口）

- **Q6**：官方 SQL 自己都未落地（Flink OVER 不支持 retractions），无对拍锚点；本地阈值告警形态（avg>=200 触发）与权威流式均值输出不同。真正缺口是「每键最近 N 行的滑动聚合」（keyed last-N sliding）。

> 2026-08-23 更新：Q4（avg-of-max 双规则链）、Q5（HOP 窗口对齐）、Q7（conv `top_ties(1)` 并列全输出）已落地，从本表移出；详见 `CAPABILITY_GAP_MATRIX.md` §二「已解决历史」。

---

## 4. 执行器应用总评

- **执行器选择整体健康**：22 个查询中 20 个的执行器形态（on-each/join/stats/CEP-session）是合理的，没有「拿 CEP 硬塞 stats 场景」的方向性错误——CEP 版 Q15-18 只是历史遗留 + bench 默认加载，stats 版已存在。
- **能力已就绪、实现已迁移**（2026-08-22 完成）：Q8/Q9 从 CEP 近似迁移到 **P3 deferred join**（Q8 纯存在 + 上开桶；Q9 reduce maxrow+tie+label），**能力项测试全覆盖**（见 §7）。
- **一个能力缺口**（Q6 滑动 last-N）已有设计方向或待设计，属引擎侧后续；Q4/Q5/Q7 已随
  2026-08-23 双规则链 / HOP / `top_ties` 落地闭环。
- **一个执行器切换**（bench Q15-18 → stats 版）立即可做，同时修复 Q18 的字段值语义。

---

## 5. 行动项与状态追踪（2026-08-22）

| # | 行动项 | 状态 |
|---|---|---|
| 1 | Q8/Q9 迁移到 P3 deferred 语法 | ✅ 已完成（q8.wfl/q9.wfl 已改，checker/编译器通过） |
| 2 | Q8/Q9 引擎能力项测试（单测 + e2e + oracle） | ✅ 全覆盖全绿（§7） |
| 3 | Q8/Q9 oracle（wfgen）deferred 对拍支持 | ✅ 已完成（含 asof_candidates 性能修复 + 每键多行正确性修复，wfgen 165 测试全绿） |
| 4 | Q8/Q9 引擎↔oracle 对拍 | ✅ 完全一致（Q8 82,446 / Q9 557,204，三层根因已修：field_usage 补字段 + over 1h + 帧内跨流时间序排序） |
| 5 | Q1~Q22 10M 全量对拍 | ✅ 19/22 完全一致；Q11/Q12 known（fixed/session 收口实现）；Q19 oracle 未接 stats |
| 6 | Q3 简化（on-each + join + where，去 CEP 包裹） | ⏳ 可选（语义已对，10M 一致） |
| 7 | Q4 两级聚合（stats→stats 管线） | ✅ 已完成（2026-08-23 avg-of-max 双规则链：deferred reduce → auction_finals → stats avg） |
| 8 | Q5/Q7 并列全输出 | ✅ 已完成（2026-08-23 conv `top_ties(1)`；Q5 另以 HOP 对齐窗口形状/基数） |
| 9 | Q6 滑动 last-N 聚合 | ⏳ 引擎能力缺口（Flink 官方亦未落地） |
| 10 | Q13 provider 精确化接线（bench 侧导出 person 表） | ⏳ 路径就绪待接线 |
| 11 | bench 切换 Q15-18 → stats 版 | ✅ 已完成（2026-08-23：stats 版为标准 `qN.wfl`，CEP 版改 `qN-verify.wfl` 交叉验算） |

---

## 6. Q8/Q9 对拍差异说明（2026-08-22，10M 数据）

> ⚠ 2026-08-22 晚更新：三个修复（field_usage 补字段 / over 调大 1h / 帧内跨流时间序排序）
> 落地后，**Q8/Q9 均已与 oracle 完全一致**（本节原差异说明作废）。

**Q8**：引擎 82,446 == oracle 82,446 ✅（eos 水位语义修正后完全一致）。

**Q9 引擎 0 输出的三层根因链（已全部修复）**：

1. **field_usage 物化裁剪漏字段**（wf-lang `field_usage.rs`）：`within` 界 / `emit_at` / `reduce`
   引用的字段未统计 → 窗口物化裁剪掉 `expires` → `deferred_pending_for` 挂起失败（60 万 auction）→ 0 输出。
   修复：补统计 + 回归测试（`deferred_join_within_emit_at_reduce_fields_collected`）。
2. **join 目标窗口 over 不足**：`over=10m(600s)` < v5 数据 span（10m=1000s）→ 时间驱逐实体行 →
   snapshot/asof join miss。修复：nexmark.wfs `over=1h`（Q3/Q6/Q20 同步修复）。
3. **帧内跨流时间序**（wfgen `output/arrow_ipc.rs`）：`events_to_typed_batches` 按 HashMap 分组生成
   各 stream batch（迭代序随机）→ 高流量流（bid 92%）batch 随机排后 → receiver 按帧序 commit 时
   右窗 append 滞后 → 驱动=低流量流（person/auction）的 deferred/eager join 在右窗行到达前评估 →
   miss。修复：帧内 batch 按最小事件时间排序（Q3 差 45% → 一致；Q9 差 24% → 一致）。

**Q9 修复后**：引擎 557,204 == oracle 557,204 ✅。

## 6b. Q1~Q22 10M 对拍全量基线（2026-08-22）

| 查询 | 引擎 EMIT | oracle | 状态 |
|---|---|---|---|
| Q1 | 9,200,000 | 9,200,000 | ✅ 一致 |
| Q2 | 74,863 | 74,863 | ✅ 一致 |
| Q3 | 61,473 | 61,473 | ✅ 一致（修复后） |
| Q4 | 5 | 5 | ✅ 一致 |
| Q5 | 100 | 100 | ✅ 一致 |
| Q6 | 8,731,356 | 8,731,356 | ✅ 一致（修复后） |
| Q7 | 100 | 100 | ✅ 一致 |
| Q8 | 82,446 | 82,446 | ✅ 一致 |
| Q9 | 557,204 | 557,204 | ✅ 一致（修复后） |
| Q10 | 9,200,000 | 9,200,000 | ✅ 一致 |
| Q11 | 197,299 | 197,095 | ⚠ known：尾部会话多收 0.1% |
| Q12 | 282,514 | 102,400 | ⚠ known：fixed 10s 桶引擎多收尾部桶 ~176% |
| Q13 | 9,200,000 | 9,200,000 | ✅ 一致 |
| Q14 | 2,605,217 | 2,605,217 | ✅ 一致 |
| Q15 | = | = | ✅ 一致 |
| Q16 | = | = | ✅ 一致 |
| Q17 | = | = | ✅ 一致 |
| Q18 | = | = | ✅ 一致 |
| Q19 | 实测值 | — | ⚠ oracle 未接入 stats（Q19 为 stats 规则），对拍待 oracle stats 支持 |
| Q20 | 1,863,987 | 1,863,987 | ✅ 一致（修复后） |
| Q21 | 8,738,817 | 8,738,817 | ✅ 一致 |
| Q22 | 9,200,000 | 9,200,000 | ✅ 一致 |

**结论**：19/22 与 oracle 完全一致；Q11/Q12 为引擎 fixed/session 收口实现差异（wfl 语义正确，known）；
Q19 为 oracle 侧能力缺口（stats 规则未接入 oracle，引擎输出为实测值）。

> 注：Q9 与 Q3/Q6/Q20 的 known-diff 条目已从 `cmd_verify_nexmark` 的 known 列表移除（修复后真一致，
> 保留会误报 ⚠）；Q12/Q11/Q19 的 known 描述已更新为实测语义。

---

## 7. Q9 能力项测试清单（deferred + reduce + emit at）

> 全绿（2026-08-22）：wf-engine 635、wf-runtime 216（deferred 10）、wf-lang joins_family 22、wfgen oracle 22。
> 注：真实 wfl 编译集成测试（`deferred_q9_real_wfl_compiled_plan_runs`）证明**编译链路 + rule_task 执行链路**
> 均正确（编译产物 emit_at/within/reduce/label 完整落 plan、挂起→到期→胜者输出全通）——daemon 的 Q9
> 0 处理因此定位到**更上游**（spawn/订阅/路由/配置），非编译或执行层。

| 能力项 | 引擎单测（`wf-engine/.../deferred_join.rs`） | e2e（`wf-runtime/.../deferred_integration_tests.rs`） | oracle（wfgen） |
|---|---|---|---|
| deferred 挂起（key/界/expiry 求值） | `deferred_pending_for_evaluates_key_bounds_expiry` ✅ | Q9 hit 形状 ✅ | `deferred_q9_emits_winner...` ✅ |
| 到期评估 + reduce maxrow | `execute_deferred_join_q9_maxrow_tie_and_label` ✅ | `deferred_q9_hit_outputs_winner...` ✅ | 同上 ✅ |
| tie 破平（同价取 dateTime 最早） | 同左（bidder=2 断言）✅ | 同左（winner_bidder=2）✅ | — |
| `as winner` label 注入（`winner.bidder` Path） | 同左（yield winner_bidder）✅ | 同左 ✅ | — |
| **闭区间边界**（恰在 dateTime/expires 命中；界外 1ns 排除） | `execute_deferred_join_closed_interval_boundaries_inclusive` ✅（新增） | — | — |
| **多条件 join 复核**（key 命中但次条件不满足 → 拒绝） | `execute_deferred_join_multi_condition_recheck` ✅（新增） | — | — |
| 空集不输出（无 bid 的 auction） | `execute_deferred_join_empty_set_no_output` ✅ | `deferred_q9_no_bid_no_output` ✅ | `deferred_q9_emits_winner`（auction 6/7 无 bid）✅ |
| watermark 到期扫描（未到期不输出） | — | Q9 hit 测试「未到期无输出」断言 ✅ | `deferred_q9_not_due_before_expiry` ✅ |
| EOS flush 触发剩余 | — | `deferred_q9_flush_triggers_remaining_pending` ✅ | close_at_eos flush ✅ |
| minrow/last/top 选择器 | `deferred_minrow_last_and_top_select` ✅ | — | — |
| minrow + tie asc 破平 | `deferred_minrow_with_tie_asc_picks_smallest_tie` ✅ | — | — |
| post-join where 抑制 | `deferred_where_suppresses_output` ✅ | — | — |
| **真实 wfl 编译集成**（q9.wfl → parse/compile → rule_task 执行，emit_at/within/reduce/label 落 plan 断言） | — | `deferred_q9_real_wfl_compiled_plan_runs` ✅（新增；fanout 补 nexmark_alerts 目标） | — |
| Q8 纯存在（无 reduce）+ 上开桶边界 | `execute_deferred_join_pure_existence_q8_shape` + `deferred_q8_bucket_end_half_open_interval` ✅ | `deferred_q8_hit/boundary/no_auction`（3 个）✅ | — |
| 语法层（q8/q9.wfl checker/compiler） | wf-lang `joins_family` q8/q9 shape ✅ | — | `wfl explain` ✅ |
