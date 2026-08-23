# NEXMark (Flink 测试集) 能力差距矩阵

> 口径：以官方 `github.com/nexmark/nexmark` 的 `nexmark-flink/.../queries/qN.sql` 为权威语义基准（见 `NEXMARK_AUTHORITATIVE_SEMANTICS.md`）。
> 本表判断基于 2026-08-23 重审的 22 个当前 `.wfl` 文件 + wfusion 当前规则能力（CEP / Window / stats<> / Join 家族）的源码核查。
> 三档定义：
> - **已有**：能力已落地且端到端可跑，输出与 Flink 语义/基数一致。
> - **待接通**：算子已实现，但规则装配/计划连线未端到端激活（纸面符合，跑不通或未被规则引擎调度）。
> - **待补强**：能力存在但语义不精确（归并单位、精度、回撤、窗口对齐方式），EMIT 行数/数值会与 Flink 对不上。

---

## 一、逐查询矩阵（q1~q22）

| 查询 | Flink 语义 | 依赖能力 | 档位 | 缺口说明 |
|------|-----------|----------|------|----------|
| **Q1** | 货币换算(0.908×price)+过滤 | CEP on-each | ✅ 已有 | — |
| **Q2** | 每拍卖计数 | CEP each | ✅ 已有 | 输出为 per-auction 一行（非 Flink per-bid） |
| **Q3** | 按州过滤(IN OR/ID/CA) | CEP filter | ✅ 已有 | — |
| **Q4** | 分类均价（累积窗口） | stats avg + 累积窗口 | 🔶 待补强 | 无累积/分层窗口算子；avg 归并单位错 |
| **Q5** | 热门新商品（distinct 新 item） | stats distinct | 🔶 待补强 | distinct key 走 f64 丢精度 |
| **Q6** | 卖家售出均价 | stats avg | 🔶 待补强 | avg 归并须用 (sum,count) |
| **Q7** | 时段最高出价 | stats max | 🔶 待补强 | 滑动回撤未处理 |
| **Q8** | 新用户+其拍卖（TUMBLE join） | deferred/asof join | 🟠 待接通 | P3，rule_task 未端到端激活 |
| **Q9** | 中标出价（asof join） | deferred/asof join | 🟠 待接通 | P3，rule_task 未端到端激活 |
| **Q10** | 1/7 任意抽样 | CEP 选择 | ✅ 已有 | 实现论文版 Q10（非 Flink 版 Log-to-FS） |
| **Q11** | 用户会话统计（session） | session 窗口 + 分片 | 🟡 已有(分片口径) | 分片口径，非单实例全量 |
| **Q12** | 每分钟均价（处理时间） | stats avg + 处理时间窗 | 🔶 待补强 | 处理时间窗 + avg 归并单位 |
| **Q13** | 有界侧输入 join | snapshot join | 🔶 待补强 | 近似快照 join |
| **Q14** | 时间戳换算+价格过滤 | CEP calc + filter | ✅ 已有 | — |
| **Q15** | 按日历天的分类统计 | stats + 日历天窗口 | 🔶 待补强 | 用 30m 固定桶代替日历天（巧合对齐） |
| **Q16** | 按日历天的竞价统计 | stats + 日历天窗口 | 🔶 待补强 | 同上 |
| **Q17** | 按日历天的拍卖统计 | stats + 日历天窗口 | 🔶 待补强 | 同上 |
| **Q18** | 每卖家 Top-10 出价 | stats top-N | 🔶 待补强 | top-N + 滑动回撤 |
| **Q19** | 拍卖 Top-10 价格 | stats<> top-N | 🟠 待接通 | StatsExecutor 已实现，stats<> 语法→计划连线未确认 |
| **Q20** | 每卖家最高价出价 | CEP maxrow | ✅ 已有 | — |
| **Q21** | 按渠道监控新用户 | CEP monitor | ✅ 已有 | wfgen 已输出 channel_id（cmd_gen_nexmark.rs:279） |
| **Q22** | URL 目录投影 | CEP projection | ✅ 已有 | — |

**汇总**：已有 8 · 待接通 3 (Q8/Q9/Q19) · 待补强 8 (Q4/5/6/7/12/13/15/16/17/18 — 实际 10 个，见下) · 已有(分片/巧合) 1 (Q11) + 巧合对齐 3 (Q15/16/17)。

> 修正计数：🔶 待补强共 **10** 个（Q4 Q5 Q6 Q7 Q12 Q13 Q15 Q16 Q17 Q18）；🟡 特殊口径 4 个（Q11 分片 + Q15/16/17 日历天巧合，已含在待补强计数里 Q15/16/17）。即 8 已有 + 3 待接通 + 10 待补强 + 1 分片口径 = 22。

---

## 二、能力缺口清单（按优先级）

### 🔴 P0 — 阻断"完全支持"的结构性缺口

**G1. 日历天 / 任意对齐窗口算子缺失**
- 现状：仅有 `fixed/sliding/session` 时长桶，无"按日历天"或"对齐到壁钟边界"的窗口。
- 影响：Q15/16/17 只能用 `30m:fixed` 桶近似。当前数据 `BASE_NS=2026-01-01T00:00` 跨 30m 恰为日历天 → **巧合对齐**，数据集拉长跨天即偏。
- 修复：新增日历天窗口模式（或 `align_to=day` 参数）。

**G2. deferred / asof join 未端到端接通**
- 现状：join 家族设计已审（join-family-design-review-v2.md），`JoinIndex`/`lookup_timestamped`/`provider_snapshot`/`asof_candidates` 均存在，但 `rule_task` 装配仍标 P3，无生产路径激活。
- 影响：Q8/Q9 映射精确（纸面符合）但**跑不通 / 不被调度**。
- 修复：在 `rule_task` 增加 deferred join 订阅与触发路径，补齐端到端单测。

### 🟠 P1 — 正确性缺口（EMIT 与 Flink 对不上）

**G3. stats<> 归并单位错误（avg）**
- 现状：§5.4 仍称"avg 可交换结合"，未规定归并以 `(sum,count)` 为单位、avg 仅输出时算。
- 影响：跨分片/跨窗口归并会算错 → Q4/Q6/Q12。

**G4. distinct 精度（f64 化丢精度）**
- 现状：distinct key 经 `ValueKey::from_value`（f64 路径），>2^53 整数 id 丢精度（与 fanout.rs:1103 已记载分歧一致）。
- 影响：Q5 热门新商品去重错误。

**G5. 滑动/会话窗口回撤（逆运算）缺失**
- 现状：sliding/session 对 distinct/top/last 的撤销完全未处理。
- 影响：Q7/Q18 滑动窗口内计数/排名漂移。

**G6. stats<> 语法→计划连线（Q19）**
- 现状：`StatsExecutor` 已实现且导出，但 `stats<>` 解析到计划的接线在设计评审中仍标"新增"，未确认编译跑通。

### 🟡 P2 — 算子能力补充

**G7. 累积 / 分层窗口算子（Q4 的 PREVIOUS 语义）**
- 现状：无一等公民累积窗口，Q4 只能近似。

**G8. 处理时间窗口（Q12）**
- 现状：窗口基于事件时间；Q12 需处理时间窗。

---

## 三、结论

wfusion 当前架构**能覆盖 NEXMark 绝大多数查询的"意图"**，但**无法逐条复现 Flink 测试集输出**：

- **真正符合（可独立验证）**：8/22（Q1 Q2 Q3 Q10 Q14 Q20 Q21 Q22）—— 均为 CEP 类投影/过滤/标量计算/取首行。
- **接通即达标**：+3（Q8 Q9 Q19，待 G2/G6 落地）。
- **需补强语义才达标**：+10（Q4 Q5 Q6 Q7 Q12 Q13 Q15 Q16 Q17 Q18，集中待 G1/G3/G4/G5/G7/G8）。
- **特殊口径**：Q11（分片 + session）。

**完整支持 Flink 测试集的最短路径**：先 G2（deferred join 端到端）→ G1（日历天窗口）→ G3/G4/G5（stats 正确性）→ G6/G7/G8（补齐算子）。
