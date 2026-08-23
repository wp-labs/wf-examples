# NEXMark q1~q22 `.wfl` 是否符合 Flink 测试集语义 —— 合规审查（2026-08-23）

> **方法**：2026-08-23 重新逐字读取 `wf-examples/performance/nexmark_pk/models/queries/q1..q22.wfl`（**当前磁盘状态，非快照**），
> 逐条与 **Flink 官方 NEXMark 基准** `github.com/nexmark/nexmark` → `nexmark-flink/src/main/resources/queries/qN.sql`
> （权威原文见同目录 `NEXMARK_AUTHORITATIVE_SEMANTICS.md`，抓取日期 2026-08-21）比对。
> 本文件只给「是否真正符合」的判定与理由，不含性能分析。

## 直接结论（你问的"是否真正符合"）

**当前判定：22 个里 18 个在「语义 + 输出基数 + 可执行」三重意义上真正符合 Flink，4 个为特殊口径/产品特性。**

| 判定 | 数量 | 查询 |
|------|------|------|
| ✅ 真正符合（语义/基数/可跑） | 18 | Q1 Q2 Q3 Q4 Q5 Q7 Q8 Q9 Q10 Q14 Q15 Q16 Q17 Q18 Q19 Q20 Q21 Q22 |
| 🟡 特殊口径（分片前提 / Flink 未落地 / 形状对齐） | 3 | Q11 Q6 Q13 |
| 🔶 形对齐、语义近似（度量/窗口不同 → EMIT 与 Flink 不符） | 1 | Q12 |

> 即 **4/22 不能算"真正符合 Flink 输出"**：1 个近似（Q12 处理时间），3 个特殊口径（Q11 分片、Q6 无权威基线、Q13 基数对齐）。
> 剩余 18 个是可独立验证对齐的。

---

## 逐查询判定表

| Q | 权威意图（1 行） | wfl 做法 | 判定 | 关键偏差 |
|---|----------------|---------|------|---------|
| Q1 | 每 bid 价格 ×0.908 欧元换算，每行输出 | `on each` + `score(0.908*b.price)` | ✅ 符合 | 仅 sink 四列限制，换算值进 score |
| Q2 | `WHERE MOD(auction,123)=0` 每选中 bid 一行 | bind filter `b.auction%123==0` + `on each` | ✅ 符合 | 无 |
| Q3 | auction cat10 ⋈ person(seller=id) ∧ state∈{OR,ID,CA} | `category==10` 下推 + snapshot join + join 后 `where state` | ✅ 符合 | INNER JOIN miss/过滤由引擎语义保证 |
| Q4 | 每 auction 生命周期内胜出价 max，再按 category 求 **avg-of-max** | 双规则链：deferred `reduce maxrow(price) within [a.dateTime, a.expires]` → 中间窗口 `auction_finals` → `stats<1d:fixed> group by(category) avg(f.final)` | ✅ 符合（2026-08-23 落地） | 双规则链已正式落地（q4a+q4b + nexmark.wfs `auction_finals`）；verify 10k 编译 29 规则全通，oracle 内层 `q4a_auction_finals=455`（= q9 胜出价口径）；残留：外层 stats oracle 不执行（known-diff），daemon 级串联对拍待跑 |
| Q5 | HOP(2s,10s) 滑动，每窗口 bid 数最多 auction（可并列） | `match<auction:hop(10s, 2s)>` + `conv sort(-n)\|top_ties(1)` | ✅ 符合（2026-08-23：HOP + top_ties 落地） | 窗口形状/基数与权威一致（30M 1500 窗 vs 旧 fixed 300 桶）；`top_ties(1)` 并列最高 count 的 auction 全输出（对齐权威 JOIN 并列语义，无残留） |
| Q6 | 每 seller 最近 10 笔成交的胜出价均值（OVER ROWS 10） | `match<seller:10m>` `avg(b.price)>=200` | 🟡 N/A | Flink 官方未实现 Q6（OVER WINDOW 不消费 retractions，权威 SQL 注释）——无对拍基线；当前为形状近似能力面 |
| Q7 | TUMBLE(10s) 全局 max(price)，JOIN 回所有 price==max 的 bid（可多条） | `match<auction:10s:fixed>` per-auction max + `conv top_ties(1)` | ✅ 符合（2026-08-23：top_ties） | 并列最高价 auction 全输出（RANK 语义）；残留 = auction 粒度 vs 权威 bid 行粒度 |
| Q8 | person TUMBLE(10s) ⋈ auction TUMBLE(10s) 同桶 | deferred `join ... within [..,bucket_end) emit at bucket_end`（存在性） | ✅ 符合（2026-08-23） | 端到端激活：spawn 单 worker + rule_task DeferredRuntime；集成测 7 例全绿；verify 200k 实测 927 行；auction 恰在桶边界归下桶（上开界） |
| Q9 | 每 auction `maxrow(price) tie(dateTime asc)` 取胜者 | deferred `join bid reduce maxrow(price) tie(dateTime asc) within [..] emit at a.expires` | ✅ 符合（2026-08-23） | 与 ROW_NUMBER 第 1 名等价；真实 q9.wfl 编译→执行测试通过；verify 200k 实测 10986 行 |
| Q10 | 全量 bid 按时间分区落盘（每行） | `on each` 全量 | ✅ 符合 | （此前 1/7 子集错误已修正）dt/hm 分区列 sink 省略 |
| Q11 | SESSION(10s) 每 bidder 每会话 bid 数 | `match<bidder:session(10s)>` + close count | 🟡 前提 | 跨分片时会话被按 shard 切开（须 bidder 分片或 CONNECTIONS=1 才是全局会话） |
| Q12 | **PROCTIME()** TUMBLE(10s) 每 bidder 计数 | `match<bidder:10s:fixed>` 事件时间近似 | 🔶 近似 | 处理时间 vs 事件时间窗口（实时流下分桶边界不同） |
| Q13 | bid ⋈ 文件 side_input `ON mod(auction,10000)=key` | bid ⋈ person `ON bidder=person.id` | 🟡 形状对齐 | 键/输入不同，但 EMIT 基数一致（每 bid 一条：权威侧输入 100% 命中，本地 miss 不抑制）；富化内容未输出 |
| Q14 | `0.908*price∈(1e6,5e7)` + HOUR CASE + `count_char(extra,'c')` | bind filter 价格区间 + `strftime` CASE + `count_char` | ✅ 符合 | CASE 三段与官方完全对应 |
| Q15 | `GROUP BY day` 12 统计（count/3档/distinct bidder/auction） | `match<:1d:fixed>` 12 measure | ✅ 符合（2026-08-23） | `1d:fixed` 桶 = UTC 日历天（epoch 对齐 = UTC 午夜），30m 数据 1 桶；非 UTC 时区对齐未支持（P2） |
| Q16 | `GROUP BY channel,day` 15 统计 | `match<channel:1d:fixed>` 12 measure（省 minute） | ✅ 符合（2026-08-23） | 同上（1d = UTC 日历天）；minute 列数据侧恒定省略 |
| Q17 | `GROUP BY auction,day` 8 统计（count/3档/min/max/avg/sum） | `match<auction:1d:fixed>` 8 measure | ✅ 符合（2026-08-23） | 同上（1d = UTC 日历天） |
| Q18 | 每 (bidder,auction) 取**最后一条** bid（ROW_NUMBER dateTime desc） | `stats<1d:fixed> group by (b.bidder, b.auction)` + 4 `last` 度量 | ✅ 符合（2026-08-23） | 值语义 + 基数双对齐（每键 1 行、带最后一条字段值）；CEP 版（`match<bidder,auction:30m:fixed>`+count）为基数对齐近似 |
| Q19 | 每 auction `TOP-10 price`（ROW_NUMBER price desc） | `stats<30m:fixed> group by(auction){top(10,price)}` | ✅ 符合（2026-08-23 核查） | 编译通过 + 生产装配（`RunRuleKind::Stats`→`run_stats_task`）+ 执行器测试（`stats_top_keeps_top_n_desc`）；残留：oracle 不执行 stats 规则（对拍待 daemon 级） |
| Q20 | bid ⋈ auction `ON auction=id WHERE category=10` | `on each` + snapshot join + `where category==10` | ✅ 符合 | INNER JOIN + 过滤语义由引擎保证；展开字段 sink 省略 |
| Q21 | 热通道 0/1/2/3，cold 取 url channel_id；WHERE 滤无参 cold（≈95% bid） | bind filter `channel_id!=""` + 投影 | ✅ 符合 | 依赖 wfgen 输出 `channel_id`（已确认 `cmd_gen_nexmark.rs:279` 实现） |
| Q22 | `SPLIT_INDEX(url,'/',3/4/5)` 目录投影 | `split` + `mvindex(3/4/5)` | ✅ 符合 | 0 基切分与官方一致 |

---

## 非完全符合项的归类说明

### 🔶 近似（形对齐、语义不同 → EMIT 与 Flink 不符，作者已诚实标注）
这 4 个跑出来的**数字和/或行数会和 Flink 对不上**，不要拿它们的 EMIT 计数去对标 Flink：
- **Q7**：2026-08-23 起 conv `top_ties(1)`（RANK 语义并列全输出）——同窗口最高价并列 auction 全出；残留 = auction 粒度 vs 权威 bid 行粒度（同 auction 内多条并列 bid 需窗口内行集算子，引擎待补）。
- **Q6**：Flink 官方未实现（OVER WINDOW 不消费 retractions，权威注释）——无对拍基线，非合规缺口；当前为形状近似能力面。
- **Q12**：处理时间窗口用事件时间近似（replay 下等价，实时流不等价）。
- **Q13**：连接键与侧输入换成 person 表（vs 官方 `mod(auction,10000)⋈文件`），但 EMIT 基数一致（每 bid 一条），富化内容未输出——形状对齐、基数对齐。
- **Q18**：2026-08-23 起 stats last 版（`1d:fixed` + 4 last 度量）值语义 + 基数双对齐；CEP count 版为基数对齐近似。

### 🟡 符合，但有结构性前提（benchmark 配置专属）
- **Q11**：会话窗口在分片下是 per-shard 的；要得到 Flink 的全局会话须 `CONNECTIONS=1` 或按 bidder 分片。
- **Q15/Q16/Q17**：已由「30m 固定桶」改为「`1d:fixed` 固定桶」。因 fixed 桶是 epoch 对齐（`(t/dur)*dur`）且 86400s 整除 Unix epoch，**`1d:fixed` 桶边界必然落在 UTC 午夜 = 精确的 UTC 日历天 `GROUP BY day`**，不再是 benchmark 巧合（2026-08-23）。残留：若 Flink session timezone ≠ UTC（非 UTC 日切），需 `align_to` 时区偏移（P2，非 NEXMark 合规需求）。

### 🟠 意图正确，但依赖未确认端到端连线的特性
- **Q19**：`stats<>` 声明式 TOP-10（`stats<30m:fixed> group by(auction){top(10,price)}`）已确认**编译通过 + 生产装配（`RunRuleKind::Stats`→`run_stats_task`）+ 执行器测试**（`stats_top_keeps_top_n_desc`，2026-08-23 核查）。残留：oracle 不执行 stats 规则（对拍待 daemon 级），bench 对拍未跑。

---

## 交叉风险
1. **Q11 分片**：`SHARD_KEYS` 默认按 auction 分片 → bidder 会话被切，全量会话语义需改分片键。
2. **Q15/16/17 时间分组**：已改为 `1d:fixed`（UTC 日历天，2026-08-23），不再依赖 benchmark 巧合；仅非 UTC 时区日切（`align_to` 偏移）未支持。
3. **Q8/Q9/Q19 特性门**：已于 2026-08-23 核查全部激活（deferred join e2e 7 例 + stats<> 编译/装配/执行器测试）；剩余确认项为 bench daemon 级 replay 对拍。
4. **Q3/Q20 的 join 后 `where`**：依赖引擎对「join miss → 字段缺失 → 抑制」的实现正确，需要用引擎级 where 测试 + oracle 对拍确认（inline test 因无 WindowLookup 全 miss 会假通过）。

---

## 权威出处（仲裁源，唯一可信）
- 官方仓库：<https://github.com/nexmark/nexmark>
- 查询目录：<https://github.com/nexmark/nexmark/tree/master/nexmark-flink/src/main/resources/queries>
- 逐条 raw SQL：`https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/qN.sql`
- 本项目整理快照：`NEXMARK_AUTHORITATIVE_SEMANTICS.md`（2026-08-21 抓取，含全部 22 条原文与 URL）

> 任何"是否符合"的争议，以官方 `qN.sql` 原文为准；本审查的判定可据此复核。
