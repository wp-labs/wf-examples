# NEXMark q1~q22 `.wfl` 是否符合 Flink 测试集语义 —— 合规审查（2026-08-23）

> **方法**：2026-08-23 重新逐字读取 `wf-examples/performance/nexmark_pk/models/queries/q1..q22.wfl`（**当前磁盘状态，非快照**），
> 逐条与 **Flink 官方 NEXMark 基准** `github.com/nexmark/nexmark` → `nexmark-flink/src/main/resources/queries/qN.sql`
> （权威原文见同目录 `NEXMARK_AUTHORITATIVE_SEMANTICS.md`，抓取日期 2026-08-21）比对。
> 本文件只给「是否真正符合」的判定与理由，不含性能分析。

## 直接结论（你问的"是否真正符合"）

**没有完全符合。22 个里只有 8 个在「语义 + 输出基数 + 可执行」三重意义上真正符合 Flink。**

| 判定 | 数量 | 查询 |
|------|------|------|
| ✅ 真正符合（语义/基数/可跑） | 8 | Q1 Q2 Q3 Q10 Q14 Q20 Q21 Q22 |
| 🟡 符合，但有结构性前提（benchmark 配置专属 / 分片） | 4 | Q11 Q15 Q16 Q17 |
| 🟠 意图正确，但依赖未确认端到端连线的特性 | 3 | Q8 Q9 Q19 |
| 🔶 形对齐、语义近似（度量/窗口不同 → EMIT 与 Flink 不符） | 7 | Q4 Q5 Q6 Q7 Q12 Q13 Q18 |

> 即 **14/22 不能算"真正符合 Flink 输出"**：7 个是作者自己诚实标注的近似（跑出来数字/行数与 Flink 不同），7 个卡在特性连线或配置前提上。
> 剩余 8 个是可独立验证对齐的。

---

## 逐查询判定表

| Q | 权威意图（1 行） | wfl 做法 | 判定 | 关键偏差 |
|---|----------------|---------|------|---------|
| Q1 | 每 bid 价格 ×0.908 欧元换算，每行输出 | `on each` + `score(0.908*b.price)` | ✅ 符合 | 仅 sink 四列限制，换算值进 score |
| Q2 | `WHERE MOD(auction,123)=0` 每选中 bid 一行 | bind filter `b.auction%123==0` + `on each` | ✅ 符合 | 无 |
| Q3 | auction cat10 ⋈ person(seller=id) ∧ state∈{OR,ID,CA} | `category==10` 下推 + snapshot join + join 后 `where state` | ✅ 符合 | INNER JOIN miss/过滤由引擎语义保证 |
| Q4 | 每 auction 生命周期内胜出价 max，再按 category 求 **avg-of-max** | `match<category:10m:fixed>` 直接 **avg(窗口内 bid 价)** | 🔶 近似 | 度量错（全 bid 均价 vs 胜出价均值）；且 10m 桶 → 30m 数据出 3 行/类，基数 ×3 |
| Q5 | HOP(2s,10s) 滑动，每窗口 bid 数最多 auction（可并列） | `match<auction:10s:fixed>` + `conv sort(-n)\|top(1)` | 🔶 近似 | 固定桶非滑动(每2s)；top(1) 非全并列 |
| Q6 | 每 seller 最近 10 笔成交的胜出价均值（OVER ROWS 10） | `match<seller:10m>` `avg(b.price)>=200` | 🔶 近似 | 全 bid 均价 vs 胜出价均值；窗口=最近10笔成交 vs 10m 滑动 |
| Q7 | TUMBLE(10s) 全局 max(price)，JOIN 回所有 price==max 的 bid（可多条） | `match<auction:10s:fixed>` per-auction max + `conv top(1)` | 🔶 近似 | 输出 auction 标识 top-1 一条 vs 所有最高价 bid 行 |
| Q8 | person TUMBLE(10s) ⋈ auction TUMBLE(10s) 同桶 | deferred `join ... within [..,bucket_end] emit at` | 🟠 待特性 | 语义表达正确，依赖 P3 deferred join 端到端连线（coverage 有单测，rule_task 未确认激活） |
| Q9 | 每 auction `maxrow(price) tie(dateTime asc)` 取胜者 | deferred `join bid reduce maxrow(price) tie(dateTime asc) within [..]` | 🟠 待特性 | 映射精确（与 ROW_NUMBER 第1名等价），同依赖 P3 |
| Q10 | 全量 bid 按时间分区落盘（每行） | `on each` 全量 | ✅ 符合 | （此前 1/7 子集错误已修正）dt/hm 分区列 sink 省略 |
| Q11 | SESSION(10s) 每 bidder 每会话 bid 数 | `match<bidder:session(10s)>` + close count | 🟡 前提 | 跨分片时会话被按 shard 切开（须 bidder 分片或 CONNECTIONS=1 才是全局会话） |
| Q12 | **PROCTIME()** TUMBLE(10s) 每 bidder 计数 | `match<bidder:10s:fixed>` 事件时间近似 | 🔶 近似 | 处理时间 vs 事件时间窗口（实时流下分桶边界不同） |
| Q13 | bid ⋈ 文件 side_input `ON mod(auction,10000)=key` | bid ⋈ person `ON bidder=person.id` | 🔶 近似 | 连接键(auction%10000 vs bidder)与侧输入(文件 vs person 表)均不同，仅"流+有界侧输入"形状对齐 |
| Q14 | `0.908*price∈(1e6,5e7)` + HOUR CASE + `count_char(extra,'c')` | bind filter 价格区间 + `strftime` CASE + `count_char` | ✅ 符合 | CASE 三段与官方完全对应 |
| Q15 | `GROUP BY day` 12 统计（count/3档/distinct bidder/auction） | `match<:30m:fixed>` 12 measure | 🟡 前提 | 按 30m 固定桶而非日历天；benchmark 恰 1 天=1 桶才对齐，跨天/多天即偏 |
| Q16 | `GROUP BY channel,day` 15 统计 | `match<channel:30m:fixed>` 12 measure（省 minute） | 🟡 前提 | 同上 30m-vs-天；minute 列数据侧恒定省略 |
| Q17 | `GROUP BY auction,day` 8 统计（count/3档/min/max/avg/sum） | `match<auction:30m:fixed>` 8 measure | 🟡 前提 | 同上 30m-vs-天 |
| Q18 | 每 (bidder,auction) 取**最后一条** bid（ROW_NUMBER dateTime desc） | `match<bidder,auction:30m:fixed>` 每对 1 行 + count | 🔶 近似 | 基数对齐（每对 1 行），但**值语义偏**：输出 count 而非最后一条 bid 的字段 |
| Q19 | 每 auction `TOP-10 price`（ROW_NUMBER price desc） | `stats<30m:fixed> group by(auction){top(10,price)}` | 🟠 待特性 | 意图精确；`StatsExecutor` 已实现，但 `stats<>` 语法→plan 连线在设计评审中标记为"新增"，未确认编译/端到端跑通 |
| Q20 | bid ⋈ auction `ON auction=id WHERE category=10` | `on each` + snapshot join + `where category==10` | ✅ 符合 | INNER JOIN + 过滤语义由引擎保证；展开字段 sink 省略 |
| Q21 | 热通道 0/1/2/3，cold 取 url channel_id；WHERE 滤无参 cold（≈95% bid） | bind filter `channel_id!=""` + 投影 | ✅ 符合 | 依赖 wfgen 输出 `channel_id`（已确认 `cmd_gen_nexmark.rs:279` 实现） |
| Q22 | `SPLIT_INDEX(url,'/',3/4/5)` 目录投影 | `split` + `mvindex(3/4/5)` | ✅ 符合 | 0 基切分与官方一致 |

---

## 非完全符合项的归类说明

### 🔶 近似（形对齐、语义不同 → EMIT 与 Flink 不符，作者已诚实标注）
这 7 个跑出来的**数字和/或行数会和 Flink 对不上**，不要拿它们的 EMIT 计数去对标 Flink：
- **Q4**：官方是「每 auction 胜出价 → 按 category 求 avg」；wfl 是「category 桶内所有 bid 价 avg」，且固定 10m 桶在 30m 数据上产生 3 个桶 → 行数×3。
- **Q5/Q7**：固定 10s 桶 + `top(1)` 替代滑动窗口 + 全并列输出。
- **Q6**：全部 bid 均价替代「最近 10 笔成交的胜出价」均值。
- **Q12**：处理时间窗口用事件时间近似（replay 下等价，实时流不等价）。
- **Q13**：连接键与侧输入都换成 person 表，与官方 `mod(auction,10000)⋈文件` 语义不同。
- **Q18**：基数对（每对一行），但丢失「最后一条 bid 的值」，输出的是计数。

### 🟡 符合，但有结构性前提（benchmark 配置专属）
- **Q11**：会话窗口在分片下是 per-shard 的；要得到 Flink 的全局会话须 `CONNECTIONS=1` 或按 bidder 分片。
- **Q15/Q16/Q17**：按「30m 固定桶」实现，而官方是「按日历天 `GROUP BY day`」。当前 benchmark 数据 `BASE_NS=2026-01-01T00:00` 跨 30m 恰为一个日历天，故退化对齐；**一旦数据跨天或 >1 天，分组立即偏离 Flink**。这是把"benchmark 巧合"当成了"语义对齐"，属脆弱合规。

### 🟠 意图正确，但依赖未确认端到端连线的特性
- **Q8/Q9**：deferred join（`emit at` + `within` + `reduce maxrow ... tie`）语义映射**精确**，但 Pend 在 P3——`coverage_extra.rs` 有单测路径、`context.rs` 注释确认 eager 路径跳过 `emit at`；**未在 `rule_task` 生产路径确认激活**，故无法断言实际产出符合预期。
- **Q19**：`stats<>` 声明式 TOP-10 意图精确，`StatsExecutor`/`StatsAccum` 已存在，但 `stats<>` 语法→`StatsPlan` 的编译连线在上一轮设计评审中仍标"新增"，**未确认可编译/端到端跑通**。

---

## 交叉风险
1. **Q11 分片**：`SHARD_KEYS` 默认按 auction 分片 → bidder 会话被切，全量会话语义需改分片键。
2. **Q15/16/17 时间分组**：30m-fixed 桶是 benchmark 数据巧合，不是日历天语义。若日后数据集拉长，需改成真正的 `GROUP BY day`。
3. **Q8/9/19 特性门**：先把 deferred join 与 `stats<>` 计划在 `rule_task` 端到端跑通，否则这 3 个只是"纸面符合"。
4. **Q3/Q20 的 join 后 `where`**：依赖引擎对「join miss → 字段缺失 → 抑制」的实现正确，需要用引擎级 where 测试 + oracle 对拍确认（inline test 因无 WindowLookup 全 miss 会假通过）。

---

## 权威出处（仲裁源，唯一可信）
- 官方仓库：<https://github.com/nexmark/nexmark>
- 查询目录：<https://github.com/nexmark/nexmark/tree/master/nexmark-flink/src/main/resources/queries>
- 逐条 raw SQL：`https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/qN.sql`
- 本项目整理快照：`NEXMARK_AUTHORITATIVE_SEMANTICS.md`（2026-08-21 抓取，含全部 22 条原文与 URL）

> 任何"是否符合"的争议，以官方 `qN.sql` 原文为准；本审查的判定可据此复核。
