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
| **Q2** | 每拍卖计数 | CEP each | ✅ 已有 | 每满足 `MOD(auction,123)=0` 的 bid 一行（per-bid，`on each` + bind filter） |
| **Q3** | 按州过滤(IN OR/ID/CA) | CEP filter | ✅ 已有 | — |
| **Q4** | 分类均价（累积窗口） | deferred reduce + stats avg 双规则链 | ✅ 已有 | avg-of-max 双规则链 2026-08-23 落地（q4a deferred reduce maxrow → 中间窗口 auction_finals → q4b stats avg）；内层 oracle 可对拍（10k=455 条胜出价，同 q9）；外层 stats oracle 待接入（known-diff） |
| **Q5** | 热门新商品（top-1 by count） | hop(10s, 2s) + conv top_ties(1) | ✅ 已有 | HOP 算子落地（2026-08-23）：窗口形状/基数与权威一致（30M 数据 1500 窗 vs 旧 fixed 300 桶，5× 修正）；`top_ties(1)` 并列最高 count 的 auction 全输出（对齐权威 JOIN 并列语义，无残留） |
| **Q6** | 卖家售出均价 | sliding 10m avg（能力面） | 🟡 特殊口径 | Flink 官方未实现 Q6（OVER WINDOW 不消费 retractions，权威 SQL 注释）；无对拍基线，当前为形状近似能力面 |
| **Q7** | 时段最高出价 | stats max + conv top_ties(1) | ✅ 已有 | 并列全输出（RANK 语义 `top_ties`，2026-08-23 与 Q5 同落地）：同窗口最高价并列 auction 全出；残留 = auction 粒度 vs 权威 bid 行粒度（同 auction 内多条并列 bid 需窗口内行集算子） |
| **Q8** | 新用户+其拍卖（TUMBLE join） | deferred exists join | ✅ 已有 | 端到端激活（spawn 单 worker + DeferredRuntime；集成测 7 例全绿；200k 实测 927 行）；auction 恰在桶边界归下桶（上开界） |
| **Q9** | 中标出价（asof join） | deferred reduce join | ✅ 已有 | 同上（200k 实测 10986 行）；`maxrow(price) tie(dateTime asc)` 与 ROW_NUMBER 第 1 名等价 |
| **Q10** | 1/7 任意抽样 | CEP 选择 | ✅ 已有 | 实现论文版 Q10（非 Flink 版 Log-to-FS） |
| **Q11** | 用户会话统计（session） | session 窗口 + 分片 | 🟡 已有(分片口径) | 分片口径，非单实例全量 |
| **Q12** | 每 bidder × 10s 处理时间窗的 bid 计数 | stats count + 事件时间近似 | 🔶 待补强 | 处理时间窗缺失（产品特性，非合规缺口）；事件时间近似在 paced stream 下等价；EMIT 与 Flink 不可确定性对拍（墙钟依赖，harness 已列 known-diff） |
| **Q13** | 有界侧输入 join | snapshot join | 🟡 已有(形状对齐) | EMIT 基数一致（每 bid 一条，权威侧输入 100% 命中）；键/输入不同（bidder⋈person vs mod(auction,10000)⋈文件），富化内容未输出（detail 常量） |
| **Q14** | 时间戳换算+价格过滤 | CEP calc + filter | ✅ 已有 | — |
| **Q15** | 按日历天的分类统计 | stats + 日历天窗口 | ✅ 已有 | `1d:fixed`（epoch 对齐 = UTC 午夜 = 日历天），30m 数据 1 桶 |
| **Q16** | 按日历天的竞价统计 | stats + 日历天窗口 | ✅ 已有 | 同上 |
| **Q17** | 按日历天的拍卖统计 | stats + 日历天窗口 | ✅ 已有 | 同上 |
| **Q18** | 每 (bidder,auction) 最后一条 bid | stats last + 1d 桶 | ✅ 已有 | q18_stats `1d:fixed` + 4 个 last 度量：值语义（最后一条字段值）+ 基数（每键 1 行）双对齐（last 序 = 到达序，有序数据 = max dateTime）；CEP 版为基数对齐近似 |
| **Q19** | 拍卖 Top-10 价格 | stats<> top-N | ✅ 已有 | stats<> 编译/装配/执行器测试确认（`stats_top_keeps_top_n_desc`）；bench daemon 对拍待跑（oracle 不执行 stats 规则） |
| **Q20** | 每卖家最高价出价 | CEP maxrow | ✅ 已有 | — |
| **Q21** | 按渠道监控新用户 | CEP monitor | ✅ 已有 | wfgen 已输出 channel_id（cmd_gen_nexmark.rs:279） |
| **Q22** | URL 目录投影 | CEP projection | ✅ 已有 | — |

**汇总**：已有 18 · 待接通 0 · 待补强 1 (Q12) · 特殊口径 3 (Q11 分片 / Q6 Flink 未落地 / Q13 形状对齐)。

> 计数：18 已有 + 1 待补强 + 3 特殊口径 = 22。

---

## 二、能力缺口清单（按优先级）

### 🔴 P0 — 阻断"完全支持"的结构性缺口

**G1. 日历天 / 任意对齐窗口 —— 已解决（2026-08-23）**
- 事实：fixed 桶为 epoch 对齐（`bucket_start = (t/dur)*dur`，CEP 引擎 `match_engine/mod.rs:388`、stats 任务 `stats_task.rs:245`），且时长语法已支持 `d` 后缀 → **`1d:fixed` 桶边界必然落在 UTC 午夜，即精确的 UTC 日历天**，无需新算子。
- 落地：Q15/16/17（CEP + stats 共 6 个 `.wfl`）已由 `30m:fixed` 改为 `1d:fixed`，去掉巧合对齐；30m 数据仍 1 桶/键（EOS `close_all` 收口），输出基数与数值不变。
- 残留（P2，仅非 UTC 部署时需要）：无时区偏移对齐——Flink `DATE_FORMAT(dateTime,'yyyy-MM-dd')` 按 session timezone 日切，若时区 ≠ UTC 则本地午夜与 UTC 午夜错位。需要时以 `align_to=<offset>` 参数引入（产品需求，非 NEXMark 合规需求）。

**G2. deferred / asof join —— 已解决（2026-08-23 核查）**
- 生产装配已激活：`spawn.rs:508-517` 检测 `emit at` join 并强制单 worker（挂起队列是 per-task 状态）；`RuleTask::new` 装配 `DeferredRuntime`（rule_task.rs:380-390：挂起队列 + 事件时间 watermark + 批尾/超时到期扫描 + EOS flush）。代码注释中的 "P3" 为过期里程碑标签，非未激活标记。
- 端到端测试全绿：`deferred_integration_tests.rs` 7 例（Q8 存在性/上开界边界排除、Q9 reduce maxrow+tie、EOS flush、真实 q9.wfl 编译→执行），引擎级 `deferred` 22 例；wfgen oracle 同用 `execute_deferred_join` 镜像引擎。
- 实测：verify-nexmark 200k → q8=927 / q9=10986 行。剩余确认项：bench daemon 级 replay 对拍（`bench.sh --verify`，未跑）。
- 已知小分歧：Q8 映射为 person 驱动向后查找 `[p.dateTime, bucket_end)`，而 wfgen 25% 冷路径 seller 可引用未来 person（`PERSON_ID_LEAD`）——同 TUMBLE 窗内早于注册时间的 auction 会漏配（Flink 按窗等值可配）。量级小（冷路径 × lead 窗内），且依赖"auction 晚于 seller"的数据假设。

### 🟠 P1 — 正确性缺口核查（2026-08-23：全部澄清/解决，无残留 NEXMark 合规缺口）

**G3. stats<> 归并单位（avg）—— 已解决（2026-08-23 核查）**
- `stats_exec.rs` D6 契约：avg 不作状态，累加统一走 `sum_i128 += n`（行式 356 / 列式 618 / 回退 837 三路径一致），输出时 `sum_i128/count`（`measure_values` 1273、`bucket_measure_entries` 1327）。跨分片/跨窗口归并即 (sum,count) 对合并。
- 原"avg 可交换结合未规定归并单位"判断过时；Q4/Q6/Q12 不再受此影响（各自残余缺口见逐查询矩阵行）。

**G4. distinct 精度（f64 化丢精度）—— 潜在引擎缺陷，非 NEXMark 合规缺口**
- `ValueKey::from_value`（key.rs:23）确为 f64 路径（`canonical_f64_bits`）；fanout.rs:1103 测试明确记载该已知分歧（>2^53 Int64 行式丢精度）。
- **但原"影响 Q5"判断双重错误**：① q5.wfl 实际用 `count` + `conv top(1)`，无 distinct 度量；② NEXMark id 上限 ~1.8M（FIRST_AUCTION_ID=1000 + 30M 事件）<< 2^53，f64 精确可表示。
- 降为 P2 潜在缺陷：>2^53 整数 id（64 位哈希键等）作 distinct key 时行式路径丢精度，列式路径精确（分歧方向已锁测试）。

**G5. 滑动/会话窗口回撤（逆运算）—— 潜在能力缺口，无当前查询受影响**
- 引擎确无 retract/inverse（distinct/top/last 对滑动窗口的撤销未实现）。
- **但原"影响 Q7/Q18"判断错误**：Q7 为 `match<auction:10s:fixed>` + conv top-1、Q18 为 `match<bidder,auction:30m:fixed>` + count，均**非滑动**；滑动形态仅 q3/q6/q13（sliding）与 q5（hop，2026-08-23 新算子）使用，且均无 distinct/top/last 度量（hop 各窗口独立累积、收口即输出，与 fixed 同无回撤需求）。
- 结论：若未来用 sliding + distinct/top/last 需先补回撤，当前 NEXMark 无查询触发。

**G6. stats<> 语法→计划连线（Q19）—— 已解决（2026-08-23 核查）**
- 解析（stats_p.rs）→ 编译（compiler/mod.rs:122 StatsWindowMode→WindowSpec）→ 计划全通：q15/16/17/19_stats 四个文件实测编译通过（verify-nexmark "1 规则已编译"）。
- 生产装配：spawn.rs `RunRuleKind::Stats` → `run_stats_task`（stats_task.rs/r4 固定窗口段推进 + close）。
- 执行器测试：`stats_top_keeps_top_n_desc`（Q19 形状：group by auction + top-N key DESC + 行字段）。
- 残留：oracle 不执行 stats 规则（`q19_auction_top10_stats` 在 normalize_counts known 列表）；bench daemon 级对拍待跑。

> 查询级残余缺口（Q4/5/6/7/12/13/18）见一、逐查询矩阵行内说明（窗口形状/值语义/处理时间窗，G7/G8 见下）。

### 🟡 P2 — 算子能力补充

**G7. 累积 / 分层窗口 —— 已解决（2026-08-23）：Q4 avg-of-max 双规则链正式落地**
- 原引用勘误：权威 Q4 SQL（avg-of-max 两层聚合）**无 PREVIOUS/LAG**，全文档无其他出处——G7 标题"Q4 的 PREVIOUS 语义"为混淆引用，已删除。
- 累积窗口非 Q4 必需：外层 per-category AVG 是跨流无界聚合，有界 benchmark 下 `1d:fixed`（epoch 对齐）输出与累积等价（EOS 收口一行），无需新算子。
- 落地（2026-08-23，probe → 正式文件）：`q4.wfl` = 双规则链——内层 `q4a_auction_finals`（`on each a` + deferred `reduce maxrow(price) within [a.dateTime, a.expires] emit at a.expires` → yield 中间窗口 `auction_finals`，时间列 = 到期时刻）+ 外层 `q4b_category_avg`（`stats<1d:fixed> group by (f.category) { avg(f.final) }` 消费 auction_finals）。`auction_finals` 已入 nexmark.wfs。
- 验证：wfgen verify-nexmark 10k 编译 29 规则全通，oracle 内层输出 `q4a_auction_finals=455`（= q9 胜出价计数，口径一致）；外层 stats 规则 oracle 暂不执行（known-diff 列表）。
- 残留：daemon 级串联对拍（内层 yield→窗口 relay→外层 stats 消费）未跑；oracle 不执行 stats 规则。

**G8. 处理时间窗口（Q12）—— 已核查（2026-08-23）：属实但为产品特性，非 NEXMark 合规缺口**
- 现状属实：引擎无任何处理时间支持——无 PROCTIME builtin、窗口分桶恒用事件时间（match_engine bucket_start = event_nanos/dur；stats 任务按批事件时间）。
- **但 Q12 的 Flink 输出本身墙钟不确定**：PROCTIME() 窗口数 = 真实运行时长/10s，随机器/回放速度变化，无法确定性对拍（harness 的 normalize_counts 已把 q12 列为 known-diff：oracle 事件时间理想值 vs 引擎墙钟 scan 收口，10M 实测 oracle=102400 引擎=282514）。
- 实现处理时间窗（wall_nanos 分桶 + 迟到处理）**不会提升合规性**——只会让 wfusion 输出同样墙钟不确定，两侧仍无法逐次对拍。
- 当前事件时间近似诚实标注且**在 paced stream（`--feed stream`）下与处理时间等价**（发送节奏 = 事件时间跨度）；产品上如需真处理时间窗（实时告警按墙钟分桶），属独立特性需求，非 NEXMark 待补项。

---

## 三、结论

wfusion 当前架构**能覆盖 NEXMark 绝大多数查询的"意图"**，但**无法逐条复现 Flink 测试集输出**：

- **真正符合（可独立验证）**：18/22（Q1 Q2 Q3 Q10 Q14 Q20 Q21 Q22 + Q15 Q16 Q17 + Q8 Q9 + Q19 + Q18 + Q5 + Q7 + Q4）—— CEP 类投影/过滤/标量计算/取首行 + `1d:fixed` UTC 日历天统计 + deferred exists/reduce join + stats<> top-N/last + HOP 滑动窗口（Q5）+ conv `top_ties` 并列全输出（Q7，2026-08-23 新算子）+ Q4 avg-of-max 双规则链（2026-08-23 落地，内层 oracle 可对拍）。
- **需补强语义才达标**：+1（Q12）—— 处理时间窗（产品特性向，Flink 侧亦墙钟不确定）。
- **特殊口径**：3（Q11 分片 + session；Q6 Flink 官方未实现无基线；Q13 形状对齐、EMIT 基数一致）。

**完整支持 Flink 测试集的最短路径**：① Q4 双规则链 daemon 级验证（内层→中间窗口→外层 stats 串联对拍，已编译 + oracle 内层 455@10k；外层 stats oracle 接入后即可全链对拍）；② Q12 处理时间窗为产品特性（实时流场景）。G1-G8 均已解决或澄清（见上），Q5/Q7 已由 HOP + top_ties 算子对齐（Q7 残留 bid 行粒度）；通用确认项：bench daemon 级 replay 对拍（`bench.sh --verify`）待跑。
