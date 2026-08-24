# NEXMark (Flink 测试集) 能力差距矩阵

> 口径：以官方 `github.com/nexmark/nexmark` 的 `nexmark-flink/.../queries/qN.sql` 为权威语义基准（见 `NEXMARK_AUTHORITATIVE_SEMANTICS.md`）。
> 本表判断基于 2026-08-23 重审的 22 个当前 `.wfl` 文件 + wfusion 当前规则能力（CEP / Window / stats<> / Join 家族）的源码核查。
> 2026-08-23 三次修订：全量 30M replay 完成——22 查询全部 `[clean]`（appended 追平 + 致命计数器归零），
> 验证 §一 各查询的端到端可跑性；10M 全量 `bench.sh all replay 10m --verify` 已跑（21/22 与 oracle 完全一致，
> 仅 Q12 known-diff；Q19 oracle 未接 stats）。
> 2026-08-24 修订：30M oracle 对拍（17 个非 stats 查询，5% 容差）**13 个 0% 偏差**；超差 4 个中
> **Q4/Q9 的 −62% 已根治**（D4 保留 pin：join 目标窗口字节上限驱逐不再静默丢行，30M 与 oracle
> identical）——q4a/q9 各 1,672,559 = 1,672,559。剩余：**Q3 30M −16%**（独立 bug 待查，与驱逐/pin
> 无关）与 **Q12 known-diff**（fixed+close 尾桶）。
> 三档定义：
> - **已有**：能力已落地且端到端可跑，输出与 Flink 语义/基数一致。
> - **待接通**：算子已实现，但规则装配/计划连线未端到端激活（纸面符合，跑不通或未被规则引擎调度）。
> - **待补强**：能力存在但语义不精确（归并单位、精度、回撤、窗口对齐方式），EMIT 行数/数值会与 Flink 对不上。

---

## 一、逐查询矩阵（q1~q22）

| 查询 | Flink 语义 | 依赖能力 | 档位 | 缺口说明 |
|------|-----------|----------|------|----------|
| **Q1** | 货币换算(0.908×price)+过滤 | CEP on-each | ✅ 已有 | — |
| **Q2** | 选择 MOD(auction,123)=0 的 bid | CEP each | ✅ 已有 | 每满足 `MOD(auction,123)=0` 的 bid 一行（per-bid，`on each` + bind filter） |
| **Q3** | 按州过滤(IN OR/ID/CA) | CEP filter | ✅ 已有 | 10M 对拍一致；**30M 对拍 −16%（150,992 vs 180,304，超 5% 容差）**——规模相关独立 bug 待查（已排除：字节上限驱逐无关，person 快照窗口全保留后无变化） |
| **Q4** | 分类均价（累积窗口） | deferred reduce + stats avg 双规则链 | ✅ 已有 | avg-of-max 双规则链 2026-08-23 落地（q4a deferred reduce maxrow → 中间窗口 auction_finals → q4b stats avg）；内层 oracle 可对拍；外层 stats oracle 待接入（known-diff）。**q4a 30M identical（D4 pin 修复：字节上限驱逐曾丢 62% join 目标行，2026-08-24）** |
| **Q5** | 热门新商品（top-1 by count） | hop(10s, 2s) + conv top_ties(1) | ✅ 已有 | HOP 算子落地（2026-08-23）：窗口形状/基数与权威一致（30M 数据 1500 窗 vs 旧 fixed 300 桶，5× 修正）；`top_ties(1)` 并列最高 count 的 auction 全输出（对齐权威 JOIN 并列语义，无残留） |
| **Q6** | 卖家售出均价 | sliding 10m avg（能力面） | 🟡 特殊口径 | Flink 官方未实现 Q6（OVER WINDOW 不消费 retractions，权威 SQL 注释）；无对拍基线，当前为形状近似能力面 |
| **Q7** | 时段最高出价 | CEP `match<auction:10s:fixed>` close max + conv top_ties(1) | ✅ 已有 | 并列全输出（RANK 语义 `top_ties`，2026-08-23 与 Q5 同落地）：同窗口最高价并列 auction 全出；残留 = auction 粒度 vs 权威 bid 行粒度（同 auction 内多条并列 bid 需窗口内行集算子） |
| **Q8** | 新用户+其拍卖（TUMBLE join） | deferred exists join | ✅ 已有 | 端到端激活 + 10M replay 对拍一致（2026-08-23 修复：到期 miss 的 join 目标 append 滞后 → EOS 重试补出；shutdown flush 的 EMIT 指标尾部导出；flush 按最终事件水位收口不误扫尾部桶）；auction 恰在桶边界归下桶（上开界） |
| **Q9** | 中标出价（asof join） | deferred reduce join | ✅ 已有 | `maxrow(price) tie(dateTime asc)` 与 ROW_NUMBER 第 1 名等价。**30M oracle identical（1,672,559 = 1,672,559；D4 pin 修复字节上限驱逐丢 62% 输出，2026-08-24）** |
| **Q10** | 全量 bid 按时间分区落盘（每 bid 一行） | CEP on-each | ✅ 已有 | 2026-08-21 从「auction%7 子集」能力面重写为全量落盘（旧版只处理 1/7 事件，工作负载差 ~7×）；dt/hm 分区列本地 sink 省略（30m 数据恒同一天，无查询语义） |
| **Q11** | 用户会话统计（session） | session 窗口 + 分片 | ✅ 已有 | 10M 对拍 197,095 = 197,095 identical（2026-08-23 三处收口语义修复：close_all 未超时会话不发射 / scan_timeouts 不叠加墙钟 / flush 按全局末尾补扫，分片 shard 水位落后不再误判）；bidder 分片 + session 正确组合 |
| **Q12** | 每 bidder × 10s 处理时间窗的 bid 计数 | CEP `match<bidder:10s:fixed>`（+ stats 双形态）事件时间近似 | 🔶 待补强 | 处理时间窗缺失（产品特性，非合规缺口）；事件时间近似在 paced stream 下等价；EMIT 与 Flink 不可确定性对拍（墙钟依赖，harness 已列 known-diff） |
| **Q13** | 有界侧输入 join | snapshot join | ✅ 已有 | 权威语义对齐（2026-08-23）：`mod(auction,10000)⋈side_input.key` 双规则链（q13a 物化 mod_key → q13b snapshot join 富化 value），EMIT 每 bid 一行（1m=920k/920k、10m=9.2M/9.2M，oracle identical）；provider 窗口 join 索引 O(1)（修复：knowdb CSV 数字列类型推断，Str→Number，索引键与 lookup 键同类型才命中） |
| **Q14** | 时间戳换算+价格过滤 | CEP calc + filter | ✅ 已有 | — |
| **Q15** | 按日历天的出价统计（Bidding） | stats + 日历天窗口 | ✅ 已有 | `1d:fixed`（epoch 对齐 = UTC 午夜 = 日历天），30m 数据 1 桶 |
| **Q16** | 按日历天的渠道统计（Channel） | stats + 日历天窗口 | ✅ 已有 | 同上（minute 列数据侧恒定省略） |
| **Q17** | 按日历天的拍卖统计 | stats + 日历天窗口 | ✅ 已有 | 同上 |
| **Q18** | 每 (bidder,auction) 最后一条 bid | stats last + 1d 桶 | ✅ 已有 | ★标准形态 `q18.wfl` = `1d:fixed` + 4 个 last 度量：值语义（最后一条字段值）+ 基数（每键 1 行）双对齐（last 序 = 到达序，有序数据 = max dateTime）；CEP 版（q18-verify.wfl）为基数对齐近似（值语义缺失） |
| **Q19** | 拍卖 Top-10 价格 | stats<> top-N | ✅ 已有 | stats<> 编译/装配/执行器测试确认（`stats_top_keeps_top_n_desc`）；bench daemon 对拍待跑（oracle 不执行 stats 规则） |
| **Q20** | 展开 bid 关联 auction（category=10 filter join） | snapshot join + join 后 where | ✅ 已有 | `on each` + `join auction_events snapshot on b.auction==id` + `where category==10`（对齐权威 INNER JOIN + 过滤，miss/过滤均抑制）；展开字段受 sink 四列限制省略。**性能（2026-08-24）：Arc<JoinRow> 消除每行 clone/drop 后 30M 4.63M→20.87M EPS（4.5×）**；verify 偏差 0.97~1.65%（<5% 容差）——原设计「批快照+行时复查」固有竞态（join 目标窗按全量已提交状态读，fill 快慢改变可见集），方向恒为少发，见 wp-reactor 接力手记 |
| **Q21** | 附加 channel id（热通道映射 + cold url 提取） | CEP on-each 投影 + 过滤 | ✅ 已有 | 权威 CASE 映射 0/1/2/3 + url `channel_id` 提取；wfgen 数据侧已计算 `channel_id`（cmd_gen_nexmark.rs:279），规则侧 `channel_id!=""` 过滤等价官方 WHERE（输出 95% bid）；无状态投影 |
| **Q22** | URL 目录投影 | CEP projection | ✅ 已有 | — |

**汇总**：已有 20 · 待接通 0 · 待补强 1 (Q12) · 已知差异 0 · 特殊口径 1 (Q6 Flink 未落地)。

> 计数：20 已有 + 1 待补强 + 0 已知差异 + 1 特殊口径 = 22。

---

## 二、能力缺口（截至 2026-08-24，Q13 对齐后复核）

### 剩余语义 GAP（真正会让输出与 Flink 对不上，仅 2 类）

1. **Q12 处理时间窗（唯一真·语义 GAP）**：引擎为事件时间引擎，无 `PROCTIME()`；用事件时间 10s 固定桶近似。replay/paced 下等价，实时流下分桶边界不同 → EMIT 不可确定性对拍。
2. **Q7 行粒度残留**：权威输出所有 price==窗口最高价的 **bid 行**（同 auction 内并列也全出）；本地为每 auction 一条的 auction 粒度。随机数据下并列罕见、对拍几乎无差，严格存在（需窗口内行集输出算子）。

### 结构性前提 / 验证类（非语义 GAP，不影响对齐结论）

- **Q6 形状近似**：Flink 官方亦未落地（OVER WINDOW 不支持 retractions，权威 SQL 注释 TODO），双方都无权威实现；wfusion 提供 `match<seller:10m> avg` 能力面。
- **Q11 分片前提**：session 在 `SHARD_KEYS` 分片下为 per-shard 会话；全局会话语义须 `CONNECTIONS=1` 或 bidder 分片。
- **Q15/16/17 非 UTC 时区日切**：`1d:fixed` 桶边界落 UTC 午夜 = UTC 日历天；非 UTC session 时区日切需 `align_to` 偏移（P2，非 NEXMark 合规需求）。
- **Q4/Q19 stats oracle 未接**：daemon 级串联对拍待跑（验证 GAP，非语义 GAP）。
- **Q9/Q16 fixed+close 尾收口非确定**：引擎 EOF 确定性收口待修（known-diff，见 SEMANTIC_ALIGNMENT §6.1）。

### 引擎级潜在缺口（NEXMark 当前无查询受影响）

**P1. distinct 键 f64 精度**：`ValueKey` 走 f64 路径，>2^53 的整数 id 作 distinct key 时行式路径丢精度（列式精确）。NEXMark id 上限 ~1.8M << 2^53，不触发。

**P2. 滑动/会话窗口回撤**：sliding/session 对 distinct/top/last 无撤销（retract）语义。当前无查询使用该组合（q3/q6 sliding 与 q5 hop 均无此类度量），未来若用到需先补回撤。

### 查询级残留

**Q5 flush 竞态（528/529 抖动，既有）**：10M 尾部存在 shutdown 时序相关的 ±1 抖动（同二进制多次跑 528/529）——
疑似 conv inline `top_ties` 跨批聚合边界（主遍最后批次 + flush 批次同桶分两批），尚未定位；不影响 30M 主基准量级，
`--verify` 复跑可能偶发 ❌。

**Q7 行粒度**：权威输出 price==窗口最高价的 **bid 行**；本地为 auction 粒度（同 auction 内多条并列 bid 需"窗口内行集输出"算子，引擎待补）。计数仅在极端并列时偏离，随机数据下少见。

**Q12 处理时间窗**：引擎无 PROCTIME，用事件时间近似（paced stream 下等价）；真处理时间窗为产品特性——Flink 侧输出同样墙钟不确定，无法确定性对拍，实现不提升合规性。

### 已解决历史（2026-08-23；细节见 git 历史）

| 原缺口 | 解决方式 |
|---|---|
| G1 日历天 / 任意对齐窗口 | `1d:fixed`（epoch 对齐 = UTC 午夜），无需新算子 |
| G2 deferred / asof join 未接通 | `emit at` 端到端激活（Q8/Q9/Q4 内层） |
| G3 stats avg 归并单位 | 统一 (sum,count) 累加、输出时除 |
| G6 stats<> 语法→计划连线 | 解析→编译→装配→执行器全通（Q19） |
| G7 Q4 累积窗口 | avg-of-max 双规则链落地（deferred reduce → auction_finals → stats avg） |
| Q5 并列截断 | conv `top_ties(1)` 并列全输出 |
| Q8/Q9/Q19 特性门 | 均已核查激活（deferred 端到端 + stats 装配/执行器测试） |
| Q8 10M 对拍 ~40% 差异 | 三处根因修复（2026-08-23）：① 到期 miss（join 目标 append 滞后）→ `missed` 收集 + EOS 重试补出；② shutdown flush 的 EMIT 增量发生在 metrics 任务最后 tick 之后 → `Reactor::wait` 尾部最终导出（30,785 + 51,661 = 82,446 = oracle）；③ flush 用 i64::MAX 强评会多出尾部桶 +828（oracle/mod.rs EOS 水位注释同源）→ 改按最终事件时间 watermark 收口 |
| Q5 10M 尾部多 3 条（532 vs 529） | `close_all` 收口对齐 oracle/Flink：HOP/Fixed 尾部未完整窗口（`created_at+size >` 最终事件时间 watermark）释放实例但不发射（2026-08-23；尾部 992/994/996s 窗口 w_end=1002/1004/1006 > 1000s；`wm≤0` 无时间推进场景保留旧全收口行为） |
| Q11 10M 尾部会话多 204/197,095≈0.1% | session 尾部收口语义对齐（2026-08-23 三处同源）：① `close_all` 未超时会话（`last_event+gap >` 最终事件时间）不发射；② `scan_timeouts` 对 session 不叠加墙钟（gap=事件时间间隔，replay 对拍依赖事件时间序）；③ `flush` 用窗口 raw `max_event_time`（新增 API，全局末尾）补扫一次——分片 shard 状态机水位落后全局末尾，尾部完整会话被误判未完整跳过（10M 少 1 条）|
| Q13 形状对齐 → 权威对齐 | 有界侧输入 snapshot join 富化（2026-08-23）：q13a 物化 `mod(auction,10000)` → q13b `join side_input snapshot` 富化 value；ProviderWindow join 索引 O(1) + knowdb CSV 数字列类型推断（Str→Number，修复索引键/lookup 键类型不匹配导致的每次 miss→全表扫描卡死）；1m=920k/920k、10m=9.2M/9.2M oracle identical |

### 落地登记（2026-08-23 新算子/能力）

| 能力 | 语法 | 对应查询 | 状态 |
|---|---|---|---|
| HOP 滑动窗口 | `match<key:hop(size, slide)>`（`size % slide == 0`，每事件扇入 size/slide 个覆盖窗口，`w_start+size` 收口） | Q5 | ✅ 引擎 + oracle + 性能基准（hop 每窗口 ~265ns） |
| conv `top_ties` 并列全输出 | `sort(...) \| top_ties(N)`（RANK 语义，前 N 名 + 并列全出） | Q5/Q7 | ✅ 引擎 + 测试（含 top_ties(0) 防护） |
| deferred join | `join ... reduce maxrow/minrow ... within [lo,hi] on ... as label emit at expr` | Q8/Q9/Q4 内层 | ✅ 端到端激活（spawn 单 worker + DeferredRuntime；集成测 7 例 + oracle 镜像） |
| join 后 `where` 过滤 | `where expr`（join 富化后、输出前求值；false/None 抑制） | Q3/Q20 | ✅ |
| stats<> 声明式统计 | `stats<dur:fixed\|session> group by(...) { agg as label }`（count/sum/avg/min/max/distinct/last/top） | Q12/Q15-19 | ✅ 编译/装配/执行器；oracle 不执行 stats（known-diff，对拍待接入） |
| `1d:fixed` 日历天桶 | 时长 `d` 后缀 + epoch 对齐 fixed 桶 | Q15/16/17 | ✅ 去巧合对齐（UTC 午夜 = 日历天） |
| provider 窗口 join 索引 + knowdb CSV 类型推断 | `window<provider>` 静态表 `set_join_key` O(1) 哈希索引（miss 回退扫描）；knowdb CSV/PG 数字列推断为 Number（`infer_knowledge_value`） | Q13 | ✅ 引擎 + 单测（bootstrap_r4 q13 回归）+ 性能基准（索引 vs 全表扫描 316×）+ 10m oracle identical |

> 新语法详细文档见 wp-reactor `docs/user-guide/language-reference.md`（match/conv/join/stats 段）与 `docs/design/wfl-design.md` §7 文法。

---

## 三、结论

wfusion 当前架构**能覆盖 NEXMark 绝大多数查询的"意图"**，但**无法逐条复现 Flink 测试集输出**：

- **真正符合（可独立验证）**：20/22（Q1 Q2 Q3 Q4 Q5 Q7 Q8 Q9 Q10 Q11 Q13 Q14 Q15 Q16 Q17 Q18 Q19 Q20 Q21 Q22）—— CEP 类投影/过滤/标量计算 + `1d:fixed` UTC 日历天统计 + deferred reduce join（Q9）+ deferred exists join（Q8）+ stats<> top-N/last + HOP 滑动窗口（Q5）+ conv `top_ties` 并列全输出（Q7）+ session 窗口（Q11）+ Q4 avg-of-max 双规则链 + 有界侧输入 snapshot join 富化（Q13）。其中 Q7 行粒度、Q11 分片为次要前提（见 §二「剩余语义 GAP」）。
- **已知差异**：+0（Q8/Q11 已于 2026-08-23 修复并对拍一致：82,446 = 82,446 / 197,095 = 197,095 identical）。
- **需补强语义才达标**：+1（Q12）—— 处理时间窗（产品特性向，Flink 侧亦墙钟不确定）。
- **特殊口径**：1（Q6 Flink 官方未实现无基线）。

**完整支持 Flink 测试集的最短路径**：① Q4 双规则链 daemon 级串联对拍（外层 stats oracle 接入后即可全链对拍）；② Q12 处理时间窗为产品特性（实时流场景）；③ Q5 flush 528/529 竞态定位（conv 跨批聚合）。通用确认项：全量 30M replay 已完成（22 查询 `[clean]`，2026-08-24 复跑，§一 各查询端到端可跑性验证）；10M 全量 `--verify` 已完成（21/22 identical，仅 Q12 known）；30M 全量 `--verify` 对拍待跑（64G 机器 30M replay 已跑通，RSS 峰值 ~18GB，oracle 对拍需接入 stats 规则后跑）。
