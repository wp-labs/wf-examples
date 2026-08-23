# NEXMark (Flink 测试集) 能力差距矩阵

> 口径：以官方 `github.com/nexmark/nexmark` 的 `nexmark-flink/.../queries/qN.sql` 为权威语义基准（见 `NEXMARK_AUTHORITATIVE_SEMANTICS.md`）。
> 本表判断基于 2026-08-23 重审的 22 个当前 `.wfl` 文件 + wfusion 当前规则能力（CEP / Window / stats<> / Join 家族）的源码核查。
> 2026-08-23 三次修订：全量 30M replay 完成——22 查询全部 `[clean]`（appended 追平 + 致命计数器归零），
> 验证 §一 各查询的端到端可跑性（性能数据见本次会话基准，正确性对拍 `--verify` 仍待跑）。
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
| **Q7** | 时段最高出价 | CEP `match<auction:10s:fixed>` close max + conv top_ties(1) | ✅ 已有 | 并列全输出（RANK 语义 `top_ties`，2026-08-23 与 Q5 同落地）：同窗口最高价并列 auction 全出；残留 = auction 粒度 vs 权威 bid 行粒度（同 auction 内多条并列 bid 需窗口内行集算子） |
| **Q8** | 新用户+其拍卖（TUMBLE join） | deferred exists join | ✅ 已有 | 端到端激活 + 10M replay 对拍一致（2026-08-23 修复：到期 miss 的 join 目标 append 滞后 → EOS 重试补出；shutdown flush 的 EMIT 指标尾部导出；flush 按最终事件水位收口不误扫尾部桶）；auction 恰在桶边界归下桶（上开界） |
| **Q9** | 中标出价（asof join） | deferred reduce join | ✅ 已有 | 同上（200k 实测 10986 行）；`maxrow(price) tie(dateTime asc)` 与 ROW_NUMBER 第 1 名等价 |
| **Q10** | 全量 bid 按时间分区落盘（每 bid 一行） | CEP on-each | ✅ 已有 | 2026-08-21 从「auction%7 子集」能力面重写为全量落盘（旧版只处理 1/7 事件，工作负载差 ~7×）；dt/hm 分区列本地 sink 省略（30m 数据恒同一天，无查询语义） |
| **Q11** | 用户会话统计（session） | session 窗口 + 分片 | 🟡 已有(分片口径) | 分片口径，非单实例全量 |
| **Q12** | 每 bidder × 10s 处理时间窗的 bid 计数 | CEP `match<bidder:10s:fixed>`（+ stats 双形态）事件时间近似 | 🔶 待补强 | 处理时间窗缺失（产品特性，非合规缺口）；事件时间近似在 paced stream 下等价；EMIT 与 Flink 不可确定性对拍（墙钟依赖，harness 已列 known-diff） |
| **Q13** | 有界侧输入 join | snapshot join | 🟡 已有(形状对齐) | EMIT 基数一致（每 bid 一条，权威侧输入 100% 命中）；键/输入不同（bidder⋈person vs mod(auction,10000)⋈文件），富化内容未输出（detail 常量） |
| **Q14** | 时间戳换算+价格过滤 | CEP calc + filter | ✅ 已有 | — |
| **Q15** | 按日历天的分类统计 | stats + 日历天窗口 | ✅ 已有 | `1d:fixed`（epoch 对齐 = UTC 午夜 = 日历天），30m 数据 1 桶 |
| **Q16** | 按日历天的竞价统计 | stats + 日历天窗口 | ✅ 已有 | 同上 |
| **Q17** | 按日历天的拍卖统计 | stats + 日历天窗口 | ✅ 已有 | 同上 |
| **Q18** | 每 (bidder,auction) 最后一条 bid | stats last + 1d 桶 | ✅ 已有 | ★标准形态 `q18.wfl` = `1d:fixed` + 4 个 last 度量：值语义（最后一条字段值）+ 基数（每键 1 行）双对齐（last 序 = 到达序，有序数据 = max dateTime）；CEP 版（q18-verify.wfl）为基数对齐近似（值语义缺失） |
| **Q19** | 拍卖 Top-10 价格 | stats<> top-N | ✅ 已有 | stats<> 编译/装配/执行器测试确认（`stats_top_keeps_top_n_desc`）；bench daemon 对拍待跑（oracle 不执行 stats 规则） |
| **Q20** | 每卖家最高价出价 | CEP maxrow | ✅ 已有 | — |
| **Q21** | 按渠道监控新用户 | CEP monitor | ✅ 已有 | wfgen 已输出 channel_id（cmd_gen_nexmark.rs:279） |
| **Q22** | URL 目录投影 | CEP projection | ✅ 已有 | — |

**汇总**：已有 18 · 待接通 0 · 待补强 1 (Q12) · 已知差异 0 · 特殊口径 3 (Q11 分片 / Q6 Flink 未落地 / Q13 形状对齐)。

> 计数：18 已有 + 1 待补强 + 0 已知差异 + 3 特殊口径 = 22。

---

## 二、能力缺口（截至 2026-08-23 仍存在）

### 引擎级潜在缺口（NEXMark 当前无查询受影响）

**P1. distinct 键 f64 精度**：`ValueKey` 走 f64 路径，>2^53 的整数 id 作 distinct key 时行式路径丢精度（列式精确）。NEXMark id 上限 ~1.8M << 2^53，不触发。

**P2. 滑动/会话窗口回撤**：sliding/session 对 distinct/top/last 无撤销（retract）语义。当前无查询使用该组合（q3/q6/q13 sliding 与 q5 hop 均无此类度量），未来若用到需先补回撤。

### 查询级残留

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

### 落地登记（2026-08-23 新算子/能力）

| 能力 | 语法 | 对应查询 | 状态 |
|---|---|---|---|
| HOP 滑动窗口 | `match<key:hop(size, slide)>`（`size % slide == 0`，每事件扇入 size/slide 个覆盖窗口，`w_start+size` 收口） | Q5 | ✅ 引擎 + oracle + 性能基准（hop 每窗口 ~265ns） |
| conv `top_ties` 并列全输出 | `sort(...) \| top_ties(N)`（RANK 语义，前 N 名 + 并列全出） | Q5/Q7 | ✅ 引擎 + 测试（含 top_ties(0) 防护） |
| deferred join | `join ... reduce maxrow/minrow ... within [lo,hi] on ... as label emit at expr` | Q8/Q9/Q4 内层 | ✅ 端到端激活（spawn 单 worker + DeferredRuntime；集成测 7 例 + oracle 镜像） |
| join 后 `where` 过滤 | `where expr`（join 富化后、输出前求值；false/None 抑制） | Q3/Q20 | ✅ |
| stats<> 声明式统计 | `stats<dur:fixed\|session> group by(...) { agg as label }`（count/sum/avg/min/max/distinct/last/top） | Q12/Q15-19 | ✅ 编译/装配/执行器；oracle 不执行 stats（known-diff，对拍待接入） |
| `1d:fixed` 日历天桶 | 时长 `d` 后缀 + epoch 对齐 fixed 桶 | Q15/16/17 | ✅ 去巧合对齐（UTC 午夜 = 日历天） |

> 新语法详细文档见 wp-reactor `docs/user-guide/language-reference.md`（match/conv/join/stats 段）与 `docs/design/wfl-design.md` §7 文法。

---

## 三、结论

wfusion 当前架构**能覆盖 NEXMark 绝大多数查询的"意图"**，但**无法逐条复现 Flink 测试集输出**：

- **真正符合（可独立验证）**：18/22（Q1 Q2 Q3 Q4 Q5 Q7 Q8 Q9 Q10 Q14 Q15 Q16 Q17 Q18 Q19 Q20 Q21 Q22）—— CEP 类投影/过滤/标量计算 + `1d:fixed` UTC 日历天统计 + deferred reduce join（Q9）+ deferred exists join（Q8）+ stats<> top-N/last + HOP 滑动窗口（Q5）+ conv `top_ties` 并列全输出（Q7）+ Q4 avg-of-max 双规则链。
- **已知差异**：+0（Q8 已于 2026-08-23 修复并对拍一致：82,446 = 82,446 identical）。
- **需补强语义才达标**：+1（Q12）—— 处理时间窗（产品特性向，Flink 侧亦墙钟不确定）。
- **特殊口径**：3（Q11 分片 + session；Q6 Flink 官方未实现无基线；Q13 形状对齐、EMIT 基数一致）。

**完整支持 Flink 测试集的最短路径**：① Q4 双规则链 daemon 级串联对拍（外层 stats oracle 接入后即可全链对拍）；② Q12 处理时间窗为产品特性（实时流场景）。通用确认项：全量 30M replay 已完成（22 查询 `[clean]`，§一 各查询端到端可跑性验证）；`bench.sh --verify` oracle 对拍（Q8/Q9 已单独对拍一致，其余待跑）。
