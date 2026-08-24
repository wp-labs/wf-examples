# NEXMark 语义对齐说明（wfusion vs 白皮书 / Flink）

> 用途：PK 对比前必须先确认"比的是同一个查询"。本文记录 wfusion 各 NEXMark 查询
> 与标准 NEXMark / 阿里白皮书（Flink）的**语义对齐状态、对齐逻辑与验证锚点**，
> 防止拿错语义的数字做 PK 结论（q3 曾因此出现 1.4× 假象，见 §5）。
> 与 `../README.md`（套件结构）、`OSS_VVR_BASELINE.md`（OSS/VVR 基线）、
> `NEXMARK.md`（基准背景/数据/正确性）互为配套。

## 1. 为什么需要语义对齐

- **分母口径本来就一致**：白皮书 RPS = 100M 全量输入 ÷ 用时；wfusion EPS = 三流
  appended 合计（如 30M/30M）÷ 用时——两边分母都含全部输入流（含查询不消费的 bid）。
- **差异在规则实际处理的量与算子语义**：过滤条件、join 方向、窗口类型、输出字段。
- **反例**：q3 旧版**无 `category = 10` 过滤** → 全部 auction 都驱动 join，工作量过重
  且语义不对 → 与白皮书标准 Q3 对比出的 1.4× 无意义（已修复，见 §5）。

## 2. 对齐判定标准（三层递进）

1. **过滤/投影**：bind filter（`events { a : win && expr }`）下推与 SQL `WHERE` 等价
   （Flink 同样下推，两边下游只见过滤后子集）。
2. **连接语义**：join 键一致；方向可不同（SQL 双流双向 vs 单边驱动 + 快照查表），
   只要数据保证命中（NEXMark 保证 seller/bidder 存在于 person）→ 输出集相同。
3. **输出与计数**：每输入行输出行数一致（`on event` fire/reset 语义）、EMIT 数
   与标准语义下的期望一致。

## 3. 对齐验证工作流

```bash
# 1) 规则引擎 oracle：产出该查询应 EMIT 数（真实 .wfl 规则，非手写模拟）
wfgen verify-nexmark <N> --query qN
# 2) 引擎实测：bench.sh 跑 30M，EMIT + [clean]
./bench.sh qN replay 30m
# 3) wfgen 内对拍：oracle == 引擎（git-diff 同款分层，退出码 0=一致 / 1=有差异）
./bench.sh qN replay 30m --verify
# 4) 期望值合理性抽查（如 q3 = auction 总数 / 26）
```

对拍锚点：oracle EMIT == 引擎 EMIT 精确相等，且数值符合语义推导（如 q3 68,979 ≈
1.8M auction ÷ 26）。

## 4. 各查询语义状态表（当前）

> 参考系：nexmark-flink 官方 `qN.sql`（逐条核实见 `REVIEW_FLINK_CONFORMANCE_2026-08-23.md`）。
> 状态三级：✅ 对齐 / ⚠️ 部分（已声明近似）/ ❌ 能力面（cap，非 Flink 语义）。

| 查询 | 对齐状态 | 标准语义（nexmark-flink qN.sql） | wfusion 语义 | 差异说明 |
|---|---|---|---|---|
| q1 | ✅ 对齐（2026-08-21） | Currency Conversion：每 bid 一行 + `0.908*price` | `on each` + `score(0.908*b.price)` | 每 bid 一条，换算在 score（fmt 进 detail 曾致 EPS 减半，已改）；计数等价 |
| q2 | ✅ 对齐（2026-08-21） | `MOD(auction,123)=0` 每行输出 | `on each` + bind filter | 每满足条件 bid 一条（旧 match per-auction 去重基数差一个量级，已修）；EMIT 226,103 对拍 identical |
| q3 | ✅ 对齐（2026-08-21：join 后 `where` 补齐州过滤） | person⋈auction join + `category=10` + `state∈(OR,ID,CA)` | auction 驱动（`category==10` 下推）+ snapshot join person + join 后 `where person_events.state in ("OR","ID","CA")` | 州过滤经 join 后 `where` 补齐（false/None 抑制输出 = INNER JOIN 丢行）；**30M 对拍 −16%（2026-08-24，规模相关 bug 待查）** |
| q4 | ✅ 对齐（2026-08-23：avg-of-max 双规则链落地） | bid⋈auction 均价（两层：每 auction max → 按 category avg） | 双规则链：deferred `reduce maxrow(price) within [a.dateTime, a.expires]`（Q9 同款）→ 中间窗口 `auction_finals` → `stats<1d:fixed> group by(category) avg(f.final)` | 双规则链正式落地（q4a+q4b + nexmark.wfs `auction_finals`，2026-08-23）；verify 10k 编译 29 规则全通、oracle 内层 455 条（= q9 口径）；残留：外层 stats oracle 不执行（known-diff），daemon 级串联对拍待跑；旧 `match<category:10m:fixed>` 直接 avg 面为度量错 + 基数 ×3，已废（见 §5） |
| q5 | ✅ 对齐（2026-08-23：HOP + top_ties 落地） | Hot Items：HOP(2s,10s) 每窗口 bid 数最多的 auction（top-1 by count，并列全输出） | `match<auction:hop(10s, 2s)>` + `and close` count + `conv { sort(-n) | top_ties(1) }` | 窗口形状/基数与权威一致（30M 数据 1500 窗 vs 旧 fixed 300 桶）；`top_ties(1)` 并列最高 count 的 auction 全输出（对齐权威 JOIN 并列语义，无残留） |
| q6 | 🟡 无权威基线（Flink 官方未实现） | 每 seller 最近 10 笔成交胜出价均值（权威 ROW_NUMBER 胜出价 + OVER ROWS 10 PRECEDING；官方注释 OVER 不支持 retractions **未落地**） | **join-then-key**：`match<seller:10m>` + `on event` **avg**（每 seller 均价） | Flink 本身不运行 Q6 → 无对拍基线；当前为形状近似能力面（仅自测） |
| q7 | ✅ 对齐（2026-08-23：top_ties 并列全输出） | TUMBLE(10s) 每窗口全局最高价 bid（跨 auction，并列全输出） | `match<auction:10s:fixed>` + close max + `conv { sort(-m) | top_ties(1) }`（auction 键可并行，批内取全局最高并列全出） | 局部 max→批内全局 max 语义等价（max 分配律）；残留 = auction 粒度 vs 权威 bid 行粒度（同 auction 内多条并列 bid 需窗口内行集算子） |
| q8 | ✅ 对齐（2026-08-23：deferred exists join 端到端激活） | person TUMBLE(10s) ⋈ auction TUMBLE(10s) 同窗 join（注册且创建拍卖的人） | `on each` + deferred `join auction_events within [p.dateTime, <bucket_end(p,10s)) emit at bucket_end(p,10s)`（存在性） | 每 (person×桶) 一行；auction 恰在桶边界 → 归下桶（上开界，`deferred_q8_boundary_auction_excluded` 单测覆盖）；已知小分歧：同窗内早于注册的 auction 漏配（25% 冷 seller 可引用未来 person） |
| q9 | ✅ 对齐（2026-08-23：deferred reduce 端到端激活） | **胜出出价（Winning Bids）**：每 auction 最高价 bid（ROW_NUMBER price DESC, dateTime ASC） | `on each` + deferred `join bid_events reduce maxrow(price) tie(dateTime asc) within [a.dateTime, a.expires] emit at a.expires` + `as winner` | 与 ROW_NUMBER 第 1 名等价；无 bid 不输出；每 auction 至多一条；30M 对拍 identical（2026-08-24 D4 pin 修复） |
| q10 | ✅ 对齐（2026-08-21 重写） | Log to File System：全量 bid 落盘 | `on each` 全量 bid 每行输出（旧 1/7 子集作废） | EMIT = 全部 bid（权威全量）；dt/hm 分区列省略（30m 数据单天无查询语义） |
| q11 | ⚠️ 部分（2026-08-21：gap 10s + 每会话一条带 count） | User Sessions：SESSION(10s) 每会话输出 bid_count | session(10s) + `and close` count（每会话一条，detail 带 count） | 计数口径对齐（旧 on-event 每行 fire）；bench 按 auction 分片时为 per-shard 会话（全局语义须 CONNECTIONS=1）；尾部会话收口 known |
| q12 | ⚠️ 部分（已声明近似） | Processing Time Windows：每 bidder × 10s 窗口计数（全量输出） | fixed 10s + `and close` count（键=bidder） | 处理时间窗口用事件时间近似（replay 同步）；fixed+close 收口非确定（见 §6） |
| q13 | ✅ 对齐（2026-08-23） | 有界侧输入 join（mod(auction,10000)=key） | 双规则链：q13a 物化 `mod(auction,10000)` → q13b `join side_input snapshot` 富化 value | EMIT 每 bid 一行（1m=920k/920k、10m=9.2M/9.2M oracle identical）；provider 窗口 join 索引 O(1) |
| q14 | ✅ 对齐（2026-08-21 重写） | Calculation：0.908*price + CASE HOUR 分型 + count_char UDF + 价格过滤 | `on each` + bind 价格过滤 + `strftime("%H")` 分型 + `count_char`（新增 UDF） | 每行输出对齐；bidTimeType/c_counts 拼入 detail（sink 四列限制） |
| q15 | ✅ 对齐（2026-08-23：`1d:fixed` UTC 日历天） | Bidding Statistics Report：按天 12 列统计（count/distinct × 价格档） | `match<:1d:fixed>` 全局统计 + 12 close measure | `1d:fixed` 桶 = UTC 日历天（epoch 对齐 = UTC 午夜），30m 数据 1 桶 → 全局=按天；性能（2026-08-24）：空键 stats 输入分区分片 + EOS 归并后 30M 4.42M→7.75M EPS |
| q16 | ✅ 对齐（2026-08-23：`1d:fixed`） | Channel Statistics Report：按 channel/天 15 列统计 | `match<channel:1d:fixed>` + 12 close measure | minute 列省略（数据侧常量）；fixed+close 尾部收口 known |
| q17 | ✅ 对齐（2026-08-23：`1d:fixed`） | Auction Statistics Report：按 auction/天 count/min/max/avg/sum + 价格档 | `match<auction:1d:fixed>` + 8 close measure | 每 auction 一行对齐；fixed+close 尾部收口 known |
| q18 | ✅ 对齐（2026-08-23：stats last + 1d 桶） | Find last bid：每 (bidder,auction) 最后一条（dateTime DESC dedup） | `stats<1d:fixed> group by (b.bidder, b.auction)` + 4 个 `last` 度量（price/channel/url/dateTime） | 值语义（最后一条字段）+ 基数（每键 1 行）双对齐；last 序 = 到达序（有序数据 = max dateTime）；CEP 版（`match<bidder,auction:30m:fixed>` + count）为基数对齐近似 |
| q19 | ✅ 对齐（2026-08-23：stats<> top-N 编译/装配/执行器确认） | Auction TOP-10 Price：每 auction 价格 top-10（ROW_NUMBER price DESC） | `stats<30m:fixed> group by(b.auction) { top(10, b.price) }`（每键有界 top-N，close 按 rank 序逐条输出） | `stats_top_keeps_top_n_desc` 覆盖 Q19 形状；oracle 不执行 stats 规则，bench daemon 对拍待跑 |
| q20 | ✅ 对齐（2026-08-24：snapshot join + join 后 where 落地） | Expand bid with auction（category=10 filter join） | `on each` + `join auction_events snapshot on b.auction==id` + `where category==10`（miss/过滤均抑制 = INNER JOIN） | 展开字段受 sink 四列限制省略；verify 偏差 0.97~1.65%（<5% 容差，批快照+行时复查固有竞态，方向恒为少发） |
| q21 | ✅ 对齐（2026-08-21 重写） | **Add channel id**：每 bid 输出 channel_id（CASE WHEN 热通道 0/1/2/3 + REGEXP_EXTRACT url） | `on each` 投影 `b.channel_id`（数据侧计算） | 见 §5：旧 anti join 能力面作废；wfl 无 CASE WHEN/正则，channel_id 在 wfgen 生成时计算（等价 SQL 值） |
| q22 | ✅ 对齐（2026-08-21 重写） | **URL Directories**：每 bid 取 url split('/') 索引 3/4/5 | `on each` + `split(b.url,"/")` + `mvindex` 投影 | 语义对齐（0 基 split + mvindex 等价 SPLIT_INDEX） |

> 判定依据：各 `../models/queries/*.wfl` 头部注释 + 对拍验证。q6 无基线、q12 近似、
> q20 需校验，PK 表格中须标注（见 §6）。

## 5. 案例结论摘要（对齐过程详见 git 历史，2026-08-21/23 commit）

> 以下为各查询语义对齐的**当前结论与仍有效的残余差异**；逐步对齐过程、验证数字与
> 优化细节（Q12 列式 close 执行器/advance 优化、Q22 `let` 绑定、Q20 Arc<JoinRow>、
> Q15 stats 分片、Q5 hop conv 分片等）已随历史清理归档，见 git 历史。

| 案例 | 对齐内容（当前语义） | 关键残余 / 注意 |
|---|---|---|
| Q3 | `category=10` 过滤下推 + snapshot join + join 后 `state∈(OR,ID,CA)` 过滤 | join 方向（person 不进规则管道）、快照 miss 不 drop（NEXMark 无 miss 实践等价）、alert 不携带 name/city/state；**30M 对拍 −16%（2026-08-24，规模相关 bug 待查）** |
| Q9 | deferred `reduce maxrow(price) tie(dateTime asc) within [a.dateTime,a.expires] emit at a.expires`（= ROW_NUMBER 胜者） | 旧 seller 计数面已废；无 bid 不输出；每 auction 至多一条；30M 对拍 identical（D4 保留 pin 修复后） |
| Q4 | 双规则链：deferred reduce maxrow → 中间窗 `auction_finals` → stats avg（avg-of-max） | 外层 stats oracle 未接入（known-diff）；旧 join-then-key 单链面已废 |
| Q12 | bidder × 10s fixed count（Processing Time Windows 的事件时间近似） | fixed+close 收口非确定（10M 差 1.1%，见 §6.1）；全量输出是 Flink 语义固有成本 |
| Q22 | url `split('/')` + `mvindex` 3/4/5 投影（数据侧 url 已改官方格式） | `let` 绑定避免重复 split（30M EPS 7.7M）；mvindex/concat 仍 ~5 次小分配/事件 |
| Q21 | `channel_id` 数据侧计算 + `on each` 投影（wfl 无 CASE WHEN/正则） | 值等价官方 SQL；若 wfl 支持正则可改规则内计算对齐「计算位置」 |

## 6. 未对齐查询的处理原则

- **q6**：白皮书未发布基线，仅自测，PK 表不列倍数。
- **q4**：已对齐（avg-of-max 双规则链，见 §4）；外层 stats oracle 未接入为 known-diff。
- **q9**：已对齐（deferred reduce，见 §4）；旧 seller 计数的 PK 倍数（261×/30×）作废。
- 对齐状态变更（如 q9 修复）后，须更新本文状态表 + `OSS_VVR_BASELINE.md` 对应行，
  并重跑验证工作流（§3）。

### 6.1 引擎 fixed+`and close` 收口的已知问题（q9/q16）

fixed 窗口 + `and close` 规则的引擎 EMIT **非确定**（`wp-reactor` wf-engine）：

- 热路径收口每批 `MAX_EXPIRY_SCAN_BUDGET = 1024` 个候选（防一次扫空堆卡死管道）；
- 尾桶收口依赖周期性 `scan_timeouts`（墙钟）在跑批期间触发——30M 快速 replay（秒级）
  可能不触发 → **丢尾部收口 EMIT**（引擎代码注释自认："q16 30M dropped the final
  bucket: 1.48M vs 1.89M ideal with a 1024 budget"）。
- 实测（30M）：q9 oracle 5,254,483 vs 引擎 4,183,632（丢 ~1.07M）；
  q16 oracle 1,886,924 vs 引擎 1,884,322（丢 ~2.6k）。
- 因此 verify-nexmark 对 q9/q16 报**已知差异 ⚠**（oracle 为"所有窗口最终收口"理想值），
  不判失败；引擎侧确定性 EOF 收口（源连接关闭后跑一次无界扫尾）为待办修复。

### 6.2 join-then-key 的 P2 悬置缺口（q4/q6，实施前必须显式处理）

join 字段作键（`match<category/seller>`）已落地 P0/P1（引擎 + checker + 测试），
但 oracle 对拍侧（P2）有两条已知缺口，实施时若不处理会返工：

1. **oracle 输出不走 join 富化**：oracle 路径调 `execute_match/execute_close`
   （不富化 join 字段），引擎走 `execute_match_with_joins/execute_close_with_joins`。
   若 join 键规则的 yield/entity/score 读额外 join 字段，富化字段会对拍失配。
   **现状**：Q4/Q6 恰好只读键（category/seller）+ 聚合（avg），不读额外 join
   字段 → 可绕开；任何读 `auction.*` 的 yield 必须二选一：oracle 切
   `*_with_joins` 并传 lookup，或约束规则面（v1 文档注明）。
2. **右窗过期 watermark 来源未定义**：引擎的 auction 行驱逐由 auction 流自身
   watermark 驱动；oracle 若用驱动事件（bid）的 event_nanos 当 watermark，late
   bid 会提前驱逐 auction 行，两者发散。NEXMark 因 auction over ≈ 拍卖时长恰好
   掩盖。v1 显式声明并验证「auction over 覆盖全部 bid 到达区间」这一 NEXMark
   专用假设（oracle 按窗口分别跟踪 watermark 为备选）。

## 7. 引用注意

- 引用任何 wfusion vs OSS/VVR 倍数前，先查 §4 状态表确认该查询语义对齐。
- q3 旧 100M 数字（11.26M，无过滤语义）已失效；对照一律以最新跑批为准
  （`BENCH_RESULTS.md`）。
