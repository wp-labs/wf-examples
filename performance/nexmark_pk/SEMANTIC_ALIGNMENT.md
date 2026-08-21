# NEXMark 语义对齐说明（wfusion vs 白皮书 / Flink）

> 用途：PK 对比前必须先确认"比的是同一个查询"。本文记录 wfusion 各 NEXMark 查询
> 与标准 NEXMark / 阿里白皮书（Flink）的**语义对齐状态、对齐逻辑与验证锚点**，
> 防止拿错语义的数字做 PK 结论（q3 曾因此出现 1.4× 假象，见 §5）。
> 与 `README.md`（套件结构）、`OSS_VVR_BASELINE.md`（OSS/VVR 基线）、
> `NEXMARK.md`（基准背景/数据/正确性）、`PK_REPORT_MAC/LINUX.md`（实测报告）互为配套。

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

## 4. 各查询语义状态表（2026-08-21）

| 查询 | 对齐状态 | 标准语义（白皮书 §1 注） | wfusion 语义 | 差异说明 |
|---|---|---|---|---|
| q1 | ✅ 对齐 | 无状态投影 | `on each` bid 投影 | 每 bid 一条，等价 |
| q2 | ✅ 对齐 | `MOD(auction,123)=0` 过滤 | bind filter 同式 | 等价 |
| q3 | ✅ 对齐（2026-08-21） | person⋈auction join + `category=10` | auction 驱动 + snapshot join person + bind 过滤 | 见 §5 全流程 |
| q4 | ✅ 部分对齐（2026-08-21，join 字段作键） | bid⋈auction 均价（两层：每 auction max → 按 category avg） | **join-then-key**：`match<category:10m:fixed>` + `and close` **avg**（每 category×桶均价） | 外层 avg-of-max 不可表达（见 §5.3）；avg 口径为窗口内直接均价；fixed+close 收口非确定 + join 可见性非确定（见 §6） |
| q5 | ✅ 对齐 | 窗口计数 | sliding 10m count 10/50/100 | 等价 |
| q6 | ✅ 部分对齐（2026-08-21，join 字段作键，自测） | 按 seller 均价（白皮书未发布基线） | **join-then-key**：`match<seller:10m>` + `on event` **avg**（每 seller 均价） | 窗口键来自 join 侧（seller）；join 可见性非确定（见 §6）；白皮书无基线，仅自测 |
| q7 | ✅ 对齐 | 最高出价 | sliding 10m maxbid | 等价 |
| q8 | ✅ 对齐 | 监控新用户 | person 会话窗口（60s gap） | 等价 |
| q9 | ✅ 对齐（2026-08-21） | **胜出出价（Winning Bids）**：每 auction 最高价 bid | fixed 10m + `and close` **max** 聚合（窗口胜者） | 见 §5.2；引擎 fixed+close 收口预算/时钟相关，EMIT 可能丢尾部收口（见 §6） |
| q10 | ✅ 对齐 | 任意选择 | `auction % 7 == 0` 确定性子集（on-each） | 按 Flink 实现口径 |
| q11 | ✅ 对齐 | 用户会话 | bidder 会话（60s gap） | 注意：bench 按 auction 分片时是 per-shard 会话；全局语义须 `CONNECTIONS=1` |
| q12 | ✅ 对齐 | Top-3 auction/窗口 | fixed 10m + conv `sort(-n)\|top(3)` | conv 全局阶段（跨分片合并） |
| q13 | ✅ 对齐 | 有界侧输入 join | bid⋈person 快照 join | 等价（person 近静态） |
| q14 | ✅ 对齐 | Top-10 seller/窗口 | 两段式：fixed 计数 + conv top-10 | conv 全局阶段 |
| q15 | ✅ 对齐 | 过滤+窗口聚合 | price>100 过滤 + 滑窗 count≥5 | 等价 |
| q16 | ✅ 对齐 | 复杂窗口聚合 | fixed + `and close` sum≥1000 | 等价（close 路径） |
| q17 | ✅ 对齐 | 去重/集合聚合 | distinct bidder ≥20 | 等价 |
| q18 | ✅ 对齐 | 累积聚合 | `on event<accu>` 累积 | 等价（fire 不清零） |
| q19 | ✅ 对齐 | 序列/时序 | `on event seq` 双 bid 60s 内 | 等价 |
| q20 | ✅ 对齐 | 无序/并行 | `on event any` count 并行 | 等价 |
| q22 | ✅ 对齐 | asof join | `join ... asof within 60s` | 等价（每 bid 必命中 person） |

> 判定依据：各 `models/queries/*.wfl` 头部注释 + 对拍验证。q4/q6/q9 三类未完全对齐
> 的查询，PK 表格中须标注（见 §6）。

## 5. 案例：Q3 语义对齐全过程（2026-08-21）

### 变更前

```wfl
events { a : auction_events }          # 无 category 过滤
```

→ 全部 1.8M auction 都驱动 join + emit；白皮书标准 Q3 只处理 `category = 10` 的
1/26 → **工作量过重且语义不对**，对比出的 1.4×（vs VVR）是假象。

### 变更后（SQL ↔ WFL 映射）

| 标准 Q3（SQL） | wfusion q3（WFL） | 对齐逻辑 |
|---|---|---|
| `WHERE A.category = 10` | `events { a : auction_events && a.category == 10 }` | 过滤下推（Flink 同款），下游只见 category=10 子集 |
| `A.seller = P.id` | `join person_events snapshot on a.seller == person_events.id` | 同键等值连接；NEXMark 保证 seller 存在 → 单边快照 == 双流 join |
| `P.name, P.city, P.state, A.id` | `id = a.id`（join 富化上下文含 person 字段） | 输出身份同为 auction id；alert schema 不承载 name/city/state，不影响 RPS 口径 |
| 每 auction 一行 | `match<id:10m> on event { a \| count >= 1 }` + fire/reset | auction id 唯一 → 每 auction 恰好输出一条，计数等价 |

### 验证数字（30M，seed=1）

| 项 | 值 |
|---|---|
| 100k oracle | EMIT 194 ≈ 6000 auction ÷ 26 ✓ |
| 30M 引擎 | EPS **37.7M**（旧 6.66M）、RSS 1.47GB、EMIT **68,979**、`[clean]` |
| 30M 对拍 | oracle **68,979 == 引擎 68,979** → `identical ✅` |
| vs VVR | **8.1×**（旧 1.4× 且语义不对等） |

> 对齐后 EPS 上升不是"变快"，是工作负载从"错的"变成"对的"：过滤下推后 wfusion
> 处理的 auction 子集（~69k）与 Flink 过滤后完全一致。

### 残余差异（不改变计数结论）

1. **join 方向**：SQL 双流 join 的 person 事件也驱动连接；wfusion person 侧由系统
   窗口全局维护，规则本身只被 auction 驱动——person 事件（2%）不进规则管道。
2. **miss 不 drop**：快照 join 查不到 person 仍 emit（SQL 内连接丢行）——NEXMark
   数据无 miss，实践等价。
3. **输出字段**：alert 不携带 name/city/state（`nexmark_alerts` schema 限制）。

### 5.2 案例：Q9 语义对齐全过程（2026-08-21）

旧 q9 是 **seller 计数**（auction 驱动 `GROUP BY seller`）——与标准 Q9「胜出出价」
完全不同的查询，却长期出现在 PK 表（261×/30×）。修复为真 Q9 面：

官方 Q9 SQL（`nexmark/nexmark` `nexmark-flink/.../queries/q9.sql`，纯 SQL 定义，
Flink planner 编译为时间窗口 join + TopN 去重算子）：

```sql
SELECT A.*, B.auction, B.bidder, B.price, B.dateTime AS bid_dateTime, B.extra AS bid_extra
FROM (SELECT A.*, B.auction, B.bidder, B.price, B.dateTime AS bid_dateTime, B.extra AS bid_extra,
        ROW_NUMBER() OVER (PARTITION BY A.id ORDER BY B.price DESC, B.dateTime ASC) AS rownum
      FROM auction A, bid B
      WHERE A.id = B.auction AND B.dateTime BETWEEN A.dateTime AND A.expires)
WHERE rownum <= 1;
```

wfusion q9（wfl）：

```wfl
rule q9_winning_bid {
    events { b : bid_events }
    match<auction:10m:fixed> {
        on event { b | count >= 1; }
        and close { w: b.price | max >= 10; }   // 窗口内最高出价为胜者
    } -> score(30.0)
    entity(digit, b.auction)
    ...
}
```

| 官方 Q9（SQL） | wfusion q9（WFL） | 对齐逻辑 |
|---|---|---|
| bid 驱动 + auction 时间 join | bid 驱动 + fixed 10m 窗口 | 工作负载 = 92M bid 进管道（对齐） |
| `B.dateTime BETWEEN A.dateTime AND A.expires` 生命周期内 | 10m 固定桶 | 平台无 per-key 过期窗口，桶近似（计数口径不同：官方每 auction 恰一条，我们每 auction×桶一条） |
| `ROW_NUMBER() ... ORDER BY price DESC, dateTime ASC` 首位 | `and close { b.price \| max >= 10 }` | 窗口内最高出价为胜者；**平手 tie-break 未定义**（官方取最早） |
| 无 reserve 门槛 | `max >= 10` 兜底（等价无条件） | 官方本就无 reserve（早期文档误记成有，已更正） |
| 无 person join，输出 auction + 胜出 bid 字段 | `id = b.auction` | 官方无 person join；alert 带 auction id 即可 |

验证（30M，seed=1）：

| 项 | 值 |
|---|---|
| 100k oracle | EMIT 17,492 ≈ 6000 auction × ~2.9 桶 ✓ |
| 30M 引擎 | EPS **5.77M**、RSS 10.0GB、EMIT 4,183,632、`[clean]` |
| vs OSS / VVR | **134× / 15.4×**（旧 seller 语义 8.56M EPS 作废） |
| 对拍 | oracle 5,254,483 ⚠ 引擎 4,183,632（fixed+close 收口非确定，见 §6.1） |

### 5.3 案例：Q4 语义对齐（2026-08-21，join 字段作窗口键）

旧 q4 先是"每 bid 一条 + auction join"的工作负载近似面（输出=全部 bid 数，非 Q4 均价），
后为 auction 键 fixed+close 近似（category 不可作键）。2026-08-21 实现
`join-field-as-key`（见 `wp-reactor/docs/design/join-field-as-key-design.md`）后，
**category 可作窗口键**——事件路由进窗口前先 snapshot join auction 拿 category。

官方 Q4（`nexmark/nexmark` `q4.sql`）为**两层聚合**：

```sql
SELECT Q.category, AVG(Q.final)
FROM (SELECT MAX(B.price) AS final, A.category
      FROM auction A, bid B
      WHERE A.id = B.auction AND B.dateTime BETWEEN A.dateTime AND A.expires
      GROUP BY A.id, A.category) Q
GROUP BY Q.category;
```

wfusion q4（wfl，join-then-key）：

```wfl
rule q4_avg_price_by_category {
    events { b : bid_events }
    match<category:10m:fixed> {
        on event { b | count >= 1; }
        and close { w: b.price | avg >= 10; }   // 每 category×桶收口时均价
    } -> score(20.0)
    join auction_events snapshot on b.auction == auction_events.id
    entity(digit, category)
    ...
}
```

| 官方 Q4（SQL） | wfusion q4（WFL） | 对齐逻辑 |
|---|---|---|
| bid 驱动 + auction 时间 join | bid 驱动 + snapshot join（b.auction == auction_events.id）+ fixed 10m 窗口 | 工作负载 = 92M bid 进管道 + join 查表（对齐） |
| 内层每 auction `MAX(price)` 胜出价 | `and close { b.price \| avg }` | 聚合面为 avg（官方 avg-of-max；max 面已由 q9 覆盖，本面取 avg×close 组合） |
| 外层按 category 分组 `AVG` | `match<category:10m:fixed>`（join 字段作键） | **join-then-key**：bid 路由前先 join auction 取 category 再分组（2026-08-21 解锁）；外层二级聚合 avg-of-max 仍需 conv 聚合算子（另行设计） |
| 每 auction 生命周期 [dateTime, expires] | 10m 固定桶 | 桶近似（计数口径不同） |

验证（2026-08-21，oracle 为语义参考值）：

| 规模 | oracle | 引擎 | 说明 |
|---|---|---|---|
| 200k | 52 | 78 | fixed+close 收口非确定（引擎批级收口多收 1 桶×26 category） |
| 10M | 52 | 26 | 引擎只收口 1 桶（尾桶收口依赖墙钟 scan_timeouts，见 §6） |

> 引擎 fixed+close 收口**非确定**（每批预算 1024 + 尾桶依赖墙钟）：200k 收 3 桶、10M 收 1 桶——同规则不同规模收口数不同。oracle 为"事件时间边界收口"的语义参考值。
> q21（anti join）随 oracle join 窗口状态实现（2026-08-21）对拍打通：oracle 0 = 引擎 0。

## 6. 未对齐查询的处理原则

- **q6**：白皮书未发布基线，仅自测，PK 表不列倍数。
- **q4**：已对齐（2026-08-21，见 §5.3）——外层 category avg 平台不可表达，为部分对齐。
- **q9**：已修复（2026-08-21），见 §5.2；旧 seller 计数的 PK 倍数（261×/30×）作废。
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

## 7. 引用注意

- 引用任何 wfusion vs OSS/VVR 倍数前，先查 §4 状态表确认该查询语义对齐。
- q3 旧 100M 数字（11.26M，无过滤语义）已失效，见 `OSS_VVR_BASELINE.md` §3.1 注。
