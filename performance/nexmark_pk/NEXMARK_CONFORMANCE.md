# NEXMark 数据符合性声明（wfgen gen-nexmark vs Flink 官方定义）

> **参照系**：[`nexmark/nexmark`](https://github.com/nexmark/nexmark)（Flink 官方 NEXMark
> benchmark 库，原 flink-benchmarks，Alibaba NEXMark 白皮书与 VVR 基线同源），默认配置
> `NexmarkConfiguration`。
>
> **结论先行**：wfgen 生成数据的**结构骨架**与 Flink 官方一致（类型比例、事件时间映射、
> ID 起始值、引用完整性、hot auction 比例）；**随机分布参数**（价格、auction 有效期、
> 热点选择机制、category/channel 值域、字符串格式）为「30m 固定 span + 字节级确定性
> 重放」目的而刻意偏离。**正确性对拍不受影响**（oracle 与引擎跑同一份数据）；与
> 白皮书/VVR **性能数字对比时须披露**这些偏离。

`wfgen gen-nexmark <count> --check` 与 `wfgen verify-nexmark`（`--engine-emit` 分支）
会自动输出本声明的摘要版（stderr）。

---

## 一、与 Flink 官方一致 ✅

| 维度 | Flink 官方（nexmark/nexmark 默认） | wfgen | 结论 |
|---|---|---|---|
| 类型比例 | `personProportion=1 / auctionProportion=3 / bidProportion=46`（total=50 → 2%/6%/92%） | 同一比例（`PERSON/AUCTION/BID_PROPORTION = 1/3/46`） | **一致** |
| 事件时间映射 | `timestampForEvent(n) = baseTime + n × interEventDelay`（线性、固定速率） | `ns = BASE_NS + n × SPAN_NS / count`（线性） | **一致**（等价固定速率，SPAN=30min） |
| 乱序 | `outOfOrderGroupSize=1` → 严格递增 | 桶序输出 = 桶间严格递增、桶内生成序（≤30s 抖动） | **一致**（桶内 30s 抖动远小于 10m over 粒度；`--check` 报乱序 0 事件） |
| ID 起始值 | `FIRST_PERSON_ID = FIRST_AUCTION_ID = 1000` | 同值（1000/1000） | **一致** |
| ID 语义 | person/auction id 唯一、随事件序递增（base0 索引 + 1000） | 同（`1000 + 连续索引`） | **一致** |
| 引用完整性 | 官方注释「most primary key/foreign key relations are correct」 | `auction.seller` / `bid.bidder` 引用**已生成** person、`bid.auction` 引用**已生成** auction（`--check` 引用校验 0 违规） | **一致**（wfgen 更严：官方允许 ±10 lead 引用未来实体，靠乱序兜底；wfgen 引用必已存在） |
| hot auction 占比 | `hotAuctionRatio=2` → P(hot)=1-1/2=50% | `rng < 0.5` → 50% | **一致** |

## 二、刻意偏离 ⚠️（含理由与影响）

### 1. 热点（hot seller / hot bidder）选择机制

| | Flink 官方 | wfgen |
|---|---|---|
| hot 比例 | `hotSellersRatio=hotBiddersRatio=4` → 75% hot | seller/bidder 各 50% hot |
| hot 实体 | 最近 4 人批次第 1 人（seller）/ 第 2 人（bidder），固定热点 | 最近 **15s** 时间窗内随机 |
| cold 实体 | 最近 `numActivePeople=1000` 人 ± 10 lead | 最近 **60s** 时间窗内随机 |

**理由**：官方用「最近 N 人」控制活跃域不随运行时长退化（`numActivePeople` 的官方注释
即为防止 bids/auctions per person 密度随时间下降）；wfgen 固定 30m span，用**固定时间窗**
（15s/60s）等价地控制活跃域，且与 count 无关、字节级确定。

**影响**：活跃实体域的形状不同——q12 活跃 bidder 域实测 ~7k（vs Flink 口径 ~1000 人 +
lead），Q3/Q9 的 join 命中面集中度不同。查询语义（按 bidder/auction 分组）不受影响。

### 2. bid 价格分布

| | Flink 官方 | wfgen |
|---|---|---|
| 分布 | `nextPrice = round(10^(6u) × 100)` 对数均匀，约 **[100, 10^8]**，与 auction 冷热无关 | hot auction 出价 **[100, 500]**、cold **[10, 150]**（阶梯） |

**理由**：阶梯分段是旧 Python 版 generator 的直译，刻意保留以维持与既有 oracle 期望值
（q2/q5/q7 等）的连续性。

**影响**：**价格阈值类查询命中面不同**——官方 Q7 阈值 10000 在 wfgen 数据下永不命中；
本地 q7 阈值改写为 200/500/1000 即为此适配。官方初始价/保留价同样为对数均匀
（`initialBid + nextPrice`），wfgen 为 `[10,1000]/[1000,10000]` 均匀，同理偏离。

### 3. auction 有效期（in-flight 面）

| | Flink 官方 | wfgen |
|---|---|---|
| expires | `1 + nextLong(2×horizonMs)`，horizon ≈ `numInFlightAuctions=100` 个 auction 的生成间隔（30M/30min 口径下 ~百 ms 级） | `ns + [600s, 1800s]`（固定 10-30 分钟） |

**理由**：wfgen 的 bid→auction 引用走「最近 60s」时间窗，须保证被引 auction 未过期；
10-30 分钟有效期保证 30m span 内 bid 总能命中，且与 10m fixed 窗口吸收语义配套。

**影响**：同时活跃 auction 数量差异巨大（wfgen 十万级 vs Flink ~100），join 匹配面与
内存面显著更大（Q4/Q6/Q17 的窗口状态量）。**对拍基准自洽**（oracle 与引擎同数据同窗口），
但与白皮书数字对比时这是最大的不可比因素之一。

### 4. category 域

| | Flink 官方 | wfgen |
|---|---|---|
| 值域 | `FIRST_CATEGORY_ID=10 + rand(5)` → **10..14**（5 类） | 均匀 **1..=26**（26 类） |

**影响**：按 category 分组查询（官方 Q5 类 counting 面、Q11/Q20 类）的桶数与每桶密度
不同（形状相似、桶数不同）。本地 q5/q11/q20 的阈值告警计数会随桶数变化，属预期内差异。

### 5. channel 域

| | Flink 官方 | wfgen |
|---|---|---|
| 值域 | 50% 热门 4 通道（Google/Facebook/Baidu/Apple）+ 50% `channel-0..9999` | 均匀 5 通道（Google/Facebook/Apple/Direct/Test） |

**影响**：仅按 channel 分组的查询（官方 Q8 类）受影响；本地查询集未按 channel 分组，
无实际影响。

### 6. 字符串字段格式

| | Flink 官方 | wfgen |
|---|---|---|
| name/email | 随机 first+last 姓名 / 随机字符串邮箱 | `person_{id}` / `person{id}@example.com` |
| city/state | 10 城 / 6 州（AZ,CA,ID,OR,WA,WY），独立随机 | 8 城 / 8 州（city↔state 成对） |
| url | https 长链接（含 `&channel_id=`） | `http://www.example.com/{n}` |

**影响**：city/state 只影响按地理分组查询（官方 Q8/Q10 类，本地未实现）；name/email/url
无查询引用。零影响。

---

## 三、对「符合 Flink 测试集定义」的判定

- **结构性定义（类型比例、时间轴、ID 体系、外键关系）——符合**：这是测试集正确性对拍
  （oracle vs 引擎）成立的基础，q2/q3/q4/q5/q7/q9 等的期望值均在该数据上精确定义。
- **分布性参数（价格、有效期、热点、值域）——偏离且已披露**：与 Flink 白皮书/VVR 的
  数字对比时，Q4/Q5/Q7/Q17 等窗口/阈值/join 面查询的结果量与资源画像不可直接对标；
  Q1/Q2/Q3/Q9（无状态/join 面与价格无关）可比性高。
- **验证口径**：`--check`（值域+引用+乱序+指纹）与 `verify-nexmark`（oracle 对拍）验证的
  是「**同一份数据的生成正确性与引擎一致性**」，不验证「与 Flink 字节级一致」——
  后者在 2026-08 明确不做（VVR 对比采用独立 oracle 对拍，非逐字节 diff）。

## 四、如需严格对齐 Flink 分布（备选，未实施）

若未来需要与白皮书数字严格可比，需在 `generate_events` 上做参数化改造：

1. `price` 改为 `round(10^(6u)×100)` 对数均匀（不区分 hot/cold）；
2. `expires` 改为 `1 + nextLong(2×horizon)`，horizon 由 `numInFlightAuctions` 与事件速率推出；
3. seller/bidder 热点改为「最近 N 人批次 + lead」机制（75% hot / 25% cold）；
4. category → `10..14`、channel → 热门 4 + `channel-N`；
5. 保留引用完整性（lead 引用未来实体依赖乱序兜底，与 wfgen 严格递增冲突，需一并评估
   `outOfOrderGroupSize>1`）。

这属于**数据口径切换**（会改变所有 oracle 期望值与既有指纹），需单独立项，避免污染
当前对拍基准。
