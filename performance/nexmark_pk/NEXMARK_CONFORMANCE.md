# NEXMark 数据符合性声明（wfgen gen-nexmark vs Flink 官方定义）

> **参照系**：[`nexmark/nexmark`](https://github.com/nexmark/nexmark)（Flink 官方 NEXMark
> benchmark 库，原 flink-benchmarks，Alibaba NEXMark 白皮书与 VVR 基线同源），默认配置
> `NexmarkConfiguration` 与其 generator 公式。
>
> **结论先行（2026-08-21 起）**：wfgen 生成语义**严格对齐** Flink 官方默认配置——
> 类型比例、事件时间映射、ID 体系、引用窗口、热点机制、价格/有效期分布、category/
> channel/city·state 值域均照搬官方公式。残余差异仅为**无查询引用**的字符串模板与
> 字段裁剪。`wfgen gen-nexmark <count> --check` 与 `wfgen verify-nexmark`（`--engine-emit`
> 分支）会自动输出本声明的摘要版（stderr）。

---

## 一、与 Flink 官方对齐 ✅（生成语义逐项）

| 维度 | Flink 官方（nexmark/nexmark 默认） | wfgen | 结论 |
|---|---|---|---|
| 类型比例 | `personProportion=1 / auctionProportion=3 / bidProportion=46`（total=50 → 2%/6%/92%） | 同一比例 | **对齐** |
| 事件时间映射 | `timestampForEvent(n) = baseTime + n × interEventDelay`（线性、固定速率） | `ns = BASE_NS + n × SPAN_NS / count`（线性） | **对齐**（等价固定速率，SPAN=30min） |
| 乱序 | `outOfOrderGroupSize=1` → 严格递增 | 桶序输出 = 桶间严格递增、桶内生成序（≤30s 抖动） | **对齐**（桶内 30s 抖动远小于 10m over 粒度；`--check` 报乱序 0） |
| ID 起始值 | `FIRST_PERSON_ID = FIRST_AUCTION_ID = 1000` | 同值 | **对齐** |
| ID 语义 | person/auction id 唯一、随事件序递增（base0 索引 + 1000） | 同 | **对齐** |
| 引用窗口 | `lastBase0PersonId`/`nextBase0PersonId`/`lastBase0AuctionId`/`nextBase0AuctionId` 公式：seller/bidder 75% 热点（最近 100 人批次第 1/2 人）+ 25% 最近 `numActivePeople=1000` 人 ± 10 lead；bid.auction 50% 热点（最近 100 个批次第 1 个）+ 50% 最近 `numInFlightAuctions=100` ± 10 lead（lead 允许引用未来实体） | 同一公式 | **对齐**（`--check` 引用校验 0 违规） |
| hot auction 占比 | `hotAuctionRatio=2` → 50% | 同 | **对齐** |
| hot seller/bidder 占比 | `hotSellersRatio=hotBiddersRatio=4` → 75% | 同 | **对齐** |
| 价格分布 | `nextPrice = round(10^(6u) × 100)` 对数均匀 [100, 1e8)，initialBid/reserve/bid.price 同分布、与冷热无关 | 同公式 | **对齐** |
| auction 有效期 | `nextAuctionLengthMs = 1 + nextLong(2×horizonMs)`，horizon = 未来 `numInFlightAuctions=100` 个 auction 的生成间隔 | 同公式 | **对齐**（30M 口径 horizon≈100ms → 平均有效期 ≈100ms，同时活跃 ~100 个 auction） |
| category | `FIRST_CATEGORY_ID=10 + rand(5)` → 10..14（5 类） | 同 | **对齐** |
| channel | 50% 热门 4 通道（Google/Facebook/Baidu/Apple）+ 50% `channel-0..9999` | 同 | **对齐** |
| city/state | 10 城 / 6 州（AZ,CA,ID,OR,WA,WY），独立随机 | 同值域、独立随机 | **对齐** |

## 二、残余差异 ⚠️（均无查询引用，零影响）

| 差异 | 说明 | 影响 |
|---|---|---|
| name/email/itemName/description/url 为确定性模板 | 官方为随机字符串（first+last 姓名、随机邮箱、随机描述、随机长 url） | 无查询引用（官方 Q8/Q10 按 city/state 分组已对齐值域） |
| 无 creditCard / extra 填充 | 官方 Person 含 creditCard、各事件 extra 补齐到 avgByteSize | 本地 schema 无 creditCard，extra 恒 `""` |
| bidder id 单加 `FIRST_PERSON_ID` | **nexmark-flink 存在 bidder 双加 1000 的 bug**（`bidder += FIRST_PERSON_ID` 出现两次 → 引用错位 +1000）；wfgen 按 Beam 官方语义单加 | 本地查询集无 bidder→person join（Q3/Q9 用 auction.seller），零影响；若未来按 bidder join person 需按 Beam 语义 |
| channel/url 用确定性 StdRng | nexmark-flink 的 `HOT_URLS`/`CHANNEL_URL_CACHE` 用**静态 SplittableRandom**，进程间不确定 | wfgen 为同 seed 字节级确定性重放而用确定 RNG；分布语义（50%/50%、4 热通道）一致 |

## 三、对「符合 Flink 测试集定义」的判定

- **生成语义（比例/时间轴/ID 体系/引用窗口/热点/价格/有效期/值域）——符合**：与官方
  默认配置逐项对齐。这是正确性对拍（oracle vs 引擎）与白皮书/VVR **数字对比可比性**
  的共同基础：
  - Q1/Q2/Q3/Q9（无状态/join 面）可比性高；
  - Q4/Q5/Q7/Q17（价格阈值/窗口/join 面）命中面与官方口径一致（官方 Q7 阈值 10000
    在价格对数均匀下按官方概率命中，本地 q7 阈值 200/500/1000 的命中面亦按官方分布）；
  - Q12（bidder 窗口计数）活跃 bidder 域 = 官方 numActivePeople=1000 口径。
- **验证口径**：`--check`（值域+引用窗口+乱序+指纹）与 `verify-nexmark`（oracle 对拍）
  验证的是「同一份数据的生成正确性与引擎一致性」；RNG 为 StdRng（非官方
  SplittableRandom），**不做与 Flink 的字节级逐事件一致**（不同 RNG 算法无法逐字节
  复现，分布语义一致即可，VVR 对比用独立 oracle 对拍）。

## 四、数据口径变更记录（2026-08-21）

此前版本（时间窗引用 15s/60s、阶梯价格 hot[100,500]/cold[10,150]、有效期固定 10-30
分钟、category 1..26、5 固定通道）与官方分布参数不一致，已按本声明对齐为官方语义。
**数据口径切换使所有 oracle 期望值与帧指纹变化**（30M 帧指纹 `25e75749…` → 新值），
`data/bench_30m_v2.frames` 与 `verify_*.txt` 锚点需重新生成。
