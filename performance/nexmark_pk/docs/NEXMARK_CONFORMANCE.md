# NEXMark 数据符合性声明（wfgen gen-nexmark vs Flink 官方定义）

> **参照系**：[`nexmark/nexmark`](https://github.com/nexmark/nexmark)（Flink 官方 NEXMark
> benchmark 库，原 flink-benchmarks，Alibaba NEXMark 白皮书与 VVR 基线同源），默认配置
> `NexmarkConfiguration` 与其 generator 公式。
>
> **结论先行（2026-08-22 起）**：wfgen 生成语义**严格对齐** Flink 官方默认配置——
> 类型比例、事件时间映射（固定 100µs/事件）、ID 体系、引用窗口、热点机制、价格/有效期
> 分布、category/channel/city·state 值域、name/email/creditCard/itemName/description/
> url/extra 均照搬官方公式（RNG 为 StdRng，分布一致、同 seed 字节级确定性可重放）。
> `wfgen gen-nexmark <count> --check` 与 `wfgen verify-nexmark`
> （`--engine-emit` 分支）会自动输出本声明的摘要版（stderr）。

---

## 一、与 Flink 官方对齐 ✅（生成语义逐项）

| 维度 | Flink 官方（nexmark/nexmark 默认） | wfgen | 结论 |
|---|---|---|---|
| 类型比例 | `personProportion=1 / auctionProportion=3 / bidProportion=46`（total=50 → 2%/6%/92%） | 同一比例 | **对齐** |
| 事件时间映射 | `timestampForEvent(n) = baseTime + n × interEventDelayUs/1000`，`interEventDelayUs=100`（固定速率，span ∝ count） | `ns = BASE_NS + n × 100µs` | **对齐**（与官方同式同值，跨度 = count×100µs；30M → 3000s / 100M → 10000s） |
| 乱序 | `outOfOrderGroupSize=1` → 严格递增 | 桶序输出 = 桶间严格递增、桶内生成序（≤30s 抖动） | **对齐**（桶内 30s 抖动远小于 10m over 粒度；`--check` 报乱序 0） |
| ID 起始值 | `FIRST_PERSON_ID = FIRST_AUCTION_ID = 1000` | 同值 | **对齐** |
| ID 语义 | person/auction id 唯一、随事件序递增（base0 索引 + 1000） | 同 | **对齐** |
| 引用窗口 | `lastBase0PersonId`/`nextBase0PersonId`/`lastBase0AuctionId`/`nextBase0AuctionId` 公式：seller/bidder 75% 热点（最近 100 人批次第 1/2 人）+ 25% 最近 `numActivePeople=1000` 人 ± 10 lead；bid.auction 50% 热点（最近 100 个批次第 1 个）+ 50% 最近 `numInFlightAuctions=100` ± 10 lead（lead 允许引用未来实体） | 同一公式 | **对齐**（`--check` 引用校验 0 违规） |
| hot auction 占比 | `hotAuctionRatio=2` → 50% | 同 | **对齐** |
| hot seller/bidder 占比 | `hotSellersRatio=hotBiddersRatio=4` → 75% | 同 | **对齐** |
| 价格分布 | `nextPrice = round(10^(6u) × 100)` 对数均匀 [100, 1e8)，initialBid/reserve/bid.price 同分布、与冷热无关 | 同公式 | **对齐** |
| auction 有效期 | `nextAuctionLengthMs = 1 + nextLong(2×horizonMs)`，horizon = 未来 `numInFlightAuctions=100` 个 auction 的生成间隔 = 1666×100µs = 0.1666s 固定 | 同公式（horizon 固定 0.1666s，随 count 不变） | **对齐**（平均有效期 ≈166ms，同时活跃 ~100 个 auction） |
| category | `FIRST_CATEGORY_ID=10 + rand(5)` → 10..14（5 类） | 同 | **对齐** |
| channel | 50% 热门 4 通道（Google/Facebook/Baidu/Apple）+ 50% `channel-0..9999`（官方 cold：`random.nextInt(CHANNELS_NUMBER)` 均匀随机；`createChannelUrlCache` 逐条 90% 概率追加 `channel_id = abs(Integer.reverse(i))`，10% 无参数） | 同 | **对齐**（`channel_id` 字段与 URL 参数一致，`--check` 校验；无参数时输出空串） |
| city/state | 10 城 / 6 州（AZ,CA,ID,OR,WA,WY），独立随机 | 同值域、独立随机 | **对齐** |
| name/email | `FIRST_NAMES×LAST_NAMES` 随机姓名、`nextString(7)@nextString(5).com` 随机邮箱 | 同 | **对齐** |
| creditCard | 4 组 4 位数字（`0000-9999`） | 同 | **对齐** |
| itemName/description | `nextString(20)` / `nextString(100)` 随机（长度 3+rand(max-3)，~1/13 special） | 同公式 | **对齐** |
| url 目录 | `nextString(5,'_')`（长度 3..5，~1/13 概率 '_'） | 同公式 | **对齐**（q22 split('/') 切分不变） |
| extra | `nextExtra`：目标 avgByteSize（200/500/100）附近 ±20% 抖动（`nextExactString` 纯小写） | 同公式 | **对齐**（平均字节≈官方，30M JSON ≈ 10.5GB） |

## 二、实现备注

- bidder id 按 Beam 官方语义单加 `FIRST_PERSON_ID`（nexmark-flink 存在双加 1000 的
  实现缺陷，不复刻）；本地查询集无 bidder→person join。
- RNG 用 StdRng 保证同 seed 字节级确定性重放（官方用 SplittableRandom，进程间不确定）；
  分布语义与官方一致。

## 三、对「符合 Flink 测试集定义」的判定

- **生成语义（比例/时间轴/ID 体系/引用窗口/热点/价格/有效期/值域/字符串字段）——符合**：
  与官方默认配置逐项对齐（含 name/email/creditCard/itemName/description 随机生成与
  extra 补齐到 avgByteSize）。这是正确性对拍（oracle vs 引擎）与白皮书/VVR **数字对比
  可比性**的共同基础：
  - Q1/Q2/Q3/Q9（无状态/join 面）可比性高；
  - Q4/Q5/Q7/Q17（价格阈值/窗口/join 面）命中面与官方口径一致（官方 Q7 阈值 10000
    在价格对数均匀下按官方概率命中，本地 q7 阈值 200/500/1000 的命中面亦按官方分布）；
  - Q12（bidder 窗口计数）活跃 bidder 域 = 官方 numActivePeople=1000 口径。
- **验证口径**：`--check`（值域+引用窗口+乱序+指纹）与 `verify-nexmark`（oracle 对拍）
  验证的是「同一份数据的生成正确性与引擎一致性」；RNG 为 StdRng（非官方
  SplittableRandom），**不做与 Flink 的字节级逐事件一致**（不同 RNG 算法无法逐字节
  复现，分布语义一致即可，VVR 对比用独立 oracle 对拍）。

## 四、数据口径变更记录

**v5（2026-08-22）**：六处数据生成偏离全部按官方公式修正（见
`REVIEW_WFGEN_DATA_GEN_DEVIATIONS.md`）：事件时间映射改为固定 100µs/事件
（原固定 30min span）、auction 有效期 horizon 固定 0.1666s（并按官方毫秒取整
复刻 166/167ms 相位抖动）、extra ±20% 体积抖动（半开区间）、字符串长度
3+rand(max-3)+special、cold 通道均匀随机 + `abs(Integer.reverse(i))` 且 90% 追加
（官方 `random.nextInt(10)>0`，10% 无参数）、URL 目录可含 '_'。**数据跨度随
count 线性增长**（30M → 3000s ≈ 50min），30s 时间桶数动态；30M 帧指纹
`25e75749…` 再次变化，`../data/bench_30m_v2.frames` 与 `verify_*.txt` 锚点需重新
生成。oracle 对拍（`verify-nexmark`）的 eos 水位与桶序同步改为动态跨度口径。
Q21 输出量随 cold 无参 10% 调整为 **95%** 的 bid（官方 WHERE 语义，`q21.wfl`
加 `channel_id != ""` 过滤）。

**v4（2026-08-21）**：bid 增 `channel_id` 字段（q21 Add channel id 数据侧对齐，
官方 CASE WHEN 映射 + url channel_id，生成时已知）；数据版本 v3 → v4，帧重生成。

此前版本（时间窗引用 15s/60s、阶梯价格 hot[100,500]/cold[10,150]、有效期固定 10-30
分钟、category 1..26、5 固定通道、无字符串随机/无 extra padding）与官方分布参数不一致，
已按本声明对齐为官方语义。

## 五、对照表来源

本声明逐项对照基于 [`nexmark/nexmark`](https://github.com/nexmark/nexmark) 仓库
（2026-08-21 抓取）的 `NexmarkConfiguration.java`、`NexmarkGenerator.java`、
`PersonGenerator.java`、`AuctionGenerator.java`、`BidGenerator.java`、
`PriceGenerator.java`、`StringsGenerator.java`。
