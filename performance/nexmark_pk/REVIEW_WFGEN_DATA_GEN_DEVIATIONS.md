# wfgen 数据生成逻辑 vs NEXMark 官方权威规则 —— 偏离审查

> 审查对象：`warp-fusion/crates/wfgen/src/cmd_gen_nexmark.rs`（`gen-nexmark` / `verify-nexmark` 的 NexMark 生成器）
> 权威基准：官方仓库 `github.com/nexmark/nexmark` 的 `nexmark-flink` 生成器源码
> （`NexmarkConfiguration.java`、`GeneratorConfig.java`、`PersonGenerator.java`、`AuctionGenerator.java`、`BidGenerator.java`，抓取日期 2026-08-21）
> 配套权威文档：`NEXMARK_AUTHORITATIVE_DATA_GEN.md`（本审查的逐条对照对象）

## 审查结论（摘要）

wfgen 的**字段级结构**（比例 / ID 基线 / PK-FK 引用窗口 / 热点概率 / 价格分布 / 类别 / 城市·州 / 字节体积）与官方高度对齐，**核心引用语义正确**。

但有 **1 处显著偏离 + 5 处次要/外观偏离**，其中显著偏离会让 `gen-nexmark --check` 自检报告里"等价固定速率"的结论不成立，并实质影响拍卖有效期与窗口密度。

| # | 偏离项 | 严重度 | wfgen 实际（修复前） | 官方权威 | 状态（2026-08-22） |
|---|--------|--------|-----------|----------|--------------------|
| 1 | 事件时间映射 | 🔴 显著 | 固定 30min span，rate ∝ 1/count | 固定 100µs/事件，span ∝ count | ✅ 已修（`BASE_NS + event_id×100µs`；horizon 按官方毫秒取整 166/167ms 抖动） |
| 2 | dateTime/expires 单位 | 🟡 中 | 纳秒 (ns) | 毫秒 (ms) | ⚠️ 保留（wfgen 全链路内部统一 ns 的约定，查询侧同口径） |
| 3 | extra 填充方差 | 🟢 低 | 固定补齐到精确目标字节 | 目标字节附近 ±20% 随机抖动 | ✅ 已修（`nextExtra` ±20% 抖动） |
| 4 | 字符串长度分布 | 🟢 低 | 固定长度、纯 a-z | 长度 3+rand(max-3)、a-z+~1/13 special | ✅ 已修（`nextString` 带 special 参数） |
| 5 | 冷渠道选择 | 🟡 中低 | 顺序轮询计数器 + 原始 channel_id（100% 追加） | 随机取 + `Integer.reverse(i)`，且缓存创建时 90% 追加（`nextInt(10)>0`，10% 无参数） | ✅ 已修（均匀随机 + `abs(Integer.reverse(i))` + 90% 追加；q21 输出量对齐官方 95%） |
| 6 | 热 URL 目录分隔符 | 🟢 低 | 纯 a-z 目录 | `nextString(5,'_')` 可能含 '_' | ✅ 已修（URL 目录用 `nextString(5,'_')`） |

另：wfgen 额外注入 `_stream`/`_window`/`_timestamp`/`channel_id` 四个非官方 schema 字段（适配器扩展，预期内；`channel_id` 现与 URL 参数一致，q21 直接消费）。

---

## ✅ 已正确对齐的项（核对无误）

- **事件类型比例** `event_id % 50` → Person 1 / Auction 3 / Bid 46（2% / 6% / 92%）。✅
- **ID 基线**：`FIRST_PERSON_ID=FIRST_AUCTION_ID=1000`、`FIRST_CATEGORY_ID=10`、`category = 10 + rand(5)` → 10..14。✅
- **`lastBase0PersonId` / `lastBase0AuctionId`**：三个分支（person / auction / bid 事件）逐字等价官方。✅
- **`nextBase0PersonId` / `nextBase0AuctionId`**：活跃窗口 `min(numPeople, 1000)`、lead=10、范围 `[last-100, last+10)` 完全一致。✅
- **热点概率分母**：seller/bidder 用 `config.hotSellersRatio/BiddersRatio=4` → 75% 热；auction 用 `hotAuctionRatio=2` → 50% 热。✅（源码内 `AuctionGenerator.HOT_SELLER_RATIO=100` 等常量是**批次大小**，不是概率分母；概率分母来自 `NexmarkConfiguration`。wfgen 把概率分母(4/4/2)与批次大小(100)正确拆分，**此处 wfgen 是对的**。）
- **热点批次索引**：seller 取批次首位 `(.. /100)*100`、bidder 取批次第 2 人 `+1`、`+ FIRST_PERSON_ID`。✅
- **价格分布**：`round(10^(rand*6) * 100)` ∈ [100, 1e8) 分，对数均匀。✅
- **拍卖有效期计数公式**：`(numInFlightAuctions * totalProportion) / auctionProportion = (100*50)/3 = 1666` 事件 → 与官方 `numEventsForAuctions` 完全一致。✅
- **`reserve = initialBid + nextPrice`**（恒 >initialBid）。✅
- **城市/州列表、姓名/邮箱格式、信用卡 4×4 位、字节体积 200/500/100、`outOfOrderGroupSize=1`（严格递增）**。✅

---

## 🔴 #1 显著偏离：事件时间映射不是"等价固定速率" — ✅ 已修复

**官方（`GeneratorConfig.timestampForEvent`）**：
```java
interEventDelayUs[0] = 1000000.0 / firstEventRate * numEventGenerators;  // = 1000000/10000*1 = 100.0 µs
public long timestampForEvent(long eventNumber) {
    return baseTime + (long)(eventNumber * interEventDelayUs[0]) / 1000L;
}
```
→ 每个事件固定间隔 **100µs**，与总事件数无关；总跨度 = `count × 100µs`（随 count 线性增长）。

**wfgen（修复后）**：
```rust
const INTER_EVENT_DELAY_NS: i64 = 100_000; // 100 µs/事件
let ns = BASE_NS + event_id * INTER_EVENT_DELAY_NS;
```
→ 与官方同式同值：`timestampForEvent = baseTime + eventNumber × 100µs`。总跨度 = `count × 100µs`
（30M → 3000s / 100M → 10000s），30s 时间桶数随跨度动态（`count×100µs/30s`）。
拍卖有效期 horizon = 1666 × 100µs = **固定 0.1666s**（不再随 count 漂移）。

验证（2026-08-22）：100k → [0s, 9s]/10s span、1M → [0s, 99s]/100s span，乱序/违规/引用均 0，指纹同 seed+count 恒等。

**实质后果已消除**：
- 拍卖有效期 `expires = dateTime + 1 + nextLong(2×horizonMs)`，horizon 固定 0.1666s，Q8（过滤已过期拍卖的 bid）与"在拍窗口"语义不再随 count 漂移。
- 窗口查询（Q3/Q5/Q7/Q8）每窗口覆盖事件数与官方固定密度一致。

`nexmark_conformance.rs` 的措辞已同步改为"与官方同式同值（interEventDelayUs=100 固定）"。

---

## 🟡 #2 中偏离：dateTime / expires 单位

- 官方 `Person/Auction/Bid.dateTime` = `Instant.ofEpochMilli(timestamp)` → **毫秒**。
- wfgen 在 `nx_to_value` 里发射 `"dateTime": ns`（原始纳秒）与 `"expires": expires`（纳秒）。
- 内部自洽（同一 ns 单位），但与官方 schema 的毫秒单位不一致。**跨系统对接 Flink 时会发生 1e6 倍偏差**。仅当 wfgen 的查询 (`q*.wfl`) 自身也以 ns 处理时才无内部问题——需确认 wfgen 查询侧统一使用 ns。

---

## 🟢 #3 低偏离：extra 填充方差 — ✅ 已修复

- 官方 `nextExtra`：在 `desiredAverageSize` 附近做 `±20%` 随机抖动（`delta = round((desired-current)*0.2)`，取 `[minSize, minSize+2δ]`），使**平均**字节数≈目标。
- wfgen `next_extra`（修复后）：`delta = round((desired-current)*0.2)`，`desiredSize = minSize + random(0..=2δ)`，`nextExactString` 纯小写——与官方公式同式同分布。
- 2026-08-22 已按官方 `nextExtra` 公式重写；不再固定精确补齐。

---

## 🟢 #4 低偏离：字符串长度分布 — ✅ 已修复

- 官方 `nextString(maxLength, special)`：长度 = `3 + random.nextInt(maxLength-3)` ∈ **[3, maxLength]**，字符 a-z 且约 1/13 概率为 special 字符，末尾 trim。
- wfgen `next_string(max_len, special)`（修复后）：`3 + random(0..max_len-3)`、每字符 1/13 概率 special、末尾 trim——与官方公式一致。
  - name/email/itemName/description 用 `special=' '`（官方默认），URL 目录用 `special='_'`。
  - 新增 `next_exact_string`（官方 `nextExactString`）：精确长度纯 a-z，供 extra 填充。
- 2026-08-22 已按官方 `nextString`/`nextExactString` 重写；影响 q22 的 URL 目录长度分布与 extra 字节分布。

---

## 🟡 #5 中低偏离：冷渠道选择 — ✅ 已修复（2026-08-22 复核补充 90% 追加）

- 官方冷渠道：`random.nextInt(CHANNELS_NUMBER)` 从 10000 条预生成缓存里**随机**取；
  `createChannelUrlCache` 创建时**每条独立 90% 概率**（`random.nextInt(10) > 0`）追加
  `channel_id = Math.abs(Integer.reverse(i))`（Java int 32 位反转），**10% 无参数**。
- wfgen 冷渠道（修复后）：`rng.random_range(0..CHANNELS_NUMBER)` 均匀随机（分布一致），
  `channel_id = (i as i32).reverse_bits().wrapping_abs()`（复刻 Java `Math.abs(Integer.MIN_VALUE)`
  溢出返回负值），且 **90% 概率携带** channel_id（`random_range(0..10) > 0`）、10% 无参数
  （url 无 `&channel_id=`，JSON 字段输出空串）。
- 输出 JSON 的 `channel_id` 字段（q21 消费）与 URL 参数一致（`check_event` 增双向校验：
  Some → url 必须含该参数；cold None → url 不得含）。
- **q21 输出量影响**：官方 WHERE 过滤无 channel_id 的 cold bid → 输出量 =
  热 50% + cold 90%×50% = **95%** 的 bid（本地 `q21.wfl` 以 `channel_id != ""`
  bind filter 等价表达，verify-nexmark 实测 100k → 87478/92000 = 95.1%）。
- 2026-08-21 首修（随机取 + 位反转），2026-08-22 复核官方源码补 90% 追加概率。

---

## 🟢 #6 低偏离：热 URL 目录分隔符 — ✅ 已修复

- 官方 `getBaseUrl` 用 `nextString(random, 5, '_')`（带 '_' 分隔符），目录段可能含下划线。
- wfgen `next_url`（修复后）：目录段 = `next_string(rng, 5, '_')`，与官方一致（长度 3..5、字符 a-z、约 1/13 概率 '_'）。
- 2026-08-22 已修；`check_event` 的 URL 校验基于 '/' 切分计数（q22 语义），'_' 不影响。

---

## 行动建议（按优先级） — ✅ 全部落实（2026-08-22）

1. **[高] #1 时间映射** ✅ 已改为官方固定 100µs/事件；`nexmark_conformance.rs` 措辞同步。
2. **[中] #2 ns 单位** ⚠️ 保留：wfgen 全链路（生成 + q*.wfl 查询）统一用 ns 的内部约定，跨 Flink 对接需显式换算（文档已注明）。
3. **[低] #3/#4/#6** ✅ 已按官方 `nextExtra`/`nextString`/`nextExactString` 重写。
4. **[低] #5 冷渠道** ✅ 改为均匀随机 + `Integer.reverse(i)`，`channel_id` 字段与 URL 参数一致并加自检。

> 注：本审查不评价 wfgen 查询侧 (`q*.wfl`) 与官方 SQL 的语义对齐——那部分见 `REVIEW_FLINK_SEMANTIC_ALIGNMENT.md`。本文件只针对**数据生成器**。
