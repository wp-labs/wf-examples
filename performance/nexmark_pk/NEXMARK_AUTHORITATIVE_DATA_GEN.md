# NEXMark 权威数据生成规则参考（Flink / nexmark-flink）

> **本文件只记录权威定义，不含任何对齐/评审结论。** 查询语义见同目录 `NEXMARK_AUTHORITATIVE_SEMANTICS.md`。
> 数据生成规则是 NEXMark 基准的另一半权威——它与查询 SQL 同等重要：NEXMark 的"正确性"取决于**生成器产出的 PK/FK 关系与时间戳**是否被正确实现。

## 权威出处（Authoritative Source）

所有规则均来自官方仓库 **`github.com/nexmark/nexmark`** 的 `nexmark-flink` 模块源码（抓取日期：2026-08-21）。关键文件与 raw URL：

| 文件 | 作用 | 权威 URL |
|------|------|---------|
| `NexmarkConfiguration.java` | 全部可调默认参数 | <https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/java/com/github/nexmark/flink/NexmarkConfiguration.java> |
| `generator/GeneratorConfig.java` | ID 基线、比例、时间戳/乱序逻辑 | <https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/java/com/github/nexmark/flink/generator/GeneratorConfig.java> |
| `generator/NexmarkGenerator.java` | 事件类型分配主循环 | <https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/java/com/github/nexmark/flink/generator/NexmarkGenerator.java> |
| `generator/model/PersonGenerator.java` | Person 生成 | <https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/java/com/github/nexmark/flink/generator/model/PersonGenerator.java> |
| `generator/model/AuctionGenerator.java` | Auction 生成 | <https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/java/com/github/nexmark/flink/generator/model/AuctionGenerator.java> |
| `generator/model/BidGenerator.java` | Bid 生成 | <https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/java/com/github/nexmark/flink/generator/model/BidGenerator.java> |
| `model/Person.java` | Person schema | <https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/java/com/github/nexmark/flink/model/Person.java> |
| `model/Auction.java` | Auction schema | <https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/java/com/github/nexmark/flink/model/Auction.java> |
| `model/Bid.java` | Bid schema | <https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/java/com/github/nexmark/flink/model/Bid.java> |
| `generator/model/PriceGenerator.java` | 价格分布 | <https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/java/com/github/nexmark/flink/generator/model/PriceGenerator.java> |
| `generator/model/StringsGenerator.java` | 字符串/extra 生成 | <https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/java/com/github/nexmark/flink/generator/model/StringsGenerator.java> |
| `generator/model/LongGenerator.java` | 长整型随机 | <https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/java/com/github/nexmark/flink/generator/model/LongGenerator.java> |

---

## 1. 三流数据模型（Schema）

NEXMark 由三条流构成：**Person**、**Auction**、**Bid**。字段（来自 `model/*.java`，类型已转述）：

### Person（`model/Person.java`）
| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | long | 主键 |
| `name` | String | 名+姓 |
| `emailAddress` | String | 邮箱 |
| `creditCard` | String | 信用卡号 |
| `city` | String | 城市 |
| `state` | String | 州 |
| `dateTime` | Instant | 事件时间 |
| `extra` | String | 负载填充（性能测试用） |

### Auction（`model/Auction.java`）
| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | long | 主键 |
| `itemName` | String | 商品名 |
| `description` | String | 描述 |
| `initialBid` | long | 起拍价（**单位：分/cents**） |
| `reserve` | long | 底价（分） |
| `dateTime` | Instant | 上架时间 |
| `expires` | Instant | 过期时间（ms；此时间及之后的 bid 被忽略） |
| `seller` | long | 外键 → `Person.id` |
| `category` | long | 类别（外键语义） |
| `extra` | String | 负载填充 |

### Bid（`model/Bid.java`）
| 字段 | 类型 | 说明 |
|------|------|------|
| `auction` | long | 外键 → `Auction.id` |
| `bidder` | long | 外键 → `Person.id` |
| `price` | long | 出价（分） |
| `channel` | String | 渠道 |
| `url` | String | URL |
| `dateTime` | Instant | 出价时间（可能早于系统事件时间） |
| `extra` | String | 负载填充 |

> **权威约束**（来自 `NexmarkGenerator` 类注释）：生成器保证"多数 PK/FK 关系正确"——一个 `Bid` 的 `auction`/`bidder` 通常能 join 到已生成的 `Auction`/`Person`。

---

## 2. ID 基线常量（`GeneratorConfig`）

```java
public static final long FIRST_AUCTION_ID  = 1000L;
public static final long FIRST_PERSON_ID   = 1000L;
public static final long FIRST_CATEGORY_ID = 10L;
```

- 实际 `Person.id` = `base0PersonId + 1000`
- 实际 `Auction.id` = `base0AuctionId + 1000`
- `category` = `10 + random.nextInt(5)` → **类别取值 10..14**（共 `NUM_CATEGORIES = 5`）

---

## 3. 全局生成参数（默认，`NexmarkConfiguration`）

| 参数 | 默认值 | 含义 |
|------|--------|------|
| `personProportion` | 1 | Person 占比权重 |
| `auctionProportion` | 3 | Auction 占比权重 |
| `bidProportion` | 46 | Bid 占比权重 |
| `totalProportion` | 50 | = 三者之和（推导） |
| `firstEventRate` | 10000 | 初始总事件率（per second） |
| `nextEventRate` | 10000 | 下一阶段事件率 |
| `rateUnit` | PER_SECOND | 速率单位 |
| `rateShape` | SQUARE | 速率曲线形状 |
| `ratePeriodSec` | 600 | 速率周期（秒） |
| `avgPersonByteSize` | 200 | Person 平均字节 |
| `avgAuctionByteSize` | 500 | Auction 平均字节 |
| `avgBidByteSize` | 100 | Bid 平均字节 |
| `hotAuctionRatio` | 2 | 热拍卖概率分母（P(热)=1-1/2） |
| `hotSellersRatio` | 4 | 热卖家概率分母（P(热)=1-1/4） |
| `hotBiddersRatio` | 4 | 热买家概率分母（P(热)=1-1/4） |
| `numInFlightAuctions` | 100 | 同时在拍拍卖数 |
| `numActivePeople` | 1000 | 活跃人数上限 |
| `windowSizeSec` | 10 | Q3/Q5/Q7/Q8 窗口大小 |
| `windowPeriodSec` | 5 | Q5 滑动步长 |
| `outOfOrderGroupSize` | 1 | 乱序分组大小（1=无乱序） |
| `probDelayedEvent` | 0.1 | 事件被延迟的概率 |
| `occasionalDelaySec` | 3 | 偶发延迟秒数 |
| `useWallclockEventTime` | false | false=用确定性的过去时间戳 |
| `isRateLimited` | false | 是否按速率节流 |
| `maxEmitSpeed` | true | true=尽可能快发射（不按事件时间限速） |
| `numEvents` | 0 | 0=尽可能多生成（受内部计数上限约束） |

**事件类型占比（按默认权重）**：Person ≈ 1/50 = **2%**，Auction ≈ 3/50 = **6%**，Bid ≈ 46/50 = **92%**。

---

## 4. 事件类型分配逻辑（`NexmarkGenerator.nextEvent`）

```java
long newEventId = getNextEventId();   // = firstEventId + nextAdjustedEventNumber(eventsCountSoFar)
long rem = newEventId % config.totalProportion;
Event event;
if (rem < config.personProportion) {
    event = new Event(PersonGenerator.nextPerson(...));
} else if (rem < config.personProportion + config.auctionProportion) {
    event = new Event(AuctionGenerator.nextAuction(...));
} else {
    event = new Event(BidGenerator.nextBid(...));
}
```

即：事件类型由 `eventId % 50` 决定——`[0,1)`→Person，`[1,4)`→Auction，`[4,50)`→Bid。

---

## 5. 时间戳与乱序（确定性，`GeneratorConfig`）

```java
// 事件间隔（微秒）：由速率推导
interEventDelayUs[0] = 1_000_000.0 / firstEventRate * numEventGenerators;
// 默认 firstEventRate=10000, numEventGenerators=1 → 100 µs/事件 → 10,000 事件/秒

// 事件时间 = baseTime + eventNumber * interEventDelayUs / 1000
public long timestampForEvent(long eventNumber) {
    return baseTime + (long)(eventNumber * interEventDelayUs[0]) / 1000L;
}
```

- **`useWallclockEventTime=false`（默认）**：使用 `baseTime`（过去某确定时刻）推导事件时间，多次运行产生**完全相同的事件流与结果**。
- **乱序（out-of-order）**：由 `outOfOrderGroupSize` 控制。默认 `1` 表示无乱序。
  ```java
  long nextAdjustedEventNumber(long numEvents) {
      long n = configuration.outOfOrderGroupSize;
      long eventNumber = nextEventNumber(numEvents);
      long base = (eventNumber / n) * n;
      long offset = (eventNumber * 953) % n;   // 组内伪随机打乱
      return base + offset;
  }
  ```
- **Watermark**：`nextEventNumberForWatermark` 取组内 base（`(eventNumber / n) * n`），即乱序边界。

---

## 6. Person 生成规则（`PersonGenerator`）

```java
long id   = lastBase0PersonId(config, nextEventId) + FIRST_PERSON_ID;   // +1000
String name  = FIRST_NAMES.get(rand) + " " + LAST_NAMES.get(rand);
String email = nextString(rand, 7) + "@" + nextString(rand, 5) + ".com";
String creditCard = "dddd dddd dddd dddd";   // 4 组 4 位数字
String city  = US_CITIES.get(rand);
String state = US_STATES.get(rand);
String extra = nextExtra(rand, currentSize, avgPersonByteSize);  // 平均 200 字节
```

- **州（US_STATES，6 个）**：`AZ, CA, ID, OR, WA, WY`
- **城市（US_CITIES，10 个）**：`Phoenix, Los Angeles, San Francisco, Boise, Portland, Bend, Redmond, Seattle, Kent, Cheyenne`
- **名（FIRST_NAMES）**：`Peter, Paul, Luke, John, Saul, Vicky, Kate, Julie, Sarah, Deiter, Walter`
- **姓（LAST_NAMES）**：`Shultz, Abrams, Spencer, White, Bartels, Walton, Smith, Jones, Noris`
- id 选择：在"活跃人数"范围内随机（`numActivePeople=1000`），外加少量 lead（`PERSON_ID_LEAD=10`），保证长运行下每人密度不下降。

---

## 7. Auction 生成规则（`AuctionGenerator`）

```java
long id      = lastBase0AuctionId(config, eventId) + FIRST_AUCTION_ID;   // +1000
long category = FIRST_CATEGORY_ID + random.nextInt(NUM_CATEGORIES);       // 10 + [0,5) = 10..14
long initialBid = PriceGenerator.nextPrice();
long reserve    = initialBid + PriceGenerator.nextPrice();               // 底价 > 起拍价
long expires    = timestamp + nextAuctionLengthMs(...);                   // 平均存活到 numInFlightAuctions 后被生成的时刻
String name = nextString(rand, 20);
String desc = nextString(rand, 100);
String extra = nextExtra(rand, currentSize, avgAuctionByteSize);         // 平均 500 字节

// seller（热卖家逻辑）：P(热) = 1 - 1/hotSellersRatio（默认 4 → 0.75）
if (random.nextInt(config.getHotSellersRatio()) > 0) {
    seller = (PersonGenerator.lastBase0PersonId(config, eventId) / HOT_SELLER_RATIO) * HOT_SELLER_RATIO;  // HOT_SELLER_RATIO=100
} else {
    seller = PersonGenerator.nextBase0PersonId(eventId, random, config);
}
seller += FIRST_PERSON_ID;
```

- `NUM_CATEGORIES = 5`；`HOT_SELLER_RATIO = 100`（热卖家取自最近 100 人批次的首位）。
- `expires` 平均 = 到"再生成 `numInFlightAuctions`(=100) 个拍卖所需事件"的时刻；范围 `[1, 2*horizon)` 毫秒。
- `reserve > initialBid` 始终成立。

---

## 8. Bid 生成规则（`BidGenerator`）

```java
// auction（热拍卖逻辑）：P(热) = 1 - 1/hotAuctionRatio（默认 2 → 0.5）
if (random.nextInt(config.getHotAuctionRatio()) > 0) {
    auction = (AuctionGenerator.lastBase0AuctionId(config, eventId) / HOT_AUCTION_RATIO) * HOT_AUCTION_RATIO;  // HOT_AUCTION_RATIO=100
} else {
    auction = AuctionGenerator.nextBase0AuctionId(eventId, random, config);
}
auction += FIRST_AUCTION_ID;

// bidder（热买家逻辑）：P(热) = 1 - 1/hotBiddersRatio（默认 4 → 0.75）
if (random.nextInt(config.getHotBiddersRatio()) > 0) {
    bidder = (PersonGenerator.lastBase0PersonId(config, eventId) / HOT_BIDDER_RATIO) * HOT_BIDDER_RATIO + 1;  // 取批次第 2 人，避免与热卖家撞车
} else {
    bidder = PersonGenerator.nextBase0PersonId(eventId, random, config);
}
bidder += FIRST_PERSON_ID;

long price = PriceGenerator.nextPrice();

// channel / url（热渠道逻辑）：P(热) = 1 - 1/HOT_CHANNELS_RATIO（默认 2 → 0.5）
if (random.nextInt(HOT_CHANNELS_RATIO) > 0) {
    channel = HOT_CHANNELS[i];   // {"Google","Facebook","Baidu","Apple"}
    url     = HOT_URLS[i];
} else {
    channelAndUrl = getNextChannelAndurl(rand);  // 从 10000 条缓存取 "channel-N" + 可能带 &channel_id=
}
String extra = nextExtra(rand, currentSize, avgBidByteSize);  // 平均 100 字节
```

- **热渠道（HOT_CHANNELS）**：`Google, Facebook, Baidu, Apple`；对应 `HOT_URLS` 形如 `https://www.nexmark.com/xxxxx/xxxxx/xxxxx/item.htm?query=1`。
- **普通渠道**：`channel-0 .. channel-9999`（共 `CHANNELS_NUMBER = 10000`），URL 有 90% 概率追加 `&channel_id=<abs(Integer.reverse(i))>`。
- `auction` 选取范围限定在"最近 `numInFlightAuctions`(=100) 个仍在拍的拍卖 + lead"，保证 bid 能 join 到有效 auction。

---

## 9. 价格分布（`PriceGenerator`）

```java
public static long nextPrice(SplittableRandom random) {
    return Math.round(Math.pow(10.0, random.nextDouble() * 6.0) * 100.0);
}
```

- `random.nextDouble() ∈ [0,1)` → 指数 `∈ [0,6)` → `10^[0,6) ∈ [1, 1_000_000)` → `*100` → **价格 ∈ [100, 100_000_000) 分**，即 **$1.00 ~ $999,999.99**。
- **分布为对数均匀（log-uniform）**：低价区更密，高价稀少。这是 Q14/Q15 价格档位（`<10000`、`10000~1e6`、`>=1e6` 分）设定的依据。

---

## 10. 字符串 / extra 生成（`StringsGenerator`）

```java
// 随机串（最长 maxLength）：长度 3 + rand(maxLength-3)，字符 a-z，约 1/13 概率为 special 字符
public static String nextString(SplittableRandom random, int maxLength) { ... }

// extra 填充：使 currentSize + len 平均为 desiredAverageSize（avgPerson/avgAuction/avgBidByteSize）
public static String nextExtra(SplittableRandom random, int currentSize, int desiredAverageSize) {
    if (currentSize > desiredAverageSize) return "";
    desiredAverageSize -= currentSize;
    int delta = (int) Math.round(desiredAverageSize * 0.2);
    int minSize = desiredAverageSize - delta;
    int desiredSize = minSize + (delta == 0 ? 0 : random.nextInt(2 * delta));
    return nextExactString(random, desiredSize);
}
```

- `MIN_STRING_LENGTH = 3`。`nextExactString` 产出精确长度的纯小写字母串。
- `extra` 仅用于"凑平均字节数"以模拟真实负载，**不参与任何查询语义**。

---

## 11. 附注

- 本文件"权威"指官方 `nexmark/nexmark` 仓库 `nexmark-flink` 模块的生成器源码。这是 Flink NexMark 基准中**数据如何产生**的唯一可执行定义。
- 与原始 NEXMark 论文（2002）的关系：本生成器是 Flink 对论文基准的**实现**，部分常量（州/城市列表、价格对数分布、`numActivePeople` 等）为 Flink 版特有，以源码为准。
- 我们的 `nexmark_pk` 自有数据生成器若要对齐 Flink-test，须满足上述：ID 基线（1000/1000/10）、事件比例（1:3:46）、价格对数分布、热拍卖/热卖家/热买家/热渠道概率、确定性时间戳（baseTime + eventNumber*100µs）、类别取值 10..14、三流 PK/FK 可 join 等。具体对齐核对见 `REVIEW_FLINK_SEMANTIC_ALIGNMENT.md`（数据生成对齐部分）。
- 若需核对最新原文，以仓库 raw URL 为准（本文件为 2026-08-21 快照）。
