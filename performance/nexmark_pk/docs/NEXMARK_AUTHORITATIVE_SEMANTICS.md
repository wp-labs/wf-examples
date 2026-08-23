# NEXMark 权威查询语义参考（Flink / nexmark-flink）

> **本文件只记录权威定义，不含任何对齐/评审结论。** 对齐情况见同目录 `REVIEW_FLINK_CONFORMANCE_2026-08-23.md`。

## 权威出处（Authoritative Source）

- **官方仓库**：<https://github.com/nexmark/nexmark>
- **查询目录（带浏览/历史）**：<https://github.com/nexmark/nexmark/tree/master/nexmark-flink/src/main/resources/queries>
- **原始 SQL（raw，每条可独立访问）**：
  `https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/qN.sql`
- **本文件所有 SQL 均逐字抄录自上述 `raw.githubusercontent.com/nexmark/nexmark/master/.../qN.sql`，抓取日期：2026-08-21。**

## 谱系（Lineage，依官方仓库标注）

官方仓库 each SQL 头部自带注释，标注其来源：

- **Q1–Q8**：原始 NEXMark 套件（NEXMark benchmark paper，2002）。
- **Q9–Q22**：SQL 头部均标注 `(Not in original suite)` —— 不在原始论文套件内。
  - **Q9–Q13**：由 Apache Beam Nexmark 移植而来。
  - **Q14–Q22**：nexmark-flink 自行扩展的查询（用以演示各类流式算子能力）。

> 说明：Q9–Q22 的"权威语义"即 nexmark-flink 仓库中这些 SQL 表达的含义；它们与原始论文 Q9–Q22（如有）可能不同。仲裁以本仓库 SQL 为准。

## 索引（Qn → 名称 → 权威地址）

| Query | 名称 | 原始套件 | 权威 URL |
|------|------|---------|---------|
| Q1  | Currency conversion | 原始 | <https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q1.sql> |
| Q2  | Selection | 原始 | <https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q2.sql> |
| Q3  | Local Item Suggestion | 原始 | <https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q3.sql> |
| Q4  | Average Price for a Category | 原始 | <https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q4.sql> |
| Q5  | Hot Items | 原始 | <https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q5.sql> |
| Q6  | Average Selling Price by Seller | 原始 | <https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q6.sql> |
| Q7  | Highest Bid | 原始 | <https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q7.sql> |
| Q8  | Monitor New Users | 原始 | <https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q8.sql> |
| Q9  | Winning Bids | 扩展(Beam) | <https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q9.sql> |
| Q10 | Log to File System | 扩展(Beam) | <https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q10.sql> |
| Q11 | User Sessions | 扩展(Beam) | <https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q11.sql> |
| Q12 | Processing Time Windows | 扩展(Beam) | <https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q12.sql> |
| Q13 | Bounded Side Input Join | 扩展(Beam) | <https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q13.sql> |
| Q14 | Calculation | 扩展 | <https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q14.sql> |
| Q15 | Bidding Statistics Report | 扩展 | <https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q15.sql> |
| Q16 | Channel Statistics Report | 扩展 | <https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q16.sql> |
| Q17 | Auction Statistics Report | 扩展 | <https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q17.sql> |
| Q18 | Find last bid | 扩展 | <https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q18.sql> |
| Q19 | Auction TOP-10 Price | 扩展 | <https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q19.sql> |
| Q20 | Expand bid with auction | 扩展 | <https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q20.sql> |
| Q21 | Add channel id | 扩展 | <https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q21.sql> |
| Q22 | Get URL Directories | 扩展 | <https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q22.sql> |

---

## 权威语义（逐条，原文抄录）

### Q1 — Currency conversion
**意图**：把每个 bid 的美元价格换算成欧元。演示简单转换。

```sql
-- -------------------------------------------------------------------------------------------------
-- Query1: Currency conversion
-- -------------------------------------------------------------------------------------------------
-- Convert each bid value from dollars to euros. Illustrates a simple transformation.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE nexmark_q1 (
  auction  BIGINT,
  bidder  BIGINT,
  price  DECIMAL(23, 3),
  `dateTime`  TIMESTAMP(3),
  extra  VARCHAR
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q1
SELECT
    auction,
    bidder,
    0.908 * price as price, -- convert dollar to euro
    `dateTime`,
    extra
FROM bid;
```

权威地址：<https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q1.sql>

---

### Q2 — Selection
**意图**：选出特定 auction 的 bid 及其价格。原始 CQL 只筛固定几个 auction id（结果极少），Flink 版改为"每第 123 个 auction"。

```sql
-- -------------------------------------------------------------------------------------------------
-- Query2: Selection
-- -------------------------------------------------------------------------------------------------
-- Find bids with specific auction ids and show their bid price.
--
-- In original Nexmark queries, Query2 is as following (in CQL syntax):
--
--   SELECT Rstream(auction, price)
--   FROM Bid [NOW]
--   WHERE auction = 1007 OR auction = 1020 OR auction = 2001 OR auction = 2019 OR auction = 2087;
--
-- However, that query will only yield a few hundred results over event streams of arbitrary size.
-- To make it more interesting we instead choose bids for every 123'th auction.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE nexmark_q2 (
  auction  BIGINT,
  price  BIGINT
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q2
SELECT auction, price FROM bid WHERE MOD(auction, 123) = 0;
```

权威地址：<https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q2.sql>

---

### Q3 — Local Item Suggestion
**意图**：谁在 OR/ID/CA 州、category 10 下卖东西，对应哪些 auction id？演示增量 join + 过滤。

```sql
-- -------------------------------------------------------------------------------------------------
-- Query 3: Local Item Suggestion
-- -------------------------------------------------------------------------------------------------
-- Who is selling in OR, ID or CA in category 10, and for what auction ids?
-- Illustrates an incremental join (using per-key state and timer) and filter.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE nexmark_q3 (
  name  VARCHAR,
  city  VARCHAR,
  state  VARCHAR,
  id  BIGINT
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q3
SELECT
    P.name, P.city, P.state, A.id
FROM
    auction AS A INNER JOIN person AS P on A.seller = P.id
WHERE
    A.category = 10 and (P.state = 'OR' OR P.state = 'ID' OR P.state = 'CA');
```

权威地址：<https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q3.sql>

---

### Q4 — Average Price for a Category
**意图**：每个 category 下所有拍卖的"中标价"平均值。演示复杂 join + 聚合。

```sql
-- -------------------------------------------------------------------------------------------------
-- Query 4: Average Price for a Category
-- -------------------------------------------------------------------------------------------------
-- Select the average of the wining bid prices for all auctions in each category.
-- Illustrates complex join and aggregation.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE nexmark_q4 (
  id BIGINT,
  final BIGINT
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q4
SELECT
    Q.category,
    AVG(Q.final)
FROM (
    SELECT MAX(B.price) AS final, A.category
    FROM auction A, bid B
    WHERE A.id = B.auction AND B.`dateTime` BETWEEN A.`dateTime` AND A.expires
    GROUP BY A.id, A.category
) Q
GROUP BY Q.category;
```

权威地址：<https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q4.sql>

---

### Q5 — Hot Items
**意图**：最近一个时段内收到 bid 最多的 auction 是哪些？演示滑动窗口 + combiner。Flink 版用 10s 滑窗、每 2s 更新（原始论文为 1h/1min）。

```sql
-- -------------------------------------------------------------------------------------------------
-- Query 5: Hot Items
-- -------------------------------------------------------------------------------------------------
-- Which auctions have seen the most bids in the last period?
-- Illustrates sliding windows and combiners.
--
-- The original Nexmark Query5 calculate the hot items in the last hour (updated every minute).
-- To make things a bit more dynamic and easier to test we use much shorter windows,
-- i.e. in the last 10 seconds and update every 2 seconds.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE nexmark_q5 (
  auction  BIGINT,
  num  BIGINT
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q5
SELECT AuctionBids.auction, AuctionBids.num
 FROM (
   SELECT
     auction,
     count(*) AS num,
     window_start AS starttime,
     window_end AS endtime
     FROM TABLE(
             HOP(TABLE bid, DESCRIPTOR(`dateTime`), INTERVAL '2' SECOND, INTERVAL '10' SECOND))
     GROUP BY auction, window_start, window_end
 ) AS AuctionBids
 JOIN (
   SELECT
     max(CountBids.num) AS maxn,
     CountBids.starttime,
     CountBids.endtime
   FROM (
     SELECT
       count(*) AS num,
       window_start AS starttime,
       window_end AS endtime
     FROM TABLE(
                HOP(TABLE bid, DESCRIPTOR(`dateTime`), INTERVAL '2' SECOND, INTERVAL '10' SECOND))
     GROUP BY auction, window_start, window_end
     ) AS CountBids
   GROUP BY CountBids.starttime, CountBids.endtime
 ) AS MaxBids
 ON AuctionBids.starttime = MaxBids.starttime AND
    AuctionBids.endtime = MaxBids.endtime AND
    AuctionBids.num >= MaxBids.maxn;
```

权威地址：<https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q5.sql>

---

### Q6 — Average Selling Price by Seller
**意图**：每个 seller 最近 10 个已结束拍卖的平均售价。演示 specialized combiner（OVER 窗口）。

```sql
-- -------------------------------------------------------------------------------------------------
-- Query 6: Average Selling Price by Seller
-- -------------------------------------------------------------------------------------------------
-- What is the average selling price per seller for their last 10 closed auctions.
-- Shares the same 'winning bids' core as for Query4, and illustrates a specialized combiner.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE nexmark_q6 (
  seller VARCHAR,
  avg_price  BIGINT
) WITH (
  'connector' = 'blackhole'
);

-- TODO: this query is not supported yet in Flink SQL, because the OVER WINDOW operator doesn't
--  support to consume retractions.
INSERT INTO nexmark_q6
SELECT
    Q.seller,
    AVG(Q.price) OVER
        (PARTITION BY Q.seller ORDER BY Q.`dateTime` ROWS BETWEEN 10 PRECEDING AND CURRENT ROW)
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY A.id, A.seller ORDER BY B.price DESC) AS rownum
    FROM (SELECT A.id, A.seller, B.price, B.`dateTime`
        FROM auction AS A,
            bid AS B
        WHERE A.id = B.auction
            and B.`dateTime` between A.`dateTime` and A.expires)
    WHERE rownum <= 1
) AS Q;
```

权威地址：<https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q6.sql>

---

### Q7 — Highest Bid
**意图**：每个时段的最高 bid 是哪些？用 side input 演示 fanout。Flink 版用 10s 滚窗（原始论文为 1min）。

```sql
-- -------------------------------------------------------------------------------------------------
-- Query 7: Highest Bid
-- -------------------------------------------------------------------------------------------------
-- What are the highest bids per period?
-- Deliberately implemented using a side input to illustrate fanout.
--
-- The original Nexmark Query7 calculate the highest bids in the last minute.
-- We will use a shorter window (10 seconds) to help make testing easier.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE nexmark_q7 (
  auction  BIGINT,
  bidder  BIGINT,
  price  BIGINT,
  `dateTime`  TIMESTAMP(3),
  extra  VARCHAR
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q7
SELECT B.auction, B.price, B.bidder, B.`dateTime`, B.extra
from bid B
JOIN (
  SELECT MAX(price) AS maxprice, window_end as `dateTime`
  FROM TABLE(
          TUMBLE(TABLE bid, DESCRIPTOR(`dateTime`), INTERVAL '10' SECOND))
  GROUP BY window_start, window_end
) B1
ON B.price = B1.maxprice
WHERE B.`dateTime` BETWEEN B1.`dateTime`  - INTERVAL '10' SECOND AND B1.`dateTime`;
```

权威地址：<https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q7.sql>

---

### Q8 — Monitor New Users
**意图**：选出"在最近时段内进入系统并创建了 auction"的人。演示简单 join。Flink 版用 10s 滚窗（原始论文为 12h/12h）。

```sql
-- -------------------------------------------------------------------------------------------------
-- Query 8: Monitor New Users
-- -------------------------------------------------------------------------------------------------
-- Select people who have entered the system and created auctions in the last period.
-- Illustrates a simple join.
--
-- The original Nexmark Query8 monitors the new users the last 12 hours, updated every 12 hours.
-- To make things a bit more dynamic and easier to test we use much shorter windows (10 seconds).
-- -------------------------------------------------------------------------------------------------

CREATE TABLE nexmark_q8 (
  id  BIGINT,
  name  VARCHAR,
  stime  TIMESTAMP(3)
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q8
SELECT P.id, P.name, P.starttime
FROM (
  SELECT id, name,
        window_start AS starttime,
        window_end AS endtime
  FROM TABLE(
            TUMBLE(TABLE person, DESCRIPTOR(`dateTime`), INTERVAL '10' SECOND))
  GROUP BY id, name, window_start, window_end
) P
JOIN (
  SELECT seller,
        window_start AS starttime,
        window_end AS endtime
  FROM TABLE(
        TUMBLE(TABLE auction, DESCRIPTOR(`dateTime`), INTERVAL '10' SECOND))
  GROUP BY seller, window_start, window_end
) A
ON P.id = A.seller AND P.starttime = A.starttime AND P.endtime = A.endtime;
```

权威地址：<https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q8.sql>

---

### Q9 — Winning Bids *(Not in original suite)*
**意图**：找出每个 auction 的中标价（最高价 bid）。

```sql
-- -------------------------------------------------------------------------------------------------
-- Query 9: Winning Bids (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- Find the winning bid for each auction.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE nexmark_q9 (
  id  BIGINT,
  itemName  VARCHAR,
  description  VARCHAR,
  initialBid  BIGINT,
  reserve  BIGINT,
  `dateTime`  TIMESTAMP(3),
  expires  TIMESTAMP(3),
  seller  BIGINT,
  category  BIGINT,
  extra  VARCHAR,
  auction  BIGINT,
  bidder  BIGINT,
  price  BIGINT,
  bid_dateTime  TIMESTAMP(3),
  bid_extra  VARCHAR
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q9
SELECT
    id, itemName, description, initialBid, reserve, `dateTime`, expires, seller, category, extra,
    auction, bidder, price, bid_dateTime, bid_extra
FROM (
   SELECT A.*, B.auction, B.bidder, B.price, B.`dateTime` AS bid_dateTime, B.extra AS bid_extra,
     ROW_NUMBER() OVER (PARTITION BY A.id ORDER BY B.price DESC, B.`dateTime` ASC) AS rownum
   FROM auction A, bid B
   WHERE A.id = B.auction AND B.`dateTime` BETWEEN A.`dateTime` AND A.expires
)
WHERE rownum <= 1;
```

权威地址：<https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q9.sql>

---

### Q10 — Log to File System *(Not in original suite)*
**意图**：把所有事件按时间分区落盘到文件系统。演示流式数据写入分区文件。

```sql
-- -------------------------------------------------------------------------------------------------
-- Query 10: Log to File System (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- Log all events to file system. Illustrates windows streaming data into partitioned file system.
--
-- Every minute, save all events from the last period into partitioned log files.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE nexmark_q10 (
  auction  BIGINT,
  bidder  BIGINT,
  price  BIGINT,
  `dateTime`  TIMESTAMP(3),
  extra  VARCHAR,
  dt STRING,
  hm STRING
) PARTITIONED BY (dt, hm) WITH (
  'connector' = 'filesystem',
  'path' = 'file://${NEXMARK_DIR}/data/output/${SUBMIT_TIME}/bid/',
  'format' = 'csv',
  'sink.partition-commit.trigger' = 'partition-time',
  'sink.partition-commit.delay' = '1 min',
  'sink.partition-commit.policy.kind' = 'success-file',
  'partition.time-extractor.timestamp-pattern' = '$dt $hm:00',
  'sink.rolling-policy.rollover-interval' = '1min',
  'sink.rolling-policy.check-interval' = '1min'
);

INSERT INTO nexmark_q10
SELECT auction, bidder, price, `dateTime`, extra, DATE_FORMAT(`dateTime`, 'yyyy-MM-dd'), DATE_FORMAT(`dateTime`, 'HH:mm')
FROM bid;
```

权威地址：<https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q10.sql>

---

### Q11 — User Sessions *(Not in original suite)*
**意图**：同一用户在每个 session 内出了多少次 bid？演示 session 窗口。

```sql
-- -------------------------------------------------------------------------------------------------
-- Query 11: User Sessions (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- How many bids did a user make in each session they were active? Illustrates session windows.
--
-- Group bids by the same user into sessions with max session gap.
-- Emit the number of bids per session.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE nexmark_q11 (
  bidder BIGINT,
  bid_count BIGINT,
  starttime TIMESTAMP(3),
  endtime TIMESTAMP(3)
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q11
SELECT
    B.bidder,
    count(*) as bid_count,
    SESSION_START(B.`dateTime`, INTERVAL '10' SECOND) as starttime,
    SESSION_END(B.`dateTime`, INTERVAL '10' SECOND) as endtime
FROM bid B
GROUP BY B.bidder, SESSION(B.`dateTime`, INTERVAL '10' SECOND);
```

权威地址：<https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q11.sql>

---

### Q12 — Processing Time Windows *(Not in original suite)*
**意图**：同一用户在固定 processing-time 窗口（10s）内出了多少次 bid？演示 processing time 窗口。

```sql
-- -------------------------------------------------------------------------------------------------
-- Query 12: Processing Time Windows (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- How many bids does a user make within a fixed processing time limit?
-- Illustrates working in processing time window.
--
-- Group bids by the same user into processing time windows of 10 seconds.
-- Emit the count of bids per window.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE nexmark_q12 (
  bidder BIGINT,
  bid_count BIGINT,
  starttime TIMESTAMP(3),
  endtime TIMESTAMP(3)
) WITH (
  'connector' = 'blackhole'
);

CREATE VIEW B AS SELECT *, PROCTIME() as p_time FROM bid;

INSERT INTO nexmark_q12
SELECT
    bidder,
    count(*) as bid_count,
    window_start AS starttime,
    window_end AS endtime
FROM TABLE(
        TUMBLE(TABLE B, DESCRIPTOR(p_time), INTERVAL '10' SECOND))
GROUP BY bidder, window_start, window_end;
```

权威地址：<https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q12.sql>

---

### Q13 — Bounded Side Input Join *(Not in original suite)*
**意图**：把流与一个有限 side input join，演示基础流 enrichment。

```sql
-- -------------------------------------------------------------------------------------------------
-- Query 13: Bounded Side Input Join (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- Joins a stream to a bounded side input, modeling basic stream enrichment.
-- -------------------------------------------------------------------------------------------------

-- TODO: use the new "filesystem" connector once FLINK-17397 is done
CREATE TABLE side_input (
  key BIGINT,
  `value` VARCHAR
) WITH (
  'connector.type' = 'filesystem',
  'connector.path' = 'file://${FLINK_HOME}/data/side_input.txt',
  'format.type' = 'csv'
);

CREATE TABLE nexmark_q13 (
  auction  BIGINT,
  bidder  BIGINT,
  price  BIGINT,
  `dateTime`  TIMESTAMP(3),
  `value`  VARCHAR
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q13
SELECT
    B.auction,
    B.bidder,
    B.price,
    B.`dateTime`,
    S.`value`
FROM (SELECT *, PROCTIME() as p_time FROM bid) B
JOIN side_input FOR SYSTEM_TIME AS OF B.p_time AS S
ON mod(B.auction, 10000) = S.key;
```

权威地址：<https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q13.sql>

---

### Q14 — Calculation *(Not in original suite)*
**意图**：把 bid 时间戳换算成类型，并筛出特定价格区间的 bid。演示重复表达式与 UDF 使用。

```sql
-- -------------------------------------------------------------------------------------------------
-- Query 14: Calculation (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- Convert bid timestamp into types and find bids with specific price.
-- Illustrates duplicate expressions and usage of user-defined-functions.
-- -------------------------------------------------------------------------------------------------

CREATE FUNCTION count_char AS 'com.github.nexmark.flink.udf.CountChar';

CREATE TABLE nexmark_q14 (
    auction BIGINT,
    bidder BIGINT,
    price  DECIMAL(23, 3),
    bidTimeType VARCHAR,
    `dateTime` TIMESTAMP(3),
    extra VARCHAR,
    c_counts BIGINT
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q14
SELECT
    auction,
    bidder,
    0.908 * price as price,
    CASE
        WHEN HOUR(`dateTime`) >= 8 AND HOUR(`dateTime`) <= 18 THEN 'dayTime'
        WHEN HOUR(`dateTime`) <= 6 OR HOUR(`dateTime`) >= 20 THEN 'nightTime'
        ELSE 'otherTime'
    END AS bidTimeType,
    `dateTime`,
    extra,
    count_char(extra, 'c') AS c_counts
FROM bid
WHERE 0.908 * price > 1000000 AND 0.908 * price < 50000000;
```

权威地址：<https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q14.sql>

---

### Q15 — Bidding Statistics Report *(Not in original suite)*
**意图**：不同价格档位的 bid/出价人/拍卖数统计。演示带过滤的多 distinct 聚合。

```sql
-- -------------------------------------------------------------------------------------------------
-- Query 15: Bidding Statistics Report (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- How many distinct users join the bidding for different level of price?
-- Illustrates multiple distinct aggregations with filters.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE nexmark_q15 (
  `day` VARCHAR,
  total_bids BIGINT,
  rank1_bids BIGINT,
  rank2_bids BIGINT,
  rank3_bids BIGINT,
  total_bidders BIGINT,
  rank1_bidders BIGINT,
  rank2_bidders BIGINT,
  rank3_bidders BIGINT,
  total_auctions BIGINT,
  rank1_auctions BIGINT,
  rank2_auctions BIGINT,
  rank3_auctions BIGINT
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q15
SELECT
     DATE_FORMAT(`dateTime`, 'yyyy-MM-dd') as `day`,
     count(*) AS total_bids,
     count(*) filter (where price < 10000) AS rank1_bids,
     count(*) filter (where price >= 10000 and price < 1000000) AS rank2_bids,
     count(*) filter (where price >= 1000000) AS rank3_bids,
     count(distinct bidder) AS total_bidders,
     count(distinct bidder) filter (where price < 10000) AS rank1_bidders,
     count(distinct bidder) filter (where price >= 10000 and price < 1000000) AS rank2_bidders,
     count(distinct bidder) filter (where price >= 1000000) AS rank3_bidders,
     count(distinct auction) AS total_auctions,
     count(distinct auction) filter (where price < 10000) AS rank1_auctions,
     count(distinct auction) filter (where price >= 10000 and price < 1000000) AS rank2_auctions,
     count(distinct auction) filter (where price >= 1000000) AS rank3_auctions
FROM bid
GROUP BY DATE_FORMAT(`dateTime`, 'yyyy-MM-dd');
```

权威地址：<https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q15.sql>

---

### Q16 — Channel Statistics Report *(Not in original suite)*
**意图**：按 channel 分组的多档位 distinct 统计。演示多 key 的带过滤多 distinct 聚合。

```sql
-- -------------------------------------------------------------------------------------------------
-- Query 16: Channel Statistics Report (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- How many distinct users join the bidding for different level of price for a channel?
-- Illustrates multiple distinct aggregations with filters for multiple keys.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE nexmark_q16 (
    channel VARCHAR,
    `day` VARCHAR,
    `minute` VARCHAR,
    total_bids BIGINT,
    rank1_bids BIGINT,
    rank2_bids BIGINT,
    rank3_bids BIGINT,
    total_bidders BIGINT,
    rank1_bidders BIGINT,
    rank2_bidders BIGINT,
    rank3_bidders BIGINT,
    total_auctions BIGINT,
    rank1_auctions BIGINT,
    rank2_auctions BIGINT,
    rank3_auctions BIGINT
) WITH (
    'connector' = 'blackhole'
);

INSERT INTO nexmark_q16
SELECT
    channel,
    DATE_FORMAT(`dateTime`, 'yyyy-MM-dd') as `day`,
    max(DATE_FORMAT(`dateTime`, 'HH:mm')) as `minute`,
    count(*) AS total_bids,
    count(*) filter (where price < 10000) AS rank1_bids,
    count(*) filter (where price >= 10000 and price < 1000000) AS rank2_bids,
    count(*) filter (where price >= 1000000) AS rank3_bids,
    count(distinct bidder) AS total_bidders,
    count(distinct bidder) filter (where price < 10000) AS rank1_bidders,
    count(distinct bidder) filter (where price >= 10000 and price < 1000000) AS rank2_bidders,
    count(distinct bidder) filter (where price >= 1000000) AS rank3_bidders,
    count(distinct auction) AS total_auctions,
    count(distinct auction) filter (where price < 10000) AS rank1_auctions,
    count(distinct auction) filter (where price >= 10000 and price < 1000000) AS rank2_auctions,
    count(distinct auction) filter (where price >= 1000000) AS rank3_auctions
FROM bid
GROUP BY channel, DATE_FORMAT(`dateTime`, 'yyyy-MM-dd');
```

权威地址：<https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q16.sql>

---

### Q17 — Auction Statistics Report *(Not in original suite)*
**意图**：每个 auction 每天的 bid 数与价格统计。演示无界 group 聚合。

```sql
-- -------------------------------------------------------------------------------------------------
-- Query 17: Auction Statistics Report (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- How many bids on an auction made a day and what is the price?
-- Illustrates an unbounded group aggregation.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE nexmark_q17 (
  auction BIGINT,
  `day` VARCHAR,
  total_bids BIGINT,
  rank1_bids BIGINT,
  rank2_bids BIGINT,
  rank3_bids BIGINT,
  min_price BIGINT,
  max_price BIGINT,
  avg_price BIGINT,
  sum_price BIGINT
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q17
SELECT
     auction,
     DATE_FORMAT(`dateTime`, 'yyyy-MM-dd') as `day`,
     count(*) AS total_bids,
     count(*) filter (where price < 10000) AS rank1_bids,
     count(*) filter (where price >= 10000 and price < 1000000) AS rank2_bids,
     count(*) filter (where price >= 1000000) AS rank3_bids,
     min(price) AS min_price,
     max(price) AS max_price,
     avg(price) AS avg_price,
     sum(price) AS sum_price
FROM bid
GROUP BY auction, DATE_FORMAT(`dateTime`, 'yyyy-MM-dd');
```

权威地址：<https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q17.sql>

---

### Q18 — Find last bid *(Not in original suite)*
**意图**：每个 (bidder, auction) 的最后一次 bid。演示 Deduplicate 查询。

```sql
-- -------------------------------------------------------------------------------------------------
-- Query 18: Find last bid (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- What's a's last bid for bidder to auction?
-- Illustrates a Deduplicate query.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE nexmark_q18 (
    auction  BIGINT,
    bidder  BIGINT,
    price  BIGINT,
    channel  VARCHAR,
    url  VARCHAR,
    `dateTime`  TIMESTAMP(3),
    extra  VARCHAR
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q18
SELECT auction, bidder, price, channel, url, `dateTime`, extra
 FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY bidder, auction ORDER BY `dateTime` DESC) AS rank_number
       FROM bid)
 WHERE rank_number <= 1;
```

权威地址：<https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q18.sql>

---

### Q19 — Auction TOP-10 Price *(Not in original suite)*
**意图**：每个 auction 价格最高的 10 个 bid。演示 TOP-N 查询。

```sql
-- -------------------------------------------------------------------------------------------------
-- Query 19: Auction TOP-10 Price (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- What's the top price 10 bids of an auction?
-- Illustrates a TOP-N query.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE nexmark_q19 (
    auction  BIGINT,
    bidder  BIGINT,
    price  BIGINT,
    channel  VARCHAR,
    url  VARCHAR,
    `dateTime`  TIMESTAMP(3),
    extra  VARCHAR,
    rank_number  BIGINT
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q19
SELECT * FROM
(SELECT *, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) AS rank_number FROM bid)
WHERE rank_number <= 10;
```

权威地址：<https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q19.sql>

---

### Q20 — Expand bid with auction *(Not in original suite)*
**意图**：把 bid 与其对应 auction 信息（category=10）展开 join。演示 filter join。

```sql
-- -------------------------------------------------------------------------------------------------
-- Query 20: Expand bid with auction (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- Get bids with the corresponding auction information where category is 10.
-- Illustrates a filter join.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE nexmark_q20 (
    auction  BIGINT,
    bidder  BIGINT,
    price  BIGINT,
    channel  VARCHAR,
    url  VARCHAR,
    bid_dateTime  TIMESTAMP(3),
    bid_extra  VARCHAR,

    itemName  VARCHAR,
    description  VARCHAR,
    initialBid  BIGINT,
    reserve  BIGINT,
    auction_dateTime  TIMESTAMP(3),
    expires  TIMESTAMP(3),
    seller  BIGINT,
    category  BIGINT,
    auction_extra  VARCHAR
) WITH (
    'connector' = 'blackhole'
);

INSERT INTO nexmark_q20
SELECT
    auction, bidder, price, channel, url, B.`dateTime`, B.extra,
    itemName, description, initialBid, reserve, A.`dateTime`, expires, seller, category, A.extra
FROM
    bid AS B INNER JOIN auction AS A on B.auction = A.id
WHERE A.category = 10;
```

权威地址：<https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q20.sql>

---

### Q21 — Add channel id *(Not in original suite)*
**意图**：给 bid 表追加 channel_id 列。演示 `CASE WHEN` + `REGEXP_EXTRACT`。

```sql
-- -------------------------------------------------------------------------------------------------
-- Query 21: Add channel id (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- Add a channel_id column to the bid table.
-- Illustrates a 'CASE WHEN' + 'REGEXP_EXTRACT' SQL.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE nexmark_q21 (
    auction  BIGINT,
    bidder  BIGINT,
    price  BIGINT,
    channel  VARCHAR,
    channel_id  VARCHAR
) WITH (
    'connector' = 'blackhole'
);

INSERT INTO nexmark_q21
SELECT
    auction, bidder, price, channel,
    CASE
        WHEN lower(channel) = 'apple' THEN '0'
        WHEN lower(channel) = 'google' THEN '1'
        WHEN lower(channel) = 'facebook' THEN '2'
        WHEN lower(channel) = 'baidu' THEN '3'
        ELSE REGEXP_EXTRACT(url, '(&|^)channel_id=([^&]*)', 2)
        END
    AS channel_id FROM bid
    where REGEXP_EXTRACT(url, '(&|^)channel_id=([^&]*)', 2) is not null or
          lower(channel) in ('apple', 'google', 'facebook', 'baidu');
```

权威地址：<https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q21.sql>

---

### Q22 — Get URL Directories *(Not in original suite)*
**意图**：URL 的目录结构是什么？演示 `SPLIT_INDEX`。

```sql
-- -------------------------------------------------------------------------------------------------
-- Query 22: Get URL Directories (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- What is the directory structure of the URL?
-- Illustrates a SPLIT_INDEX SQL.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE nexmark_q22 (
      auction  BIGINT,
      bidder  BIGINT,
      price  BIGINT,
      channel  VARCHAR,
      dir1  VARCHAR,
      dir2  VARCHAR,
      dir3  VARCHAR
) WITH (
    'connector' = 'blackhole'
);

INSERT INTO nexmark_q22
SELECT
    auction, bidder, price, channel,
    SPLIT_INDEX(url, '/', 3) as dir1,
    SPLIT_INDEX(url, '/', 4) as dir2,
    SPLIT_INDEX(url, '/', 5) as dir3 FROM bid;
```

权威地址：<https://raw.githubusercontent.com/nexmark/nexmark/master/nexmark-flink/src/main/resources/queries/q22.sql>

---

## 附注

- 本文件的"权威"指 **`github.com/nexmark/nexmark` 仓库的 `nexmark-flink` 模块**（Flink 官方 NexMark 基准实现）。该仓库是当前业界对照 Flink 语义的事实标准来源。
- 我们自己的实现位于 `wf-examples/performance/nexmark_pk/models/queries/qN.wfl`，其与上述权威语义的对齐情况**不在本文件讨论范围**，见 `REVIEW_FLINK_CONFORMANCE_2026-08-23.md`。
- 若需核对最新原文，请以仓库 raw URL 为准（本文件为 2026-08-21 快照）。
