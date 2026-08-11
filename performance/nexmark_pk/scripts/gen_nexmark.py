#!/usr/bin/env python3
"""Nexmark 事件生成器（确定性）→ wfusion JSONL。

按 NEXMark 事件模型生成 Person/Auction/Bid 三流（10m 窗口基准）：
  - 事件占比：Person 2% / Auction 6% / Bid 92%（bid 是 firehose）
  - hot 分布：50% hot auctions、25% hot bidders、25% hot sellers
  - 事件时间覆盖 ~30 分钟（供 3 个 10m 窗口填充），确定性（seed）

字段对齐标准 NEXMark schema（见 models/schemas/nexmark.wfs）：
  Person(id, name, email, city, state, dateTime)
  Auction(id, itemName, description, initialBid, reserve, dateTime, expires, seller, category, extra)
  Bid(auction, bidder, price, channel, url, dateTime, extra)

用法: gen_nexmark.py <count> [seed]
输出: 每行一个 wfusion JSONL 事件（含 _stream/_window/_timestamp 元数据）。
"""
import json, random, sys
from datetime import datetime, timezone

count = int(sys.argv[1])
seed = int(sys.argv[2]) if len(sys.argv) > 2 else 1
rnd = random.Random(seed)

BASE_NS = 1767225600000000000      # 2026-01-01T00:00:00Z
SPAN_NS = 1800_000_000_000         # 30 分钟事件跨度
PERSONS = 1000
HOT_SELLERS = 250                  # 25% 卖家为 hot
HOT_BIDDERS = 250                  # 25% 出价者为 hot
HOT_AUCTION_RATIO = 0.50           # 50% 拍卖为 hot

CITIES = ["Mountain View", "San Francisco", "Sunnyvale", "New York", "Los Angeles",
          "Chicago", "Boston", "Austin"]
STATES = ["CA", "CA", "CA", "NY", "CA", "IL", "MA", "TX"]
CHANNELS = ["Google", "Facebook", "Apple", "Direct", "Test"]
CATEGORIES = list(range(1, 27))    # NEXMark 用 category 1..26
BASE_URL = "http://www.example.com/"


def iso(ns):
    dt = datetime.fromtimestamp(ns / 1e9, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def event(stream, fields, ns):
    ev = {"_stream": stream, "_window": stream, "_timestamp": iso(ns)}
    ev.update(fields)
    return ev


def main():
    num_person = int(count * 0.02)
    num_auction = int(count * 0.06)
    num_bid = count - num_person - num_auction

    # ---- 预生成 persons（注册集中在前 5% 时间窗）----
    persons = []                     # id -> (state, hot_seller, hot_bidder)
    for pid in range(1, PERSONS + 1):
        persons.append({
            "state": rnd.choice(STATES),
            "hot_seller": pid <= HOT_SELLERS,
            "hot_bidder": pid <= HOT_BIDDERS,
        })

    # ---- 预生成 auctions ----
    auctions = []                    # id -> (seller, category, hot)
    for aid in range(1, num_auction + 1):
        hot = rnd.random() < HOT_AUCTION_RATIO
        # hot seller 优先被 hot auction 引用；否则随机
        seller_pool = list(range(1, HOT_SELLERS + 1)) if hot else list(range(1, PERSONS + 1))
        auctions.append({
            "seller": rnd.choice(seller_pool),
            "category": rnd.choice(CATEGORIES),
            "hot": hot,
        })

    out = []

    # ---- persons ----
    for i in range(num_person):
        pid = (i % PERSONS) + 1
        p = persons[pid - 1]
        ns = BASE_NS + rnd.randint(0, int(SPAN_NS * 0.10))
        out.append(event("person_events", {
            "id": pid, "name": f"person_{pid}", "email": f"person{pid}@example.com",
            "city": rnd.choice(CITIES), "state": p["state"],
            "dateTime": ns,
        }, ns))

    # ---- auctions（时间窗 10%-100%）----
    for i in range(num_auction):
        a = auctions[i]
        ns = BASE_NS + rnd.randint(int(SPAN_NS * 0.10), SPAN_NS)
        out.append(event("auction_events", {
            "id": i + 1,
            "itemName": f"item_{i}",
            "description": f"desc {i}",
            "initialBid": rnd.randint(10, 1000),
            "reserve": rnd.randint(1000, 10000),
            "dateTime": ns,
            "expires": ns + rnd.randint(600_000_000_000, 1800_000_000_000),
            "seller": a["seller"],
            "category": a["category"],
            "extra": "",
        }, ns))

    # ---- bids（92% firehose）----
    for i in range(num_bid):
        aidx = rnd.randrange(num_auction)
        a = auctions[aidx]
        # hot auction 提价概率高
        price = rnd.randint(100, 500) if a["hot"] else rnd.randint(10, 150)
        bidder = rnd.choice(list(range(1, HOT_BIDDERS + 1))) if rnd.random() < 0.5 \
            else rnd.randint(1, PERSONS)
        ns = BASE_NS + rnd.randint(int(SPAN_NS * 0.20), SPAN_NS)
        out.append(event("bid_events", {
            "auction": aidx + 1,
            "bidder": bidder,
            "price": price,
            "channel": rnd.choice(CHANNELS),
            "url": BASE_URL + str(rnd.randint(100, 999)),
            "dateTime": ns,
            "extra": "",
        }, ns))

    # 按时间排序输出（引擎按事件时间分窗）
    out.sort(key=lambda e: e["dateTime"])
    for e in out:
        print(json.dumps(e, ensure_ascii=False))


if __name__ == "__main__":
    main()
