#!/usr/bin/env python3
"""perf_diag_case — 确定性事件生成（单流 evt_events，seed=42）。

用法: python3 gen_events.py [N=100000] > ../data/evt.jsonl
字段: sip/action/code/blocked/bytes/event_time（覆盖规则三类成本：
count 无 guard / guard 过滤 / distinct 去重）。
"""
import json, random, sys

count = int(sys.argv[1]) if len(sys.argv) > 1 else 100000
rnd = random.Random(42)
BASE_NS = 1767225600000000000  # 2026-01-01T00:00:00Z

SIPS = [f"10.0.{i // 250}.{i % 250 + 1}" for i in range(1000)]
ACTIONS = ["allowed", "denied", "syn"]

for i in range(count):
    sip = SIPS[i % len(SIPS)]
    blocked = rnd.random() < 0.10
    action = "denied" if blocked else rnd.choice(["allowed", "allowed", "syn"])
    # 事件时间 1ms 步进：1000 个 sip 轮转 → 同 sip 事件间隔 1s（2m 窗口内
    # 每个 sip 积累 ~120 条 → count/guard/distinct 规则真实触发，规则墙可见）。
    ts = BASE_NS + i * 1_000_000
    ev = {
        "_stream": "evt_events",
        "_timestamp": ts,
        "_window": "evt_events",
        "sip": sip,
        "action": action,
        "code": rnd.randint(0, 999),
        "blocked": blocked,
        "bytes": rnd.randint(1, 10000),
        "event_time": ts,
    }
    print(json.dumps(ev))
