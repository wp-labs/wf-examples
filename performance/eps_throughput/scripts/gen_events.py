#!/usr/bin/env python3
"""生成 EPS 压测输入事件（jsonl，供 wfgen send 发送）。

用法: gen_events.py <count> <mode>
  mode:
    global   — 所有事件一个 sip（全局实例，最纯吞吐路径）
    distinct — 每个事件 distinct sip（实例 map churn，最大压力）
    pool     — 固定 sip 池循环复用（实例复用，贴近真实）[默认]
"""
import json, sys

count = int(sys.argv[1])
mode = sys.argv[2] if len(sys.argv) > 2 else "pool"
BASE_NS = 1767225600000000000  # 2026-01-01T00:00:00Z
POOL = 1000

for i in range(count):
    if mode == "global":
        sip = "10.0.0.1"
    elif mode == "distinct":
        sip = f"10.{(i >> 16) & 255}.{(i >> 8) & 255}.{i & 255}"
    else:
        sip = f"10.0.{i % (POOL >> 8)}.{i % POOL}"
    ev = {
        "_stream": "conn_events",
        "_timestamp": "2026-01-01T00:00:00.000Z",
        "_window": "conn_events",
        "action": "established",
        "bytes": 100,
        "bytes_in": 50,
        "bytes_out": 50,
        "dip": f"3.138.{i % 250}.{i % 250}",
        "dport": 443,
        "duration": 10,
        "event_time": BASE_NS + i * 1000,  # 1µs 间隔 — 事件时间不成为瓶颈
        "protocol": "tcp",
        "sip": sip,
    }
    print(json.dumps(ev))
