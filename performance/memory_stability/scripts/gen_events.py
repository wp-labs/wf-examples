#!/usr/bin/env python3
"""生成 memory_stability 的输入事件（jsonl，供 wfgen send 发送）。

用法: gen_events.py <count> <offset_seconds> [band]
  count:          事件数，每个事件一个 distinct sip
  offset_seconds: event_time 相对基准的偏移（秒）；trickle 用它推进 watermark
  band:           sip 第三段（默认 0；trickle 用 9 避免与 burst 撞 key）
"""
import json, sys

count = int(sys.argv[1])
offset_s = int(sys.argv[2]) if len(sys.argv) > 2 else 0
band = int(sys.argv[3]) if len(sys.argv) > 3 else 0
BASE_NS = 1767225600000000000  # 2026-01-01T00:00:00Z

for i in range(count):
    t = BASE_NS + offset_s * 1_000_000_000 + i * 1_000_000  # 1ms apart
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
        "event_time": t,
        "protocol": "tcp",
        "sip": f"10.{band}.{i // 250}.{i % 250}",
    }
    print(json.dumps(ev))
