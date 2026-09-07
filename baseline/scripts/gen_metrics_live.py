#!/usr/bin/env python3
"""生成长跑注入事件（jsonl，供 wfgen send）。

用法: gen_metrics_live.py <count> <span_s> <offset_s>
  count:    事件总数（entity/metric 低基数池旋转 → 每窗每键多样本）
  span_s:   事件时间跨度（秒）——连续分布跨多个 15s 窗，驱动逐窗收盘
  offset_s: event_time 相对基准的偏移（秒）——每轮前移推进 watermark
值：qps ≈ N(1000, 25) 整数（无小数，避免 float→i128 截断语义干扰）。
"""
import json
import random
import sys

count = int(sys.argv[1])
span_s = float(sys.argv[2])
offset_s = int(sys.argv[3]) if len(sys.argv) > 3 else 0
BASE_NS = 1767225600000000000  # 2026-01-01T00:00:00Z
SVCS = ["svc_a", "svc_b", "svc_c", "svc_d", "svc_e"]
rng = random.Random(42 + offset_s)

rows = []
for i in range(count):
    t = BASE_NS + offset_s * 1_000_000_000 + int(span_s * 1e9 * i / count)
    svc = SVCS[i % len(SVCS)]
    value = 1000 + rng.randint(-25, 25)
    rows.append(
        {
            "_stream": "metrics_stream",
            "_timestamp": "2026-01-01T00:00:00.000Z",
            "_window": "metrics_stream",
            "entity": svc,
            "event_time": t,
            "metric": "qps",
            "value": float(value),
        }
    )

for r in rows:
    print(json.dumps(r))
