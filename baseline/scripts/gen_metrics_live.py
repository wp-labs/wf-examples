#!/usr/bin/env python3
"""生成长跑注入事件（jsonl，供 wfgen send）。

用法: gen_metrics_live.py <count> <span_s> <offset_s> [spike_idx]
  count:    事件总数（entity/metric 低基数池旋转 → 每窗每键多样本）
  span_s:   事件时间跨度（秒）——连续分布跨多个 15s 窗，驱动逐窗收盘
  offset_s: event_time 相对基准的偏移（秒）——每轮前移推进 watermark
  spike_idx: 可选——把该序号的 5号线 事件值压成 9000（闭环判定的确定性越界注入；
             同 offset 同序号 → 每次重放同一越界点）
值：客流量 ≈ N(1000, 25) 整数（人次/采样点；无小数，避免 float→i128 截断语义干扰）。
"""
import json
import random
import sys

count = int(sys.argv[1])
span_s = float(sys.argv[2])
offset_s = int(sys.argv[3]) if len(sys.argv) > 3 else 0
spike_idx = int(sys.argv[4]) if len(sys.argv) > 4 else -1
BASE_NS = 1767225600000000000  # 2026-01-01T00:00:00Z
LINES = ["1号线", "2号线", "3号线", "4号线", "5号线"]
rng = random.Random(42 + offset_s)

rows = []
for i in range(count):
    t = BASE_NS + offset_s * 1_000_000_000 + int(span_s * 1e9 * i / count)
    svc = LINES[i % len(LINES)]
    value = 1000 + rng.randint(-25, 25)
    if i == spike_idx:
        assert svc == "5号线", f"spike_idx {spike_idx} 不是 5号线 事件"
        value = 9000
    rows.append(
        {
            "_stream": "metrics_stream",
            "_timestamp": "2026-01-01T00:00:00.000Z",
            "_window": "metrics_stream",
            "entity": svc,
            "event_time": t,
            "metric": "flow",
            "value": float(value),
        }
    )

for r in rows:
    print(json.dumps(r))
