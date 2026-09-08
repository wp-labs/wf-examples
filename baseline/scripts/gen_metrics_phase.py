#!/usr/bin/env python3
"""相位同窗 e2e 事件生成（jsonl，供 wfgen send）。

用法: gen_metrics_phase.py <count> <span_s> <offset_s> [spike_idx]
  与 gen_metrics_live.py 同构，唯一差异：值按**事件时间的相位位置**取档——
  周期 240s（16 个 15s 相位格），忙时格 8..15 → 水平 3000，闲时格 0..7 →
  水平 1000（各加 ±25 均匀噪声；噪声有界 → 正常事件 z 恒 ≤1.7，绝不误报）。

场景语义（设计文档 §11.7 与 scripts/run_phase.sh）：
  - 每轮事件时间 +120s = 半个周期 → 奇轮落在忙半周期、偶轮落在闲半周期，
    同相位位置每 2 轮严格复现（该格的历史窗隔轮产生）；
  - 相位模式下 judge 只与**同相位历史**比较：忙时正常事件（3000±25）相对
    上轮同相位窗（同为 3000±25）不越界；若无相位（滚动模式），闲轮（1000）
    会撞上忙轮留下的基线（3000）→ 全量误报——这就是 run_phase 判定
    "judge 轮 1/2 静默、轮 r≥3 每轮恰 1 条" 的区分基础；
  - spike_idx 同 gen_metrics_live：该序号的 5号线 事件压成 9000（确定性越界）。
"""
import json
import random
import sys

count = int(sys.argv[1])
span_s = float(sys.argv[2])
offset_s = int(sys.argv[3]) if len(sys.argv) > 3 else 0
spike_idx = int(sys.argv[4]) if len(sys.argv) > 4 else -1
BASE_NS = 1767225600000000000  # 2026-01-01T00:00:00Z（15s/240s 网格对齐）
PERIOD_S = 240
BUCKET_S = 15
BUSY_SLOTS = range(8, 16)  # 忙时相位格（周期内 120s..240s）
BUSY_LEVEL = 3000.0
IDLE_LEVEL = 1000.0
LINES = ["1号线", "2号线", "3号线", "4号线", "5号线"]
rng = random.Random(42 + offset_s)

rows = []
for i in range(count):
    t = BASE_NS + offset_s * 1_000_000_000 + int(span_s * 1e9 * i / count)
    d_s = (t - BASE_NS) // 1_000_000_000
    slot = (d_s // BUCKET_S) % (PERIOD_S // BUCKET_S)
    level = BUSY_LEVEL if slot in BUSY_SLOTS else IDLE_LEVEL
    svc = LINES[i % len(LINES)]
    value = level + rng.randint(-25, 25)
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
