#!/usr/bin/env python3
"""读取 metrics NDJSON，返回指定 stage/name/label 的最新值。

用法: read_metrics.py <metrics_file> <stage> <name> <label>
示例: read_metrics.py data/metrics.ndjson router delivered_total ""
"""
import json, sys

metrics_file, stage, name, label = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
last = 0
try:
    with open(metrics_file) as f:
        for line in f:
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if obj.get("stage") == stage and obj.get("name") == name and obj.get("label") == label:
                try:
                    last = int(obj.get("value", 0))
                except (TypeError, ValueError):
                    pass
except FileNotFoundError:
    pass
print(last)
