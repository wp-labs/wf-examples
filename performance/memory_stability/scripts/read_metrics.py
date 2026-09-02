#!/usr/bin/env python3
"""读取 metrics NDJSON，返回指定 stage/name/label 的最新值。

用法: read_metrics.py <metrics_file> <stage> <name> [label]
  label: 精确匹配；`-`/`none` 匹配无 label 的指标（alloc/evictor 等）。
示例:
  read_metrics.py data/metrics.ndjson rule instances instance_growth
  read_metrics.py data/metrics.ndjson alloc current_commit_bytes -
"""
import json, sys

metrics_file, stage, name = sys.argv[1], sys.argv[2], sys.argv[3]
label = sys.argv[4] if len(sys.argv) > 4 else "-"
last = 0
try:
    with open(metrics_file) as f:
        for line in f:
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if obj.get("stage") != stage or obj.get("name") != name:
                continue
            rec_label = obj.get("label")
            if label not in ("-", "none") and rec_label != label:
                continue
            if label in ("-", "none") and rec_label is not None:
                continue
            try:
                last = int(obj.get("value", 0))
            except (TypeError, ValueError):
                pass
except FileNotFoundError:
    pass
print(last)
