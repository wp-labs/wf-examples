#!/usr/bin/env python3
"""Extract per-rule emitted_total and correctness counters from metrics.ndjson.

Counters are emitted per 1s interval; sum all interval lines per (name, label).
Usage: extract_emitted.py [metrics_file]
"""
import json
import sys
from collections import defaultdict

path = sys.argv[1] if len(sys.argv) > 1 else "data/metrics.ndjson"
emitted = defaultdict(int)
append = defaultdict(int)
other = defaultdict(int)
for line in open(path):
    try:
        o = json.loads(line)
    except Exception:
        continue
    name = o.get("name")
    label = o.get("label", "")
    try:
        val = int(float(o.get("value", 0) or 0))
    except (TypeError, ValueError):
        continue
    if name == "emitted_total":
        emitted[label] += val
    elif name == "append_total":
        append[label] += val
    elif name in ("append_failed_total", "dropped_late_total",
                  "time_evicted_total", "memory_evicted_total",
                  "dispatch_total", "drain_dropped_records_total",
                  "no_sink_records_total", "channel_full_total",
                  "channel_send_failed_total", "sink_dispatch_failed_total"):
        other[name] += val
    elif name == "cursor_gap_total":
        other[f"cursor_gap[{label}]"] += val
print("== emitted_total per rule ==")
for k in sorted(emitted):
    print(f"  {k}: {emitted[k]}")
print("== append_total per stream ==")
for k in sorted(append):
    print(f"  {k}: {append[k]}")
print("== correctness counters ==")
for k in sorted(other):
    print(f"  {k}: {other[k]}")
