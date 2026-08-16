#!/usr/bin/env python3
"""Trace one auction: per-event count evolution + fire times, sim vs engine."""
import json
import re
import sys
from datetime import datetime

SPAN = 600_000_000_000
T = int(sys.argv[3]) if len(sys.argv) > 3 else 10
TARGET = int(sys.argv[4]) if len(sys.argv) > 4 else 1
STREAM_RE = re.compile(r'"_stream":\s*"(\w+)"')
AUC_RE = re.compile(r'"auction":\s*(\d+)')
DT_RE = re.compile(r'"dateTime":\s*(\d+)')

# collect engine fire times (seconds) for target auction
eng = []
for line in open(sys.argv[2]):
    o = json.loads(line)
    if int(o["__wfu_entity_id"]) == TARGET:
        eng.append(o["__wfu_fired_at"][11:19])  # HH:MM:SS

# per-event sim trace for target auction (events still processed globally
# for expiry sweep correctness; only target printed)
state = {}
HEAP = []
sim = []
log = []

def sweep(ns):
    while HEAP and HEAP[0][0] <= ns:
        exp, hauc = heappop(HEAP)
        hst = state.get(hauc)
        if hst is None:
            continue
        if hst[0] is not None and hst[0] + SPAN <= ns:
            hst[0], hst[1] = None, 0
            if hauc == TARGET:
                log.append((ns, f"EXPIRE auc={hauc} (created+600s <= now)"))

from heapq import heappush, heappop
idx = 0
for line in open(sys.argv[1]):
    s = STREAM_RE.search(line)
    if not s or s.group(1) != "bid_events":
        continue
    idx += 1
    auc = int(AUC_RE.search(line).group(1))
    ns = int(DT_RE.search(line).group(1))
    sweep(ns)
    st = state.get(auc)
    if st is None:
        st = state[auc] = [None, 0]
    if st[0] is None:
        st[0] = ns
        heappush(HEAP, (ns + SPAN, auc))
    st[1] += 1
    if st[1] >= T:
        if auc == TARGET:
            sim.append((ns, idx, st[0]))
            log.append((ns, f"FIRE auc={auc} arrival_idx={idx} created={st[0]}"))
        st[0], st[1] = ns, 0
        heappush(HEAP, (ns + SPAN, auc))
    elif auc == TARGET:
        log.append((ns, f"      auc={auc} idx={idx} count={st[1]} created={st[0]}"))

t0 = 1767225600  # 2026-01-01T00:00:00Z base, for readable seconds
def sec(ns):
    return ns // 1_000_000_000 - t0

print(f"=== sim fires for auc {TARGET}: {len(sim)}  eng: {len(eng)} ===")
i = j = 0
while i < len(sim) or j < len(eng):
    s_ns = sec(sim[i][0]) if i < len(sim) else None
    e_s = None
    if j < len(eng):
        hh, mm, ss = map(int, eng[j].split(":"))
        e_s = hh * 3600 + mm * 60 + ss
    if s_ns is not None and (e_s is None or s_ns <= e_s):
        mark = "  both" if e_s is not None and e_s == s_ns else "  SIM-ONLY"
        print(f"sim t+{s_ns:5d}s idx={sim[i][1]:6d}{mark}")
        if e_s is not None and e_s == s_ns:
            j += 1
        i += 1
    else:
        print(f"eng t+{e_s:5d}s{'  ENG-ONLY' if not (s_ns is not None and s_ns == e_s) else ''}")
        j += 1

print("\n=== trace around first divergence ===")
