#!/usr/bin/env python3
"""Q5 per-alert diff with EXACT engine heap semantics.

Mirrors wf-engine push_expiry_candidate dedup: one pending heap entry per key.
Fire/reset pushes are DROPPED while a pending entry exists, so an instance's
actual expiry is driven by the stale heap entry (created-at-at-push-time
+ SPAN), refreshed only when popped (re-read current created_at + SPAN,
requeue if still in the future).
"""
import json
import re
import sys
from collections import Counter
from datetime import datetime
from heapq import heappush, heappop

SPAN = 600_000_000_000
T = 10
STREAM_RE = re.compile(r'"_stream":\s*"(\w+)"')
AUC_RE = re.compile(r'"auction":\s*(\d+)')
DT_RE = re.compile(r'"dateTime":\s*(\d+)')

state = {}   # auc -> [created_ns_or_None, count]; None == no instance
HEAP = []    # (expire_at, auc)
pending = set()

def sweep(ns):
    while HEAP and HEAP[0][0] <= ns:
        exp, hauc = heappop(HEAP)
        pending.discard(hauc)
        hst = state.get(hauc)
        if hst is None or hst[0] is None:
            continue  # stale entry for a removed instance
        cur = hst[0] + SPAN
        if cur > ns:
            pending.add(hauc)
            heappush(HEAP, (cur, hauc))
            continue
        hst[0], hst[1] = None, 0  # expire: instance removed

def push(auc, created):
    if auc in pending:
        return
    pending.add(auc)
    heappush(HEAP, (created + SPAN, auc))

sim_fires = []
for line in open(sys.argv[1]):
    s = STREAM_RE.search(line)
    if not s or s.group(1) != "bid_events":
        continue
    auc = int(AUC_RE.search(line).group(1))
    ns = int(DT_RE.search(line).group(1))
    sweep(ns)
    st = state.get(auc)
    if st is None:
        st = state[auc] = [None, 0]
    if st[0] is None:
        st[0] = ns
        push(auc, ns)
    st[1] += 1
    if st[1] >= T:
        sim_fires.append((auc, ns))
        st[0], st[1] = ns, 0
        push(auc, ns)  # dropped if pending (engine dedup)

eng_fires = []
for line in open(sys.argv[2]):
    o = json.loads(line)
    auc = int(o["__wfu_entity_id"])
    fa = datetime.fromisoformat(o["__wfu_fired_at"].replace("Z", "+00:00"))
    eng_fires.append((auc, int(fa.timestamp() * 1_000_000_000)))

print(f"sim fires: {len(sim_fires)}  eng fires: {len(eng_fires)}")

sim_c = Counter((a, ns // 1_000_000_000) for a, ns in sim_fires)
eng_c = Counter((a, ns // 1_000_000_000) for a, ns in eng_fires)
only_sim = sim_c - eng_c
only_eng = eng_c - sim_c
print(f"matched (auc, fire_s) pairs: {sum((sim_c & eng_c).values())}")
print(f"only in sim: {sum(only_sim.values())}   only in eng: {sum(only_eng.values())}")
if only_eng:
    print("sample eng-only:", sorted(only_eng.elements())[:8])
if only_sim:
    print("sample sim-only:", sorted(only_sim.elements())[:8])

n = min(len(sim_fires), len(eng_fires))
div = next((i for i in range(n) if sim_fires[i] != eng_fires[i]), None)
print(f"first arrival-order divergence at index: {div}")
