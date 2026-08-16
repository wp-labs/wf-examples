#!/usr/bin/env python3
"""Compare corrected-semantics simulator fires against engine alerts on 28k slice.

Produces per-fire records (auc, fire_event_ns) from simulation and matches them
against alerts.ndjson (entity_id, fired_at truncated to second).
"""
import json
import re
import sys
from collections import Counter
from datetime import datetime, timezone
from heapq import heappush, heappop

SPAN = 600_000_000_000
T = 10
STREAM_RE = re.compile(r'"_stream":\s*"(\w+)"')
AUC_RE = re.compile(r'"auction":\s*(\d+)')
DT_RE = re.compile(r'"dateTime":\s*(\d+)')

state = {}   # auc -> [created_ns_or_None, count]
HEAP = []
sim_fires = []  # (auc, fire_ns) in arrival order

def sweep(ns):
    while HEAP and HEAP[0][0] <= ns:
        exp, hauc = heappop(HEAP)
        hst = state.get(hauc)
        if hst is None:
            continue
        if hst[0] is not None and hst[0] + SPAN <= ns:
            hst[0], hst[1] = None, 0

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
        heappush(HEAP, (ns + SPAN, auc))
    st[1] += 1
    if st[1] >= T:
        sim_fires.append((auc, ns))
        st[0], st[1] = ns, 0
        heappush(HEAP, (ns + SPAN, auc))

eng_fires = []
for line in open(sys.argv[2]):
    o = json.loads(line)
    auc = int(o["__wfu_entity_id"])
    fa = datetime.fromisoformat(o["__wfu_fired_at"].replace("Z", "+00:00"))
    eng_fires.append((auc, int(fa.timestamp() * 1_000_000_000)))

print(f"sim fires: {len(sim_fires)}  eng fires: {len(eng_fires)}")

# exact (auc, fire_ns) multiset comparison at second granularity
sim_c = Counter((a, ns // 1_000_000_000) for a, ns in sim_fires)
eng_c = Counter((a, ns // 1_000_000_000) for a, ns in eng_fires)
only_sim = sim_c - eng_c
only_eng = eng_c - sim_c
print(f"matched pairs: {sum((sim_c & eng_c).values())}")
print(f"only in sim: {sum(only_sim.values())}   only in eng: {sum(only_eng.values())}")
if only_eng:
    print("sample eng-only (auc, fire_s):", sorted(only_eng.elements())[:10])
if only_sim:
    print("sample sim-only (auc, fire_s):", sorted(only_sim.elements())[:10])

# per-auction count comparison
sim_auc = Counter(a for a, _ in sim_fires)
eng_auc = Counter(a for a, _ in eng_fires)
diff_aucs = {a: (sim_auc.get(a, 0), eng_auc.get(a, 0))
             for a in set(sim_auc) | set(eng_auc)
             if sim_auc.get(a, 0) != eng_auc.get(a, 0)}
print(f"auctions with differing fire counts: {len(diff_aucs)}")
for a, (s_, e_) in sorted(diff_aucs.items())[:15]:
    print(f"  auc {a}: sim {s_} eng {e_}")

# arrival-order alignment: first divergence index
n = min(len(sim_fires), len(eng_fires))
div = next((i for i in range(n) if sim_fires[i][0] != eng_fires[i][0]), None)
print(f"first arrival-order divergence at index: {div}")
if div is not None and div < 20:
    print("  sim around:", sim_fires[max(0,div-2):div+4])
    print("  eng around:", eng_fires[max(0,div-2):div+4])
