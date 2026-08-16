#!/usr/bin/env python3
"""q5 span sweep: which effective window length reproduces the engine count."""
import re
import sys
from heapq import heappush, heappop

STREAM_RE = re.compile(r'"_stream":"(\w+)"')
AUC_RE = re.compile(r'"auction":(\d+)')
DT_RE = re.compile(r'"dateTime":(\d+)')

SPANS = [600, 660, 720, 780, 900, 1020, 1200, 1800, 10**9]  # seconds; last = no expiry
NS = [s * 1_000_000_000 for s in SPANS]
state = {}   # auc -> [[created, count] x len(SPANS)]
heaps = [[] for _ in SPANS]
emit = [0] * len(SPANS)

for line in sys.stdin:
    s = STREAM_RE.search(line)
    if not s or s.group(1) != "bid_events":
        continue
    m = AUC_RE.search(line); d = DT_RE.search(line)
    auc, ns = int(m.group(1)), int(d.group(1))
    st = state.get(auc)
    if st is None:
        st = [[None, 0] for _ in SPANS]
        state[auc] = st
    for i, span in enumerate(NS):
        sst = st[i]
        if sst[0] is None:
            sst[0] = ns
            heappush(heaps[i], (ns + span, auc))
        sst[1] += 1
        if sst[1] >= 10:
            emit[i] += 1
            sst[0], sst[1] = None, 0
    # sweeps
    for i, span in enumerate(NS):
        h = heaps[i]
        while h and h[0][0] <= ns:
            exp, hauc = heappop(h)
            hst = state.get(hauc)
            if hst and hst[i][0] is not None and hst[i][0] + span <= ns:
                hst[i][0], hst[i][1] = None, 0

for s, e in zip(SPANS, emit):
    print(f"span={s}s: {e}")
