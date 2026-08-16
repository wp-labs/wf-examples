#!/usr/bin/env python3
"""q5 with time-sorted bid stream (tests whether the pipeline sorts rows)."""
import re
import sys
from heapq import heappush, heappop

STREAM_RE = re.compile(r'"_stream":"(\w+)"')
AUC_RE = re.compile(r'"auction":(\d+)')
DT_RE = re.compile(r'"dateTime":(\d+)')

SPAN = 600_000_000_000
bids = []
for line in sys.stdin:
    s = STREAM_RE.search(line)
    if not s or s.group(1) != "bid_events":
        continue
    m = AUC_RE.search(line); d = DT_RE.search(line)
    bids.append((int(d.group(1)), int(m.group(1))))

bids.sort(key=lambda x: x[0])  # stable by event time

state = {}
HEAP = []
emit = 0
for ns, auc in bids:
    st = state.get(auc)
    if st is None:
        st = [None, 0]; state[auc] = st
    while HEAP and HEAP[0][0] <= ns:
        exp, hauc = heappop(HEAP)
        hst = state.get(hauc)
        if hst and hst[0] is not None and hst[0] + SPAN <= ns:
            hst[0], hst[1] = None, 0
    if st[0] is None:
        st[0] = ns
        heappush(HEAP, (ns + SPAN, auc))
    st[1] += 1
    if st[1] >= 10:
        emit += 1
        st[0], st[1] = None, 0
print(f"sorted-stream vA fires: {emit}")
