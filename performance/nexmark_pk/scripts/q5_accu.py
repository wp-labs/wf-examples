#!/usr/bin/env python3
"""q5 accu-semantics hypothesis: rearm keeps count, created_at stays at epoch
start, sliding expiry removes at created+600s. fires = sum(max(0, c-9))."""
import re
import sys
from heapq import heappush, heappop

STREAM_RE = re.compile(r'"_stream":"(\w+)"')
AUC_RE = re.compile(r'"auction":(\d+)')
DT_RE = re.compile(r'"dateTime":(\d+)')
SPAN = 600_000_000_000

state = {}  # auc -> [created, count]
HEAP = []
emit = 0
for line in sys.stdin:
    s = STREAM_RE.search(line)
    if not s or s.group(1) != "bid_events":
        continue
    m = AUC_RE.search(line); d = DT_RE.search(line)
    auc, ns = int(m.group(1)), int(d.group(1))
    while HEAP and HEAP[0][0] <= ns:
        exp, hauc = heappop(HEAP)
        hst = state.get(hauc)
        if hst and hst[0] is not None and hst[0] + SPAN <= ns:
            hst[0], hst[1] = None, 0
    st = state.get(auc)
    if st is None:
        st = [None, 0]; state[auc] = st
    if st[0] is None:
        st[0] = ns
        heappush(HEAP, (ns + SPAN, auc))
    st[1] += 1
    if st[1] >= 10:
        emit += 1   # accu: keep count, keep created_at — fires on EVERY event
                    # while count >= 10 until the instance expires
print(f"accu fires: {emit}")
