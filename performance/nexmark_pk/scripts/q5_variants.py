#!/usr/bin/env python3
"""q5_bidcount_10 semantic-variant explorer: computes expected fires under
several candidate expiry/reset semantics in one pass, to identify which
matches the engine's observed 1,712,4xx."""
import re
import sys
from heapq import heappush, heappop
HA=[]; HD=[]

SPAN = 600_000_000_000
STREAM_RE = re.compile(r'"_stream":"(\w+)"')
AUC_RE = re.compile(r'"auction":(\d+)')
DT_RE = re.compile(r'"dateTime":(\d+)')

# per auction state per variant:
# vA global-sweep, created reset on fire (baseline model)
# vB no expiry at all
# vC expiry by original creation time (created never updated on fire)
# vD sweep with ratcheting global max watermark
vA = {}   # auc -> [created, count]
vB = {}   # auc -> count
vC = {}   # auc -> [created0, count]
vD = {}   # auc -> [created, count]
emitA = emitB = emitC = emitD = 0
gmax = 0

def step_v(state, emit, ns, sweep_all):
    global _
    return None

for line in sys.stdin:
    s = STREAM_RE.search(line)
    if not s or s.group(1) != "bid_events":
        continue
    m = AUC_RE.search(line); d = DT_RE.search(line)
    auc, ns = int(m.group(1)), int(d.group(1))

    # vA: global sweep with current event time
    # (approximated per-key check + periodic global pass below is replaced by
    #  full global check only when this event's time advances; out-of-order
    #  events with lower t must NOT sweep — engine uses t as watermark)
    # vD: global sweep with ratcheting max
    gmax_new = max(gmax, ns)

    # vA sweep: only if ns advances the watermark, other instances with
    # expire <= ns pop. Model with lazy heaps.
    # (implemented below with heaps for exactness)
    st = vA.get(auc)
    if st is None:
        st = [None, 0]; vA[auc] = st
    if st[0] is None:
        st[0] = ns
        heappush(HA, (ns + SPAN, auc))
    st[1] += 1
    if st[1] >= 10:
        emitA += 1; st[0], st[1] = None, 0

    st = vB.get(auc)
    if st is None:
        vB[auc] = st = 0
    st += 1; vB[auc] = st
    if st >= 10:
        emitB += 1; vB[auc] = 0

    st = vC.get(auc)
    if st is None:
        st = [None, 0]; vC[auc] = st
    if st[0] is None:
        st[0] = ns
    elif st[0] + SPAN <= ns:
        st[0], st[1] = ns, 0
    st[1] += 1
    if st[1] >= 10:
        emitC += 1; st[1] = 0  # count resets, created0 KEPT

    st = vD.get(auc)
    if st is None:
        st = [None, 0]; vD[auc] = st
    if st[0] is None:
        st[0] = ns
        heappush(HD, (ns + SPAN, auc))
    st[1] += 1
    if st[1] >= 10:
        emitD += 1; st[0], st[1] = None, 0

    # vA heap sweep with watermark = ns (current event time)
    while HA and HA[0][0] <= ns:
        exp, hauc = heappop(HA)
        hst = vA.get(hauc)
        if hst and hst[0] is not None and hst[0] + SPAN <= ns:
            hst[0], hst[1] = None, 0
    # vD heap sweep with watermark = ratcheting max
    while HD and HD[0][0] <= gmax_new:
        exp, hauc = heappop(HD)
        hst = vD.get(hauc)
        if hst and hst[0] is not None and hst[0] + SPAN <= gmax_new:
            hst[0], hst[1] = None, 0
    gmax = gmax_new

print(f"vA global-sweep/current-t     : {emitA}")
print(f"vB no-expiry                  : {emitB}")
print(f"vC created0-kept/reset-count  : {emitC}")
print(f"vD global-sweep/ratchet-max   : {emitD}")
