#!/usr/bin/env python3
"""Nexmark Q2-Q7 correctness ground-truth simulator.

Streams `wfgen gen-nexmark <N>` JSONL from stdin and computes the exact
expected emitted_total per rule, mirroring wf-engine semantics:

- Each rule is an independent MatchEngine with its own instance table,
  expiry heap and pending-expiry dedup set.
- Sliding window `match<key:10m>`: instances expire when
  created_at + 600s <= watermark, where the watermark is the CURRENT
  event's time and the expiry scan sweeps the whole instance table
  (global per shard machine) before the event is processed.
  CRITICAL engine detail (verified by per-alert diff on a 28k-event
  probe, 2679/2679 exact match): push_expiry_candidate DEDUPS per key
  (pending_expiry set) — a fire/reset push is DROPPED while a heap
  entry is pending. An instance's real expiry is therefore driven by
  the stale heap entry (created-at-at-push-time + span); it is only
  refreshed when popped (re-read current created_at + span, requeue
  if still in the future). In out-of-order streams this keeps
  instances alive LONGER than naive created_at tracking would.
- Plain `on event` (no <accu>): after a fire the instance RESETS
  (count/max cleared, created_at = firing event time).
- Aggregates evaluated with the current event already bound.
- Snapshot join miss does NOT drop the event (enrich-only), so q3/q4
  emit for every driving event regardless of join outcome.

Bid lines have compact (wfgen, serde_json BTreeMap) or spaced
(python json.dumps) key order, so regexes tolerate optional whitespace;
any line that fails regex falls back to json.loads.
"""
import json
import re
import sys
from heapq import heappush, heappop

SPAN = 600_000_000_000  # 10m sliding window
STREAM_RE = re.compile(r'"_stream":\s*"(\w+)"')
AUC_RE = re.compile(r'"auction":\s*(\d+)')
PRICE_RE = re.compile(r'"price":\s*(\d+)')
DT_RE = re.compile(r'"dateTime":\s*(\d+)')

q2 = 0            # bids with auction % 123 == 0
n_auction = 0     # q3 expected (every auction fires count>=1)
n_bid = 0         # q4 expected
Q5_T = (10, 50, 100)
Q7_T = (200, 500, 1000)
Q5_EMIT = [0, 0, 0]
Q7_EMIT = [0, 0, 0]
Q6_EMIT = 0       # q6: running avg bid price >= 200 per auction (fire+reset)
Q8_EMIT = 0       # q8: person session — every person_events fires count>=1
Q10_EMIT = 0      # q10: on-each subset — bids with auction % 7 == 0
Q13_EMIT = 0      # q13: bid ⋈ person snapshot join — every bid joins a person
# q11: per-shard — session windows live in each shard's rule-task machine, and
#   bid_events is sharded by auction (Q2/Q4/Q5/Q7 need it), so a bidder's sessions
#   are fragmented across shards. Single-machine sim can't match; validate global
#   sessions with CONNECTIONS=1 (or bidder-sharding).
# q12 / q14: NOT per-shard — the conv stage merges all shards' closes and applies
#   a GLOBAL top-N (ConvStageTask barrier + apply_conv over the merged batch).
#   Not modeled here yet (would need fixed-window close + conv simulation).

# state[auc] = [c10,n10, c50,n50, c100,n100, c200,m200, c500,m500, c1000,m1000,
#               c_avg, sum_avg, cnt_avg]
# c = created_at (ns) or None (no instance); n = bind count; m = running max
state = {}
NEW = [None, 0, None, 0, None, 0, None, None, None, None, None, None,
       None, 0, 0]

# lazy expiry heap: (expire_at, auc, slot_index_of_created)
# PENDING mirrors the per-key pending_expiry dedup: at most ONE live
# heap entry per (auc, slot) — the engine drops fire/reset pushes
# while an entry is pending.
HEAP = []
PENDING = set()

def push(auc, ci, created):
    if (auc, ci) in PENDING:
        return
    PENDING.add((auc, ci))
    heappush(HEAP, (created + SPAN, auc, ci))

for line in sys.stdin:
    s = STREAM_RE.search(line)
    if not s or s.group(1) != "bid_events":
        if s and s.group(1) == "auction_events":
            n_auction += 1
        elif s and s.group(1) == "person_events":
            # q8: each person event opens a session and fires count>=1
            # (NEXMark persons appear once; a session per person = one fire).
            Q8_EMIT += 1
        continue
    n_bid += 1
    m = AUC_RE.search(line)
    p = PRICE_RE.search(line)
    d = DT_RE.search(line)
    if not (m and p and d):
        o = json.loads(line)
        auc, price, ns = o["auction"], o["price"], o["dateTime"]
    else:
        auc, price, ns = int(m.group(1)), int(p.group(1)), int(d.group(1))
    if auc % 123 == 0:
        q2 += 1
    if auc % 7 == 0:
        Q10_EMIT += 1  # q10 on-each subset (every bid with auction % 7 == 0)
    Q13_EMIT += 1  # q13 snapshot join: every bidder has a person in the window

    # -- global watermark sweep: expire all instances past cutoff --------
    # pop -> pending released -> re-read current created_at -> requeue
    # with the CURRENT expiry if still in the future, else expire.
    while HEAP and HEAP[0][0] <= ns:
        exp, hauc, hci = heappop(HEAP)
        PENDING.discard((hauc, hci))
        hst = state.get(hauc)
        if hst is None or hst[hci] is None:
            continue  # stale entry for a removed instance
        cur = hst[hci] + SPAN
        if cur > ns:
            PENDING.add((hauc, hci))
            heappush(HEAP, (cur, hauc, hci))
            continue
        # expire: instance removed (count / max / avg cleared)
        hst[hci] = None
        if hci < 6:
            hst[hci + 1] = 0
        elif hci < 12:
            hst[hci + 1] = None
        else:  # avg slot (hci == 12): clear sum + cnt
            hst[13] = 0
            hst[14] = 0

    st = state.get(auc)
    if st is None:
        st = list(NEW)
        state[auc] = st
    # q5 rules (independent state machines, all see every bid)
    for i, T in enumerate(Q5_T):
        ci, ni = i * 2, i * 2 + 1
        if st[ci] is None:
            st[ci] = ns
            push(auc, ci, ns)
        st[ni] += 1
        if st[ni] >= T:
            Q5_EMIT[i] += 1
            # engine: instance.reset(plan, fire_time) — instance STAYS
            # alive with created_at = fire time; count restarts from 0.
            # The engine's follow-up push_expiry_candidate is DROPPED
            # by the pending dedup in almost all cases.
            st[ci], st[ni] = ns, 0
            push(auc, ci, ns)
    # q7 rules
    for i, T in enumerate(Q7_T):
        ci, mi = 6 + i * 2, 7 + i * 2
        if st[ci] is None:
            st[ci] = ns
            push(auc, ci, ns)
        st[mi] = price if st[mi] is None else max(st[mi], price)
        if st[mi] >= T:
            Q7_EMIT[i] += 1
            st[ci], st[mi] = ns, None
            push(auc, ci, ns)
    # q6: running avg bid price >= 200 (fire + reset: sum/cnt cleared)
    ci = 12  # c_avg
    if st[ci] is None:
        st[ci] = ns
        push(auc, ci, ns)
    st[13] += price
    st[14] += 1
    if st[13] / st[14] >= 200:
        Q6_EMIT += 1
        st[ci], st[13], st[14] = ns, 0, 0
        push(auc, ci, ns)

out = {
    "q2_mod123": q2,
    "q3_auction_seller": n_auction,
    "q4_real_avg_100": n_bid,
    "q5_bidcount_10": Q5_EMIT[0],
    "q5_bidcount_50": Q5_EMIT[1],
    "q5_bidcount_100": Q5_EMIT[2],
    "q6_avg_price_200": Q6_EMIT,
    "q7_maxbid_200": Q7_EMIT[0],
    "q7_maxbid_500": Q7_EMIT[1],
    "q7_maxbid_1000": Q7_EMIT[2],
    "q8_monitor_new_user": Q8_EMIT,
    "q10_arbitrary_selection": Q10_EMIT,
    "q13_bid_person_join": Q13_EMIT,
    # q11 is per-shard (session fragmented across shards); q12/q14 conv top-N is
    # global (cross-shard merge) but the fixed-window close+conv is not modeled
    # here. Validate their EMIT determinism across runs / CONNECTIONS=1 instead.
    "_counts": {"auctions": n_auction, "bids": n_bid},
}
json.dump(out, sys.stdout, indent=1)
print()
