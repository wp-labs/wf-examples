#!/usr/bin/env python3
"""Nexmark Q2-Q21 correctness ground-truth simulator.

Streams `wfgen gen-nexmark <N>` JSONL from stdin and computes the exact
expected emitted_total per rule, mirroring wf-engine semantics:

- Each rule is an independent MatchEngine with its own instance table,
  expiry heap and pending-expiry dedup set.
- Sliding window `match<key:10m>`: instances expire when
  created_at + 600s <= watermark, where the watermark is the monotonic
  max event time seen so far (the engine advances `watermark_nanos` with
  `fetch_max`, so out-of-order events never move it backwards) and the
  expiry scan sweeps the whole instance table before the event is processed.
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
- `on event<accu>`: after a fire the instance REARMS (count / evidence
  KEPT, created_at unchanged) — so the step re-fires on EVERY subsequent
  qualifying event with the running cumulative count until the window
  expires. (q18: fires on the 5th, 6th, 7th, ... bid, not just every 5th.)
- Aggregates evaluated with the current event already bound.
- Snapshot join miss does NOT drop the event (enrich-only), so q3/q4/q13
  emit for every driving event regardless of join outcome.
- Anti join (q21) DROPS the event when a matching row is found, keeps it
  otherwise.

Validation status (fresh 10m engine runs, scaled x3 to 30m):
  q15 / q17 / q18 / q19 / q20 match the engine within ~0.3%.
  q16 (fixed + close sum) is the IDEAL count (every fixed bucket closes and
  fires sum>=1000); the engine's actual EMIT is lower and timing-dependent
  (incremental expiry budget `MAX_EXPIRY_SCAN_BUDGET=1024` means earlier
  buckets can be lost before the pipeline drains — see
  window-actor-pull-model.md).
  q21 (anti join) is the NAIVE expectation (every bidder is a person -> 0).
  The engine actually emits a small non-zero count because the person window
  is not always complete at lookup time (time/memory eviction), so treat
  q21's exact EMIT as timing-dependent rather than a fixed ground truth.

Bid lines have compact (wfgen, serde_json BTreeMap) or spaced
(python json.dumps) key order, so regexes tolerate optional whitespace;
any line that fails regex falls back to json.loads.
"""
import json
import re
import sys
from heapq import heappush, heappop

SPAN = 600_000_000_000  # 10m sliding window
WITHIN_60S = 60_000_000_000  # q19 seq second step within 60s
BUCKET_NS = 600_000_000_000  # q16 fixed window bucket size (10m)

STREAM_RE = re.compile(r'"_stream":\s*"(\w+)"')
AUC_RE = re.compile(r'"auction":\s*(\d+)')
PRICE_RE = re.compile(r'"price":\s*(\d+)')
DT_RE = re.compile(r'"dateTime":\s*(\d+)')
BIDDER_RE = re.compile(r'"bidder":\s*(\d+)')
PERSON_ID_RE = re.compile(r'"id":\s*(\d+)')

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

# ---- q15-q21 (new) ----
Q15_EMIT = 0      # q15: filter price>100 + sliding count>=5 (fire+reset)
Q16_EMIT = 0      # q16: fixed 10m bucket + close sum(price) >= 1000
Q17_EMIT = 0      # q17: distinct bidder count >= 20 (fire+reset, set cleared)
Q18_EMIT = 0      # q18: accu count >= 5 (fire on every subsequent bid)
Q19_EMIT = 0      # q19: seq has b; has b within 60s
Q20_EMIT = 0      # q20: any { count>=2; count>=3 }  == count >= 3 (fire+reset)
Q21_EMIT = 0      # q21: anti join — keep bids whose bidder is not a person

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

# ---- q15/q17/q18/q19/q20 sliding rule state (independent machines) ---------
# A single shared heap carries (expire_at, rule_id, auc); PENDING2 dedups
# per (rule_id, auc) exactly like the q5/q7 heap above.
q15_count = {}
q15_created = {}
q17_set = {}
q17_created = {}
q18_count = {}
q18_created = {}
q19_step = {}
q19_t0 = {}
q19_created = {}
q20_count = {}
q20_created = {}
HEAP2 = []
PENDING2 = set()

_CREATED = {
    "q15": q15_created,
    "q17": q17_created,
    "q18": q18_created,
    "q19": q19_created,
    "q20": q20_created,
}

def push2(rule_id, auc, created):
    key = (rule_id, auc)
    if key in PENDING2:
        return
    PENDING2.add(key)
    heappush(HEAP2, (created + SPAN, rule_id, auc))

def expire2(rule_id, auc):
    if rule_id == "q15":
        q15_count.pop(auc, None)
        q15_created.pop(auc, None)
    elif rule_id == "q17":
        q17_set.pop(auc, None)
        q17_created.pop(auc, None)
    elif rule_id == "q18":
        q18_count.pop(auc, None)
        q18_created.pop(auc, None)
    elif rule_id == "q19":
        q19_step.pop(auc, None)
        q19_t0.pop(auc, None)
        q19_created.pop(auc, None)
    elif rule_id == "q20":
        q20_count.pop(auc, None)
        q20_created.pop(auc, None)

def sweep2(watermark):
    while HEAP2 and HEAP2[0][0] <= watermark:
        _, rule_id, auc = heappop(HEAP2)
        PENDING2.discard((rule_id, auc))
        created = _CREATED[rule_id].get(auc)
        if created is None:
            continue  # stale entry for a removed instance
        cur = created + SPAN
        if cur > watermark:
            PENDING2.add((rule_id, auc))
            heappush(HEAP2, (cur, rule_id, auc))
            continue
        expire2(rule_id, auc)

# q16 fixed-window: sum of bid prices per (auction, bucket_start).
q16_sum = {}

# q21 anti join: person ids present in the window.
# (See the note in the output for why the engine can differ from the naive
# "all bidders are persons -> 0" expectation.)
person_ids = set()

watermark = 0  # monotonic event-time watermark (bid machine)

for line in sys.stdin:
    s = STREAM_RE.search(line)
    if not s or s.group(1) != "bid_events":
        if s and s.group(1) == "auction_events":
            n_auction += 1
        elif s and s.group(1) == "person_events":
            # q8: each person event opens a session and fires count>=1
            # (NEXMark persons appear once; a session per person = one fire).
            Q8_EMIT += 1
            m = PERSON_ID_RE.search(line)
            if m:
                person_ids.add(int(m.group(1)))
            else:
                person_ids.add(json.loads(line)["id"])
        continue
    n_bid += 1
    m = AUC_RE.search(line)
    p = PRICE_RE.search(line)
    d = DT_RE.search(line)
    b = BIDDER_RE.search(line)
    if not (m and p and d and b):
        o = json.loads(line)
        auc, price, ns, bidder = o["auction"], o["price"], o["dateTime"], o["bidder"]
    else:
        auc, price, ns, bidder = (int(m.group(1)), int(p.group(1)),
                                  int(d.group(1)), int(b.group(1)))
    if auc % 123 == 0:
        q2 += 1
    if auc % 7 == 0:
        Q10_EMIT += 1  # q10 on-each subset (every bid with auction % 7 == 0)
    Q13_EMIT += 1  # q13 snapshot join: every bidder has a person in the window

    # -- global watermark sweep: expire all instances past cutoff --------
    # pop -> pending released -> re-read current created_at -> requeue
    # with the CURRENT expiry if still in the future, else expire.
    if ns > watermark:
        watermark = ns
    while HEAP and HEAP[0][0] <= watermark:
        exp, hauc, hci = heappop(HEAP)
        PENDING.discard((hauc, hci))
        hst = state.get(hauc)
        if hst is None or hst[hci] is None:
            continue  # stale entry for a removed instance
        cur = hst[hci] + SPAN
        if cur > watermark:
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
    sweep2(watermark)

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

    # q15: price>100 filter + count>=5 (fire+reset)
    if price > 100:
        if auc not in q15_created:
            q15_created[auc] = ns
            q15_count[auc] = 0
            push2("q15", auc, ns)
        q15_count[auc] += 1
        if q15_count[auc] >= 5:
            Q15_EMIT += 1
            q15_count[auc] = 0
            q15_created[auc] = ns
            push2("q15", auc, ns)

    # q17: distinct bidder count >= 20 (fire+reset, distinct set cleared)
    if auc not in q17_created:
        q17_created[auc] = ns
        q17_set[auc] = set()
        push2("q17", auc, ns)
    if bidder not in q17_set[auc]:
        q17_set[auc].add(bidder)
        if len(q17_set[auc]) >= 20:
            Q17_EMIT += 1
            q17_set[auc] = set()
            q17_created[auc] = ns
            push2("q17", auc, ns)

    # q18: accu count>=5 (rearm — count kept, fires on every subsequent bid)
    if auc not in q18_created:
        q18_created[auc] = ns
        q18_count[auc] = 0
        push2("q18", auc, ns)
    q18_count[auc] += 1
    if q18_count[auc] >= 5:
        Q18_EMIT += 1
        # rearm: count kept, created_at unchanged

    # q19: seq { has b; has b within 60s; }
    if auc not in q19_created:
        q19_created[auc] = ns
        q19_step[auc] = 0
        q19_t0[auc] = None
        push2("q19", auc, ns)
    if q19_step[auc] == 0:
        q19_step[auc] = 1
        q19_t0[auc] = ns
    else:
        gap = ns - q19_t0[auc]
        if 0 <= gap <= WITHIN_60S:
            Q19_EMIT += 1
            q19_step[auc] = 0
            q19_t0[auc] = None
            q19_created[auc] = ns
            push2("q19", auc, ns)
        else:
            # `within` violated -> reset (this bid is consumed, not replayed)
            q19_step[auc] = 0
            q19_t0[auc] = None
            q19_created[auc] = ns
            push2("q19", auc, ns)

    # q20: any { count>=2; count>=3 } == count >= 3 (fire+reset)
    if auc not in q20_created:
        q20_created[auc] = ns
        q20_count[auc] = 0
        push2("q20", auc, ns)
    q20_count[auc] += 1
    if q20_count[auc] >= 3:
        Q20_EMIT += 1
        q20_count[auc] = 0
        q20_created[auc] = ns
        push2("q20", auc, ns)

    # q16: fixed 10m bucket — accumulate sum(price) per (auction, bucket)
    bucket = (ns // BUCKET_NS) * BUCKET_NS
    q16_sum[(auc, bucket)] = q16_sum.get((auc, bucket), 0) + price

    # q21: anti join — keep the bid iff its bidder is NOT in the person window.
    if bidder not in person_ids:
        Q21_EMIT += 1

# q16 close: a fixed bucket fires when its sum(price) >= 1000 (event step
# count>=1 is always true when sum>0, so it reduces to the sum threshold).
Q16_EMIT = sum(1 for s in q16_sum.values() if s >= 1000)

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
    "q15_high_bid_count_5": Q15_EMIT,
    "q16_sum_price_1000": Q16_EMIT,
    "q17_distinct_bidders_20": Q17_EMIT,
    "q18_accumulate_fires": Q18_EMIT,
    "q19_seq_two_bids": Q19_EMIT,
    "q20_any_count_3": Q20_EMIT,
    "q21_anti_person": Q21_EMIT,
    # q11 is per-shard (session fragmented across shards); q12/q14 conv top-N is
    # global (cross-shard merge) but the fixed-window close+conv is not modeled
    # here. Validate their EMIT determinism across runs / CONNECTIONS=1 instead.
    "_counts": {"auctions": n_auction, "bids": n_bid},
}
json.dump(out, sys.stdout, indent=1)
print()
