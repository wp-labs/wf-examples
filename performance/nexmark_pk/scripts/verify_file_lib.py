#!/usr/bin/env python3
"""verify_daemon.sh 的度量/校验库（2026-08-30 从 verify_file.sh 内嵌 heredoc 抽出，后者已移除）。

verify_daemon.sh 逐查询跑完后调用本库完成三个口径 + 一道守卫：
- dirty   <rule_names>          指标口径脏检测：metrics.ndjson 的 emitted_total label
                                 必须恰为当前 query 规则集（缺/外来 = 脏 → 重跑）
- counts  <emit_path> <cnt_path> 权威计数：metrics.emitted_total（→EMIT 文件）+ 输出
                                 文件 benchmark.ndjson 逐规则行数（→CNT 文件）+ 致命
                                 计数器摘要（stdout）
- emitted <rule>                 metrics 中某规则 emitted_total 求和（交叉检查用）
- cross   <cnt_path>             文件输出 vs 指标计数的交叉检查摘要（尾批丢失/残留）
- content <query> <rule_names>   内容断言：benchmark.ndjson 行字段通用 + per-rule 强语义

工作目录约定：脚本在 nexmark_pk 根运行（data/metrics.ndjson、data/alerts/benchmark.ndjson
为相对路径）。
"""
import json
import sys
from collections import Counter, defaultdict

METRICS = "data/metrics.ndjson"
ALERTS = "data/alerts/benchmark.ndjson"
FATAL_COUNTERS = (
    "append_failed_total",
    "dropped_late_total",
    "cursor_gap_total",
    "drain_dropped_records_total",
    "sink_dispatch_failed_total",
    "channel_full_total",
)


def iter_metrics():
    for line in open(METRICS, errors="replace"):
        try:
            o = json.loads(line)
        except Exception:
            continue
        yield o


# ---------------------------------------------------------------------------
# dirty — 指标口径脏检测（残留进程污染守卫）
# ---------------------------------------------------------------------------
def cmd_dirty(rule_names: str) -> int:
    expect = set(rule_names.split())
    found = set()
    for o in iter_metrics():
        if o.get("name") == "emitted_total" and o.get("label"):
            found.add(o["label"])
    if not expect <= found:
        print("missing=" + ",".join(sorted(expect - found)))
        return 1
    if not found <= expect:
        print("foreign=" + ",".join(sorted(found - expect)))
        return 1
    print("ok")
    return 0


# ---------------------------------------------------------------------------
# counts — 权威计数 + 致命计数器摘要
# ---------------------------------------------------------------------------
def cmd_counts(emit_path: str, cnt_path: str) -> int:
    emitted = defaultdict(int)
    fatal = defaultdict(int)
    for o in iter_metrics():
        name = o.get("name")
        label = o.get("label", "")
        try:
            val = int(float(o.get("value", 0) or 0))
        except (TypeError, ValueError):
            continue
        if name == "emitted_total":
            emitted[label] += val
        elif name in FATAL_COUNTERS:
            fatal[name] += val
    with open(emit_path, "w") as f:
        for k in sorted(emitted):
            f.write(f"EMIT {k} {emitted[k]}\n")
    # 输出文件（benchmark.ndjson）每规则计数
    file_cnt = defaultdict(int)
    try:
        for line in open(ALERTS, errors="replace"):
            try:
                file_cnt[json.loads(line).get("__wfu_rule_name")] += 1
            except Exception:
                continue
    except FileNotFoundError:
        pass
    with open(cnt_path, "w") as f:
        for k in sorted(file_cnt):
            f.write(f"{k} {file_cnt[k]}\n")
    bad = [f"{k}={v}" for k, v in sorted(fatal.items()) if v > 0]
    print(";".join(bad) if bad else "clean")
    return 0


# ---------------------------------------------------------------------------
# emitted — 单规则 emitted_total（交叉检查）
# ---------------------------------------------------------------------------
def cmd_emitted(rule: str) -> int:
    s = 0
    for o in iter_metrics():
        if o.get("name") == "emitted_total" and o.get("label") == rule:
            try:
                s += int(float(o.get("value", 0) or 0))
            except (TypeError, ValueError):
                continue
    print(s)
    return 0


# ---------------------------------------------------------------------------
# cross — 文件输出 vs 指标计数的交叉检查摘要（替代 shell 逐规则调 emitted）
# 输入 CNT 文件（`counts` 产出的每行 `规则 文件行数`），与 metrics.emitted_total
# 对比：文件 < 指标 = 尾批丢失（⚠）；文件 > 指标 = 残留累积（异常）。
# ---------------------------------------------------------------------------
def cmd_cross(cnt_path: str) -> int:
    emitted = defaultdict(int)
    for o in iter_metrics():
        if o.get("name") == "emitted_total":
            try:
                emitted[o.get("label", "")] += int(float(o.get("value", 0) or 0))
            except (TypeError, ValueError):
                continue
    out = []
    try:
        lines = open(cnt_path, errors="replace").readlines()
    except FileNotFoundError:
        lines = []
    for raw in lines:
        parts = raw.split()
        if len(parts) < 2:
            continue
        rule, n = parts[0], int(parts[1])
        m = emitted.get(rule, 0)
        if m > n:
            gap = m - n
            pct = gap * 100 // m
            tag = "⚠" if pct < 1 else "⚠⚠"
            out.append(f"{tag}{rule} 文件{n}/指标{m}（缺{gap}={pct}%）")
        elif m < n:
            out.append(f"⚠⚠{rule} 文件{n} > 指标{m}（异常，检查是否残留累积）")
    print("; ".join(out))
    return 0


# ---------------------------------------------------------------------------
# content — alert 内容断言（计数对拍之上的字段值校验）
# ---------------------------------------------------------------------------
def _int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return -1


# per-rule 强语义断言: rule -> (predicate, 描述)。未列出的规则只做通用断言。
CHECKS = {
    "q1_bid_passthrough": (
        lambda r: r.get("alert_type") == "q1_passthrough" and r.get("detail") == "bid",
        "alert_type/detail 恒定",
    ),
    "q2_mod_123": (lambda r: _int(r.get("id")) % 123 == 0, "id 必须 mod(id,123)==0"),
    "q3_auction_seller": (
        lambda r: r.get("alert_type") == "q3" and bool(r.get("detail")),
        "alert_type=q3 & detail(seller) 非空",
    ),
    "q4b_category_avg": (lambda r: r.get("alert_type") == "q4_avg", "alert_type=q4_avg"),
    "q5_hottest_auction": (lambda r: r.get("alert_type") == "q5_hot", "alert_type=q5_hot"),
    "q6_avg_price_by_seller": (
        lambda r: r.get("alert_type") == "q6_avg200",
        "alert_type=q6_avg200",
    ),
    "q7_highest_bid": (
        lambda r: r.get("alert_type") == "q7_hi"
        and str(r.get("detail", "")).startswith("max "),
        "alert_type=q7_hi & detail=max ...",
    ),
    "q8_monitor_new_user": (
        lambda r: r.get("alert_type") == "q8_new_user",
        "alert_type=q8_new_user",
    ),
    "q9_winning_bid": (
        lambda r: r.get("alert_type") == "q9_win"
        and str(r.get("detail", "")).startswith("winner "),
        "alert_type=q9_win & detail=winner <bidder>",
    ),
    "q10_log_all": (
        lambda r: r.get("alert_type") == "q10_log" and r.get("detail") == "log bid",
        "alert_type/detail 恒定",
    ),
    "q11_bidder_session": (
        lambda r: r.get("alert_type") == "q11_session"
        and str(r.get("detail", "")).isdigit(),
        "alert_type=q11_session & detail=count",
    ),
    "q12_bidder_10s_window_count": (
        lambda r: r.get("alert_type") == "q12_window",
        "alert_type=q12_window",
    ),
    "q13b_side_input_join": (
        lambda r: r.get("alert_type") == "q13_sidejoin",
        "alert_type=q13_sidejoin",
    ),
    "q14_calculation": (
        lambda r: r.get("alert_type") == "q14_calc" and "c=" in str(r.get("detail", "")),
        "alert_type=q14_calc & detail 含 c=",
    ),
    "q15_bidding_stats": (lambda r: r.get("alert_type") == "q15_stats", "alert_type=q15_stats"),
    "q16_channel_stats": (lambda r: r.get("alert_type") == "q16_stats", "alert_type=q16_stats"),
    "q17_auction_stats": (lambda r: r.get("alert_type") == "q17_stats", "alert_type=q17_stats"),
    "q18_last_bid_stats": (
        lambda r: r.get("alert_type") == "q18_last_stats",
        "alert_type=q18_last_stats",
    ),
    "q19_auction_top10_stats": (
        lambda r: r.get("alert_type") == "q19_top10_stats",
        "alert_type=q19_top10_stats",
    ),
    "q20_expand_bid": (lambda r: r.get("alert_type") == "q20_expand", "alert_type=q20_expand"),
    "q21_channel_id": (
        lambda r: r.get("alert_type") == "q21_cid" and bool(r.get("detail")),
        "alert_type=q21_cid & detail(channel_id) 非空",
    ),
    "q22_url_dirs": (
        lambda r: r.get("alert_type") == "q22_dir" and "/" in str(r.get("detail", "")),
        "alert_type=q22_dir & detail 含 '/' ",
    ),
}


def cmd_content(query: str, rule_names: str) -> int:
    expect = set(rule_names.split())
    bad_json = 0
    wrong_rule = 0
    gen = 0
    per = 0
    gen_msgs = set()
    per_msgs = set()
    q9_by_id = Counter()
    for line in open(ALERTS, errors="replace"):
        try:
            o = json.loads(line)
        except Exception:
            bad_json += 1
            continue
        rule = o.get("__wfu_rule_name", "")
        if rule not in expect:
            wrong_rule += 1
            continue
        if _int(o.get("id")) < 0:
            gen += 1
            gen_msgs.add("id 缺失/非法")
        if not o.get("alert_type"):
            gen += 1
            gen_msgs.add("alert_type 缺失")
        if not isinstance(o.get("detail"), str) or not o["detail"]:
            gen += 1
            gen_msgs.add("detail 缺失/空")
        if o.get("request_count") != 1:
            gen += 1
            gen_msgs.add(f"request_count={o.get('request_count')}")
        check = CHECKS.get(rule)
        if check is not None and not check[0](o):
            per += 1
            per_msgs.add(f"{rule}: {check[1]}")
        if rule == "q9_winning_bid":
            q9_by_id[o.get("id")] += 1
    dup = sum(1 for c in q9_by_id.values() if c > 1)

    out = []
    if bad_json:
        out.append(f"bad_json={bad_json}")
    if wrong_rule:
        out.append(f"外来规则={wrong_rule}")
    if gen:
        out.append(f"通用违规={gen} ({';'.join(sorted(gen_msgs))})")
    if per:
        out.append(f"语义违规={per} ({';'.join(sorted(per_msgs))})")
    if dup:
        out.append(f"q9重复auction={dup}")
    print(";".join(out) if out else "ok")
    return 0


def main(argv: list[str]) -> int:
    if not argv:
        print(__doc__, file=sys.stderr)
        return 2
    cmd, args = argv[0], argv[1:]
    if cmd == "dirty" and len(args) == 1:
        return cmd_dirty(args[0])
    if cmd == "counts" and len(args) == 2:
        return cmd_counts(args[0], args[1])
    if cmd == "emitted" and len(args) == 1:
        return cmd_emitted(args[0])
    if cmd == "cross" and len(args) == 1:
        return cmd_cross(args[0])
    if cmd == "content" and len(args) == 2:
        return cmd_content(args[0], args[1])
    print(f"verify_file_lib.py: bad args for '{cmd}'", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
