#!/usr/bin/env python3
"""生成 100 条规则的综合压测 WFL（覆盖主要引擎路径 + 多 key/阈值网格）。

用法: python3 gen_rules.py > ../models/rules/throughput.wfl

规则模板基于 eps_throughput_obj 已验证的 20 条模式：
  count / sum / avg / min / max / distinct / accu / guard（bool/float/object
  嵌套/array/字符串/数学函数）/ close / 多事件 / 序列 / pipeline。
实体一律用 ip 字段（conn=c.sip, auth=a.source_ip, dns=d.sip），match key 可
独立变化（sip/dip/dport/protocol/user/domain）。

生成数量不足 100 时用 count 规则补齐，保证正好 100 条。
"""
import sys

RULES = []


def emit(name, events, match_key, on_body, entity_alias, entity_field, window="conn_events"):
    """events: alias -> (window, filter_expr or None)."""
    ev_decls = []
    for alias, (win, flt) in events.items():
        ev_decls.append(f"        {alias} : {win}" + (f" && {flt}" if flt else ""))
    RULES.append(f"""rule {name} {{
    events {{
{chr(10).join(ev_decls)}
    }}
    match<{match_key}:2m> {{
        on event {on_body}
    }} -> score(50.0)
    entity(ip, {entity_alias}.{entity_field})
    yield network_alerts (
        sip = {entity_alias}.{entity_field},
        alert_type = "{name}",
        detail = "{name} triggered",
        request_count = 1
    )
    limits {{ max_memory = "512MB"; max_instances = 100000; on_exceed = throttle; }}
}}""")


def count_rules():
    # (name, key, threshold)
    specs = [
        ("c_sip_3", "sip", 3), ("c_sip_8", "sip", 8), ("c_sip_20", "sip", 20),
        ("c_sip_50", "sip", 50), ("c_sip_100", "sip", 100), ("c_sip_150", "sip", 150),
        ("c_dip_3", "dip", 3), ("c_dip_8", "dip", 8), ("c_dip_20", "dip", 20),
        ("c_dip_50", "dip", 50), ("c_dip_80", "dip", 80), ("c_dip_120", "dip", 120),
        ("c_dport_3", "dport", 3), ("c_dport_5", "dport", 5), ("c_dport_9", "dport", 9),
        ("c_proto_5", "protocol", 5), ("c_proto_15", "protocol", 15), ("c_proto_40", "protocol", 40),
    ]
    for name, key, n in specs:
        emit(name, {"c": ("conn_events", None)}, key, f"{{ c | count >= {n}; }}", "c", "sip")


def agg_rules():
    # sum / avg / min / max over scalar fields
    specs = [
        # (name, key, field, op, threshold, cmp)
        ("s_bytes_200k", "sip", "bytes", "sum", 200000, ">="),
        ("s_bytes_500k", "sip", "bytes", "sum", 500000, ">="),
        ("s_bytes_900k", "sip", "bytes", "sum", 900000, ">="),
        ("s_bytes_dip_300k", "dip", "bytes", "sum", 300000, ">="),
        ("s_in_300k", "sip", "bytes_in", "sum", 300000, ">="),
        ("s_in_600k", "sip", "bytes_in", "sum", 600000, ">="),
        ("s_out_300k", "sip", "bytes_out", "sum", 300000, ">="),
        ("s_out_600k", "sip", "bytes_out", "sum", 600000, ">="),
        ("s_dur_2000", "sip", "duration", "sum", 2000, ">="),
        ("avg_dur_100", "sip", "duration", "avg", 100, ">="),
        ("avg_dur_300", "sip", "duration", "avg", 300, ">="),
        ("avg_dur_dip_150", "dip", "duration", "avg", 150, ">="),
        ("avg_pr_5000", "sip", "packet_rate", "avg", 5000, ">="),
        ("avg_pr_10000", "sip", "packet_rate", "avg", 10000, ">="),
        ("max_b_6000", "sip", "bytes", "max", 6000, ">="),
        ("max_b_7500", "sip", "bytes", "max", 7500, ">="),
        ("max_b_dip_7000", "dip", "bytes", "max", 7000, ">="),
        ("max_pr_15000", "sip", "packet_rate", "max", 15000, ">="),
        ("min_d_1", "sip", "duration", "min", 1, "<="),
        ("min_d_3", "sip", "duration", "min", 3, "<="),
        ("min_d_dip_2", "dip", "duration", "min", 2, "<="),
    ]
    for name, key, field, op, n, cmp in specs:
        emit(name, {"c": ("conn_events", None)}, key, f"{{ c.{field} | {op} {cmp} {n}; }}", "c", "sip")


def distinct_rules():
    specs = [
        ("dist_dip_15", "sip", "dip", 15),
        ("dist_dip_30", "sip", "dip", 30),
        ("dist_dip_45", "sip", "dip", 45),
        ("dist_dport_4", "sip", "dport", 4),
        ("dist_dport_6", "sip", "dport", 6),
    ]
    for name, key, field, n in specs:
        emit(name, {"c": ("conn_events", None)}, key, f"{{ c.{field} | distinct | count >= {n}; }}", "c", "sip")


def accu_rules():
    specs = [("accu_sip_60", "sip", 60), ("accu_sip_150", "sip", 150),
             ("accu_dip_60", "dip", 60), ("accu_dip_150", "dip", 150)]
    for name, key, n in specs:
        emit(name, {"c": ("conn_events", None)}, key, f"<accu>{{ c | count >= {n}; }}", "c", "sip")


def guard_rules():
    specs = [
        # (name, key, guard_expr, count)
        ("g_block_10", "sip", "c.blocked == true", 10),
        ("g_block_30", "sip", "c.blocked == true", 30),
        ("g_block_dip_15", "dip", "c.blocked == true", 15),
        ("g_pr_5000_3", "sip", "c.packet_rate >= 5000.0", 3),
        ("g_pr_12000_3", "sip", "c.packet_rate >= 12000.0", 3),
        ("g_geo_30", "sip", 'c.conn_info.geo.country == "CN"', 30),
        ("g_geo_80", "sip", 'c.conn_info.geo.country == "CN"', 80),
        ("g_geo_dip_40", "dip", 'c.conn_info.geo.country == "CN"', 40),
        ("g_tag_prod_30", "sip", 'c.tags[0] == "prod"', 30),
        ("g_tag_edge_30", "sip", 'c.tags[0] == "edge"', 30),
        ("g_app_0a_10", "sip", 'c.app_id == "0a0001"', 10),
        ("g_app_0b_10", "sip", 'c.app_id == "0b0002"', 10),
        ("g_str_e_10", "sip", 'indexof(c.action, "e") > 0 && startswith_any(c.action, "all", "den")', 10),
        ("g_str_a_10", "sip", 'indexof(c.action, "a") > 0 && startswith_any(c.action, "all", "den")', 10),
        ("g_math_4096_10", "sip", "abs(c.bytes - 4096) < 500", 10),
        ("g_math_1024_10", "sip", "abs(c.bytes - 1024) < 200", 10),
    ]
    for name, key, guard, n in specs:
        emit(name, {"c": ("conn_events", None)}, key, f"{{ c && {guard} | count >= {n}; }}", "c", "sip")


def auth_rules():
    specs = [
        ("auth_fail_5", "user", "a.result == \"failed\"", 5),
        ("auth_fail_15", "user", "a.result == \"failed\"", 15),
        ("auth_risk_08_3", "user", "a.risk >= 0.8", 3),
        ("auth_risk_05_3", "user", "a.risk >= 0.5", 3),
        ("auth_attempt_10_2", "user", "a.attempts >= 10", 2),
    ]
    for name, key, guard, n in specs:
        emit(name, {"a": ("auth_events", None)}, key, f"{{ a && {guard} | count >= {n}; }}", "a", "source_ip")


def dns_rules():
    specs = [
        ("dns_avg_250", "sip", "resp_size", "avg", 250, ">="),
        ("dns_avg_500", "sip", "resp_size", "avg", 500, ">="),
        ("dns_count_5", "sip", None, "count", 5, ">="),
        ("dns_count_20", "sip", None, "count", 20, ">="),
        ("dns_txt_3", "sip", "query_type == \"TXT\"", "count", 3, ">="),
    ]
    for name, key, field, op, n, cmp in specs:
        if op == "count":
            body = f"{{ d | count >= {n}; }}"
        elif field.startswith("query_type"):
            body = f"{{ d && d.{field} | count >= {n}; }}"
        else:
            body = f"{{ d.{field} | {op} {cmp} {n}; }}"
        emit(name, {"d": ("dns_events", None)}, key, body, "d", "sip")


def close_rules():
    specs = [("close_3", "sip", "allowed", 3), ("close_5", "sip", "allowed", 5),
             ("close_denied_3", "dip", "denied", 3)]
    for name, key, action, n in specs:
        emit(name, {"c": ("conn_events", f'c.action == "{action}"')}, key,
             f"{{ c | count >= 1; }} and close {{ total: c | count >= {n}; }}", "c", "sip")


def multi_event_rules():
    # Multi-source rules must share a match key field name across sources.
    # conn_events and dns_events both have `sip`, so conn+dns works; auth_events
    # uses `source_ip` (no shared name with conn) so it is not used here.
    RULES.append("""rule multi_conn_dns_denied {
    events {
        c : conn_events && c.action == "denied"
        d : dns_events
    }
    match<sip:5m> {
        on event {
            c | count >= 10;
            d | count >= 3;
        }
    } -> score(70.0)
    entity(ip, c.sip)
    yield network_alerts (sip = c.sip, alert_type = "multi_conn_dns_denied", detail = "denied + dns", request_count = 1)
    limits { max_memory = "512MB"; max_instances = 100000; on_exceed = throttle; }
}""")
    RULES.append("""rule multi_conn_dns_allowed {
    events {
        c : conn_events && c.action == "allowed"
        d : dns_events && d.query_type == "TXT"
    }
    match<sip:5m> {
        on event {
            c | count >= 30;
            d | count >= 2;
        }
    } -> score(70.0)
    entity(ip, c.sip)
    yield network_alerts (sip = c.sip, alert_type = "multi_conn_dns_allowed", detail = "allowed + dns txt", request_count = 1)
    limits { max_memory = "512MB"; max_instances = 100000; on_exceed = throttle; }
}""")


def chain_rules():
    RULES.append("""rule chain_scan_bf {
    events {
        scan : conn_events && action == "syn"
        login : conn_events && action == "login_fail"
    }
    match<sip:30m> {
        on event {
            scan | count >= 5;
            login | count >= 3;
        }
    } -> score(90.0)
    entity(ip, scan.sip)
    yield network_alerts (sip = scan.sip, alert_type = "chain_scan_bf", detail = "scan then brute force", request_count = 8)
    limits { max_memory = "512MB"; max_instances = 100000; on_exceed = throttle; }
}""")
    RULES.append("""rule chain_deny_scan {
    events {
        deny : conn_events && action == "denied"
        scan : conn_events && action == "syn"
    }
    match<sip:30m> {
        on event {
            deny | count >= 10;
            scan | count >= 3;
        }
    } -> score(85.0)
    entity(ip, deny.sip)
    yield network_alerts (sip = deny.sip, alert_type = "chain_deny_scan", detail = "deny then scan", request_count = 1)
    limits { max_memory = "512MB"; max_instances = 100000; on_exceed = throttle; }
}""")


def pipeline_rules():
    RULES.append("""rule pipe_denied_burst {
    events {
        c : conn_events && c.action == "denied"
    }
    match<sip:1m:fixed> {
        on event { c | count >= 1; }
        and close { burst: c | count >= 10; }
    }
    |> match<sip:5m:fixed> {
        on event { _in | count >= 1; }
        and close { bursts: _in | count >= 2; }
    } -> score(70.0)
    entity(ip, _in.sip)
    yield network_alerts (sip = _in.sip, alert_type = "pipe_denied_burst", detail = ">=2 denied bursts", request_count = 2)
    limits { max_memory = "512MB"; max_instances = 100000; on_exceed = throttle; }
}""")
    RULES.append("""rule pipe_allowed_burst {
    events {
        c : conn_events && c.action == "allowed"
    }
    match<sip:1m:fixed> {
        on event { c | count >= 1; }
        and close { burst: c | count >= 15; }
    }
    |> match<sip:5m:fixed> {
        on event { _in | count >= 1; }
        and close { bursts: _in | count >= 2; }
    } -> score(65.0)
    entity(ip, _in.sip)
    yield network_alerts (sip = _in.sip, alert_type = "pipe_allowed_burst", detail = ">=2 allowed bursts", request_count = 2)
    limits { max_memory = "512MB"; max_instances = 100000; on_exceed = throttle; }
}""")


def main():
    count_rules()
    agg_rules()
    distinct_rules()
    accu_rules()
    guard_rules()
    auth_rules()
    dns_rules()
    close_rules()
    multi_event_rules()
    chain_rules()
    pipeline_rules()

    # Pad with count rules to exactly 100.
    pad = 0
    while len(RULES) < 100:
        pad += 1
        n = pad * 5 + 2
        emit(f"c_pad_{pad}", {"c": ("conn_events", None)}, "sip", f"{{ c | count >= {n}; }}", "c", "sip")

    if len(RULES) > 100:
        sys.stderr.write(f"ERROR: generated {len(RULES)} rules (>100)\n")
        sys.exit(1)

    print('use "network.wfs"')
    print()
    for r in RULES:
        print(r)
        print()
    sys.stderr.write(f"generated {len(RULES)} rules\n")


if __name__ == "__main__":
    main()
