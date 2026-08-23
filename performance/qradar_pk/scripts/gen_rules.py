#!/usr/bin/env python3
"""生成 450 条规则的综合压测 WFL（6 个窗口、多数据类型、多 key/阈值网格）。

对标 IBM QRadar Event Processor 认证负载（80k EPS @ 451 条规则）：规则数 450 与
451 同量级。用法: python3 gen_rules.py > ../models/rules/throughput.wfl

窗口：conn_events（含 object/bool/float/chars/array）、auth_events、dns_events、
proxy_events（含 hex）、firewall_events、file_events（chars 实体 user）。

规则类别：count / sum / avg / min / max / distinct / accu / guard（bool/float/
object 嵌套/array/hex/字符串/数学函数）/ close / 多事件 / 序列 / pipeline，
多 key × 阈值网格。生成数量不足 450 时用 count 规则补齐，保证正好 450 条。
"""
import sys

RULES = []


def emit(name, events, match_key, on_body, entity_alias, entity_field,
         entity_type="ip", sip_expr=None):
    """events: alias -> (window, filter_expr or None).

    file 窗口用 chars 实体（user）：entity_type='user'，sip_expr 给占位 ip。
    """
    ev_decls = []
    for alias, (win, flt) in events.items():
        ev_decls.append(f"        {alias} : {win}" + (f" && {flt}" if flt else ""))
    if sip_expr is None:
        sip_expr = f"{entity_alias}.{entity_field}"
    RULES.append(f"""rule {name} {{
    events {{
{chr(10).join(ev_decls)}
    }}
    match<{match_key}:2m> {{
        on event {on_body}
    }} -> score(50.0)
    entity({entity_type}, {entity_alias}.{entity_field})
    yield network_alerts (
        sip = {sip_expr},
        alert_type = "{name}",
        detail = "{name} triggered",
        request_count = 1
    )
    limits {{ max_memory = "512MB"; max_instances = 100000; on_exceed = throttle; }}
}}""")


# ===========================================================================
# conn_events 规则
# ===========================================================================

def count_rules():
    keys = ["sip", "dip", "dport", "protocol", "duration"]
    for key in keys:
        for t in [3, 8, 20, 50, 100, 200]:
            emit(f"c_{key}_{t}", {"c": ("conn_events", None)}, key,
                 f"{{ c | count >= {t}; }}", "c", "sip")
    for key, t in [("bytes", 200), ("bytes_in", 500), ("bytes_out", 500)]:
        emit(f"c_{key}_{t}", {"c": ("conn_events", None)}, "sip",
             f"{{ c.{key} | distinct | count >= {t}; }}", "c", "sip")


def agg_rules():
    specs = [
        # (name, key, field, op, threshold, cmp)
        ("s_bytes_200k", "sip", "bytes", "sum", 200000, ">="),
        ("s_bytes_500k", "sip", "bytes", "sum", 500000, ">="),
        ("s_bytes_900k", "sip", "bytes", "sum", 900000, ">="),
        ("s_bytes_dip_300k", "dip", "bytes", "sum", 300000, ">="),
        ("s_bytes_dip_700k", "dip", "bytes", "sum", 700000, ">="),
        ("s_in_300k", "sip", "bytes_in", "sum", 300000, ">="),
        ("s_in_600k", "sip", "bytes_in", "sum", 600000, ">="),
        ("s_in_dip_400k", "dip", "bytes_in", "sum", 400000, ">="),
        ("s_out_300k", "sip", "bytes_out", "sum", 300000, ">="),
        ("s_out_600k", "sip", "bytes_out", "sum", 600000, ">="),
        ("s_out_dip_400k", "dip", "bytes_out", "sum", 400000, ">="),
        ("s_dur_2000", "sip", "duration", "sum", 2000, ">="),
        ("s_dur_5000", "sip", "duration", "sum", 5000, ">="),
        ("avg_dur_100", "sip", "duration", "avg", 100, ">="),
        ("avg_dur_300", "sip", "duration", "avg", 300, ">="),
        ("avg_dur_dip_150", "dip", "duration", "avg", 150, ">="),
        ("avg_dur_dip_250", "dip", "duration", "avg", 250, ">="),
        ("avg_pr_5000", "sip", "packet_rate", "avg", 5000, ">="),
        ("avg_pr_10000", "sip", "packet_rate", "avg", 10000, ">="),
        ("avg_pr_15000", "sip", "packet_rate", "avg", 15000, ">="),
        ("max_b_6000", "sip", "bytes", "max", 6000, ">="),
        ("max_b_7500", "sip", "bytes", "max", 7500, ">="),
        ("max_b_dip_7000", "dip", "bytes", "max", 7000, ">="),
        ("max_in_3000", "sip", "bytes_in", "max", 3000, ">="),
        ("max_out_3000", "sip", "bytes_out", "max", 3000, ">="),
        ("max_pr_15000", "sip", "packet_rate", "max", 15000, ">="),
        ("max_pr_18000", "sip", "packet_rate", "max", 18000, ">="),
        ("min_d_1", "sip", "duration", "min", 1, "<="),
        ("min_d_3", "sip", "duration", "min", 3, "<="),
        ("min_d_dip_2", "dip", "duration", "min", 2, "<="),
        ("min_d_dip_5", "dip", "duration", "min", 5, "<="),
        ("min_b_50", "sip", "bytes", "min", 50, "<="),
        ("avg_b_1000", "sip", "bytes", "avg", 1000, ">="),
        ("avg_b_3000", "sip", "bytes", "avg", 3000, ">="),
    ]
    for name, key, field, op, n, cmp in specs:
        emit(name, {"c": ("conn_events", None)}, key, f"{{ c.{field} | {op} {cmp} {n}; }}", "c", "sip")


def distinct_rules():
    specs = [
        ("dist_dip_15", "sip", "dip", 15), ("dist_dip_30", "sip", "dip", 30),
        ("dist_dip_45", "sip", "dip", 45), ("dist_dport_4", "sip", "dport", 4),
        ("dist_dport_6", "sip", "dport", 6), ("dist_dport_8", "dip", "dport", 8),
        ("dist_proto_3", "sip", "protocol", 3), ("dist_proto_4", "dip", "protocol", 4),
        ("dist_action_2", "sip", "action", 2),
    ]
    for name, key, field, n in specs:
        emit(name, {"c": ("conn_events", None)}, key, f"{{ c.{field} | distinct | count >= {n}; }}", "c", "sip")


def accu_rules():
    for key, t in [("sip", 60), ("sip", 150), ("sip", 300), ("dip", 60), ("dip", 150), ("dport", 20), ("protocol", 40)]:
        emit(f"accu_{key}_{t}", {"c": ("conn_events", None)}, key,
             f"<accu>{{ c | count >= {t}; }}", "c", "sip")


def guard_rules():
    specs = [
        ("g_block_10", "sip", "c.blocked == true", 10),
        ("g_block_30", "sip", "c.blocked == true", 30),
        ("g_block_dip_15", "dip", "c.blocked == true", 15),
        ("g_block_dip_40", "dip", "c.blocked == true", 40),
        ("g_pr_5000_3", "sip", "c.packet_rate >= 5000.0", 3),
        ("g_pr_12000_3", "sip", "c.packet_rate >= 12000.0", 3),
        ("g_pr_18000_3", "sip", "c.packet_rate >= 18000.0", 3),
        ("g_geo_30", "sip", 'c.geo_country == "CN"', 30),
        ("g_geo_80", "sip", 'c.geo_country == "CN"', 80),
        ("g_geo_dip_40", "dip", 'c.geo_country == "CN"', 40),
        ("g_geo_dip_90", "dip", 'c.geo_country == "CN"', 90),
        ("g_tag_prod_30", "sip", 'c.tags[0] == "prod"', 30),
        ("g_tag_edge_30", "sip", 'c.tags[0] == "edge"', 30),
        ("g_tag_prod_60", "dip", 'c.tags[0] == "prod"', 60),
        ("g_app_0a_10", "sip", 'c.app_id == "0a0001"', 10),
        ("g_app_0b_10", "sip", 'c.app_id == "0b0002"', 10),
        ("g_app_0a_30", "dip", 'c.app_id == "0a0001"', 30),
        ("g_str_e_10", "sip", 'indexof(c.action, "e") > 0 && startswith_any(c.action, "all", "den")', 10),
        ("g_str_a_10", "sip", 'indexof(c.action, "a") > 0 && startswith_any(c.action, "all", "den")', 10),
        ("g_str_end_d", "sip", 'endswith(c.action, "d")', 20),
        ("g_str_den", "sip", 'startswith(c.action, "den")', 15),
        ("g_math_4096_10", "sip", "abs(c.bytes - 4096) < 500", 10),
        ("g_math_1024_10", "sip", "abs(c.bytes - 1024) < 200", 10),
        ("g_math_2048_20", "sip", "abs(c.bytes - 2048) < 400", 20),
        ("g_math_round_5", "sip", "round(c.bytes / 1000.0) >= 5", 10),
        ("g_concat_pre", "sip", 'concat("x", c.protocol) == "xtcp"', 20),
        ("g_len_5", "sip", 'length(c.protocol) >= 3', 20),
        ("g_geo_vlan_100", "sip", "c.vlan >= 1000", 20),
        ("g_geo_flow", "dip", 'indexof(c.flow_id, "flow-") == 0', 30),
    ]
    for name, key, guard, n in specs:
        emit(name, {"c": ("conn_events", None)}, key, f"{{ c && {guard} | count >= {n}; }}", "c", "sip")


def close_rules():
    specs = [
        ("close_3", "sip", 'c.action == "allowed"', 3),
        ("close_5", "sip", 'c.action == "allowed"', 5),
        ("close_10", "sip", 'c.action == "allowed"', 10),
        ("close_denied_3", "dip", 'c.action == "denied"', 3),
        ("close_denied_6", "dip", 'c.action == "denied"', 6),
        ("close_dur_2", "sip", 'c.duration >= 2', 2),
        ("close_pr_5", "sip", 'c.packet_rate >= 5000.0', 5),
    ]
    for name, key, flt, n in specs:
        emit(name, {"c": ("conn_events", flt)}, key,
             f"{{ c | count >= 1; }} and close {{ total: c | count >= {n}; }}", "c", "sip")


def multi_event_rules():
    # Multi-source rules must share a match key field name across sources.
    # conn/proxy/firewall/dns all have `sip`; auth (source_ip) and file (user)
    # don't share a name, so they are not used in multi-source rules.
    combos = [
        ("multi_conn_dns_denied", "c", "conn_events", 'c.action == "denied"', "d", "dns_events", None, 10, 3),
        ("multi_conn_dns_allowed", "c", "conn_events", 'c.action == "allowed"', "d", "dns_events", 'd.query_type == "TXT"', 30, 2),
        ("multi_conn_proxy", "c", "conn_events", 'c.action == "denied"', "p", "proxy_events", 'p.status == 500', 10, 2),
        ("multi_conn_firewall", "c", "conn_events", 'c.action == "denied"', "fw", "firewall_events", 'fw.action == "deny"', 10, 5),
        ("multi_proxy_dns", "p", "proxy_events", 'p.status == 500', "d", "dns_events", None, 3, 3),
        ("multi_firewall_dns", "fw", "firewall_events", 'fw.action == "deny"', "d", "dns_events", None, 8, 2),
    ]
    for name, a1, w1, f1, a2, w2, f2, n1, n2 in combos:
        ev = {a1: (w1, f1), a2: (w2, f2)}
        body = f"{{ {a1} | count >= {n1}; {a2} | count >= {n2}; }}"
        emit(name, ev, "sip", body, a1, "sip")


def chain_rules():
    RULES.append("""rule chain_scan_bf {
    events {
        scan : conn_events && action == "syn"
        login : conn_events && action == "login_fail"
    }
    match<sip:30m> {
        on event { scan | count >= 5; login | count >= 3; }
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
        on event { deny | count >= 10; scan | count >= 3; }
    } -> score(85.0)
    entity(ip, deny.sip)
    yield network_alerts (sip = deny.sip, alert_type = "chain_deny_scan", detail = "deny then scan", request_count = 1)
    limits { max_memory = "512MB"; max_instances = 100000; on_exceed = throttle; }
}""")
    RULES.append("""rule chain_deny_dns {
    events {
        deny : conn_events && action == "denied"
        q : dns_events && query_type == "TXT"
    }
    match<sip:30m> {
        on event { deny | count >= 5; q | count >= 2; }
    } -> score(80.0)
    entity(ip, deny.sip)
    yield network_alerts (sip = deny.sip, alert_type = "chain_deny_dns", detail = "deny then txt dns", request_count = 1)
    limits { max_memory = "512MB"; max_instances = 100000; on_exceed = throttle; }
}""")


def pipeline_rules():
    RULES.append("""rule pipe_denied_burst {
    events { c : conn_events && c.action == "denied" }
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
    events { c : conn_events && c.action == "allowed" }
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
    RULES.append("""rule pipe_proxy_500 {
    events { p : proxy_events && p.status == 500 }
    match<sip:1m:fixed> {
        on event { p | count >= 1; }
        and close { burst: p | count >= 5; }
    }
    |> match<sip:5m:fixed> {
        on event { _in | count >= 1; }
        and close { bursts: _in | count >= 2; }
    } -> score(60.0)
    entity(ip, _in.sip)
    yield network_alerts (sip = _in.sip, alert_type = "pipe_proxy_500", detail = ">=2 proxy-500 bursts", request_count = 2)
    limits { max_memory = "512MB"; max_instances = 100000; on_exceed = throttle; }
}""")


def auth_rules():
    specs = [
        ("auth_fail_5", "user", "a.result == \"failed\"", 5),
        ("auth_fail_15", "user", "a.result == \"failed\"", 15),
        ("auth_fail_30", "user", "a.result == \"failed\"", 30),
        ("auth_risk_08_3", "user", "a.risk >= 0.8", 3),
        ("auth_risk_05_3", "user", "a.risk >= 0.5", 3),
        ("auth_risk_09_2", "source_ip", "a.risk >= 0.9", 2),
        ("auth_attempt_10_2", "user", "a.attempts >= 10", 2),
        ("auth_attempt_20_1", "user", "a.attempts >= 20", 1),
        ("auth_agent_curl", "user", 'a.agent == "curl"', 5),
        ("auth_agent_unk", "source_ip", 'a.agent == "unknown"', 3),
        ("auth_dest_fail", "dest_ip", "a.result == \"failed\"", 10),
    ]
    for name, key, guard, n in specs:
        emit(name, {"a": ("auth_events", None)}, key, f"{{ a && {guard} | count >= {n}; }}", "a", "source_ip")


def dns_rules():
    specs = [
        ("dns_avg_250", "sip", "resp_size", "avg", 250, ">="),
        ("dns_avg_500", "sip", "resp_size", "avg", 500, ">="),
        ("dns_avg_800", "sip", "resp_size", "avg", 800, ">="),
        ("dns_count_5", "sip", None, "count", 5, ">="),
        ("dns_count_20", "sip", None, "count", 20, ">="),
        ("dns_count_40", "domain", None, "count", 40, ">="),
        ("dns_txt_3", "sip", "query_type == \"TXT\"", "count", 3, ">="),
        ("dns_txt_8", "sip", "query_type == \"TXT\"", "count", 8, ">="),
        ("dns_aaaa_3", "sip", "query_type == \"AAAA\"", "count", 3, ">="),
        ("dns_max_2000", "sip", "resp_size", "max", 2000, ">="),
        ("dns_max_4000", "domain", "resp_size", "max", 4000, ">="),
        ("dns_min_50", "sip", "resp_size", "min", 50, "<="),
        ("dns_sum_3000", "sip", "resp_size", "sum", 3000, ">="),
    ]
    for name, key, field, op, n, cmp in specs:
        if op == "count":
            body = f"{{ d | count >= {n}; }}"
        elif field and field.startswith("query_type"):
            body = f"{{ d && d.{field} | count >= {n}; }}"
        else:
            body = f"{{ d.{field} | {op} {cmp} {n}; }}"
        emit(name, {"d": ("dns_events", None)}, key, body, "d", "sip")


# ===========================================================================
# proxy_events 规则（hex 类型）
# ===========================================================================

def proxy_rules():
    # count
    for key, t in [("sip", 2), ("sip", 5), ("sip", 10), ("sip", 30), ("method", 20), ("status", 100)]:
        emit(f"pr_c_{key}_{t}", {"p": ("proxy_events", None)}, key,
             f"{{ p | count >= {t}; }}", "p", "sip")
    # agg
    for name, key, field, op, n, cmp in [
        ("pr_s_bytes_1m", "sip", "bytes", "sum", 1000000, ">="),
        ("pr_s_bytes_3m", "sip", "bytes", "sum", 3000000, ">="),
        ("pr_avg_risk_08", "sip", "risk", "avg", 0.8, ">="),
        ("pr_avg_risk_05", "sip", "risk", "avg", 0.5, ">="),
        ("pr_max_bytes_40k", "sip", "bytes", "max", 40000, ">="),
        ("pr_min_bytes_100", "sip", "bytes", "min", 100, "<="),
        ("pr_count_404_3", "sip", "status", "distinct", 3, ">="),
    ]:
        body = f"{{ p.{field} | {op} {cmp} {n}; }}"
        if op == "distinct":
            body = f"{{ p.{field} | distinct | count >= {n}; }}"
        emit(name, {"p": ("proxy_events", None)}, key, body, "p", "sip")
    # guards (method, status, user_agent — hex 不支持字面量比较，改用 yield 读取)
    for name, key, guard, n in [
        ("pr_g_500_3", "sip", 'p.status == 500', 3),
        ("pr_g_500_10", "sip", 'p.status == 500', 10),
        ("pr_g_404_5", "sip", 'p.status == 404', 5),
        ("pr_g_delete_2", "sip", 'p.method == "DELETE"', 2),
        ("pr_g_post_10", "sip", 'p.method == "POST"', 10),
        ("pr_g_risk08_3", "sip", "p.risk >= 0.8", 3),
        ("pr_g_ua_curl", "sip", 'p.user_agent == "curl"', 5),
        ("pr_g_ua_chrome", "sip", 'p.user_agent == "chrome"', 5),
        ("pr_g_url_5", "sip", 'indexof(p.url, "resource") > 0', 10),
        ("pr_g_risk_method", "sip", "p.risk >= 0.6 && p.method == \"POST\"", 5),
    ]:
        emit(name, {"p": ("proxy_events", None)}, key, f"{{ p && {guard} | count >= {n}; }}", "p", "sip")
    # distinct url
    for n in [10, 30]:
        emit(f"pr_dist_url_{n}", {"p": ("proxy_events", None)}, "sip",
             f"{{ p.url | distinct | count >= {n}; }}", "p", "sip")
    # hex 类型覆盖：yield trace_id（hex → chars 输出字段），读取 hex 数据但不比较
    RULES.append("""rule pr_hex_yield_5 {
    events { p : proxy_events }
    match<sip:2m> { on event { p | count >= 5; } } -> score(50.0)
    entity(ip, p.sip)
    yield network_alerts (sip = p.sip, alert_type = "pr_hex_yield", detail = p.trace_id, request_count = 1)
    limits { max_memory = "512MB"; max_instances = 100000; on_exceed = throttle; }
}""")


# ===========================================================================
# firewall_events 规则
# ===========================================================================

def firewall_rules():
    for key, t in [("sip", 3), ("sip", 10), ("sip", 30), ("dip", 5), ("rule_id", 20)]:
        emit(f"fw_c_{key}_{t}", {"fw": ("firewall_events", None)}, key,
             f"{{ fw | count >= {t}; }}", "fw", "sip")
    for name, key, field, op, n, cmp in [
        ("fw_s_bytes_500k", "sip", "bytes", "sum", 500000, ">="),
        ("fw_s_bytes_1m", "sip", "bytes", "sum", 1000000, ">="),
        ("fw_max_bytes_6000", "sip", "bytes", "max", 6000, ">="),
        ("fw_avg_bytes_1000", "sip", "bytes", "avg", 1000, ">="),
        ("fw_count_deny_5", "sip", "action", "distinct", 5, ">="),
    ]:
        body = f"{{ fw.{field} | {op} {cmp} {n}; }}"
        if op == "distinct":
            body = f"{{ fw.{field} | distinct | count >= {n}; }}"
        emit(name, {"fw": ("firewall_events", None)}, key, body, "fw", "sip")
    for name, key, guard, n in [
        ("fw_g_deny_5", "sip", 'fw.action == "deny"', 5),
        ("fw_g_deny_20", "sip", 'fw.action == "deny"', 20),
        ("fw_g_rule0_10", "sip", 'fw.rule_id == "fw-0"', 10),
        ("fw_g_rule7_5", "sip", 'fw.rule_id == "fw-7"', 5),
        ("fw_g_tcp_10", "sip", 'fw.protocol == "tcp"', 10),
        ("fw_g_udp_5", "sip", 'fw.protocol == "udp"', 5),
        ("fw_g_deny_tcp", "sip", 'fw.action == "deny" && fw.protocol == "tcp"', 10),
        ("fw_g_deny_dip", "dip", 'fw.action == "deny"', 10),
        ("fw_g_allow_big", "sip", 'fw.action == "allow" && fw.bytes >= 4000', 10),
        ("fw_dist_proto", "sip", 'fw.protocol | distinct | count >= 3', 3),
    ]:
        if "distinct" in guard:
            emit(name, {"fw": ("firewall_events", None)}, key, f"{{ {guard}; }}", "fw", "sip")
        else:
            emit(name, {"fw": ("firewall_events", None)}, key, f"{{ fw && {guard} | count >= {n}; }}", "fw", "sip")


# ===========================================================================
# file_events 规则（chars 实体 user）
# ===========================================================================

def file_rules():
    for key, t in [("user", 2), ("user", 5), ("user", 10), ("file", 20), ("action", 100)]:
        emit(f"fl_c_{key}_{t}", {"f": ("file_events", None)}, key,
             f"{{ f | count >= {t}; }}", "f", "user", entity_type="user", sip_expr='"0.0.0.0"')
    for name, key, field, op, n, cmp in [
        ("fl_s_size_1m", "user", "size", "sum", 1000000, ">="),
        ("fl_s_size_5m", "user", "size", "sum", 5000000, ">="),
        ("fl_max_size_80k", "user", "size", "max", 80000, ">="),
        ("fl_avg_size_10k", "user", "size", "avg", 10000, ">="),
        ("fl_min_size_10", "user", "size", "min", 10, "<="),
        ("fl_count_write_5", "user", "action", "distinct", 5, ">="),
    ]:
        body = f"{{ f.{field} | {op} {cmp} {n}; }}"
        if op == "distinct":
            body = f"{{ f.{field} | distinct | count >= {n}; }}"
        emit(name, {"f": ("file_events", None)}, key, body, "f", "user",
             entity_type="user", sip_expr='"0.0.0.0"')
    for name, key, guard, n in [
        ("fl_g_sensitive_3", "user", "f.sensitive == true", 3),
        ("fl_g_sensitive_10", "user", "f.sensitive == true", 10),
        ("fl_g_delete_2", "user", 'f.action == "delete"', 2),
        ("fl_g_write_10", "user", 'f.action == "write"', 10),
        ("fl_g_secret_5", "user", 'f.file == "/home/user/secret.txt"', 5),
        ("fl_g_etc_10", "user", 'f.file == "/etc/app/config.yaml"', 10),
        ("fl_g_big_5", "user", "f.size >= 50000", 5),
        ("fl_g_sens_big", "user", "f.sensitive == true && f.size >= 20000", 5),
    ]:
        emit(name, {"f": ("file_events", None)}, key, f"{{ f && {guard} | count >= {n}; }}", "f", "user",
             entity_type="user", sip_expr='"0.0.0.0"')


# ===========================================================================
# 450 规则补齐（对标 QRadar EP 80k EPS @ 451 规则规格）— ~150 条新增
# ===========================================================================

def conn_extra_rules():
    # action 过滤计数（denied/allowed × sip/dip）
    for name, key, action, n in [
        ("c_denied_5", "sip", "denied", 5), ("c_denied_20", "sip", "denied", 20),
        ("c_denied_50", "sip", "denied", 50), ("c_allowed_10", "sip", "allowed", 10),
        ("c_allowed_40", "sip", "allowed", 40), ("c_allowed_100", "sip", "allowed", 100),
        ("c_denied_dip_8", "dip", "denied", 8), ("c_denied_dip_30", "dip", "denied", 30),
        ("c_allowed_dip_15", "dip", "allowed", 15), ("c_allowed_dip_60", "dip", "allowed", 60),
    ]:
        emit(name, {"c": ("conn_events", f'c.action == "{action}"')}, key,
             f"{{ c | count >= {n}; }}", "c", "sip")
    # 更高阈值
    for key in ["dport", "protocol", "duration"]:
        for t in [400, 800]:
            emit(f"c_{key}_{t}", {"c": ("conn_events", None)}, key, f"{{ c | count >= {t}; }}", "c", "sip")
    for t in [500, 1000]:
        emit(f"c_bytes_{t}", {"c": ("conn_events", None)}, "sip", f"{{ c | count >= {t}; }}", "c", "sip")
    # 聚合扩展（sum/avg/max/min）
    for name, key, field, op, n in [
        ("s_pr_30000", "sip", "packet_rate", "sum", 30000),
        ("s_pr_60000", "sip", "packet_rate", "sum", 60000),
        ("s_bytes_dport_500k", "dport", "bytes", "sum", 500000),
        ("s_bytes_dport_1m", "dport", "bytes", "sum", 1000000),
        ("s_bytes_proto_800k", "protocol", "bytes", "sum", 800000),
        ("avg_pr_dip_8000", "dip", "packet_rate", "avg", 8000),
        ("avg_pr_dip_12000", "dip", "packet_rate", "avg", 12000),
        ("avg_b_dip_1500", "dip", "bytes", "avg", 1500),
        ("avg_b_dport_2500", "dport", "bytes", "avg", 2500),
        ("max_pr_dip_20000", "dip", "packet_rate", "max", 20000),
        ("max_b_dport_9000", "dport", "bytes", "max", 9000),
        ("max_in_5000", "sip", "bytes_in", "max", 5000),
        ("max_out_5000", "sip", "bytes_out", "max", 5000),
        ("min_pr_100", "sip", "packet_rate", "min", 100),
        ("min_b_dip_20", "dip", "bytes", "min", 20),
    ]:
        emit(name, {"c": ("conn_events", None)}, key, f"{{ c.{field} | {op} >= {n}; }}", "c", "sip")
    # distinct 扩展
    for name, key, field, n in [
        ("dist_sip_10", "dip", "sip", 10), ("dist_sip_25", "dip", "sip", 25),
        ("dist_dport_10", "sip", "dport", 10), ("dist_dport_15", "dip", "dport", 15),
        ("dist_app_2", "sip", "app_id", 2), ("dist_app_3", "dip", "app_id", 3),
        ("dist_action_3", "sip", "action", 3), ("dist_action_5", "dip", "action", 5),
    ]:
        emit(name, {"c": ("conn_events", None)}, key, f"{{ c.{field} | distinct | count >= {n}; }}", "c", "sip")
    # guard 扩展
    for name, key, guard, n in [
        ("g_denied_10", "sip", 'c.action == "denied"', 10),
        ("g_denied_40", "dip", 'c.action == "denied"', 40),
        ("g_allowed_20", "sip", 'c.action == "allowed"', 20),
        ("g_pr_8000_12000", "sip", "c.packet_rate >= 8000.0 && c.packet_rate < 12000.0", 3),
        ("g_pr_under_1000", "sip", "c.packet_rate < 1000.0", 5),
        ("g_geo_city_30", "sip", 'c.geo_city == "Shanghai"', 30),
        ("g_geo_city_60", "dip", 'c.geo_city == "Shanghai"', 60),
        ("g_geo_flow_60", "sip", 'indexof(c.flow_id, "flow-") == 0', 60),
        ("g_tag_edge_60", "dip", 'c.tags[1] == "edge"', 60),
        ("g_tag_dmz_30", "sip", 'c.tags[2] == "dmz"', 30),
        ("g_app_0b_30", "dip", 'c.app_id == "0b0002"', 30),
        ("g_app_0c_10", "sip", 'c.app_id == "0c0003"', 10),
        ("g_app_0c_20", "dip", 'c.app_id == "0c0003"', 20),
        ("g_proto_tcp_10", "sip", 'c.protocol == "tcp"', 10),
        ("g_proto_udp_5", "sip", 'c.protocol == "udp"', 5),
        ("g_proto_icmp_3", "sip", 'c.protocol == "icmp"', 3),
    ]:
        emit(name, {"c": ("conn_events", None)}, key, f"{{ c && {guard} | count >= {n}; }}", "c", "sip")


def auth_extra_rules():
    guards = [
        ("auth_fail_dip_8", "dest_ip", 'a.result == "failed"', 8),
        ("auth_fail_dip_25", "dest_ip", 'a.result == "failed"', 25),
        ("auth_ok_risk_10", "user", 'a.result == "ok" && a.risk >= 0.6', 10),
        ("auth_risk_03_5", "user", "a.risk >= 0.3", 5),
        ("auth_risk_07_3", "user", "a.risk >= 0.7", 3),
        ("auth_risk_09_5", "source_ip", "a.risk >= 0.9", 5),
        ("auth_attempt_5_3", "user", "a.attempts >= 5", 3),
        ("auth_attempt_15_2", "user", "a.attempts >= 15", 2),
        ("auth_attempt_30_1", "user", "a.attempts >= 30", 1),
        ("auth_agent_python_5", "user", 'a.agent == "python"', 5),
        ("auth_agent_unknown_8", "source_ip", 'a.agent == "unknown"', 8),
        ("auth_agent_chrome_3", "user", 'a.agent == "chrome"', 3),
        ("auth_fail_agent_3", "user", 'a.result == "failed" && a.agent == "curl"', 3),
        ("auth_risk_agent_2", "user", 'a.risk >= 0.8 && a.agent == "unknown"', 2),
        ("auth_src_fail_5", "source_ip", 'a.result == "failed"', 5),
        ("auth_usr_risk_2", "user", "a.risk >= 0.9 && a.attempts >= 5", 2),
    ]
    for name, key, guard, n in guards:
        emit(name, {"a": ("auth_events", None)}, key, f"{{ a && {guard} | count >= {n}; }}", "a", "source_ip")
    for name, key, field, n in [
        ("auth_dist_dest_5", "user", "dest_ip", 5),
        ("auth_dist_agent_3", "user", "agent", 3),
        ("auth_sum_attempts_30", "user", "attempts", 30),
        ("auth_max_attempts_5", "source_ip", "attempts", 5),
    ]:
        if name.startswith("auth_dist"):
            emit(name, {"a": ("auth_events", None)}, key, f"{{ a.{field} | distinct | count >= {n}; }}", "a", "source_ip")
        elif name.startswith("auth_sum"):
            emit(name, {"a": ("auth_events", None)}, key, f"{{ a.{field} | sum >= {n}; }}", "a", "source_ip")
        else:
            emit(name, {"a": ("auth_events", None)}, key, f"{{ a.{field} | max >= {n}; }}", "a", "source_ip")


def dns_extra_rules():
    for name, key, body in [
        ("dns_count_domain_10", "domain", "{ d | count >= 10; }"),
        ("dns_count_domain_25", "domain", "{ d | count >= 25; }"),
        ("dns_count_domain_60", "domain", "{ d | count >= 60; }"),
        ("dns_any_3", "sip", '{ d && d.query_type == "ANY" | count >= 3; }'),
        ("dns_any_8", "sip", '{ d && d.query_type == "ANY" | count >= 8; }'),
        ("dns_cname_5", "domain", '{ d && d.query_type == "CNAME" | count >= 5; }'),
        ("dns_aaaa_domain_5", "domain", '{ d && d.query_type == "AAAA" | count >= 5; }'),
        ("dns_avg_domain_300", "domain", "{ d.resp_size | avg >= 300; }"),
        ("dns_avg_domain_600", "domain", "{ d.resp_size | avg >= 600; }"),
        ("dns_max_sip_2500", "sip", "{ d.resp_size | max >= 2500; }"),
        ("dns_min_domain_100", "domain", "{ d.resp_size | min <= 100; }"),
        ("dns_sum_domain_5000", "domain", "{ d.resp_size | sum >= 5000; }"),
        ("dns_dist_qt_3", "sip", "{ d.query_type | distinct | count >= 3; }"),
        ("dns_dist_qt_4", "domain", "{ d.query_type | distinct | count >= 4; }"),
        ("dns_high_2", "sip", "{ d && d.resp_size >= 2000 | count >= 2; }"),
    ]:
        emit(name, {"d": ("dns_events", None)}, key, body, "d", "sip")


def proxy_extra_rules():
    for name, key, guard, n in [
        ("pr_c_status_200_5", "sip", 'p.status == 200', 5),
        ("pr_c_status_300_3", "sip", 'p.status == 300', 3),
        ("pr_c_status_500_20", "sip", 'p.status == 500', 20),
        ("pr_c_method_GET_8", "sip", 'p.method == "GET"', 8),
        ("pr_c_method_PUT_3", "sip", 'p.method == "PUT"', 3),
    ]:
        emit(name, {"p": ("proxy_events", None)}, key, f"{{ p && {guard} | count >= {n}; }}", "p", "sip")
    for name, key, field, op, n in [
        ("pr_s_bytes_sip_2m", "sip", "bytes", "sum", 2000000),
        ("pr_avg_risk_status_07", "status", "risk", "avg", 0.7),
        ("pr_max_bytes_80k", "sip", "bytes", "max", 80000),
        ("pr_min_bytes_50", "sip", "bytes", "min", 50),
    ]:
        emit(name, {"p": ("proxy_events", None)}, key, f"{{ p.{field} | {op} >= {n}; }}", "p", "sip")
    for name, key, guard, n in [
        ("pr_g_get_8", "sip", 'p.method == "GET"', 8),
        ("pr_g_put_3", "sip", 'p.method == "PUT"', 3),
        ("pr_g_url_resource_5", "sip", 'indexof(p.url, "resource") > 0', 5),
        ("pr_g_url_query_10", "sip", 'indexof(p.url, "query") > 0', 10),
        ("pr_g_ua_python_5", "sip", 'p.user_agent == "python-requests"', 5),
        ("pr_g_ua_safari_3", "sip", 'p.user_agent == "safari"', 3),
        ("pr_g_500_post_3", "sip", 'p.status == 500 && p.method == "POST"', 3),
        ("pr_g_404_get_2", "sip", 'p.status == 404 && p.method == "GET"', 2),
        ("pr_g_risk_ua_2", "sip", 'p.risk >= 0.7 && p.user_agent == "curl"', 2),
        ("pr_g_bytes_1k_5", "sip", "p.bytes >= 1000", 5),
    ]:
        emit(name, {"p": ("proxy_events", None)}, key, f"{{ p && {guard} | count >= {n}; }}", "p", "sip")
    for name, key, field, n in [
        ("pr_dist_status_4", "sip", "status", 4),
        ("pr_dist_method_3", "sip", "method", 3),
    ]:
        emit(name, {"p": ("proxy_events", None)}, key, f"{{ p.{field} | distinct | count >= {n}; }}", "p", "sip")


def firewall_extra_rules():
    for name, key, body in [
        ("fw_c_dip_8", "dip", "{ fw | count >= 8; }"),
        ("fw_c_dip_20", "dip", "{ fw | count >= 20; }"),
        ("fw_c_dip_40", "dip", "{ fw | count >= 40; }"),
        ("fw_c_rule_8", "rule_id", "{ fw | count >= 8; }"),
        ("fw_c_rule_30", "rule_id", "{ fw | count >= 30; }"),
    ]:
        emit(name, {"fw": ("firewall_events", None)}, key, body, "fw", "sip")
    for name, key, field, op, n in [
        ("fw_s_bytes_dip_800k", "dip", "bytes", "sum", 800000),
        ("fw_avg_bytes_dip_1500", "dip", "bytes", "avg", 1500),
        ("fw_max_bytes_dip_9000", "dip", "bytes", "max", 9000),
        ("fw_min_bytes_20", "sip", "bytes", "min", 20),
    ]:
        emit(name, {"fw": ("firewall_events", None)}, key, f"{{ fw.{field} | {op} >= {n}; }}", "fw", "sip")
    for name, key, guard, n in [
        ("fw_g_deny_dip_15", "dip", 'fw.action == "deny"', 15),
        ("fw_g_deny_dip_30", "dip", 'fw.action == "deny"', 30),
        ("fw_g_allow_dip_10", "dip", 'fw.action == "allow"', 10),
        ("fw_g_rule5_10", "sip", 'fw.rule_id == "fw-5"', 10),
        ("fw_g_rule42_5", "sip", 'fw.rule_id == "fw-42"', 5),
        ("fw_g_icmp_3", "sip", 'fw.protocol == "icmp"', 3),
        ("fw_g_sctp_2", "sip", 'fw.protocol == "sctp"', 2),
        ("fw_g_deny_icmp_2", "sip", 'fw.action == "deny" && fw.protocol == "icmp"', 2),
        ("fw_g_allow_big_dip", "dip", 'fw.action == "allow" && fw.bytes >= 4000', 5),
        ("fw_dist_action_2", "sip", "action", 2),
        ("fw_dist_rule_5", "sip", "rule_id", 5),
    ]:
        if name.startswith("fw_dist"):
            emit(name, {"fw": ("firewall_events", None)}, key, f"{{ fw.{guard} | distinct | count >= {n}; }}", "fw", "sip")
        else:
            emit(name, {"fw": ("firewall_events", None)}, key, f"{{ fw && {guard} | count >= {n}; }}", "fw", "sip")


def file_extra_rules():
    for name, key, body in [
        ("fl_c_user_20", "user", "{ f | count >= 20; }"),
        ("fl_c_user_50", "user", "{ f | count >= 50; }"),
        ("fl_c_file_50", "file", "{ f | count >= 50; }"),
        ("fl_c_write_15", "user", '{ f && f.action == "write" | count >= 15; }'),
    ]:
        emit(name, {"f": ("file_events", None)}, key, body, "f", "user", entity_type="user", sip_expr='"0.0.0.0"')
    for name, key, field, op, n in [
        ("fl_s_size_user_2m", "user", "size", "sum", 2000000),
        ("fl_avg_size_user_20k", "user", "size", "avg", 20000),
        ("fl_max_size_file_100k", "file", "size", "max", 100000),
        ("fl_min_size_user_5", "user", "size", "min", 5),
    ]:
        emit(name, {"f": ("file_events", None)}, key, f"{{ f.{field} | {op} >= {n}; }}", "f", "user", entity_type="user", sip_expr='"0.0.0.0"')
    for name, key, guard, n in [
        ("fl_g_write_sens_3", "user", 'f.sensitive == true && f.action == "write"', 3),
        ("fl_g_read_5", "user", 'f.action == "read"', 5),
        ("fl_g_home_10", "user", 'f.file == "/home/user/secret.txt"', 10),
        ("fl_g_config_5", "user", 'f.file == "/etc/app/config.yaml"', 5),
        ("fl_g_sens_etc_2", "user", 'f.sensitive == true && f.file == "/etc/app/config.yaml"', 2),
        ("fl_dist_action_3", "user", "action", 3),
        ("fl_dist_file_5", "user", "file", 5),
    ]:
        if name.startswith("fl_dist"):
            emit(name, {"f": ("file_events", None)}, key, f"{{ f.{guard} | distinct | count >= {n}; }}", "f", "user", entity_type="user", sip_expr='"0.0.0.0"')
        else:
            emit(name, {"f": ("file_events", None)}, key, f"{{ f && {guard} | count >= {n}; }}", "f", "user", entity_type="user", sip_expr='"0.0.0.0"')


def multi_extra_rules():
    combos = [
        ("multi_conn_proxy_dip", "c", "conn_events", 'c.action == "denied"', "p", "proxy_events", 'p.status == 500', 15, 3),
        ("multi_proxy_firewall", "p", "proxy_events", 'p.status == 500', "fw", "firewall_events", 'fw.action == "deny"', 5, 5),
        ("multi_dns_firewall", "d", "dns_events", None, "fw", "firewall_events", 'fw.action == "deny"', 5, 3),
        ("multi_conn_dns_txt", "c", "conn_events", 'c.action == "denied"', "d", "dns_events", 'd.query_type == "TXT"', 20, 3),
        ("multi_proxy_dns_txt", "p", "proxy_events", 'p.status == 404', "d", "dns_events", 'd.query_type == "TXT"', 5, 2),
        ("multi_conn_proxy_deny", "c", "conn_events", 'c.action == "denied"', "p", "proxy_events", 'p.status >= 500', 25, 2),
    ]
    for name, a1, w1, f1, a2, w2, f2, n1, n2 in combos:
        ev = {a1: (w1, f1), a2: (w2, f2)}
        body = f"{{ {a1} | count >= {n1}; {a2} | count >= {n2}; }}"
        emit(name, ev, "sip", body, a1, "sip")
    RULES.append("""rule chain_deny_proxy {
    events {
        deny : conn_events && action == "denied"
        err : proxy_events && status == 500
    }
    match<sip:30m> {
        on event { deny | count >= 5; err | count >= 2; }
    } -> score(75.0)
    entity(ip, deny.sip)
    yield network_alerts (sip = deny.sip, alert_type = "chain_deny_proxy", detail = "deny then proxy-500", request_count = 1)
    limits { max_memory = "512MB"; max_instances = 100000; on_exceed = throttle; }
}""")
    RULES.append("""rule chain_deny_dns2 {
    events {
        deny : conn_events && action == "denied"
        q : dns_events && query_type == "TXT"
    }
    match<sip:30m> {
        on event { deny | count >= 8; q | count >= 2; }
    } -> score(70.0)
    entity(ip, deny.sip)
    yield network_alerts (sip = deny.sip, alert_type = "chain_deny_dns2", detail = "deny then txt dns", request_count = 1)
    limits { max_memory = "512MB"; max_instances = 100000; on_exceed = throttle; }
}""")


def main():
    count_rules()
    agg_rules()
    distinct_rules()
    accu_rules()
    guard_rules()
    close_rules()
    multi_event_rules()
    chain_rules()
    pipeline_rules()
    auth_rules()
    dns_rules()
    proxy_rules()
    firewall_rules()
    file_rules()
    conn_extra_rules()
    auth_extra_rules()
    dns_extra_rules()
    proxy_extra_rules()
    firewall_extra_rules()
    file_extra_rules()
    multi_extra_rules()

    # Pad with count rules to exactly 450 (对标 QRadar EP 451 规则规格)。
    pad = 0
    while len(RULES) < 450:
        pad += 1
        n = pad * 2 + 3
        emit(f"c_pad_{pad}", {"c": ("conn_events", None)}, "sip", f"{{ c | count >= {n}; }}", "c", "sip")

    if len(RULES) > 450:
        sys.stderr.write(f"ERROR: generated {len(RULES)} rules (>450)\n")
        sys.exit(1)

    print('use "network.wfs"')
    print()
    for r in RULES:
        print(r)
        print()
    sys.stderr.write(f"generated {len(RULES)} rules\n")


if __name__ == "__main__":
    main()
