#!/usr/bin/env python3
"""PK-Flink 专用：~250 条 CEP 风格（seq/close/multi/pipe/guard 为主）高压规则。

对标锚点：PatternStudio（Flink 运行时 CEP，单节点，250 条 pattern，~47.9k EPS）。
本 case 只测引擎在**模式密集**规则下的吞吐，不做 #18 门禁。

模式分布（250 条，pattern 占 ~86%）：
  seq 2 步 96 + seq 3 步 24（状态排序，CEP 最典型）
  close（and-close）40
  multi 多事件关联 36（join，状态最大）
  pipeline 10（两阶段）
  guard + count 44（bool/float/object/字符串 过滤计数）

数据复用 eps_throughput_rules100 的 6 类事件源（conn/firewall/proxy/auth/dns/file），
事件由 scripts/gen_events.py 生成。match key 独立变化（sip/dip），实体统一 ip。
用法: python3 gen_rules.py > ../models/rules/pk.wfl
"""

RULES = []


def emit(name, events, match_spec, on_body, entity_alias, entity_field,
         entity_type="ip", sip_expr=None):
    """events: alias -> (window, filter_expr or None)."""
    ev_decls = []
    for alias, (win, flt) in events.items():
        ev_decls.append(f"        {alias} : {win}" + (f" && {flt}" if flt else ""))
    if sip_expr is None:
        sip_expr = f"{entity_alias}.{entity_field}"
    RULES.append(f"""rule {name} {{
    events {{
{chr(10).join(ev_decls)}
    }}
    match<{match_spec}> {{
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


def seq_rules():
    # 2 步序列：conn 动作对 × 阈值对 × key
    pairs = [
        ("syn", "denied"), ("denied", "allowed"), ("login_fail", "allowed"),
        ("denied", "syn"), ("allowed", "denied"), ("syn", "allowed"),
        ("login_fail", "denied"), ("denied", "denied"),
    ]
    thresholds = [(5, 3), (10, 5), (20, 8), (30, 10), (50, 15), (100, 30)]
    for key in ["sip", "dip"]:
        for i, (a, b) in enumerate(pairs):
            for j, (ta, tb) in enumerate(thresholds):
                name = f"seq2_{key}_{i}_{j}"
                RULES.append(f"""rule {name} {{
    events {{
        s1 : conn_events && action == "{a}"
        s2 : conn_events && action == "{b}"
    }}
    match<{key}:5m> {{
        on event {{ s1 | count >= {ta}; s2 | count >= {tb}; }}
    }} -> score(85.0)
    entity(ip, s1.sip)
    yield network_alerts (sip = s1.sip, alert_type = "{name}", detail = "{a} then {b}", request_count = 1)
    limits {{ max_memory = "512MB"; max_instances = 100000; on_exceed = throttle; }}
}}""")
    # 3 步序列
    triples = [
        ("syn", "denied", "allowed"), ("login_fail", "denied", "allowed"),
        ("denied", "syn", "denied"), ("allowed", "denied", "allowed"),
    ]
    for key in ["sip", "dip"]:
        for i, (a, b, c) in enumerate(triples):
            for j, (ta, tb, tc) in enumerate([(5, 3, 2), (10, 5, 3), (20, 8, 5)]):
                name = f"seq3_{key}_{i}_{j}"
                RULES.append(f"""rule {name} {{
    events {{
        s1 : conn_events && action == "{a}"
        s2 : conn_events && action == "{b}"
        s3 : conn_events && action == "{c}"
    }}
    match<{key}:5m> {{
        on event {{ s1 | count >= {ta}; s2 | count >= {tb}; s3 | count >= {tc}; }}
    }} -> score(90.0)
    entity(ip, s1.sip)
    yield network_alerts (sip = s1.sip, alert_type = "{name}", detail = "{a}/{b}/{c}", request_count = 1)
    limits {{ max_memory = "512MB"; max_instances = 100000; on_exceed = throttle; }}
}}""")


def close_rules():
    specs = [
        ("conn_events", 'c.action == "denied"', "c"),
        ("conn_events", 'c.action == "allowed"', "c"),
        ("conn_events", "c.blocked == true", "c"),
        ("proxy_events", 'p.status == 500', "p"),
        ("firewall_events", 'fw.action == "deny"', "fw"),
    ]
    thresholds = [3, 5, 10, 20, 40, 80, 150, 300]
    for i, (win, flt, alias) in enumerate(specs):
        for j, n in enumerate(thresholds):
            # proxy_events 无 dip 字段，只能按 sip 分 key
            if win == "proxy_events":
                key = "sip"
            else:
                key = "sip" if (i + j) % 2 == 0 else "dip"
            name = f"close_{i}_{j}"
            emit(name, {alias: (win, flt)}, f"{key}:2m",
                 f"{{ {alias} | count >= 1; }} and close {{ total: {alias} | count >= {n}; }}",
                 alias, "sip")


def multi_rules():
    combos = [
        ("m_conn_dns", "c", "conn_events", 'c.action == "denied"', "d", "dns_events", None),
        ("m_conn_proxy", "c", "conn_events", 'c.action == "denied"', "p", "proxy_events", 'p.status == 500'),
        ("m_conn_fw", "c", "conn_events", 'c.action == "denied"', "fw", "firewall_events", 'fw.action == "deny"'),
        ("m_proxy_dns", "p", "proxy_events", 'p.status == 500', "d", "dns_events", None),
        ("m_fw_dns", "fw", "firewall_events", 'fw.action == "deny"', "d", "dns_events", None),
        ("m_proxy_fw", "p", "proxy_events", 'p.status == 500', "fw", "firewall_events", 'fw.action == "deny"'),
    ]
    thresholds = [(5, 2), (10, 3), (20, 5), (30, 8), (50, 10), (100, 20)]
    for base, a1, w1, f1, a2, w2, f2 in combos:
        for j, (n1, n2) in enumerate(thresholds):
            name = f"{base}_{j}"
            ev = {a1: (w1, f1), a2: (w2, f2)}
            body = f"{{ {a1} | count >= {n1}; {a2} | count >= {n2}; }}"
            emit(name, ev, "sip:5m", body, a1, "sip")


def pipe_rules():
    variants = [
        ("pipe_denied", "conn_events", 'x.action == "denied"', 10),
        ("pipe_allowed", "conn_events", 'x.action == "allowed"', 15),
        ("pipe_proxy500", "proxy_events", 'x.status == 500', 5),
        ("pipe_fwdeny", "firewall_events", 'x.action == "deny"', 5),
        ("pipe_blocked", "conn_events", "x.blocked == true", 8),
    ]
    for base, win, flt, close_n in variants:
        for j in range(2):
            name = f"{base}_{j}"
            RULES.append(f"""rule {name} {{
    events {{ x : {win} && {flt} }}
    match<sip:1m:fixed> {{
        on event {{ x | count >= 1; }}
        and close {{ burst: x | count >= {close_n}; }}
    }}
    |> match<sip:5m:fixed> {{
        on event {{ _in | count >= 1; }}
        and close {{ bursts: _in | count >= 2; }}
    }} -> score(60.0)
    entity(ip, _in.sip)
    yield network_alerts (sip = _in.sip, alert_type = "{name}", detail = "pipeline bursts", request_count = 2)
    limits {{ max_memory = "512MB"; max_instances = 100000; on_exceed = throttle; }}
}}""")


def guard_rules():
    specs = [
        ("c.blocked == true", "c"),
        ("c.packet_rate >= 5000.0", "c"),
        ('c.conn_info.geo.country == "CN"', "c"),
        ('indexof(c.action, "e") > 0', "c"),
        ("abs(c.bytes - 4096) < 500", "c"),
        ('c.tags[0] == "prod"', "c"),
        ('c.app_id == "0a0001"', "c"),
        ("p.risk >= 0.8", "p"),
        ('p.method == "POST"', "p"),
        ('fw.action == "deny"', "fw"),
        ('fw.protocol == "tcp"', "fw"),
    ]
    counts = [3, 5, 10, 20]
    wins = {"c": "conn_events", "p": "proxy_events", "fw": "firewall_events"}
    for i, (guard, alias) in enumerate(specs):
        for j, n in enumerate(counts):
            name = f"g_{i}_{j}"
            emit(name, {alias: (wins[alias], None)}, f"sip:2m",
                 f"{{ {alias} && {guard} | count >= {n}; }}", alias, "sip")


def main():
    seq_rules()
    close_rules()
    multi_rules()
    pipe_rules()
    guard_rules()

    print(f"// flink_pk: generated {len(RULES)} pattern-heavy rules", file=sys.stderr)
    if len(RULES) != 250:
        sys.stderr.write(f"ERROR: generated {len(RULES)} rules (expected 250)\n")
        sys.exit(1)

    print('use "network.wfs"')
    print()
    print("\n\n".join(RULES))


if __name__ == "__main__":
    import sys
    main()
