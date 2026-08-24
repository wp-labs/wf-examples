#!/usr/bin/env python3
"""perf_diag_case — 规则生成（~24 条，三类成本：count/guard/distinct）。

用法: python3 gen_rules.py > ../models/rules/basic.wfl
"""

RULES = []


def emit(name, key, on_body):
    RULES.append(f"""rule {name} {{
    events {{
        c : evt_events
    }}
    match<{key}:2m> {{
        on event {on_body}
    }} -> score(50.0)
    entity(ip, c.sip)
    yield network_alerts (
        sip = c.sip,
        alert_type = "{name}",
        detail = "{name} triggered",
        request_count = 1
    )
    limits {{ max_memory = "512MB"; max_instances = 100000; on_exceed = throttle; }}
}}""")


# ---- count（无 guard，key 查找 + 计数）----
for key, t in [("sip", 5), ("sip", 10), ("sip", 20), ("sip", 50),
               ("code", 10), ("code", 50), ("code", 100)]:
    emit(f"c_{key.replace('.', '_')}_{t}", key, f"{{ c | count >= {t}; }}")

# ---- guard（bool/字符串过滤 + 计数）----
for name, key, guard, n in [
    ("g_block_5", "sip", "c.blocked == true", 5),
    ("g_block_10", "sip", "c.blocked == true", 10),
    ("g_deny_10", "sip", 'c.action == "denied"', 10),
    ("g_deny_20", "sip", 'c.action == "denied"', 20),
    ("g_bytes_10", "sip", "c.bytes >= 5000", 10),
    ("g_bytes_20", "sip", "c.bytes >= 5000", 20),
    ("g_code_5", "code", "c.code >= 500", 5),
]:
    emit(name, key, f"{{ c && {guard} | count >= {n}; }}")

# ---- distinct（去重集合维护）----
for name, key, field, n in [
    ("d_action_3", "sip", "action", 3),
    ("d_action_4", "sip", "action", 4),
    ("d_code_5", "sip", "code", 5),
    ("d_code_10", "sip", "code", 10),
    ("d_code_20", "code", "code", 20),
    ("d_bytes_10", "sip", "bytes", 10),
    ("d_bytes_20", "sip", "bytes", 20),
]:
    emit(name, key, f"{{ c.{field} | distinct | count >= {n}; }}")

print("\n".join(RULES))
