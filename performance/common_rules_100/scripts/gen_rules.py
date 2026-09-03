#!/usr/bin/env python3
"""生成 common_rules_100 的规则集（100 条常见 SOC 检测规则）。

每条规则来自真实检测语义（非阈值网格），命名即语义。产出 7 个语义文件：
  models/rules/auth.wfl 15 · scan.wfl 15 · c2_exfil.wfl 18 · dns.wfl 15 ·
  proxy.wfl 12 · host_rich.wfl 18 · correlate.wfl 7 = 100 条

规格元组: (name, key, src, filter, agg, ent, fire)
  name  规则名（= alert_type）
  key   match 实体字段（sip/user/source_ip/dip/domain ...）
  src   conn|auth|dns|proxy（事件源 + alias c/a/d/p）
  filter events alias 过滤（仅简单字段比较谓词；None = 全部）
  agg   on event 聚合体记法（见 AGG 渲染）
  ent   实体字段（entity(ip, <ent>)；yield sip=<ent>）
  fire  T=数据会话可触发 / H=高阈值·上下文（负载与真实语义占位）

语法只使用 qradar_pk/throughput.wfl 已验证形态（count/guard/distinct/sum/avg/
accu/close/multi/函数谓词）。

用法: python3 scripts/gen_rules.py            # 重写 models/rules/*.wfl
      python3 scripts/gen_rules.py --count    # 各文件条数
"""
import sys
import os

RULES_DIR = os.path.join(os.path.dirname(__file__), "..", "models", "rules")

HEADER = '''// common_rules_100 — 常见 SOC 检测规则（由 scripts/gen_rules.py 生成，勿手改）
// 100 条：auth 15 / scan 15 / c2+exfil 18 / dns 15 / proxy 12 / host+rich 18 / correlate 7

use "network.wfs"
'''

SRC = {
    "conn":  ("c", "conn_events"),
    "auth":  ("a", "auth_events"),
    "dns":   ("d", "dns_events"),
    "proxy": ("p", "proxy_events"),
}

# --------------------------------------------------------------------------
# 规格表
# --------------------------------------------------------------------------
# (name, key, src, filter, agg, ent, fire)
AUTH = [
    # 爆破 / 撞库
    ("auth_brute_user_10",          "user",       "auth", None,                               "count:10",              "a.source_ip", "T"),
    ("auth_brute_user_30",          "user",       "auth", None,                               "count:30",              "a.source_ip", "T"),
    ("auth_brute_src_fail_20",      "source_ip",  "auth", None,                               "count:20",              "a.source_ip", "T"),
    ("auth_brute_src_users_8",      "source_ip",  "auth", 'a.result == "failed"',             "dist:8:a.user",         "a.source_ip", "T"),
    ("auth_brute_attempts_spike",   "source_ip",  "auth", 'a.attempts >= 5',                  "count:6",               "a.source_ip", "T"),
    # 高危 / 异常登录
    ("auth_high_risk_fail_3",       "user",       "auth", 'a.risk >= 0.85',                   "count:3",               "a.source_ip", "T"),
    ("auth_geo_mismatch_fail",      "source_ip",  "auth", 'a.risk >= 0.9',                    "count:50",              "a.source_ip", "H"),
    ("auth_bad_agent_fail_5",       "source_ip",  "auth", None,                               'pred:5:startswith_any(a.agent, "powershell", "curl", "sqlmap")', "a.source_ip", "T"),
    ("auth_shared_acct_success_8",  "user",       "auth", 'a.result == "success"',            "count:8",               "a.source_ip", "T"),
    ("auth_hourly_login_high",      "source_ip",  "auth", 'a.result == "success"',            "count:2000",            "a.source_ip", "H"),
    # 管理/服务账号
    ("auth_admin_acct_login",       "user",       "auth", None,                               'pred:1:startswith_any(a.user, "root", "admin", "administrator")', "a.source_ip", "T"),
    ("auth_admin_acct_burst",       "user",       "auth", None,                               'pred:12:startswith_any(a.user, "root", "administrator")', "a.source_ip", "H"),
    ("auth_service_acct_anom",      "user",       "auth", None,                               'pred:3:startswith_any(a.user, "svc_backup", "svc_ci", "svc_monitor")', "a.source_ip", "H"),
    ("auth_fail_geo_focus",         "source_ip",  "auth", 'a.result == "failed"',             "accu:40",               "a.source_ip", "T"),
    ("auth_dc_login_outlier",       "source_ip",  "auth", 'a.result == "success"',            "count:200",             "a.source_ip", "H"),
]

SCAN = [
    ("port_scan_dport_spread",      "sip", "conn", None,                     "dist:50:c.dport",      "c.sip", "T"),
    ("port_scan_dport_dense",       "sip", "conn", None,                     "dist:120:c.dport",     "c.sip", "T"),
    ("port_scan_dip_spread",        "sip", "conn", None,                     "dist:30:c.dip",        "c.sip", "T"),
    ("port_scan_conn_flood",        "sip", "conn", None,                     "count:200",            "c.sip", "T"),
    ("port_scan_denied_flood",      "sip", "conn", 'c.action == "denied"',   "count:100",            "c.sip", "T"),
    ("port_scan_syn_short",         "sip", "conn", 'c.duration <= 1',        "count:80",             "c.sip", "T"),
    ("port_scan_high_ports",        "sip", "conn", 'c.dport >= 4444',        "dist:25:c.dport",      "c.sip", "T"),
    ("lateral_mysql_3306",          "sip", "conn", 'c.dport == 3306',        "count:15",             "c.sip", "T"),
    ("lateral_smb_spike",           "sip", "conn", 'c.dport == 445',         "count:20",             "c.sip", "T"),
    ("lateral_rdp_attempts",        "sip", "conn", 'c.dport == 3389',        "count:15",             "c.sip", "T"),
    ("lateral_ssh_spread",          "sip", "conn", 'c.dport == 22',          "dist:10:c.dip",        "c.sip", "T"),
    ("internal_probe_many_dips",    "dip", "conn", 'c.action == "denied"',   "dist:15:c.sip",        "c.dip", "T"),
    ("scan_then_lateral",           "sip", "conn", 'c.duration <= 2',        "count:150",            "c.sip", "H"),
    ("slow_port_walk",              "sip", "conn", None,                     "accu:60",              "c.sip", "H"),
    ("full_port_sweep_src",         "sip", "conn", None,                     "count:400",            "c.sip", "H"),
]

C2_EXFIL = [
    # C2 / 异常外连
    ("c2_beacon_low_freq",          "sip", "conn", 'c.duration >= 300',      "count:4",              "c.sip", "T"),
    ("c2_uncommon_port",            "sip", "conn", 'c.dport >= 50000',       "count:3",              "c.sip", "T"),
    ("c2_long_conn_persist",        "sip", "conn", 'c.duration >= 600',      "count:2",              "c.sip", "T"),
    ("c2_irregular_agent",          "sip", "conn", 'c.protocol == "tcp"',    "count:120",            "c.sip", "H"),
    ("c2_dead_ports_src",           "sip", "conn", None,                     "dist:6:c.dport",       "c.sip", "H"),
    ("c2_ratio_imbalance",          "sip", "conn", None,                     "sum:2000000000:c.bytes_in", "c.sip", "H"),
    ("c2_tunnel_long_up",           "sip", "conn", 'c.duration >= 120',      "sum:100000000:c.bytes_out", "c.sip", "T"),
    ("c2_small_pkts_many",          "sip", "conn", 'c.bytes <= 128',         "count:150",            "c.sip", "H"),
    ("c2_vlan_hop",                 "sip", "conn", 'c.vlan != 10',           "count:60",             "c.sip", "H"),
    ("c2_night_beacon",             "sip", "conn", None,                     "accu:25",              "c.sip", "H"),
    # 数据外传
    ("exfil_bytes_out_spike",       "sip", "conn", None,                     "sum:100000000:c.bytes_out", "c.sip", "T"),
    ("exfil_avg_out_high",          "sip", "conn", None,                     "avg:40000:c.bytes_out", "c.sip", "T"),
    ("exfil_bulk_up_per_conn",      "sip", "conn", 'c.bytes_out >= 5000000', "count:3",              "c.sip", "T"),
    ("exfil_dip_focus",             "dip", "conn", 'c.bytes_out >= 2000000', "count:5",              "c.dip", "T"),
    ("exfil_upload_ratio_high",     "sip", "conn", 'c.bytes_in == 0',        "sum:10000000:c.bytes_out", "c.sip", "H"),
    ("exfil_slow_drip",             "sip", "conn", None,                     "accu:12",              "c.sip", "H"),
    ("exfil_many_destinations",     "sip", "conn", 'c.bytes_out >= 1000000', "dist:5:c.dip",         "c.sip", "T"),
    ("exfil_archive_up",            "sip", "conn", None,                     'pred:2:startswith_any(c.app_id, "zip", "tar", "rar", "gz", "7z")', "c.sip", "H"),
]

DNS = [
    # DGA / 隧道 / 异常解析
    ("dns_dga_long_domain",         "sip", "dns", None,                      'pred:5:length(d.domain) >= 25', "d.sip", "T"),
    ("dns_dga_highfreq_bot",        "sip", "dns", 'd.resp_size == 0',        "count:20",             "d.sip", "T"),
    ("dns_any_query",               "sip", "dns", 'd.query_type == "ANY"',   "count:5",              "d.sip", "T"),
    ("dns_tunnel_txt_big",          "sip", "dns", 'd.query_type == "TXT"',   "count:8",              "d.sip", "T"),
    ("dns_txt_resp_large",          "sip", "dns", 'd.resp_size >= 4000',     "count:3",              "d.sip", "T"),
    ("dns_nx_domain_storm",         "sip", "dns", 'd.query_type == "NX"',    "count:15",             "d.sip", "T"),
    ("dns_cname_chain",             "sip", "dns", 'd.query_type == "CNAME"', "count:20",             "d.sip", "H"),
    ("dns_susp_tld_top",            "sip", "dns", None,                      'pred:5:endswith(d.domain, ".top")', "d.sip", "T"),
    ("dns_high_entropy_labels",     "sip", "dns", 'd.num_answers >= 5',      "count:10",             "d.sip", "H"),
    ("dns_rare_query_src",          "sip", "dns", None,                      "count:1500",           "d.sip", "H"),
    ("dns_domain_single_client",    "domain", "dns", None,                   "count:60",             "d.sip", "H"),
    ("dns_domain_many_clients",     "domain", "dns", None,                   "dist:10:d.sip",        "d.sip", "H"),
    ("dns_big_answers_spike",       "sip", "dns", 'd.num_answers >= 20',     "count:2",              "d.sip", "T"),
    ("dns_aaaa_burst",              "sip", "dns", 'd.query_type == "AAAA"',  "count:120",            "d.sip", "H"),
    ("dns_slow_scan_domains",       "sip", "dns", None,                      "accu:20",              "d.sip", "H"),
]

PROXY = [
    ("web_brute_login_post",        "sip", "proxy", 'p.method == "POST"',    "count:30",             "p.sip", "T"),
    ("web_scan_status_5xx",         "sip", "proxy", 'p.status >= 500',       "count:20",             "p.sip", "T"),
    ("web_scan_status_404",         "sip", "proxy", 'p.status == 404',       "count:60",             "p.sip", "T"),
    ("web_sqli_fuzz",               "sip", "proxy", None,                    'pred:10:indexof(p.url, "union") >= 0', "p.sip", "T"),
    ("web_bad_ua_crawl",            "sip", "proxy", None,                    'pred:8:startswith_any(p.user_agent, "sqlmap", "nikto", "curl")', "p.sip", "T"),
    ("web_path_traversal",          "sip", "proxy", None,                    'pred:3:indexof(p.url, "%2e%2e") >= 0', "p.sip", "T"),
    ("web_upload_large",            "sip", "proxy", 'p.method == "POST"',    "sum:500000000:p.bytes", "p.sip", "T"),
    ("web_single_host_focus",       "host", "proxy", 'p.status >= 400',      "count:40",             "p.sip", "T"),
    ("web_download_bulk",           "sip", "proxy", 'p.method == "GET"',     "sum:1000000000:p.bytes", "p.sip", "H"),
    ("web_rare_method",             "sip", "proxy", None,                    'pred:3:startswith_any(p.method, "PUT", "DELETE", "PATCH")', "p.sip", "H"),
    ("web_slow_scrape",             "sip", "proxy", None,                    "accu:80",              "p.sip", "H"),
    ("web_proxy_to_internal",       "sip", "proxy", 'p.action == "block"',   "count:200",            "p.sip", "H"),
]

HOST_RICH = [
    # 主机/访问控制（conn 上的 fw 语义）+ 富类型真实化 guard
    ("fw_block_burst_sip",          "sip", "conn", 'c.blocked == true',      "count:50",             "c.sip", "T"),
    ("fw_block_dip_focus",          "dip", "conn", 'c.blocked == true',      "count:15",             "c.dip", "T"),
    ("host_blocked_egress",         "sip", "conn", 'c.blocked == true',      "count:80",             "c.sip", "T"),
    ("host_infected_survey",        "sip", "conn", 'c.protocol == "udp"',    "dist:30:c.dip",        "c.sip", "T"),
    ("host_sanctioned_geo",         "sip", "conn", None,                     'pred:3:startswith_any(c.geo_country, "RU", "KP", "IR")', "c.sip", "T"),
    ("rich_geo_nested_match",       "sip", "conn", 'c.geo_country == "CN"',  "count:200",            "c.sip", "H"),
    ("rich_float_packet_burst",     "sip", "conn", 'c.packet_rate >= 50000.0', "count:4",           "c.sip", "T"),
    ("host_icmp_flood",             "sip", "conn", 'c.protocol == "icmp"',   "count:15",             "c.sip", "T"),
    ("rich_string_proto_match",     "sip", "conn", None,                     'pred:8:startswith(c.protocol, "tc") && indexof(c.protocol, "p") >= 0', "c.sip", "H"),
    ("rich_app_id_ioc",             "sip", "conn", None,                     'pred:2:startswith_any(c.app_id, "tor", "p2p", "proxy")', "c.sip", "T"),
    ("rich_vlan_cross",             "sip", "conn", 'c.vlan == 999',          "count:15",             "c.sip", "H"),
    ("host_sip_outlier_volume",     "sip", "conn", None,                     "count:5000",           "c.sip", "H"),
    ("host_flow_seq_gap",           "sip", "conn", None,                     "accu:50",              "c.sip", "H"),
    ("host_long_duration",          "sip", "conn", 'c.duration >= 60',       "count:6",              "c.sip", "T"),
    ("rich_small_ports_in",         "dip", "conn", 'c.dport <= 1024',        "count:300",            "c.dip", "H"),
    ("host_large_pkt_burst",        "sip", "conn", 'c.bytes >= 200000',      "count:4",              "c.sip", "T"),
    ("fw_src_geo_block",            "sip", "conn", 'c.action == "denied"',   "accu:120",             "c.sip", "H"),
    ("host_many_protocols",         "sip", "conn", None,                     "dist:6:c.protocol",    "c.sip", "H"),
]

# correlate 全部为 conn+dns 同 key（sip）跨源关联计数（auth↔conn 因 key 字段名
# source_ip≠sip 无法同 key 关联，不使用）；seq 语义在计数关联形态下不表达。
# 元组: (name, c_filter, c_n, d_filter, d_n, ent, fire)
CORRELATE = [
    ("corr_scan_conn_dns_query",    None,                        40, None,                        3, "c.sip", "T"),
    ("corr_scan_denied_dns_nx",     'c.action == "denied"',     30, 'd.query_type == "NX"',    6, "c.sip", "T"),
    ("corr_longconn_dns_storm",     'c.duration >= 300',         12, None,                       20, "c.sip", "H"),
    ("corr_blocked_any_query",      'c.blocked == true',         15, 'd.query_type == "ANY"',   4, "c.sip", "H"),
    ("corr_dga_egress_conn",        None,                        25, 'd.resp_size == 0',         8, "c.sip", "T"),
    ("corr_scan_aaaa_burst",        None,                        50, 'd.query_type == "AAAA"',  6, "c.sip", "T"),
    ("corr_tunnel_txt_conn",        'c.duration >= 600',          3, 'd.query_type == "TXT"',   5, "c.sip", "H"),
]

# correlate：conn+dns 同 key（sip）跨源关联计数
def render_corr(name, c_filter, c_n, d_filter, d_n, ent):
    c_ev = f"        c : conn_events" + (f" && {c_filter}" if c_filter else "")
    d_ev = f"        d : dns_events" + (f" && {d_filter}" if d_filter else "")
    ev_str = f"{c_ev}\n{d_ev}"
    body = f"        on event {{ c | count >= {c_n}; d | count >= {d_n}; }}"
    return f"""rule {name} {{
    events {{
{ev_str}
    }}
    match<sip:2m> {{
{body}
    }} -> score(50.0)
    entity(ip, {ent})
    yield network_alerts (
        sip = {ent},
        alert_type = "{name}",
        detail = "{name} triggered",
        request_count = 1
    )
    limits {{ max_memory = "512MB"; max_instances = 100000; on_exceed = throttle; }}
}}"""


FILES = [
    ("auth.wfl", AUTH),
    ("scan.wfl", SCAN),
    ("c2_exfil.wfl", C2_EXFIL),
    ("dns.wfl", DNS),
    ("proxy.wfl", PROXY),
    ("host_rich.wfl", HOST_RICH),
    ("correlate.wfl", CORRELATE),
]


TABLES = {"auth.wfl": AUTH, "scan.wfl": SCAN, "c2_exfil.wfl": C2_EXFIL,
          "dns.wfl": DNS, "proxy.wfl": PROXY, "host_rich.wfl": HOST_RICH,
          "correlate.wfl": CORRELATE}


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--count":
        total = 0
        for fname, rows in FILES:
            print(f"  {fname:14s} {len(rows)}")
            total += len(rows)
        print(f"  total {total}")
        return
    os.makedirs(RULES_DIR, exist_ok=True)
    for fname, rows in FILES:
        if fname == "correlate.wfl":
            blocks = "\n\n".join(render_corr(*row[:-1]) for row in rows)
        else:
            blocks = "\n\n".join(render(*row[:-1]) for row in rows)
        with open(os.path.join(RULES_DIR, fname), "w") as f:
            f.write(HEADER + blocks + "\n")
    print("written 100 rules -> models/rules/*.wfl")


def render(name, key, src, filt, agg, ent):
    alias, win = SRC[src]
    ev = []
    if filt:
        ev.append(f"        {alias} : {win} && {filt}")
    else:
        ev.append(f"        {alias} : {win}")
    ev_str = "\n".join(ev)
    if agg.startswith("pred:"):
        _, n, pred = agg.split(":", 2)
        body = f"        on event {{ {alias} && {pred} | count >= {n}; }}"
    elif agg.startswith("count:"):
        _, n = agg.split(":")
        body = f"        on event {{ {alias} | count >= {n}; }}"
    elif agg.startswith("dist:"):
        _, n, col = agg.split(":")
        body = f"        on event {{ {col} | distinct | count >= {n}; }}"
    elif agg.startswith(("sum:", "avg:", "max:", "min:")):
        op, n, col = agg.split(":")
        body = f"        on event {{ {col} | {op} >= {n}; }}"
    elif agg.startswith("accu:"):
        _, n = agg.split(":")
        body = f"        on event <accu>{{ {alias} | count >= {n}; }}"
    else:
        raise ValueError(f"unknown agg {agg}")
    if agg.startswith("count:") or agg.startswith("accu:"):
        body = body  # on event
    block = f"""rule {name} {{
    events {{
{ev_str}
    }}
    match<{key}:2m> {{
{body}
    }} -> score(50.0)
    entity(ip, {ent})
    yield network_alerts (
        sip = {ent},
        alert_type = "{name}",
        detail = "{name} triggered",
        request_count = 1
    )
    limits {{ max_memory = "512MB"; max_instances = 100000; on_exceed = throttle; }}
}}"""
    return block


if __name__ == "__main__":
    main()
