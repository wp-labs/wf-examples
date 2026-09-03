#!/usr/bin/env python3
"""生成 common_rules_100 的输入事件（jsonl，供 wfgen send）。

形态：正常底噪（~55%）+ 针对性攻击会话（~45%，事件时间跨度 ~30 分钟）。
攻击会话按常见 SOC 检测主题设计（爆破/扫描/外传/C2/DGA/Web 攻击/被控主机），
与 gen_rules.py 标记 fire=T 的规则触发对齐（单会话内事件量 > 阈值）。
事件时间单调（输出按 t 排序）→ 窗口随 watermark 自然老化、内存有界
（allowed_lateness=30s）。

用法: gen_events.py [count=200000] > burst.jsonl
确定性 RNG（seed=42）。
"""
import json
import os
import random
import string
import sys

count = int(sys.argv[1]) if len(sys.argv) > 1 else 200000
rnd = random.Random(42)
BASE_NS = 1767225600000000000  # 2026-01-01T00:00:00Z
SPAN_S = 30 * 60               # 事件时间跨度 30min

# ---- 确定性 IP / 主机池 ----
NORMAL_IPS = [f"10.0.{k // 250}.{k % 250 + 1}" for k in range(600)]
HOT_IPS = [f"10.1.{k % 20 + 1}.{k % 200 + 1}" for k in range(40)]
EXT_DIPS = [f"203.0.113.{k + 1}" for k in range(8)]   # 外部（公网模拟）
C2_DIPS = ["198.18.0.11", "198.18.0.22"]
DGA_DOMAINS = [f"198.18.0.{k}" for k in (101, 102)]   # dns 客户端用内部 ip 段
USERS = ["jsmith", "jdoe", "admin", "root", "svc_backup", "svc_ci", "svc_monitor"]
UAS = ["chrome", "safari", "firefox", "edge", "curl", "powershell", "sqlmap"]
WEBS = ["corp-app.example.com", "portal.example.com", "docs.example.com"]

evs = []          # (t_ns, payload_dict) —— 最后按 t 排序输出
_t_cur = BASE_NS  # 单调时钟推进


def now(step_s):
    """推进事件时间（秒，可带小抖动）并返回当前 ns。"""
    global _t_cur
    _t_cur += int(step_s * 1_000_000_000)
    return _t_cur


def ts(t):
    return t  # ns 直接作为 event_time


def conn(t, sip, dip, dport, action="allowed", bytes_in=800, bytes_out=900,
         duration=5, protocol="tcp", blocked=False, geo="US", vlan=10,
         app_id="http", sport=None, packet_rate=None, conn_extra=None):
    sport = sport if sport is not None else rnd.randint(1024, 65500)
    pr = packet_rate if packet_rate is not None else round(rnd.uniform(100.0, 20000.0), 1)
    c = {
        "_stream": "conn_events", "_timestamp": "2026-01-01T00:00:00.000Z",
        "_window": "conn_events",
        "sip": sip, "dip": dip, "dport": dport, "sport": sport,
        "bytes": bytes_in + bytes_out, "bytes_in": bytes_in, "bytes_out": bytes_out,
        "protocol": protocol, "action": action, "duration": duration,
        "packet_rate": pr,
        "blocked": blocked, "geo_country": geo, "vlan": vlan,
        "app_id": app_id, "tags": ["prod"], "event_time": ts(t),
        "conn_info": {"iface": "eth0", "vlan": vlan},
    }
    if conn_extra:
        c.update(conn_extra)
    evs.append((t, c))


def auth(t, sip, user, result="success", attempts=1, agent="chrome",
         risk=0.1, dest="10.2.0.10", geo="US"):
    evs.append((t, {
        "_stream": "auth_events", "_timestamp": "2026-01-01T00:00:00.000Z",
        "_window": "auth_events",
        "source_ip": sip, "dest_ip": dest, "user": user, "result": result,
        "attempts": attempts, "agent": agent, "risk": round(risk, 2),
        "geo_country": geo, "event_time": ts(t),
    }))


def dns(t, sip, domain, qtype="A", resp=120, answers=1):
    evs.append((t, {
        "_stream": "dns_events", "_timestamp": "2026-01-01T00:00:00.000Z",
        "_window": "dns_events",
        "sip": sip, "domain": domain, "query_type": qtype,
        "resp_size": resp, "num_answers": answers, "event_time": ts(t),
    }))


def proxy(t, sip, url, host, method="GET", status=200, ua="chrome", out=4000):
    evs.append((t, {
        "_stream": "proxy_events", "_timestamp": "2026-01-01T00:00:00.000Z",
        "_window": "proxy_events",
        "sip": sip, "url": url, "host": host, "method": method,
        "status": status, "user_agent": ua, "bytes": out,
        "action": "allow", "event_time": ts(t),
    }))


def ran_str(n, alphabet=string.ascii_lowercase + string.digits):
    return "".join(rnd.choice(alphabet) for _ in range(n))


# --------------------------------------------------------------------------
# 攻击会话（各会话给独立攻击者 IP；事件集中在一个 ~60-90s 时间窗内）
# --------------------------------------------------------------------------
def sess_scan(atk, base_s):
    """端口/服务扫描：320 conn + dns NX/普通 → scan 族 + corr。"""
    t = BASE_NS + int(base_s * 1e9)
    _t_cur_tmp = _t_cur
    # 主要目标内网主机 12 台（横向 ssh/rdp/smb + 全端口扫其中 2 台）
    inner = [f"10.2.0.{i + 1}" for i in range(12)]
    cnt = 0
    def _t():
        nonlocal t, cnt
        t += rnd.randint(15, 40) * 1_000_000     # 15-40ms 间隔
        cnt += 1
        return t
    for i in range(120):  # 全端口/随机端口扫描（含高端口段）
        dport = rnd.choice([21, 25, 53, 111, 137, 139, 445, 1433, 3306, 5432,
                            8080, 8443, 4444, 6667, 50000, 60000, 65535])
        dur = 0 if i % 2 == 0 else 1
        out = 64 if dur == 0 else rnd.randint(64, 400)
        conn(_t(), atk, rnd.choice(inner), dport, action="denied",
             bytes_in=32, bytes_out=out, duration=dur, protocol="tcp",
             blocked=(i % 3 == 0))
    for i in range(80):  # 高并发短连接（denied 洪水）
        conn(_t(), atk, rnd.choice(inner), rnd.choice([80, 443, 8080]),
             action="denied", bytes_in=32, bytes_out=48, duration=0)
    for i in range(60):  # 445/3389/3306 横向
        conn(_t(), atk, rnd.choice(inner), [445, 445, 3389, 3306][i % 4],
             action="allowed", duration=rnd.randint(1, 4))
    for i in range(40):  # ssh 横移（多内网主机）
        conn(_t(), atk, rnd.choice(inner), 22, action="denied", duration=0,
             bytes_out=52)
    for i in range(20):  # 高端口段（distinct 25+）
        conn(_t(), atk, "10.2.0.99", rnd.randint(4444, 7000), action="denied",
             duration=0, bytes_out=44)
    for i in range(8):  # dns：NX（corr scan denied + dns nx）
        dns(_t(), atk, f"scan{rnd.randint(0,999)}.probe.example", qtype="NX",
            resp=0, answers=0)
    # 1.6k 事件太多则分散多轮；单会话 ~330 conn——已在上面覆盖 dense/spread/flood。
    # 16 个不同攻击者探测同一内网 dip（internal_probe_many_dips）
    for k in range(16):
        conn(_t(), f"198.18.2.{k + 1}", "10.2.0.7", rnd.randint(1, 1024),
             action="denied", duration=0, bytes_out=44)
    return cnt


def sess_brute(atk, base_s):
    t = BASE_NS + int(base_s * 1e9)
    cnt = 0
    def _t():
        nonlocal t, cnt
        t += rnd.randint(120, 500) * 1_000_000
        cnt += 1
        return t
    for i in range(40):  # admin 连续失败（触发 user_30 / admin_acct）
        auth(_t(), atk, "admin", result="failed", attempts=rnd.randint(3, 8),
             agent=rnd.choice(["curl", "powershell", "chrome"]),
             risk=rnd.choice([0.9, 0.95, 0.99]), geo=rnd.choice(["RU", "CN", "BR"]))
    for k, u in enumerate(["root", "jsmith", "jdoe", "svc_backup", "svc_ci",
                           "svc_monitor", "qa_user", "ops_user"]):
        for i in range(6):  # 跨 8 用户（users_8）
            auth(_t(), atk, u, result="failed", attempts=rnd.randint(1, 6),
                 agent=rnd.choice(["powershell", "curl"]),
                 risk=rnd.uniform(0.6, 0.95))
    return cnt


def sess_exfil(client, base_s):
    t = BASE_NS + int(base_s * 1e9)
    cnt = 0
    def _t():
        nonlocal t, cnt
        t += rnd.randint(400, 2000) * 1_000_000
        cnt += 1
        return t
    for i in range(48):  # 大上行到 6 个外部 dip
        dip = EXT_DIPS[i % 6]
        mb = rnd.randint(5, 12) * 1024 * 1024
        conn(_t(), client, dip, 443, action="allowed", bytes_in=2000,
             bytes_out=mb, duration=rnd.randint(80, 200),
             app_id="zip-enc" if i % 4 == 0 else "https")
    for i in range(6):  # 长连接隧道（c2_tunnel_long_up / host_long_duration）
        conn(_t(), client, C2_DIPS[i % 2], 443, bytes_in=800, bytes_out=6 * 1024 * 1024,
             duration=300, app_id="https")
    return cnt


def sess_c2(client, base_s):
    t = BASE_NS + int(base_s * 1e9)
    cnt = 0
    def _t():
        nonlocal t, cnt
        t += rnd.randint(800, 3000) * 1_000_000
        cnt += 1
        return t
    for i in range(20):  # 心跳：长连接、非常见高端口
        dport = rnd.choice([6667, 50000, 55555, 60000, 4444])
        conn(_t(), client, C2_DIPS[i % 2], dport, bytes_in=400, bytes_out=900,
             duration=rnd.choice([320, 400, 700, 900]), app_id="unknown")
    return cnt


def sess_dga(bot, base_s):
    t = BASE_NS + int(base_s * 1e9)
    cnt = 0
    def _t():
        nonlocal t, cnt
        t += rnd.randint(200, 900) * 1_000_000
        cnt += 1
        return t
    for i in range(30):  # DGA：长随机域名 .top，NX/无应答
        dom = ran_str(rnd.randint(25, 40)) + ".top"
        dns(_t(), bot, dom, qtype="NX", resp=0, answers=0)
    for i in range(6):  # 大 TXT 隧道应答
        dom = ran_str(10) + ".example.net"
        dns(_t(), bot, dom, qtype="TXT", resp=rnd.randint(4200, 6000), answers=4)
    for i in range(6):  # ANY 滥用
        dns(_t(), bot, ran_str(12) + ".com", qtype="ANY", resp=900, answers=5)
    for i in range(2):  # 大答案集
        dns(_t(), bot, ran_str(8) + ".info", qtype="ANY", resp=3000, answers=25)
    for i in range(28):  # 外连流量（corr dga + conn）
        conn(_t(), bot, C2_DIPS[0], rnd.choice([80, 443]), bytes_in=300,
             bytes_out=1500, duration=30)
    return cnt


def sess_web(atk, base_s):
    t = BASE_NS + int(base_s * 1e9)
    cnt = 0
    def _t():
        nonlocal t, cnt
        t += rnd.randint(60, 300) * 1_000_000
        cnt += 1
        return t
    host = WEBS[0]
    for i in range(45):  # 登录爆破 POST
        proxy(_t(), atk, f"https://{host}/login", host, method="POST",
              status=rnd.choice([200, 401, 403]), ua=rnd.choice(["curl", "python-requests"]),
              out=2500)
    for i in range(70):  # 目录扫描 404
        proxy(_t(), atk, f"https://{host}/{ran_str(6)}.php", host, status=404,
              ua="sqlmap" if i % 2 else "curl", out=900)
    for i in range(25):  # 5xx 错误风暴
        proxy(_t(), atk, f"https://{host}/api/v1/query", host, method="POST",
              status=500, ua="curl", out=2000)
    for i in range(12):  # SQLi 特征
        proxy(_t(), atk, f"https://{host}/search?q={ran_str(4)}%27union%27select", host,
              status=200, ua="sqlmap", out=1500)
    for i in range(5):   # 路径穿越
        proxy(_t(), atk, f"https://{host}/download?f=%2e%2e%2fetc%2fpasswd", host,
              status=200, ua="curl", out=800)
    for i in range(10):  # 大上传
        proxy(_t(), atk, f"https://{host}/upload", host, method="POST",
              status=201, ua="curl", out=rnd.randint(30, 55) * 1024 * 1024)
    return cnt


def sess_blocked(host, base_s):
    """内网主机被 fw 大量 block（fw_block_* / host_blocked_egress）+ 富类型杂项。"""
    t = BASE_NS + int(base_s * 1e9)
    cnt = 0
    def _t():
        nonlocal t, cnt
        t += rnd.randint(100, 500) * 1_000_000
        cnt += 1
        return t
    for i in range(110):  # 出网被拒（前 30 条定向同一外部 dip → fw_block_dip_focus）
        dip = EXT_DIPS[0] if i < 30 else EXT_DIPS[i % 8]
        conn(_t(), host, dip, rnd.randint(1, 65535), action="denied",
             blocked=True, duration=0, bytes_out=44)
    for i in range(24):   # 被控主机：tor/p2p app、RU geo、icmp flood、udp survey
        conn(_t(), host, EXT_DIPS[i % 4], 443, app_id=rnd.choice(["tor", "p2p"]),
             geo=rnd.choice(["RU", "CN"]), bytes_in=600, bytes_out=1200, duration=120)
    for i in range(6):
        conn(_t(), host, "8.8.8.8", 53, protocol="icmp", action="allowed",
             bytes_in=64, bytes_out=64, duration=0)
    for i in range(40):  # udp survey（distinct dip>=30）
        conn(_t(), host, f"10.9.{i // 250}.{i % 250 + 1}", 53, protocol="udp",
             action="denied", bytes_out=64)
    for i in range(5):   # 高 packet_rate
        conn(_t(), host, EXT_DIPS[0], 443, packet_rate=rnd.uniform(52000, 90000),
             bytes_in=400, bytes_out=700, duration=8)
    return cnt


# --------------------------------------------------------------------------
# 会话时间表（span 内分布；每族 1-3 轮，2m 窗口内完整含一轮）
# --------------------------------------------------------------------------
def sessions():
    atk_auth = ["198.18.1.10", "198.18.1.11"]
    atk_scan = ["198.18.1.20", "198.18.1.21", "198.18.1.22"]
    atk_web = ["198.18.1.30"]
    victim_exfil = ["10.0.10.1", "10.0.10.2"]
    victim_c2 = ["10.0.11.5"]
    bot_dga = ["10.0.12.7"]
    host_blocked = ["10.0.13.3"]
    ev = []
    for atk in atk_auth:
        ev.append(sess_brute(atk, rnd.randint(120, 1600)))
    for atk in atk_scan:
        ev.append(sess_scan(atk, rnd.randint(200, 1650)))
    ev.append(sess_web(atk_web[0], rnd.randint(300, 1500)))
    for v in victim_exfil:
        ev.append(sess_exfil(v, rnd.randint(400, 1700)))
    ev.append(sess_c2(victim_c2[0], rnd.randint(500, 1400)))
    ev.append(sess_dga(bot_dga[0], rnd.randint(600, 1500)))
    ev.append(sess_blocked(host_blocked[0], rnd.randint(700, 1550)))
    return ev


def main():
    # 先铺攻击会话（时间线早段 ~0-28min 内），再填正常底噪把总事件凑到 count。
    sess_events = sessions()          # 返回每会话事件数的列表（副作用是填充 evs）
    attack_n = sum(sess_events)
    norm_budget = max(count - attack_n, 0)

    # 正常底噪：conn 55% / dns 15% / proxy 15% / auth 15%
    normal_mix = (
        [("conn", 0.55), ("dns", 0.15), ("proxy", 0.15), ("auth", 0.15)]
    )
    t_base = BASE_NS + 1700 * 1_000_000_000  # 正常流从 28:20 起铺（与攻击重叠区避免）
    _t = [t_base]

    def nxt():
        _t[0] += rnd.randint(800, 1600) * 1_000  # 0.8-1.6ms/事件 → 200k≈4min 跨度(>窗口 2m)
        return _t[0]

    for i in range(norm_budget):
        t = nxt()
        r = rnd.random()
        if r < 0.55:
            sip = rnd.choice(HOT_IPS) if rnd.random() < 0.3 else rnd.choice(NORMAL_IPS)
            conn(t, sip, EXT_DIPS[rnd.randint(0, 7)], rnd.choice([80, 443, 53, 22, 8080]),
                 action="allowed", bytes_in=rnd.randint(64, 8192),
                 bytes_out=rnd.randint(64, 8192), duration=rnd.randint(1, 60),
                 geo=rnd.choice(["US", "US", "CN", "DE", "JP"]),
                 app_id=rnd.choice(["http", "https", "dns", "mail"]))
        elif r < 0.70:
            sip = rnd.choice(NORMAL_IPS)
            dom = ran_str(rnd.randint(6, 18)) + rnd.choice([".com", ".net", ".org"])
            dns(t, sip, dom, qtype=rnd.choice(["A", "AAAA", "A", "A"]),
                resp=rnd.randint(50, 3000), answers=rnd.randint(1, 6))
        elif r < 0.85:
            sip = rnd.choice(NORMAL_IPS)
            host = WEBS[rnd.randint(0, 2)]
            proxy(t, sip, f"https://{host}/{ran_str(8)}", host,
                  ua=rnd.choice(["chrome", "safari", "firefox"]),
                  status=rnd.choice([200, 200, 200, 301]), out=rnd.randint(500, 200000))
        else:
            sip = rnd.choice(NORMAL_IPS)
            u = rnd.choice(USERS)
            ok = rnd.random() < 0.96
            auth(t, sip, u, result="success" if ok else "failed",
                 attempts=1, agent=rnd.choice(["chrome", "edge", "safari"]),
                 risk=rnd.uniform(0.05, 0.4))
    # 正常 jsmith 成功 10 次（auth_shared_acct_success_8 由多源成功触发）
    for i in range(12):
        t = nxt()
        auth(t, rnd.choice(NORMAL_IPS), "jsmith", result="success", agent="chrome")

    evs.sort(key=lambda x: x[0])
    for _, ev in evs:
        print(json.dumps(ev))


if __name__ == "__main__":
    main()
