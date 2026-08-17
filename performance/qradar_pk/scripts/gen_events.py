#!/usr/bin/env python3
"""生成 450 规则综合压测输入事件（jsonl，供 wfgen send 发送）。

用法: gen_events.py <count>

数据形态：sip 复用池（1000），正常流量长尾——贴近真实部署。
（历史 single/flood 模式已删除：极端基数内存压力口径不对外，见 README。）

数据多样性：
  - 六类事件：conn 50% / firewall 15% / proxy 10% / auth 10% / dns 10% / file 5%
  - conn 富类型：object(conn_info, 嵌套 geo/vlan) / bool(blocked) / float(packet_rate) /
    chars(app_id) / array/chars(tags)
  - proxy 新增 hex(trace_id) 类型；file 用 chars 实体（user）
  - #18 回归：conn_info object 每行 ~400B JSON，100k conn 单批内容 ~40MB；
    IPC 往返膨胀 ~7x 后修复前 >256MB 会被 conn_events 窗口静默丢批
"""
import json, random, sys

count = int(sys.argv[1])
rnd = random.Random(42)
BASE_NS = 1767225600000000000  # 2026-01-01T00:00:00Z
# 事件时间步长（ns/事件）：300µs → 5m 窗口装 ~1M 事件（窗口内容 ~2.5GB，与 1M 基线一致）。
# 修复前 1µs/事件把 N 个事件压缩在 N µs 事件时间里（10M 仅 10s），远小于窗口 over_cap=5m，
# 窗口无法按时间老化、内容随 N 线性涨（10M 需 ~25GB → 驱逐风暴 → 引擎失联卡死）。
# 300µs/事件让事件时间跨度随 N 拉开到 ≥ 窗口时长，窗口按 5m 老化、内存有界
# ∝ (窗口时长 × 速率) ≈ 1M 事件 ≈ 2.5GB，任意 N 都能在固定内存上跑。
EVENT_TIME_STEP_NS = 300_000  # 300µs
POOL = 1000
DPORTS = [80, 443, 22, 3389, 53, 8080, 8443, 137]
PROTOS = ["tcp", "udp", "icmp", "sctp"]
USERS = 200
QTYPES = ["A", "AAAA", "TXT", "CNAME"]


def sip(i):
    return f"10.0.{i % (POOL >> 8)}.{i % 250 + 1}"


def dip(i):
    return f"192.168.{i % 40}.{(i // 40) % 250 + 1}"


METHODS = ["GET", "POST", "PUT", "DELETE"]
UAS = ["curl", "chrome", "python-requests", "safari"]
STATUSES = [200, 301, 404, 500]
FILES = ["/etc/app/config.yaml", "/var/log/auth.log", "/data/db.sqlite", "/home/user/secret.txt"]
for i in range(count):
    t = BASE_NS + i * EVENT_TIME_STEP_NS + rnd.randint(0, 400)  # 基准步长 + 抖动
    r = i % 20
    if r >= 19:  # 5% file_events
        ev = {
            "_stream": "file_events",
            "_timestamp": "2026-01-01T00:00:00.000Z",
            "_window": "file_events",
            "user": f"user_{i % USERS}",
            "file": FILES[i % len(FILES)],
            "action": rnd.choice(["read", "write", "delete"]),
            "size": rnd.randint(1, 100000),
            "sensitive": rnd.random() < 0.30,
            "event_time": t,
        }
    elif r >= 17:  # 10% proxy_events
        ev = {
            "_stream": "proxy_events",
            "_timestamp": "2026-01-01T00:00:00.000Z",
            "_window": "proxy_events",
            "sip": sip(i),
            "method": rnd.choice(METHODS),
            "url": f"/api/v1/resource/{i % 100}",
            "status": rnd.choice(STATUSES),
            "bytes": rnd.randint(100, 50000),
            "user_agent": rnd.choice(UAS),
            "risk": round(rnd.uniform(0.0, 1.0), 3),
            "trace_id": "0a" + f"{i % 0x100:02x}",
            "event_time": t,
        }
    elif r >= 14:  # 15% firewall_events
        ev = {
            "_stream": "firewall_events",
            "_timestamp": "2026-01-01T00:00:00.000Z",
            "_window": "firewall_events",
            "sip": sip(i),
            "dip": dip(i),
            "action": "deny" if rnd.random() < 0.40 else "allow",
            "rule_id": f"fw-{i % 50}",
            "bytes": rnd.randint(64, 8192),
            "protocol": rnd.choice(PROTOS),
            "event_time": t,
        }
    elif r >= 12:  # 10% auth_events
        ev = {
            "_stream": "auth_events",
            "_timestamp": "2026-01-01T00:00:00.000Z",
            "_window": "auth_events",
            "source_ip": sip(i),
            "user": f"user_{i % USERS}",
            "result": "failed" if rnd.random() < 0.30 else "success",
            "dest_ip": dip(i),
            "attempts": rnd.randint(1, 20),
            "agent": rnd.choice(["curl", "chrome", "python", "unknown"]),
            "risk": round(rnd.uniform(0.0, 1.0), 3),
            "event_time": t,
        }
    elif r >= 10:  # 10% dns_events
        ev = {
            "_stream": "dns_events",
            "_timestamp": "2026-01-01T00:00:00.000Z",
            "_window": "dns_events",
            "sip": sip(i),
            "domain": f"dom{i % 200}.example.com",
            "query_type": rnd.choice(QTYPES),
            # 30% 大响应（触发 dns_avg avg>=300）
            "resp_size": rnd.randint(1000, 5000) if rnd.random() < 0.30 else rnd.randint(50, 500),
            "event_time": t,
        }
    else:  # 50% conn_events
        denied = rnd.random() < 0.30
        # #18 回归：object 字段（含嵌套 geo），每行 ~400B JSON 负载。
        conn_info = {
            "iface": f"eth{i % 16}",
            "tenant": f"acme-{i % 50:03d}",
            "vlan": i % 4096,
            "flow_id": f"flow-{i:08d}-{rnd.randint(1000, 9999)}",
            "tags": ["prod", "edge", "dmz"],
            "desc": "NAT traversal session established via edge gateway with 30s keepalive",
            "geo": {
                "country": "CN",  # 全部 CN → object_nested_path 触发
                "city": "Shanghai",
                "asn": 4134,
                "lat": 31.23,
                "lon": 121.47,
            },
        }
        ev = {
            "_stream": "conn_events",
            "_timestamp": "2026-01-01T00:00:00.000Z",
            "_window": "conn_events",
            "action": "denied" if denied else "allowed",
            "bytes": rnd.randint(64, 8192),
            "bytes_in": rnd.randint(32, 4096),
            "bytes_out": rnd.randint(32, 4096),
            "dip": dip(i),
            "dport": rnd.choice(DPORTS),
            "duration": rnd.randint(1, 600),
            "event_time": t,
            "protocol": rnd.choice(PROTOS),
            "sip": sip(i),
            "conn_info": conn_info,
            # 富数据类型
            "blocked": rnd.random() < 0.25,
            "packet_rate": round(rnd.uniform(100.0, 20000.0), 1),
            "app_id": "0a0001" if rnd.random() < 0.40 else rnd.choice(["0b0002", "0c0003"]),
            "tags": ["prod", "edge", "dmz"],
        }
    print(json.dumps(ev))
