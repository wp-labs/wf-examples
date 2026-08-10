#!/usr/bin/env python3
"""生成 20 规则综合压测输入事件（jsonl，供 wfgen send 发送）。

用法: gen_events.py <count> <mode>
  mode:
    global   — 所有事件一个 sip（全局实例，最纯吞吐路径）
    distinct — 每个事件 distinct sip（实例 map churn，最大压力）
    pool     — 固定 sip 池循环复用（实例复用，贴近真实）[默认]

数据多样性：
  - 三类事件：~75% conn_events（网络流）+ ~15% auth_events（登录）+ ~10% dns_events（DNS）
  - conn 富类型：object(conn_info, 嵌套 geo/vlan) / bool(blocked) / float(packet_rate) /
    hex(app_id) / array/chars(tags)
  - 规则触发：denied_probe/traffic_sum/accu_tracker（#18 门禁）+ 新增 14 条各覆盖一类引擎路径
  - #18 回归：conn_info object 每行 ~400B JSON，150k conn 单批内容 ~60MB；
    IPC 往返膨胀 ~7x 后修复前 >256MB 会被 conn_events 窗口（max_window_bytes=256MB）静默丢批
"""
import json, random, sys

count = int(sys.argv[1])
mode = sys.argv[2] if len(sys.argv) > 2 else "pool"
rnd = random.Random(42)
BASE_NS = 1767225600000000000  # 2026-01-01T00:00:00Z
POOL = 1000
DPORTS = [80, 443, 22, 3389, 53, 8080, 8443, 137]
PROTOS = ["tcp", "udp", "icmp", "sctp"]
USERS = 200
QTYPES = ["A", "AAAA", "TXT", "CNAME"]


def sip(i):
    if mode == "global":
        return "10.0.0.1"
    if mode == "distinct":
        # 100000 唯一 sip，循环复用（对齐规则 max_instances=100000/规则封顶）。
        # 200000 事件 → 每个 sip 出现 ~2 次，实例数正好 100k/规则、无 throttle 截断，
        # 压测的是完整的高基数实例集，而非被封顶截断的子集。
        j = i % 100000
        return f"10.{(j >> 16) & 255}.{(j >> 8) & 255}.{j & 255}"
    return f"10.0.{i % (POOL >> 8)}.{i % 250 + 1}"


def dip(i):
    return f"192.168.{i % 40}.{(i // 40) % 250 + 1}"


def distinct_sip(j):
    return f"10.{(j >> 16) & 255}.{(j >> 8) & 255}.{j & 255}"


nc = 0  # conn 事件计数（distinct 模式给 conn 独立 ~100k 唯一 sip）
for i in range(count):
    t = BASE_NS + i * 1000 + rnd.randint(0, 400)  # 1µs 基准 + 抖动
    r = i % 20
    if r >= 18:  # 10% dns_events
        ev = {
            "_stream": "dns_events",
            "_timestamp": "2026-01-01T00:00:00.000Z",
            "_window": "dns_events",
            "sip": sip(i),
            "domain": f"dom{i % 200}.example.com",
            "query_type": rnd.choice(QTYPES),
            # 30% 大响应（触发 dns_avg_tunnel avg>=300）
            "resp_size": rnd.randint(1000, 5000) if rnd.random() < 0.30 else rnd.randint(50, 500),
            "event_time": t,
        }
    elif r >= 15:  # 15% auth_events
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
    else:  # 75% conn_events
        conn_sip = distinct_sip(nc % 100000) if mode == "distinct" else sip(i)
        nc += 1
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
            "sip": conn_sip,
            "conn_info": conn_info,
            # 富数据类型
            "blocked": rnd.random() < 0.25,
            "packet_rate": round(rnd.uniform(100.0, 20000.0), 1),
            "app_id": "0a0001" if rnd.random() < 0.40 else rnd.choice(["0b0002", "0c0003"]),
            "tags": ["prod", "edge", "dmz"],
        }
    print(json.dumps(ev))
