#!/usr/bin/env python3
"""生成 EPS 压测输入事件（jsonl，供 wfgen send 发送）。

用法: gen_events.py <count> <mode>
  mode:
    global   — 所有事件一个 sip（全局实例，最纯吞吐路径）
    distinct — 每个事件 distinct sip（实例 map churn，最大压力）
    pool     — 固定 sip 池循环复用（实例复用，贴近真实）[默认]

数据多样性：
  - 两种事件类型：~75% conn_events（网络流）+ ~25% auth_events（登录）
  - 端口/协议/动作/字节数/时长随机分布；login 结果带 30% failed
  - 两类事件：conn_events + auth_events
  - 注：structured object 字段在高压 Arrow 下使规则不触发（引擎问题，待查）
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


def sip(i):
    if mode == "global":
        return "10.0.0.1"
    if mode == "distinct":
        return f"10.{(i >> 16) & 255}.{(i >> 8) & 255}.{i & 255}"
    return f"10.0.{i % (POOL >> 8)}.{i % 250 + 1}"


def dip(i):
    return f"192.168.{i % 40}.{(i // 40) % 250 + 1}"


for i in range(count):
    t = BASE_NS + i * 1000 + rnd.randint(0, 400)  # 1µs 基准 + 抖动
    if i % 4 == 3:  # 25% auth_events
        ev = {
            "_stream": "auth_events",
            "_timestamp": "2026-01-01T00:00:00.000Z",
            "_window": "auth_events",
            "source_ip": sip(i),
            "user": f"user_{i % USERS}",
            "result": "failed" if rnd.random() < 0.30 else "success",
            "dest_ip": dip(i),
            "event_time": t,
        }
    else:
        denied = rnd.random() < 0.30
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
        }
    print(json.dumps(ev))
