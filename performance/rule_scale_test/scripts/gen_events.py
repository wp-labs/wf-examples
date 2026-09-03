#!/usr/bin/env python3
"""生成综合压测输入事件（jsonl，供 wfgen send / dump-frames 发送）。

用法: gen_events.py <count>

数据形态：**对齐 QRadar EP 认证 + 符合现实**（2026-08-19 重设计）：
  - 源 IP 键基数**长尾（zipf）**：唯一内部 IP 10000 + 40 热点 + 30 固定攻击源，
    替代旧版 1000 均匀池——真实 SIEM 是"大量正常 IP 稀疏 + 少数攻击 IP 集中"，
    让 match 实例基数压力真实，告警呈稀疏异常而非每事件命中。
  - 事件时间**泊松到达 + 热点时段**（非均匀），跨度 ~12min（3+ 个 over=2m 窗口
    周期）→ 窗口能按事件时间自然老化、内存有界，不再靠 max_ingest_rate 硬撑。
  - **异常源集中**：conn denied/syn/login_fail、auth failed、dns 大响应绑定到
    固定攻击源——制造"少量异常源触发高频规则、大量正常源稀疏不触发"的现实形态。

确定性 RNG（seed=42），任意 count 可复现。旧版字段/事件类型覆盖全部保留。

数据多样性（不变）：
  - 六类事件：conn 50% / firewall 15% / proxy 10% / auth 10% / dns 10% / file 5%
  - conn 富类型：object(conn_info, 嵌套 geo/vlan) / bool(blocked) / float(packet_rate) /
    chars(app_id) / array/chars(tags)
  - proxy hex(trace_id)；file chars 实体(user)；#18 object 大批次日志
"""
import json, os, random, math, sys

count = int(sys.argv[1])
rnd = random.Random(42)
BASE_NS = 1767225600000000000  # 2026-01-01T00:00:00Z

# ---- 源 IP 长尾参数（对齐现实；UNIQUE_IPS 是可调的实例基数旋钮）----
SINGLE_IP = os.environ.get("GEN_SINGLE_IP", "")   # env 覆盖：非空则强制所有事件用该单 IP
FLAT = os.environ.get("GEN_FLAT", "")   # env: 非空则 object 字段扁平化(测 obj/decode 墙)
UNIQUE_IPS = 1000     # 长尾正常内部 IP（SINGLE_IP 为空时生效）
HOT_IPS = 40          # 热点正常 IP（高频、正常流量）
ATTACK_IPS = 30       # 固定异常源（denied/syn/login_fail/大dns 集中）
# 事件时间跨度 ~12min（3+ 个 over=2m 窗口周期）→ 窗口自然老化
SPAN_NS = 12 * 60 * 1_000_000_000
HOT_RATE_X = 3        # 热点时段的瞬时速率倍率（模拟忙时风潮）

DPORTS = [80, 443, 22, 3389, 53, 8080, 8443, 137]
PROTOS = ["tcp", "udp", "icmp", "sctp"]
USERS = 200
QTYPES = ["A", "AAAA", "TXT", "CNAME"]
METHODS = ["GET", "POST", "PUT", "DELETE"]
UAS = ["curl", "chrome", "python-requests", "safari"]
STATUSES = [200, 301, 404, 500]
FILES = ["/etc/app/config.yaml", "/var/log/auth.log", "/data/db.sqlite", "/home/user/secret.txt"]

# ---- 确定性 IP 池：分成 正常源池 与 异常源（攻击）池。----
# 正常源池：热点 + 长尾，几乎均匀分布让唯一基数高；异常源池：一小撮固定源，异常集中。
def _normal_pool():
    pool = []
    weights = []
    for h in range(HOT_IPS):                       # 10.1.x.y 热点正常段
        pool.append(f"10.1.{h % 32 + 1}.{h % 200 + 1}")
        weights.append(30)
    for k in range(UNIQUE_IPS):                    # 10.0.x.y 长尾正常段，弱 zipf
        pool.append(f"10.0.{k // 250}.{k % 250 + 1}")  # 两 octet 独立 → k 海唯一
        # rank 靠前略高，但保持足够多样性（唯一基数高）；s 小 → 平缓
        weights.append(max(1, int(6 / (1 + k / UNIQUE_IPS))))
    return pool, weights
NORMAL_POOL, NORMAL_W = _normal_pool()
NORMAL_TOTAL = sum(NORMAL_W)
ATTACK_POOL = [f"10.60.{(a % 8) + 1}.{a + 10}" for a in range(ATTACK_IPS)]


def pick_normal_ip():
    """从正常源池按权重选，确定性可复现。"""
    r = rnd.random() * NORMAL_TOTAL
    acc = 0
    for ip, w in zip(NORMAL_POOL, NORMAL_W):
        acc += w
        if r < acc:
            return ip
    return NORMAL_POOL[-1]


def pick_attack_ip():
    """从固定攻击源池选（异常集中于小撮源）。"""
    return rnd.choice(ATTACK_POOL)


# ---------- 事件时间：泊松到达 + 热点时段（非均匀）----------
# 先一次性生成 N 个到达间隔，让事件时间覆盖 SPAN，且含热点期（interval 缩短）。
# 热点：把时间轴切成若干忙时/闲时段，忙时到达率 × HOT_RATE_X。
def gen_event_times(n, span_ns, hot_rate_x):
    """返回长度为 n 的递增 event_time 列表（nanos，相对 0 起），泊松+热点。"""
    times = []
    base_interval = span_ns / n
    t = 0.0
    # 预生成 n 个间隔：每个间隔 = 指数分布均值 base_interval，叠加热点时缩短。
    for _ in range(n):
        # 热点判定：以周期出现（每 1/4 跨度出现一次繁忙窗口，模拟风潮）
        in_hot = (int(t / span_ns * 4)) % 3 == 0   # 每 4 段中 1 段是忙时
        mean_iv = base_interval / hot_rate_x if in_hot else base_interval
        iv = rnd.expovariate(1.0 / mean_iv)        # 指数间隔（泊松到达）
        # 钳制，避免极端长/短间隔漂移总跨度太多
        iv = max(mean_iv * 0.05, min(iv, mean_iv * 5.0))
        t += iv
        times.append(t)
    # 归一化到 BASE_NS..BASE_NS+span（保持相对形状，避免累积漂移爆跨度）
    if times:
        span = times[-1] - times[0]
        if span > 0:
            times = [BASE_NS + (x - times[0]) * (span_ns / span) for x in times]
        else:
            times = [BASE_NS + i * base_interval for i in range(n)]
    return times


EVENT_TIMES = gen_event_times(count, SPAN_NS, HOT_RATE_X)


# 每个事件按行为决定来源：异常行为（denied/syn/failed/大dns）≫ 攻击源池，
# 正常行为 ≫ 正常源池（长尾、高唯一基数）。attack_rate 是「事件为异常」的先验。
def decide_source(attack_rate):
    """返回 (ip, is_attack)。异常行为从攻击池选，正常行为从正常池选。
    SINGLE_IP 非空时强制单 IP（测纯规则路径）。"""
    if SINGLE_IP:
        # 单 IP：is_attack 仍按 attack_rate 决定（保留异常标志），IP 固定。
        return SINGLE_IP, (rnd.random() < attack_rate)
    if rnd.random() < attack_rate:
        return pick_attack_ip(), True
    return pick_normal_ip(), False


def dip():
    return f"192.168.{rnd.randint(0, 39)}.{rnd.randint(1, 250)}"


def ev_timestamp(i):
    ns = int(EVENT_TIMES[i])
    # 秒 + 纳秒 → ISO
    secs = ns // 1_000_000_000
    rem = ns % 1_000_000_000
    dt = __import__("datetime").datetime.fromtimestamp(secs, tz=__import__("datetime").timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + f".{rem//1_000_000:03d}Z"


for i in range(count):
    t = int(EVENT_TIMES[i])
    r = i % 20
    if r >= 19:  # 5% file_events（正常用户行为，实体 user）
        ev = {
            "_stream": "file_events",
            "_timestamp": ev_timestamp(i),
            "_window": "file_events",
            "user": f"user_{i % USERS}",
            "file": FILES[i % len(FILES)],
            "action": rnd.choice(["read", "write", "delete"]),
            "size": rnd.randint(1, 100000),
            "sensitive": rnd.random() < 0.30,
            "event_time": t,
        }
    elif r >= 17:  # 10% proxy_events（正常代理流量；少量异常源带敌意 UA/风险）
        ip, atk = decide_source(0.15)
        ev = {
            "_stream": "proxy_events",
            "_timestamp": ev_timestamp(i),
            "_window": "proxy_events",
            "sip": ip,
            "method": rnd.choice(METHODS),
            "url": f"/api/v1/resource/{i % 100}",
            "status": rnd.choice(STATUSES),
            "bytes": rnd.randint(100, 50000),
            "user_agent": "curl" if atk else rnd.choice(UAS),
            "risk": round(rnd.uniform(0.7, 1.0) if atk else rnd.uniform(0.0, 0.5), 3),
            "trace_id": "0a" + f"{i % 0x100:02x}",
            "event_time": t,
        }
    elif r >= 14:  # 15% firewall_events（攻击源常 deny）
        ip, atk = decide_source(0.45)
        ev = {
            "_stream": "firewall_events",
            "_timestamp": ev_timestamp(i),
            "_window": "firewall_events",
            "sip": ip,
            "dip": dip(),
            "action": "deny" if atk else rnd.choice(["allow", "allow", "allow", "deny"]),
            "rule_id": f"fw-{i % 50}",
            "bytes": rnd.randint(64, 8192),
            "protocol": rnd.choice(PROTOS),
            "event_time": t,
        }
    elif r >= 12:  # 10% auth_events（攻击源常 failed）
        ip, atk = decide_source(0.55)
        failed = atk
        ev = {
            "_stream": "auth_events",
            "_timestamp": ev_timestamp(i),
            "_window": "auth_events",
            "source_ip": ip,
            "user": f"user_{i % USERS}",
            "result": "failed" if failed else rnd.choice(["success", "success", "success", "failed"]),
            "dest_ip": dip(),
            "attempts": rnd.randint(2 if atk else 1, 20),
            "agent": rnd.choice(["curl", "python", "unknown"] if atk else ["chrome", "chrome", "unknown"]),
            "risk": round(rnd.uniform(0.7, 1.0) if atk else rnd.uniform(0.0, 0.5), 3),
            "event_time": t,
        }
    elif r >= 10:  # 10% dns_events（攻击源常大响应）
        ip, atk = decide_source(0.4)
        ev = {
            "_stream": "dns_events",
            "_timestamp": ev_timestamp(i),
            "_window": "dns_events",
            "sip": ip,
            "domain": f"dom{i % 200}.example.com",
            "query_type": rnd.choice(QTYPES),
            "resp_size": rnd.randint(1000, 5000) if atk else rnd.randint(50, 500),
            "event_time": t,
        }
    else:  # 50% conn_events
        ip, atk = decide_source(0.3)
        denied = atk or rnd.random() < 0.08
        action = "denied" if denied else "allowed"
        if atk and rnd.random() < 0.5:
            action = "syn"
        # 平坦计算字段（2026-08-23）：规则 guard 用的字段在**顶层**复制一份——
        # 嵌套路径求值每事件克隆中间对象（conn_info 7 字段 + geo 5 字段，实测
        # g_geo_30 advance 303ns vs 平坦同形 ~100ns 级，3.5×）。顶层平坦字段
        # 贴近真实 SIEM 规范化形态（LEEF/CEF 平铺 key=value）。conn_info 对象
        # 保留（#18 门禁：object 大批次内存记账）。
        vlan = i % 4096
        flow_id = f"flow-{i:08d}-{rnd.randint(1000, 9999)}"
        geo_country = "CN"
        geo_city = "Shanghai"
        if FLAT:
            conn_info = {"iface": f"eth{i % 16}", "vlan": vlan}   # 扁平化：去嵌套 geo/array/deep
        else:
            conn_info = {
                "iface": f"eth{i % 16}",
                "tenant": f"acme-{i % 50:03d}",
                "vlan": vlan,
                "flow_id": flow_id,
                "tags": ["prod", "edge", "dmz"],
                "desc": "NAT traversal session established via edge gateway with 30s keepalive",
                "geo": {
                    "country": geo_country, "city": geo_city, "asn": 4134,
                    "lat": 31.23, "lon": 121.47,
                },
            }
        ev = {
            "_stream": "conn_events",
            "_timestamp": ev_timestamp(i),
            "_window": "conn_events",
            "action": action,
            "bytes": rnd.randint(64, 8192),
            "bytes_in": rnd.randint(32, 4096),
            "bytes_out": rnd.randint(32, 4096),
            "dip": dip(),
            "dport": rnd.choice(DPORTS),
            "duration": rnd.randint(1, 600),
            "event_time": t,
            "protocol": rnd.choice(PROTOS),
            "sip": ip,
            "conn_info": conn_info,
            "blocked": denied,
            "packet_rate": round(rnd.uniform(100.0, 20000.0), 1),
            "app_id": "0a0001" if rnd.random() < 0.40 else rnd.choice(["0b0002", "0c0003"]),
            "tags": ["prod", "edge", "dmz"],
            # 平坦计算字段（规则 guard 用；避免嵌套路径中间克隆）
            "geo_country": geo_country,
            "geo_city": geo_city,
            "vlan": vlan,
            "flow_id": flow_id,
        }
    print(json.dumps(ev))
