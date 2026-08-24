# QRadar PK 画像合规核对表（Profile Compliance Checklist）

> 目的：把 `scripts/gen_rules.py` 产出的 **450 条规则** 与 **IBM QRadar EP 认证公开画像参数**
> 逐项勾对，明确标出「哪些对得上官方、哪些是我们自己编的」，让对外表述不越界。
>
> **权威出处（唯一可信源）**：IBM《QRadar Maximum EPS Certification Methodology》
> https://www.ibm.com/docs/en/SS42VS_7.5/com.ibm.qradar.doc/c_qradar_max_EPS_cert_meth.html
>
> 读这张表前先记住一句话：**450 条规则逐条与 IBM 451 条无任何对应关系，全部为自研压测规则；
> 能与官方对齐的只是「画像信封」（规则规模 / 源基数方向 / 吞吐方法论），不是规则内容。**

---

## 0. 两个必须先澄清的口径（避免对外说错）

| 对话/文档里出现的说法 | 真实官方含义 | 我们实际可比的是哪个 |
|---|---|---|
| 「50k 源」 | **Unique log sources = 50,000**（日志源/设备数） | ❌ 我们的数据模型没有「日志源」概念，不可直接比 |
| 「250k 唯一源 IP」（TEST_PLAN §2.1/§4.4） | **Unique source IP addresses = 250,000**（源 IP 数） | ✅ 我们的 ~10100 源 IP 应对标这个，方向对齐、缩量约 25× |
| 「流量混合比」 | 官方给的是 **设备类型构成比**（Windows 25% / Linux 25% / Cisco IOS 15% / ASA 10% / DHCP 5% / …）+ **coalescing 15%** | ❌ 我们的 conn/firewall/proxy/auth/dns/file = 50/15/10/10/10/5 是自编事件流分类，既不是官方设备构成比，也不是 coalescing |

> ⚠️ 以前对话把「50k 源」和「250k 源 IP」当成同一参数的两种说法，这是错的——它们是 IBM 画像里
> **两个独立参数**。对外表述时，「50k」必须带定语「日志源（log sources）」，否则会被当成源 IP 而失真。

---

## 1. 画像信封参数级核对（Profile Envelope）

> 判定图例：✅ 对得上官方 ｜ ⚠️ 方向对齐但有偏差/缩量 ｜ ❌ 自编/未建模

| # | 参数 | 官方值（IBM cert methodology） | 我们 qradar_pk 的建模 | 判定 | 说明 |
|---|---|---|---|---|---|
| 1 | Custom Rules + Building Blocks | **451** | 450 条合成 WFL 规则（gen_rules.py） | ✅ 规模对标 | 450 ≈ 451 **同量级**，但仅数量对标；逐条语义无对应 |
| 2 | Unique log sources（日志源/设备） | 50,000 | 无此概念 | ❌ 未建模 | 我们的分类是 6 类事件窗口，非「日志源」 |
| 3 | Unique source IP addresses（源 IP） | 250,000 | ~10,100（60 攻击 +40 热点 +10,000 长尾） | ⚠️ 方向对齐、缩量 | 受 200K 事件量约束，TEST_PLAN §2.1 已诚实说明 |
| 4 | Unique destination IP addresses（目的 IP） | 250,000 | 未显式约束（dip 仅作聚合 key） | ❌ 未建模 | — |
| 5 | Unique username（用户名） | 300,000 | auth_events.user 基数远未到此量级 | ❌ 未建模 | 受事件量约束 |
| 6 | Coalescing ratio（事件合并比） | 15% | 无 coalescing 机制 | ❌ 未建模 | 引擎无重复事件合并概念 |
| 7 | Average raw event size（平均原始事件大小） | 382 B | 实测 ~240–244 B/事件 | ⚠️ 偏小约 37% | 影响字节级限速换算，EPS 另算（TEST_PLAN §3.2） |
| 8 | Traffic composition（设备类型构成比） | Windows 25% / Linux 25% / Cisco IOS 15% / ASA 10% / DHCP 5% / Aruba 5% / BlueCoat 3% / McAfee 3% / 其余各 1% | conn 50% / firewall 15% / proxy 10% / auth 10% / dns 10% / file 5%（§2.4） | ❌ 自编分类法 | 官方按「日志源设备类型」；我们按「事件流类别」—— taxonomy 不同，不可声称符合官方构成比 |
| 9 | Max certified EPS（认证持续吞吐） | 80,000 EPS @ 451（host 级认证数字） | 测持续能力 ~110–150k EPS（笔记本单/多连接，每核效率对标） | ⚠️ 测法/硬件不等 | 必须说「每核效率」+「诚实边界」（TEST_PLAN §4.4），不可直接说「超过认证」 |
| 10 | Network Hierarchy（网络层级对象） | 1,000 objects | 未建模 | ❌ 未建模 | — |
| 11 | Custom properties（自定义属性提取） | 350 | 未建模（字段为 schema 内定义） | ❌ 未建模 | — |
| 12 | Indexes（索引） | 20 | 未建模 | ❌ 未建模 | — |
| 13 | Offenses（案件） | 3,000 | 仅到 alert/network_alerts，无 offense 关联层 | ❌ 未建模 | — |
| 14 | Assets（资产） | 365,000 | 未建模 | ❌ 未建模 | — |
| 15 | Reference Data（参考数据） | 11 结构 / 100,000 元素 | 未建模 | ❌ 未建模 | — |
| 16 | User load（并发搜索） | 最多 16 并发搜索 | 未建模 | ❌ 未建模 | — |

**信封级小结**：16 项官方参数中——
- ✅ 对得上：**1 项**（规则规模 450≈451）
- ⚠️ 方向对齐/缩量：**3 项**（源 IP 基数、平均事件大小、认证 EPS 对标法）
- �️ 自编或未建模：**12 项**（含全部系统配置、资产、offense、reference data、流量构成比）

---

## 2. 450 条规则内容级核对（Rule Content）

> 核心结论：**450 条规则 100% 自研，无任何一条来自 IBM 451 条规则集。**
> IBM 的 451 条（含大量 building blocks）是专有内容，官方从未公开规则体。
> 我们的 450 条是为压测引擎有状态路径而造的「阈值/类别网格」。

| 规则类别（gen_rules.py 函数） | 我们生成的内容 | 官方 451 条里有什么 | 判定 | 说明 |
|---|---|---|---|---|
| count（count_rules / c_denied_* / c_pad_*） | 按 sip/dip/dport/protocol/duration/bytes 多阈值计数 | 含同类计数类规则，但阈值/字段/实体均不同 | ❌ 自研 | 类别同名，规则内容无对应 |
| agg：sum/avg/min/max（agg_rules / *_extra_rules） | 字节/时长/包率聚合网格 | 含聚合类规则 | ❌ 自研 | 仅能力类别重叠 |
| distinct（distinct_rules / dist_*） | distinct count 网格 | 含 | ❌ 自研 | 同 |
| accu（accu_rules） | `<accu>` 累计计数 | QRadar 有 response accumulation 概念 | ❌ 自研 | 概念类似，实现/语义无对应 |
| guard（guard_rules / g_*） | bool/float/object/array/hex/字符串/数学函数守卫 | 含丰富 guard 逻辑 | ❌ 自研 | 字段名(sip/dip/geo_country/tags…)、阈值全自编 |
| close（close_rules） | count + close 双段 | QRadar 有 rule response 关闭逻辑 | ❌ 自研 | 同 |
| multi_event（multi_event_rules / multi_*） | 跨窗口多源关联（conn+dns/proxy/firewall…） | 含多条件关联规则 | ❌ 自研 | 关联模式类似，内容无对应 |
| chain（chain_*） | 序列检测（scan→brute force 等） | 含序列/关联规则 | ❌ 自研 | 语义自编 |
| pipeline（pipe_*） | 多段管道（burst→聚合） | 无直接等价 | ❌ 自研 | 引擎特有管线语法 |
| auth / dns / proxy / firewall / file 各窗口规则 | 各事件源专用阈值网格 | 各设备类型有对应规则 | ❌ 自研 | 全部合成 |
| **padding（c_pad_*）** | 不足 450 时补齐的 count 规则 | — | ❌ 自研 | 纯凑数，更无官方依据 |

**内容级小结**：450/450 = **0 条** 与官方规则体有对应关系。全部为自研压测规则。

---

## 3. 对外表述红绿灯（DO / DON'T）

### ✅ 可以这么说（有官方画像支撑）
- 「本场景使用 **450 条有状态规则**，规模对标 QRadar EP 认证 **451 条规则**的负载量级。」
- 「源 IP 基数采用重尾设计，方向对齐官方 **250,000 唯一源 IP** 画像（受事件量约束缩量）。」
- 「吞吐按 **每核效率（EPS/核）** 与官方 **80k EPS @ 451** 认证方法论做可比口径对比，并明说硬件不对等。」
- 「规则覆盖引擎全部有状态路径（count/sum/avg/min/max/distinct/accu/guard/close/多事件/序列/管道）。」

### ⚠️ 必须带定语/边界说
- 说「50k」必须写成「**50,000 日志源（log sources）**」，**不能**省略成「50k 源」让人误读为源 IP。
- 说「流量混合」必须说明是**我们自编的事件流分类**，不是官方设备构成比；不要写「符合官方流量构成」。
- 说吞吐数字必须明说「持续注入口径 / 每核效率 / 硬件不等」，不能写「超过 QRadar 认证」。

### ❌ 绝对不能说（越界）
- 「我们的 450 条规则就是 QRadar EP 认证规则 / 来自 IBM 规则集。」（错：0 对应，全自研）
- 「我们的流量构成比符合官方画像。」（错：taxonomy 不同，官方按设备类型）
- 「我们建模了 50k 源 IP。」（错：50k 是 log sources，源 IP 官方是 250k）
- 「我们复现了 QRadar EP 认证的 451 条规则语义。」（错：规则体专有，从未公开）
- 任何暗示「通过 QRadar EP 认证 / 等价于认证」的表述。（错：我们只是性能压测对标，非认证）

---

## 4. 一句话总结（对外可贴）

> wfusion 的 qradar_pk 是一个**合成压测场景**：450 条自研有状态规则对标 QRadar EP 认证
> **451 条规则规模**，数据信封方向对齐官方画像（250k 源 IP / 80k EPS 方法论），用于验证引擎在
> 真实 SIEM 量级负载下的吞吐与有界内存。**它不是、也不声称是 QRadar EP 认证规则集**——IBM 的
> 451 条规则体为专有内容，从未公开。检测语义正确性若要对外举证，应改用 **Sigma 规则 / MITRE ATT&CK**
> 这类社区公开标准逐条比对，或引用 IBM docs 中散落的公开 building block 样例（非 451 全集）。
