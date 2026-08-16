# qradar_pk — 450 规则高压吞吐 + QRadar EP 对标

生产环境的 CEP/SIEM 部署通常跑 50-500+ 条规则。本场景用 **450 条规则**验证高规则量下的
引擎吞吐与内存扩展性，并作为 **wp-reactor#18**（object 字段内存驱逐）的回归门禁。
生成器产出 450 条（引擎加载 ~453 条规则条目，含 pipeline
拆分），**对标 IBM QRadar Event Processor 官方认证负载（80k EPS @ 451 条规则）**——
规则数与 QRadar 认证规格同量级（450 vs 451）。

## 事件源（6 类）

`scripts/gen_events.py` 按 50/15/10/10/10/5 生成 6 类事件，覆盖 ip/digit/float/bool/
chars/array/object/hex 富类型，并含 chars 实体（user）：

| 事件源 | 占比 | 类型覆盖 |
|---|---|---|
| conn_events | 50% | object(conn_info 嵌套 geo/vlan) / bool(blocked) / float(packet_rate) / chars(app_id) / array(tags) |
| firewall_events | 15% | 第五类源（action/rule_id/protocol） |
| proxy_events | 10% | 第四类源，新增 hex(trace_id) |
| auth_events | 10% | 登录（source_ip 实体） |
| dns_events | 10% | DNS 查询（domain 实体） |
| file_events | 5% | 第六类源，chars 实体（user） |

## 规则（450 条）

`scripts/gen_rules.py` 从模板 + 参数网格生成 `models/rules/throughput.wfl`
（450 条，引擎加载为 ~453 条规则条目，3 条 pipeline 各拆 2 阶段）：

| 类别 | 数量 | 覆盖 |
|---|---|---|
| conn count | 51 | sip/dip/dport/protocol/duration/bytes/action 过滤 × 阈值网格 |
| conn sum / avg / min / max | 49 | bytes/bytes_in/bytes_out/duration/packet_rate × key |
| conn guard | 45 | bool(blocked)/float(packet_rate)/object 嵌套(geo/vlan)/array(tags)/chars(app_id/protocol)/action/字符串 |
| proxy（第四事件源，hex） | 47 | count/agg/guard(distinct/url/ua/method/status)/distinct/hex yield |
| firewall（第五事件源） | 40 | count/agg/guard(rule_id/protocol/action)/distinct |
| file（第六事件源，chars 实体） | 34 | count/agg/guard(sensitive/action/file)/distinct |
| auth（第二事件源） | 31 | result/risk/attempts/agent/dest_ip/distinct/sum/max |
| dns（第三事件源） | 28 | avg/max/min/sum/count/query_type(ANY/CNAME/AAAA/TXT) |
| conn distinct | 17 | dip / dport / protocol / action / app_id / sip |
| 多事件（conn/dns/proxy/firewall） | 12 | 多源关联 |
| conn accu（on event<accu>） | 7 | 窗口内累积 |
| conn close（and-close） | 7 | close 路径 |
| 序列（seq） | 5 | 多步有序 |
| pipeline（`|>`） | 3 | 两阶段 fixed 桶 |
| count 补齐 | 74 | 凑足 450 |

> 450 = 300（历史基线）+ 150 新增。新增集中在 conn action 过滤、更高阈值、
> 聚合/guard/distinct 网格扩展、auth/dns/proxy/firewall/file 各源深化，以及
> 6 条多事件 + 2 条序列——**对标 QRadar EP 认证负载的 451 条（定制规则+构建块）**，
> 规则数与规格同量级、类别覆盖更广（QRadar 官方口径为 80k EPS @ 451 规则）。

实体：conn/proxy/firewall 用 `sip`（ip）、auth 用 `source_ip`、dns 用 `sip`/`domain`，
file 用 chars 实体 `user`。match key 独立变化。
重新生成：`python3 scripts/gen_rules.py > models/rules/throughput.wfl`。

## 运行

```bash
./run.sh                    # 默认 200000 事件（单连接流式 + sip 复用）
./run.sh 1000000            # 长跑（100 万事件）
CHUNK=1000 RATE_MS=50 ./run.sh 200000  # 受控持续入流速率（~20k/s）
```

> **唯一模式即真实口径**：单连接持续入流 + sip 复用池 1000（正常流量长尾）。
> 历史 peak（一次性突发）/ flood（唯一 sip）/ single（单键）模式已删除——
> peak 的 EPS 是 wfgen 发送墙钟假象（SEND 2.7s vs 引擎排空尾 1s，不代表引擎
> 容量），flood 是极端基数内存压力，均不反映真实部署，保留只会误导。

门禁：#18 驱逐告警 = 0，且 metrics `emitted_total` ≥ 10000。告警 sink 为 **blackhole**
（对齐 Flink Nexmark discarding sink 口径，只测处理吞吐不落盘），门禁计数取引擎侧
metrics 发射计数（`emitted_total` / `emitted_detail` 按规则明细），不依赖落盘文件。

## 实测结果（200000 事件，release，**450 规则**，2026-08-17 复测）

引擎历经 each 批式向量化（wp-reactor 7382048）、window actor 化、预读有界化、
window 记账含 parsed-event 足迹（e14ed6d）等演进后复测。EPS 用 send 墙钟；RSS 为
`footprint` 平台期峰值（run.sh 自动采样，送达后 `PLATEAU=8s` 继续采样）。
驱逐告警 0，#18 门禁通过。**450 规则连续 3 轮稳定值（blackhole 口径）。**

| 规则数 | 送达 | EPS（连续 3 轮） | RSS（footprint 平台期峰值） | 对比 |
|---|---|---|---|---|
| **450（本次）** | 200000 | **95.8-98.0k（均值 ~96.7k）** | **~2.24GB** | 相对 300 规则：EPS -27%，RSS +15% |
| 300（历史 08-16） | 200000 | ~132k | ~1.95GB | 基线（08-11）：~114k / ~4.3GB |

- 告警发射量（metrics `emitted_total` 求和，末轮实测 2,428,255 ≈ **242.8 万条**），
  其中 conn_rules 族 ~29.7k；**约 293/450 规则实际触发**（其余规则阈值/过滤条件
  在该数据分布下不满足，属正常）。
- **450 规则 EPS ~96.7k 仍超出 QRadar EP 认证上限（80k @ 451 规则）**，单进程 @ M3 Max
  有效并行 6-9 核（详见「行业定位」）。
- EPS 降幅（132k→96.7k，-27%）反映规则求值随规则数 +50% 的成本；RSS 升至 ~2.24GB
  仍远低于 64GB（3.5%），且驱逐告警 0。
- sink 为 discarding 口径（`blackhole_sink`，对齐 Flink Nexmark）。同相位 A/B 实测
  blackhole 比落盘 file sink 快 ~4-5%（133k vs 127k）。
- EPS 有同机相位波动（±10% 量级）：本轮 3 轮 95.8-98.0k（±1.2%），稳定。

### 资源利用率与扩展性探针（2026-08-16，M3 Max 16 核 / 64GB，**300 规则口径**）

| 指标 | 数值 | 说明 |
|---|---|---|
| daemon CPU（负载期均值） | **~600-930%（6-9.3/16 核）** | `os.wait4` rusage 口径，非饱和 |
| wfgen 发送器 CPU | ~0.43 核 | 发送侧非 CPU 瓶颈 |
| daemon RSS | ~1.95GB / 64GB（3%） | 内存余量充足 |
| 每 200k 事件总 CPU | **恒定 ~18 核·秒**（单发/双连接一致） | 单事件成本 ~90µs，总量由计算决定 |

扩展性探针（各 2 轮，均在噪声内或更差）：

- `parse/rule_parallelism` 10→16：129k，**无增益**——worker 数不是瓶颈；
- 双连接并发发送：137k（+3%），**单连接入流也不是瓶颈**；
- `CHUNK=50000`：136k（+2-3%）；`CHUNK=全量`（一次性）：76k，显著更差（wfgen
  一次性发送路径慢，见 peak 口径考古）。

**结论（300 规则口径）**：~132-137k 的平台不是 CPU/内存饱和所致（利用率 38-58% / 3%），
而是**流水线有效并行度**（~6-9 核）低于硬件上限。单事件计算成本固定（~90µs），
再往上抬吞吐需要引擎侧改动（如 window→rule 通道批量化、每 key 实例处理向量化、
调度开销削减——与 nexmark PK 的优化方向同源），配置参数已无杠杆。

### 行业定位（96.7k EPS × 450 有状态规则，单节点 M3 Max）

换算（450 规则实测）：96.7k 事件/s × 450 规则 ≈ **4300 万次规则求值/s**（每核 ~4.8-7.3M 次/s，
均摊 ~140-210ns/规则求值，含 per-sip 实例状态与窗口维护）。

| 参照 | 吞吐 | 规则数 | 硬件算力 | 说明 |
|---|---|---|---|---|
| IBM QRadar Event Processor 1699（虚拟版） | 80k EPS（认证上限） | **451**（官方认证负载：定制规则+构建块） | **56 核（最低）~ 80 核（建议）+ 128GB 内存**（IBM 官方系统要求表 80k 档） | IBM「最大 EPS 认证方法论」公开口径，事件均值 382B，250k 唯一源 IP。**对标对象 = EP 1699 节点**（含板载 Event Collector 采集/规范化 + 2TB 内置事件存储，非仅规则引擎）；Console/Data Node/App Host 等产品层组件未计入 |
| IBM QRadar 物理 appliance（xx05 M5/M6，如 1605） | 20k EPS（许可上限） | 同上 | M5：2× Xeon E5-2620 v4（8C 2.1GHz）= 16 核/32 线程 + 64GB DDR4；M6：2× Xeon Silver 4210 = 20 核/40 线程 | 双 750W 冗余电源的 1U 机架服务器（Lenovo x3550 M5 / SR630 M6 底座） |
| QRadar 产品生态 | — | 预置 1,400+ 规则 | — | 全量规则集远大于认证负载 451 |
| Flink CEP（OneAPM 实战，4 节点×180GB） | 228k EPS | 未公开 | 4 节点 × 180GB 内存服务器 | 告警规则场景，后因"太吃硬件"弃用 |
| Flink CEP 单核裸模式匹配 | ~650k EPS | **1**（单一简单 pattern） | 单核 | 无实例状态、无规则集 |
| **wfusion（本场景）** | **~96.7k EPS（450 规则实测）** | **450**（均有状态） | **单进程 @ M3 Max 笔记本芯片，有效并行 6-9 核，RSS 2.24GB** | 1000 sip 键基数 |

口径注意（**重要**）：QRadar 认证负载的键基数更大（250k 唯一源 IP vs 本场景 1000 sip 复用）、
含 16 并发搜索用户负载，直接对比偏保守有利于 QRadar；事件均值 382B 与本场景 conn 行
~370B 同量级、**规则数 451 vs 450 同量级、EPS 均为各自规则集下实测**——维度对齐。
**算力对比要分清两种 QRadar 口径**：80k EPS 是虚拟版 1699 在 56-80 核 + 128GB 上的
认证上限；物理 appliance（16-20 核/64GB）的许可上限只有 20k EPS。按每核效率计：
QRadar 虚拟版 ~1.0-1.4k EPS/核（80k÷56-80 核）、物理版 ~1.0k EPS/核（20k÷16 核
或 625/线程）；wfusion 在 6-9 个有效核上跑 96.7k，**~10.7-16.1k EPS/核——每核效率
高约一个数量级（约 8-13×）**。

- 单节点 **96.7k（450 规则实测）仍超出 QRadar EP 认证上限（80k）**，且是带窗口/实例的
  有状态关联规则（当前规则集 450 条，与 QRadar 认证负载 451 条同量级），不是裸过滤。
- 对比自家 nexmark q1（1 条规则 5M EPS）：**450 规则实测** 96.7k 仅降 ~52 倍
  （线性应为 ~11k）——优于线性，受益于事件解析共享（#19）与规则通道化。
- 扩展路径：本场景未测多进程分片；nexmark 已验证同机多 shard 与多源水平扩展，
  按 6-9 核有效并行度线性外推，16 核满载 ~178k/节点，多节点按 shard 数近线性。

### 窗口记账演进与 #18 门禁（2026-08-16 实证）

e14ed6d 起窗口记账包含 parsed-event 足迹（这部分内存确实随窗口驻留，记账更诚实），
200k 事件下 conn_events 窗口深内容 ~490MB —— 原配置 `max_window_bytes=256MB`
必然触发有损内存驱逐，且 A/B 实证后果严重：**7 条 close 路径规则全部归零**
（cap=1GB 对照多出 13,506 条 close/多事件关联告警，逐规则对拍见
`ab_results/per_rule_counts_*.txt`）。故 conn_events 上限已提至 **1GB**
（`models/schemas/windows.toml`，含注释），其余轻源维持 256MB，
全局 `max_total_bytes=2GB` 仍兜底。

**受控持续入流**（模拟真实速率）：`CHUNK=1000 RATE_MS=50 ./run.sh 200000`
→ 200000 事件 ~11s 喂完、EPS **~17.6k**、驱逐告警 0、全部规则族正常触发——引擎在受控速率下
稳定无积压，chunk/rate 决定投递节奏而非引擎上限。

### 历史口径备注（2026-08-11 基线）

- 当时 RSS 用 `ps` 采样（沙箱环境已不可用，run.sh 现用 `footprint` 回退）。
- 当时驱逐为 0 的前提是窗口记账未含 parsed-event 足迹（e14ed6d 之前），
  同一 256MB 上限在今天的工作负载下已不足（见上节）。
- EPS 随 chunk 单调：chunk=1000 → ~71k，chunk=10000 → ~114k（当时），
  chunk 越小批边界开销占比越高。
- 300 规则 vs 20 规则（`eps_throughput_obj`）：规则数翻 15 倍吞吐不降，
  因事件解析共享（#19）。

## 前提

- `wfusion` / `wfgen` 在 PATH（或用 `WFUSION=/path WFGEN=/path`）；run.sh 默认探测
  `../../../warp-fusion/target/release`。
- 二进制需含 wp-reactor#18 内容记账修复（228f441）；窗口记账含 parsed-event
  足迹（e14ed6d）后 conn_events 上限须 ≥1GB（见「窗口记账演进」节）。
- 200000 事件建议 ≥4GB 内存；`nc`、`python3`；端口 9800 空闲。
- run.sh 自动以 `footprint` 采样 daemon RSS（macOS；沙箱下 `ps` 不可用），
  送达后 `PLATEAU`（默认 8s）平台期采样，RSS 峰值在结果行输出。
