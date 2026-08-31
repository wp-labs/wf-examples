# qradar_pk — 376 规则高压吞吐 + QRadar EP 对标

> 性能结论与诚实边界见 `PK_REPORT_MAC.md`（16 核 M3 Max 口径）与 `PK_REPORT_LINUX.md`
> （Linux 8 核对等口径），2026-08-17。
> **规则成本分析（2026-08-23，当前引擎下性能回归排查）**：见 `RULE_SET_BISECTION.md`
> ——规则集二分定位瓶颈子集（conn count ~30% + guard ~16%）+ stats<> 对照反证。

生产环境的 CEP/SIEM 部署通常跑 50-500+ 条规则。本场景用 **376 条规则**验证高规则量下的
引擎吞吐与内存扩展性，并作为 **wp-reactor#18**（object 字段内存驱逐）的回归门禁。
生成器产出 376 条（引擎加载 ~379 条规则条目，含 pipeline
拆分），**对标 IBM QRadar Event Processor 官方认证负载（80k EPS @ 451 条规则）**——
规则数量级与 QRadar 认证规格同量级。

> **规则集变更（2026-08-24）：450 → 376**。`diag.sh` 家族档定位出 74 条 `c_pad_*` 补齐
> 规则（纯凑数、无 guard 的冗余 conn count，阈值与正式 `c_sip_*` 重叠）贡献 c 家族近半
> 成本，已从 `gen_rules.py` 删除——全量规则墙 31.8 → 26.0µs/事件（−18%），EPS 31.4k →
> 38.4k（+22%），语义零损失。

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

## 规则（376 条）

`scripts/gen_rules.py` 从模板 + 参数网格生成 `models/rules/throughput.wfl`
（376 条，引擎加载为 ~379 条规则条目，3 条 pipeline 各拆 2 阶段）：

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

> 376 = 300（历史基线）+ 76 新增 − **74 补齐（2026-08-24 删除）**。新增集中在 conn
> action 过滤、更高阈值、聚合/guard/distinct 网格扩展、auth/dns/proxy/firewall/file
> 各源深化，以及 6 条多事件 + 2 条序列——**对标 QRadar EP 认证负载的 451 条（定制
> 规则+构建块）**，规则数量级与规格同量级、类别覆盖更广（QRadar 官方口径为 80k EPS
> @ 451 规则）。历史 450 规则口径（含补齐）记录保留于 git 历史与下方实测表。

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

## 性能墙定位：diag.sh

`run.sh` 回答「吞吐是多少」，`diag.sh` 回答「**墙在哪一段/哪一族**」——把
`RULE_SET_BISECTION.md` 里手工做的规则集二分**机制化**（引擎内置 perf-diag 诊断模式，
单 daemon 不重启、哨兵驱动自切换）：

```bash
./diag.sh 200000                          # 三档墙梯：floor → rules → full（预热档默认开）
WARMUP=0 ./diag.sh 200000                 # 关预热（仅粗看方向时用）
FAMILIES=c,dist,close,pipe ./diag.sh 200000   # 规则家族档（哪一族贵）
./diag.sh --list-families                      # 列出 17 个可用家族前缀 + 规则数
```

- **三档墙梯**（叠加式，尾部向前切）：`floor`=注入+解码+窗口 / `rules`=+376 规则求值 /
  `full`=+输出链；每档增量 = 该段成本。
- **家族档**：按 rule 名前缀抽子集（`data/diag_rules_<fam>.wfl`），**每家族独立 daemon
  会话启动即加载**（不是多档热 reload——子集引用窗口 < 全量时 reload 必 Blocked
  [hot_reload/topology.rs：编译后 schema 集合有移除 → requires restart]，会测成全量
  墙，2026-08-24 实测确认）；各家族增量**一律相对各自 floor**，报告同时给出「每规则 ns」。
- 输出 `data/diag_<N>.txt`：每档 EPS/耗时/每事件 ns/增量成本/占全链/CPU%/RSS + 墙判定
  （主墙附基线占比；CPU 占核比 >50% = 忙墙、<15% = 等/供给墙并细分 RSS 堆积/供给侧，
  方法论 §2.4）+ 健康校验（`appended` 追平、致命计数器）。预热档只占位、不显示数字。
- **度量逻辑在共享库** `../scripts/bench_lib.py` + `../scripts/diag_analyze.py`（与
  nexmark_pk 的 bench.sh/diag.sh 共用）：run.sh / diag.sh 只做流程编排，不内嵌代码。

### 实测（2026-08-24，M3 Max 12 核，N=200k，预热档开，规则集 376）

| 档 | EPS | 每事件 ns | 增量 ns | 占比 | CPU 占用 | 判定 |
|---|---|---|---|---|---|---|
| floor（管道净段） | 3,560k | 281 | — | — | — | — |
| rules（+376 规则） | 38,389 | 26,049 | **+25,768** | **100%** | 94% | **忙墙**（计算密集） |
| full（+输出链） | 37,719 | 26,512 | +463 | ≈0 | — | 输出墙已消失（055d330 起） |

> 删 74 条 pad 前后：rules 档 31,800 → **26,049 ns/事件（−18%）**，EPS 31.4k → **38.4k（+22%）**。

家族档（增量相对各自 floor，每家族独立 daemon 启动加载；`c` 家族已为删 pad 后的 51 条）：

| 家族 | 规则数 | 增量 ns/事件 | 每规则 ns | 形态 |
|---|---|---|---|---|
| `c`（conn count） | 51 | **+3,086** | 61 | 无 guard，每事件真实计数 |
| `g`（conn guard） | 45 | **+3,870** | 86 | guard 过滤后计数 |
| `dist`（distinct） | 17 | +1,875 | 110 | 去重集合维护 |
| `avg`（conn 聚合） | 13 | +1,336 | 103 | 聚合状态 |
| `fw` / `s` / `pipe` / `pr` / `multi` | 3-47 | +600~1,100 | 17-269 | 中 |
| `auth` / `accu` / `max` / `min` / `dns` / `close` / `chain` / `fl` | 5-34 | +200~500 | 6-64 | 便宜（强 guard/低源占比/低触发率） |

> **关键结论（2026-08-24 家族档定位）**：性能关键规则是 **`c`（conn count）家族**
> （无 guard 每事件全量计数）+ **`g`（guard）**。早期家族档里的 `c` 是 125 条（含 74 条
> `c_pad_*` 凑数补齐），定位后**已删除 pad**：c 家族 8.8 → 3.1µs，全量墙 31.8 → 26.0µs。
> `chain` 并不贵（+0.23µs，强 action 绑定过滤触发率低）——早期「所有 match 家族等成本 /
> 规则数无关 / chain 每规则最贵」的结论是 reload Blocked 污染测成全量墙的假象，已废弃。

### 诊断纪律（qradar_pk 特有，违反会得出假结论）

1. **必须解除 `max_ingest_rate`**（diag.sh 默认解除，`KEEP_RATE=1` 可保留）：150k 限速会把
   三档**全部封顶在 150k**，墙梯彻底失去区分度（测量纪律 §3 的同一条）。
2. **`report_interval` 保持 1s，不要改 100ms**（与 nexmark_pk 的 diag.sh 相反）：数百条规则下
   每区间导出近万行指标（450 规则时实测 ~8.7k），改 100ms 会让 exporter 自己成为负载并**丢样本**
   ——实测 40s 跑批只导出 52 个区间（≈5.2s），`append_total` 求和只到 19.5%，健康校验误报。
   哨兵口径不依赖 metrics 粒度（这正是设计文档 §4.5 引入哨兵的理由），保持 1s 对 EPS 无影响。
3. **绝对 EPS 不可与 run.sh 比较**：`wfgen perf-diag` 是**单连接**发送（`send_payload` 只开一条
   TCP），而 run.sh 用 4 连接键闭包分片注入 → source `instances=4` 只激活 1 个。实测 `rules` 档
   38.4k（376 规则，见上表）vs run.sh 200k 口径 ~96.7k（450 规则历史实测）。**相对增量（墙归属）
   仍成立**，因为各档同为单连接。
4. **必须 `WARMUP=1`**：墙梯在单 daemon 内顺序跑，首档独自承担窗口冷分配/page fault
   （nexmark 实测偏差可达 25%，大于弱段的真实成本）。脚本在出现负增量时会报警提示。
5. **家族档每家族独立 daemon，不要用多档热 reload 切规则子集**：子集引用窗口 < 全量时
   reload 必 Blocked（`reload blocked — requires restart blockers=1`，hot_reload/topology.rs：
   编译后 schema 集合有移除）→ 实际跑全量规则而非子集，所有家族测出同一个「全量墙」
   （450 规则时 ~31k、376 规则时 ~26k EPS；早期「所有 match 家族等成本 / chain 最贵」的
   错误结论即由此而来）。diag.sh 已改为
   每家族独立 daemon 启动加载子集（`data/diag_rules_<fam>.wfl`），无需 reload。
6. **看 EPS 随规则集变化，不看 `emitted_detail`**：`emitted_detail` 是抽样指标。家族档
   可信度证据 = 各家族增量显著不同（c 51 条 +3.1µs/事件 vs chain 5 条 +0.23µs）+ `appended` 仍 100%。

## 实测结果（release，**450 规则**，2026-08-17）

引擎历经 each 批式向量化（wp-reactor 7382048）、window actor 化、预读有界化、
window 记账含 parsed-event 足迹（e14ed6d）等演进后复测。EPS 用 send 墙钟；RSS 为
`footprint` 平台期峰值（run.sh 自动采样，送达后 `PLATEAU=8s` 继续采样）。
**当前口径（1M 稳态，见下节）：EPS 150-162k / RSS ~6.7GB / 驱逐 0。**
下表 200k 记录为**旧二进制/小总量口径**（保留作演进对比），当前结论以 1M 稳态为准：

| 规则数 | 送达 | EPS | RSS（footprint 平台期峰值） | 对比 |
|---|---|---|---|---|
| 450（200k 旧口径） | 200000 | ~95.8-98.0k | ~2.24GB | 相对 300 规则：EPS -27%，RSS +15% |
| **450（1M 稳态，当前）** | **1000000** | **150-162k（三轮 150.4/156.0/162.4k）** | **~6.7GB** | 总量 ×5 消除固定开销稀释；窗口 4GB 驱逐 0 |
| 300（历史 08-16） | 200000 | ~132k | ~1.95GB | 基线（08-11）：~114k / ~4.3GB |

### nexmark 参数应用（2026-08-17 晚，当前二进制）

基于 nexmark_pk 的调参经验（content 记账预算、总量放大到稳态、窗口 cap 与数据量
匹配、批大小对比）复测：

| 配置 | EPS | RSS | eviction | 说明 |
|---|---|---|---|---|
| 基线 200k / CHUNK=10000（窗口 1GB） | 147.6k | 2.25GB | 0 ✅ | 当前二进制即高于旧记录（96.7k） |
| 1M / CHUNK=10000（窗口 1GB） | 151.7k | 5.93GB | **38 ✗** | conn 窗口 1GB 不够 → 有损驱逐（#18 门禁失败） |
| **1M / CHUNK=10000（窗口 4GB）** | **150-162k** | **~6.7GB** | **0 ✅** | **稳态口径（总量 ×5 消除固定开销稀释）** |
| 1M / CHUNK=100000（窗口 4GB） | 156.0k | 6.55GB | 0 ✅ | 大帧略降（规则求值成本主导，非批大小） |

- **EPS 150-162k（1M 稳态）**，正确性侧证：emitted 10.2-10.4M / 371-372 规则触发
  （200k 时 293 规则），驱逐 0；
- **conn_events 窗口 1GB → 4GB**：1M 长跑下窗口深内容 ~2.5GB+（200k 的 5×），
  1GB 触发 38 条有损驱逐；4GB 后驱逐 0（nexmark 经验：窗口 cap 须与数据量匹配，
  bid 窗口 15GB）——`models/schemas/windows.toml` 已更新；
- **`parse_buffer_bytes=2GB`（content 记账）、`instances=4`、p/r=10 沿用 nexmark
  调优值**；qradar 为规则计算密集（450 规则求值是瓶颈），供给参数非杠杆
  （README 旧记录：双连接仅 +3%）——单连接 1M 已测稳态；
- **CHUNK=10000 最优**（162.4 vs 156.0k @ 100000）：与 nexmark 100k 帧甜点不同，
  qradar 的批边界开销占比低、规则求值主导。

- 告警发射量（metrics `emitted_total` 求和，1M 末轮 10.2-10.4M ≈ **1000 万+ 条**），
  其中 conn_rules 族 ~120k；**约 371/450 规则实际触发**（200k 时 293 条；其余
  规则阈值/过滤条件在该数据分布下不满足，属正常）。
- **450 规则稳态 EPS 150-162k，远超 QRadar EP 认证上限（80k @ 451 规则）**，
  单进程 @ M3 Max 有效并行 6-9 核（详见「行业定位」）。
- sink 为 discarding 口径（`blackhole_sink`，对齐 Flink Nexmark）。同相位 A/B 实测
  blackhole 比落盘 file sink 快 ~4-5%（133k vs 127k，300 规则口径）。
- EPS 有同机相位波动（±8% 量级）：1M 稳态三轮 150.4 / 156.0 / 162.4k。

### 资源利用率与扩展性探针（2026-08-16，M3 Max 16 核 / 64GB，**300 规则口径**）

| 指标 | 数值 | 说明 |
|---|---|---|
| daemon CPU（负载期均值） | **~600-930%（6-9.3/16 核）** | `os.wait4` rusage 口径，非饱和 |
| wfgen 发送器 CPU | ~0.43 核 | 发送侧非 CPU 瓶颈 |
| daemon RSS | ~1.95GB / 64GB（3%） | 内存余量充足 |
| 每 200k 事件总 CPU | **恒定 ~18 核·秒**（单发/双连接一致） | 单事件成本 ~90µs，总量由计算决定 |

扩展性探针（各 2 轮，均在噪声内或更差）：

- `parse_parallelism` 10→16 / `rule_shards` 10→16：129k，**无增益**——worker 数不是瓶颈；
- 双连接并发发送：137k（+3%），**单连接入流也不是瓶颈**；
- `CHUNK=50000`：136k（+2-3%）；`CHUNK=全量`（一次性）：76k，显著更差（wfgen
  一次性发送路径慢，见 peak 口径考古）。

**结论（300 规则口径）**：~132-137k 的平台不是 CPU/内存饱和所致（利用率 38-58% / 3%），
而是**流水线有效并行度**（~6-9 核）低于硬件上限。单事件计算成本固定（~90µs），
再往上抬吞吐需要引擎侧改动（如 window→rule 通道批量化、每 key 实例处理向量化、
调度开销削减——与 nexmark PK 的优化方向同源），配置参数已无杠杆。

### 行业定位（150-162k EPS × 450 有状态规则，单节点 M3 Max）

换算（450 规则，1M 稳态）：~156k 事件/s × 450 规则 ≈ **7000 万次规则求值/s**
（每核 ~17-27M 次/s，均摊 ~37-60ns/规则求值，含 per-sip 实例状态与窗口维护）。

| 参照 | 吞吐 | 规则数 | 硬件算力 | 说明 |
|---|---|---|---|---|
| IBM QRadar Event Processor 1699（虚拟版） | 80k EPS（认证上限） | **451**（官方认证负载：定制规则+构建块） | **56 核（最低）~ 80 核（建议）+ 128GB 内存**（IBM 官方系统要求表 80k 档） | IBM「最大 EPS 认证方法论」公开口径，事件均值 382B，250k 唯一源 IP。**对标对象 = EP 1699 节点**（含板载 Event Collector 采集/规范化 + 2TB 内置事件存储，非仅规则引擎）；Console/Data Node/App Host 等产品层组件未计入 |
| IBM QRadar 物理 appliance（xx05 M5/M6，如 1605） | 20k EPS（许可上限） | 同上 | M5：2× Xeon E5-2620 v4（8C 2.1GHz）= 16 核/32 线程 + 64GB DDR4；M6：2× Xeon Silver 4210 = 20 核/40 线程 | 双 750W 冗余电源的 1U 机架服务器（Lenovo x3550 M5 / SR630 M6 底座） |
| QRadar 产品生态 | — | 预置 1,400+ 规则 | — | 全量规则集远大于认证负载 451 |
| Flink CEP 单核裸模式匹配 | ~650k EPS | **1**（单一简单 pattern） | 单核 | 无实例状态、无规则集 |
| **wfusion（本场景）** | **~150-162k EPS（450 规则，1M 稳态）** | **450**（均有状态） | **单进程 @ M3 Max 笔记本芯片，有效并行 6-9 核，RSS ~6.7GB** | 1000 sip 键基数 |

口径注意（**重要**）：QRadar 认证负载的键基数更大（250k 唯一源 IP vs 本场景 1000 sip 复用）、
含 16 并发搜索用户负载，直接对比偏保守有利于 QRadar；事件均值 382B 与本场景 conn 行
~370B 同量级、**规则数 451 vs 450 同量级、EPS 均为各自规则集下实测**——维度对齐。
**算力对比要分清两种 QRadar 口径**：80k EPS 是虚拟版 1699 在 56-80 核 + 128GB 上的
认证上限；物理 appliance（16-20 核/64GB）的许可上限只有 20k EPS。按每核效率计：
QRadar 虚拟版 ~1.0-1.4k EPS/核（80k÷56-80 核）、物理版 ~1.0k EPS/核（20k÷16 核
或 625/线程）；wfusion 在 6-9 个有效核上跑 150-162k，**~17-27k EPS/核——每核效率
高约一个数量级（约 12-27×）**。

- 单节点 **150-162k（450 规则，1M 稳态）仍远超 QRadar EP 认证上限（80k）**，且是带
  窗口/实例的有状态关联规则（当前规则集 450 条，与 QRadar 认证负载 451 条同量级），
  不是裸过滤。
- 对比自家 nexmark q1（1 条规则 7.5M EPS，P0-② 后）：**450 规则实测** ~156k 降 ~48 倍
  （线性应为 ~17k）——优于线性，受益于事件解析共享（#19）与规则通道化。
- 扩展路径：本场景未测多进程分片；nexmark 已验证同机多 shard 与多源水平扩展，
  按 6-9 核有效并行度线性外推，16 核满载 ~300k/节点，多节点按 shard 数近线性。

### 窗口记账演进与 #18 门禁（2026-08-16 实证 + 2026-08-17 长跑修正）

e14ed6d 起窗口记账包含 parsed-event 足迹（这部分内存确实随窗口驻留，记账更诚实），
200k 事件下 conn_events 窗口深内容 ~490MB —— 原配置 `max_window_bytes=256MB`
必然触发有损内存驱逐，且 A/B 实证后果严重：**7 条 close 路径规则全部归零**
（cap=1GB 对照多出 13,506 条 close/多事件关联告警，逐规则对拍见
`ab_results/per_rule_counts_*.txt`）。故 conn_events 上限先提至 **1GB**
（`models/schemas/windows.toml`，含注释），其余轻源维持 256MB，
全局 `max_total_bytes=2GB` 仍兜底。

**2026-08-17 长跑修正（1M 稳态口径）**：1M 事件下 conn 窗口深内容 ~2.5GB+
（200k 的 5×），1GB 上限触发 38 条有损驱逐（close 路径规则受影响）——
conn_events 上限提至 **4GB**、全局 `max_total_bytes` 提至 **8GB**
（nexmark 经验：窗口 cap 须与数据量匹配）。200k 场景维持 1GB 即可。

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

- `wfusion` / `wfgen` 在 PATH（或用 `WFUSION=/path WFGEN=/path` 覆盖）；run.sh 优先探测
  本地 `../../../warp-fusion/target/$PROFILE`（`PROFILE=release|debug`），无本地构建则回退 PATH。
- 二进制需含 wp-reactor#18 内容记账修复（228f441）；窗口记账含 parsed-event
  足迹（e14ed6d）后 conn_events 上限须 ≥1GB（见「窗口记账演进」节）。
- 200000 事件建议 ≥4GB 内存、**1M 长跑建议 ≥8GB**（conn 窗口 4GB + 规则实例/输出）；
  `nc`、`python3`；端口 9800 空闲。
- run.sh 自动采样 daemon RSS：macOS `footprint` → Linux `/proc/<pid>/status` VmRSS → `ps`
  兜底；送达后 `PLATEAU`（默认 8s）平台期采样，RSS 峰值在结果行输出（Linux 口径为 VmRSS，
  与 macOS footprint phys_footprint 略异）。
