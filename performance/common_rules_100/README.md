# common_rules_100 — 100 条常见 SOC 检测规则 · 性能 case

在**贴近现实语义**的规则集（非阈值网格合成集）下测量引擎吞吐/内存/告警触发。
qradar_pk（376 条合成网格）回答"大规则集合压力"，本 case 回答**"100 条常见检测
规则"的现实负载**：每一条规则都有真实检测语义（爆破/扫描/横移/外传/C2/DGA/
Web 攻击/被控主机），由 `scripts/gen_rules.py` 从语义规格表生成。

## 规则构成（100 条 = 7 个语义文件）

| 文件 | 条数 | 主题（语义化命名，非网格） |
|---|---|---|
| `auth.wfl` | 15 | 认证爆破/撞库（user/源 维度 count、distinct 用户）、高危登录（risk/attempts/agent）、管理·服务账号、accu 失败集中 |
| `scan.wfl` | 15 | 端口/服务扫描（dport/dip distinct spread、conn flood、denied/syn 短连接）、横向移动（smb/rdp/ssh）、反向探测 |
| `c2_exfil.wfl` | 18 | C2（低频心跳长连接/非常见端口/持久会话）、数据外传（大上行 sum/avg、单连接大包、多目标分发、归档流量） |
| `dns.wfl` | 15 | DGA（长随机域名、NX storm、可疑 TLD）、隧道（TXT 大响应/ANY 滥用）、异常解析（大答案集/CNAME 链） |
| `proxy.wfl` | 12 | Web 攻击（登录爆破 POST、5xx/404 扫描、SQLi/路径穿越特征、恶意 UA）、大上传、单主机 4xx 集中 |
| `host_rich.wfl` | 18 | 防火墙/主机（blocked 爆发、被控主机外联、制裁地 geo、udp/icmp 探测）+ 富类型 guard（float/字符串/函数/vlan/app_id/长连接） |
| `correlate.wfl` | 7 | conn+dns 跨源关联（扫描↔异常解析、DGA↔外连、被拒↔ANY 查询等） |

语法只使用 qradar_pk 已验证形态（count / guard / distinct / sum·avg / accu /
跨源计数关联 / `startswith_any`·`indexof`·`endswith`·`length` 函数谓词）。

## 事件数据

`scripts/gen_events.py`（seed=42，确定性）：**正常底噪 ~55%（4 源混合，内部 IP
池 + 外部目标，事件时间 4min+ 跨度，allowed_lateness=30s → 窗口随 watermark
老化、内存有界）+ 针对性攻击会话 ~45%**——爆破/扫描/外传/C2/DGA/Web 攻击/被控
主机各会话在 ~60-90s 窗内发足量事件，与 `gen_rules.py` 的 `fire=T` 规则触发对齐。

## 运行

```bash
./run.sh              # 默认 200000 事件（~1.5s 引擎消化 + 平台期）
./run.sh 1000000      # 长跑
N=20000 ./run.sh      # 小规模冒烟（env N）
```

门禁：`alert.emitted_total ≥ 1000`（规则真实触发）且内存驱逐告警 = 0（#18）；
另报 EPS 与 RSS 峰值。告警 sink 为 blackhole（对齐 Nexmark discarding 口径，
计数取引擎 metrics `alert.emitted_total`，按规则 label 累计）。

## 限定 EPS 下的资源消耗统计（sweep.sh）

以目标注入速率（`max_ingest_rate` 引擎限速，qradar/nexmark 同款口径）稳态喂入规则
负载，统计该速率下的资源：CPU 核占（avg/p95/max）、RSS、allocator commit，并判
「跟上 / 达上限」（目标超过引擎可持续吞吐时实际 EPS 到顶）。每档独立 daemon +
0.1s RSS/CPU 采样（`../scripts/bench_lib.py` rss-sampler）。

**控时间而非控数据量**：每档稳态测量窗固定 `SWEEP_SECS`（默认 8s），行数 =
目标 EPS × SWEEP_SECS——各档窗口等长、采样样本数一致可比（>10w 档行数达
百万级、jsonl 大，默认 MAX_LINES=100 万拦截，`ALLOW_BIG=1` 放行）。注入 =
全速分块 send（chunk 只做传输拆分），目标 EPS 由 daemon 端限速精确平滑消费；
实际 EPS = 引擎消化段速率（send 完成 → `router.delivered_total` 累计 ≥ 行数 且
`acked_lag=0`）。

```bash
./sweep.sh                    # 默认 1w,2w,5w,10w（每档稳态 8s）
./sweep.sh 1w,2w,10w          # 指定档位（k/w/m 后缀）
./sweep.sh all                # 1w,2w,5w,10w,20w,50w
SWEEP_SECS=15 ./sweep.sh 1w,5w   # 更长的稳态窗（更稳的 avg/p95）
```

### 实测（mac mini M4 24G，release，100 条规则，每档稳态窗 8s = 行数 EPS×8）

| 档 | EPS 目标 | 实际 EPS | CPU% avg/p95/max | RSS | commit | 达成 | 档耗时 |
|---|---|---|---|---|---|---|---|
| 1w | 10000 | 10,637 | 38/115/593 | 184M | 373M | 106% ok | 12s |
| 2w | 20000 | 18,246 | 49/390/586 | 240M | 442M | 91% ok | 16s |
| 5w | 50000 | 41,950 | 87/513/722 | 332M | 561M | 84% ok | 21s |
| 10w | 100000 | 80,195 | 149/632/744 | 417M | 596M | 80% ok | 26s |
| 20w | 200000 | —（需 160 万行 > MAX_LINES=100 万，ALLOW_BIG=1 放行） |

（>10w 档需显式 `ALLOW_BIG=1`：行数=EPS×8s 达百万级，jsonl 大、gen 慢）

**达成** = 实际消化速率 ÷ 目标限速：≥80% 记 **ok（可服务）**——该 EPS 引擎能稳态
维持，资源列为服务该速率所需成本；<80% 记 **cap（能力封顶）**——目标超出引擎
能力、限速失效，实际 EPS 即引擎真实吞吐顶。解读：本负载（100 条常见规则，单
规则任务）下引擎**可持续吞吐随数据规模回落**——40 万行档实测能力 ≈13.4w，
80 万行档 ≈10w（窗口 417M 后单位成本上升）；测上限用小行数档（调小 SWEEP_SECS
或 ALLOW_BIG）。CPU avg 随档位升至 ~150%（12 核余量大 → 瓶颈是结构/单规则任务
而非 CPU 饱和）；p95/max 高于 avg 是**注入块到达瞬间**的瞬态（每块 2 万行全速到
→ 解码/窗口/emit parallel=8 瞬时并行），稳态看 avg。每档结果留档
`data/sweep_eps.txt`（含达成率与档耗时列）。

## 实测（mac mini M4 24G，200,000 事件，release，seed=42）

```
接收 200012 / 200012，引擎消化墙钟 1.51s
EPS = 132284 events/sec（引擎消化口径）  RSS_peak = 314MB
emitted_total = 303266，触发规则 70/100，内存驱逐 0
```

触发覆盖：auth 15 中 12、scan 15 中 12、c2+exfil 18 中 16、dns 15 中 10、
proxy 12 中 9、host+rich 18 中 16、correlate 7 中 3（计数类全部命中；部分
correlate/dense 类需更大会话或专门编排，属预期——见 `gen_rules.py` fire 标记）。

## 前提

- `wfusion` / `wfgen` 在 PATH（或 `WFUSION=`/`WFGEN=` 覆盖）；优先探测本地
  `../../../warp-fusion/target/release`。
- 端口 9800 空闲；`nc`、`python3`。
