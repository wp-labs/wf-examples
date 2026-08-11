# eps_throughput_rules100 — 300 规则高压吞吐 + 内存扩展性

生产环境的 CEP/SIEM 部署通常跑 50-500+ 条规则。本场景用 **300 条规则**验证高规则量下的
引擎吞吐与内存扩展性，并作为 **wp-reactor#18**（object 字段内存驱逐）的回归门禁。
目录名保留历史 `rules100`，实际由生成器产出 300 条（引擎加载 303 条规则条目）。

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

## 规则（300 条）

`scripts/gen_rules.py` 从模板 + 参数网格生成 `models/rules/throughput.wfl`
（300 条，引擎加载为 303 条规则条目，3 条 pipeline 各拆 2 阶段）：

| 类别 | 数量 | 覆盖 |
|---|---|---|
| conn count | 33 | sip/dip/dport/protocol/duration/bytes × 阈值网格 |
| conn sum / avg / min / max | 34 | bytes/bytes_in/bytes_out/duration/packet_rate × key |
| conn distinct | 9 | dip / dport / protocol / action |
| conn accu（on event<accu>） | 7 | 窗口内累积 |
| conn guard | 29 | bool(blocked)/float(packet_rate)/object 嵌套(#64)/array(tags)/chars(app_id)/字符串/数学函数 |
| conn close（and-close） | 7 | close 路径 |
| auth（第二事件源） | 11 | result/risk/attempts/agent/dest_ip |
| dns（第三事件源） | 13 | avg/max/min/sum/count/query_type |
| proxy（第四事件源，hex） | 26 | count/agg/guard/distinct/hex yield |
| firewall（第五事件源） | 20 | count/agg/guard/distinct |
| file（第六事件源，chars 实体） | 19 | count/agg/guard |
| 多事件（conn+dns/proxy/firewall） | 6 | 多源关联 |
| 序列（seq） | 3 | 多步有序 |
| pipeline（`|>`） | 3 | 两阶段 fixed 桶 |
| count 补齐 | 80 | 凑足 300 |

实体：conn/proxy/firewall 用 `sip`（ip）、auth 用 `source_ip`、dns 用 `sip`/`domain`，
file 用 chars 实体 `user`。match key 独立变化。
重新生成：`python3 scripts/gen_rules.py > models/rules/throughput.wfl`。

## 运行

```bash
./run.sh                          # 默认 stream 200000 normal（单连接流式持续）
./run.sh peak 200000 flood        # 峰值突发 + 洪水压力（100k 唯一 sip）
./run.sh stream 1000000 normal    # 长跑（100 万事件）
CHUNK=1000 RATE_MS=50 ./run.sh stream 200000 normal  # 受控持续入流速率（~20k/s）
./run.sh peak 50000 normal        # 小规模快速验证
./validate.sh <wfusion> <wfgen> [N]  # RSS/告警汇总
```

> 模式命名：发送 `peak`（一次性峰值）/ `stream`（流式持续）；数据 `normal`（sip 复用，正常流量）/
> `flood`（唯一 sip，洪水压力）/ `single`（单键）。兼容旧名：burst/peak、sustain/stream、
> pool/normal、distinct/flood、global/single。

门禁：#18 驱逐告警 = 0，且 conn 规则告警 > 0（conn = 全部告警 - auth_/dns_/pr_/fw_/fl_ 前缀，
flood 模式 conn 阈值不触发为预期）。

## 实测结果（200000 事件，release，2026-08-11）

`stream`（单连接流式，`wfgen send --chunk 10000 --rate-ms 0`）**为主**——贴近真实持续入流；
`peak`（一次性突发）作对比。EPS 用 **send 墙钟**计时（避免 metrics 1s 上报拖延 elapsed）。
各次运行驱逐告警全为 0，#18 门禁通过。

### stream（单连接流式 · 主）

| 模式 | 送达 | EPS | RSS（送达后平台期峰值） |
|---|---|---|---|
| `stream` normal | 200000 | **~114k** | **~4.3GB** |
| `stream` flood | 200000 | **~83k** | **~11.5GB** |

**受控持续入流**（模拟真实速率）：`CHUNK=1000 RATE_MS=50 ./run.sh stream 200000 normal`
→ 200000 事件 ~11s 喂完、EPS **~17.6k**、驱逐告警 0、全部规则族正常触发——引擎在受控速率下
稳定无积压，chunk/rate 决定投递节奏而非引擎上限。

### peak（一次性突发 · 对比）

| 模式 | 送达 | EPS | RSS（送达后平台期峰值） |
|---|---|---|---|
| `peak` normal | 200000 | **~204k** | **~3.5GB** |
| `peak` flood | 200000 | **~194k** | **~16GB** |

- RSS 为送达后继续采样 8s 的平台期峰值：实例存活至 2m 窗口关闭，送达即杀进程会
  严重低估（早期测得 0.7GB / 14GB，实际 3.5GB / 12-19GB）。flood RSS 运行间有波动。
- stream EPS 约为 peak 的 4-6 成，来自每 chunk 一个批次的 pipeline 批边界开销：
  chunk 越小越低（chunk=1000 → ~71k，chunk=10000 → ~114k，chunk=50000 → ~132k，
  全量 → 接近 peak）。早期「20 进程分片」测得的 ~61k/~47k 是发送侧假象，已改为单连接。
- EPS 同机多跑有波动（机器负载相关），本表为代表值。
- 300 规则 vs 20 规则（`eps_throughput_obj`）：peak EPS 量级相当（~190-200k vs ~117k，
  后者为旧计时口径）——规则数翻 15 倍吞吐不降，因事件解析共享（#19）。
- flood 模式成本在**实例内存**：100000 独立 sip × per-sip 规则 = 千万级实例，RSS ~12-19GB；
  normal 模式（1000 sip 复用）实例少，RSS ~3-4GB。

### 100000 独立 sip 是什么水准

flood 模式在一个 2 分钟窗口内出现 **100000 个独立源 IP**。现实对应：

| 部署规模 | 单窗口内独立 sip |
|---|---|
| 企业内网 | 几千 |
| **大型企业 / 校园网** | **1-10 万（本场景）** |
| ISP / CDN 边缘 | 5-20 万 |
| 攻击风暴（botnet 扫描） | 可到 20 万+ |

- **normal 模式（1000 sip）贴近典型负载**（真实流量是长尾分布，多数 IP 出现一两次）。
- **flood 模式（100000 唯一 sip）代表"大型企业 / ISP 边缘"高负载**，与规则
  `max_instances=100000` 封顶对齐——每个 sip 都拿到实例，压测完整的高基数实例集。

## 前提

- `wfusion` / `wfgen` 在 PATH（或用 `WFUSION=/path WFGEN=/path`）。
- 二进制需含 wp-reactor#18 内容记账修复（228f441）。
- 200000 事件建议 ≥24GB 内存（flood 模式 ~19GB）；`nc`、`python3`；端口 9800 空闲。
