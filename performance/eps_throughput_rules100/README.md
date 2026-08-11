# eps_throughput_rules100 — 100 规则高压吞吐 + 内存扩展性

生产环境的 CEP/SIEM 部署通常跑 50-500+ 条规则。本场景用 **100 条规则**验证高规则量下的
引擎吞吐与内存扩展性，并作为 **wp-reactor#18**（object 字段内存驱逐）的回归门禁。

## 规则（100 条）

`scripts/gen_rules.py` 从模板 + 参数网格生成 `models/rules/throughput.wfl`
（100 条，引擎加载为 102 条规则条目，pipeline 两阶段拆 2 个内部规则）：

| 类别 | 数量 | 覆盖 |
|---|---|---|
| count | 18+ | sip/dip/dport/protocol × 多阈值 |
| sum / avg / min / max | 21 | bytes/bytes_in/bytes_out/duration/packet_rate × key |
| distinct | 5 | dip / dport |
| accu（on event<accu>） | 4 | 窗口内累积 |
| guard | 16 | bool(blocked)/float(packet_rate)/object 嵌套(#64)/array(tags)/chars(app_id)/字符串/数学函数 |
| auth（第二事件源） | 5 | result/risk/attempts |
| dns（第三事件源） | 5 | avg/count/query_type |
| close（and-close） | 3 | close 路径 |
| 多事件（conn+dns） | 2 | 多源关联 |
| 序列（seq） | 2 | 多步有序 |
| pipeline（`|>`） | 2 | 两阶段 fixed 桶 |
| count 补齐 | ~17 | 凑足 100 |

实体统一用 ip 字段（conn=c.sip、auth=a.source_ip、dns=d.sip），match key 独立变化。
重新生成：`python3 scripts/gen_rules.py > models/rules/throughput.wfl`。

## 运行

```bash
./run.sh                         # 默认 burst 200000 pool
./run.sh burst 200000 distinct   # 实例 churn 压力（100000 独立实例/规则 × 100 规则）
./run.sh sustain 200000 pool     # 持续吞吐
./validate.sh <wfusion> <wfgen> [N]  # RSS/告警汇总
```

门禁：#18 驱逐告警 = 0，且 conn 规则告警 > 0（按规则名前缀统计，排除 auth_/dns_）。

## 实测结果（200000 事件，release）

| 模式 | 送达 | EPS | RSS |
|---|---|---|---|
| `burst` pool | 200000 | **~110k** | **~2.5GB** |
| `burst` distinct | 200000 | **~95k** | **~11.3GB**（~900 万实例） |

- 100 规则 vs 20 规则（`eps_throughput_obj`）：EPS ~110k vs ~117k，规则数对吞吐影响很小。
- distinct 模式成本在**实例内存**：100000 独立 sip × ~90 条 conn 规则 = ~900 万实例。
  pool 模式（1000 sip）只有 ~10 万实例，RSS 2.5GB。

### 100000 独立 sip 是什么水准

distinct 模式在一个 2 分钟窗口内出现 **100000 个独立源 IP**。现实对应：

| 部署规模 | 单窗口内独立 sip |
|---|---|
| 企业内网 | 几千 |
| **大型企业 / 校园网** | **1-10 万（本场景）** |
| ISP / CDN 边缘 | 5-20 万 |
| 攻击风暴（botnet 扫描） | 可到 20 万+ |

- **pool 模式（1000 sip）贴近典型负载**（真实流量是长尾分布，多数 IP 出现一两次）。
- **distinct 模式（100000 唯一 sip）代表"大型企业 / ISP 边缘"高负载**，与规则
  `max_instances=100000` 封顶对齐——每个 sip 都拿到实例，压测完整的高基数实例集。

## 前提

- `wfusion` / `wfgen` 在 PATH（或用 `WFUSION=/path WFGEN=/path`）。
- 二进制需含 wp-reactor#18 内容记账修复（228f441）。
- 200000 事件建议 ≥8GB 内存（distinct 模式 ~11GB）；`nc`、`python3`；端口 9800 空闲。
