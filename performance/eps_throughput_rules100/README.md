# eps_throughput_rules100 — 100 规则高压吞吐 + 内存扩展性

生产环境的 CEP/SIEM 部署通常跑 50-500+ 条规则。本场景用 **100 条规则**（`scripts/gen_rules.py`
生成）验证高规则量下的引擎吞吐与内存扩展性——尤其是 **wp-reactor#19 共享解析** 之后，
规则数增加对性能的影响应接近"免费"（事件解析只做一次，所有规则共享）。

## 规则（100 条）

`scripts/gen_rules.py` 从模板 + 参数网格生成 `models/rules/throughput.wfl`（100 条，
引擎加载为 102 条规则条目，pipeline 两阶段拆 2 个内部规则）：

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
./run.sh burst 200000 distinct   # 实例 churn 压力（200000 独立实例 × 100 规则）
./run.sh sustain 200000 pool     # 持续吞吐
./validate.sh <wfusion> <wfgen> [N]  # RSS/告警汇总
```

门禁：#18 驱逐告警 = 0，且 conn 规则告警 > 0（按规则名前缀统计，排除 auth_/dns_）。

## 实测（200000 事件，release，共享解析 aa5267e）

| 模式 | 送达 | EPS | RSS |
|---|---|---|---|
| `burst` pool | 200000 | **~110k**（20 规则 ~117k，几乎不降） | **~2.8GB** |
| `burst` distinct | ~170000 | ~1.4k*（200000 实例 × 100 规则 churn） | — |

\* distinct 压力远大于 pool（每个规则 200000 个独立实例 = 2000 万实例），速度慢属预期。

### 内存扩展性（burst pool）

| 规则数 | RSS |
|---|---|
| 20 | 1.4GB |
| 40 | 1.9GB |
| **100** | **~2.8GB** |

共享解析后 RSS 亚线性增长（~15-20MB/规则，规则实例状态增量）。修复前每规则 ~0.7GB
重复解析 → 100 规则约 70GB，生产不可用；现在 100 规则 ~3GB。

### 吞吐

100 规则 EPS ~110k vs 20 规则 ~117k：事件解析共享后，规则数对吞吐的影响接近免费
（多出的成本是每事件的规则评估，非解析）。

## 前提

- `wfusion` / `wfgen` 在 PATH（或用 `WFUSION=/path WFGEN=/path`）。
- 二进制需含 wp-reactor#19 共享解析修复（aa5267e）与 #18 内容记账修复（228f441）。
- 200000 事件建议 ≥8GB 内存；`nc`、`python3`；端口 9800 空闲。
