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

## 实测（200000 事件，release，含 #19 全套优化）

| 模式 | 送达 | EPS | RSS |
|---|---|---|---|
| `burst` pool | 200000 | **~110k**（20 规则 ~117k，几乎不降） | **~2.5GB** |
| `burst` distinct | 200000 | **~95k** | **~11.8GB**（~900 万实例） |

> distinct 的吞吐并不慢（~95k，全送达）；成本在**实例内存**：200000 独立 sip ×
> ~90 条 conn 规则 ≈ **900 万实例**（规则 `max_instances=100000` 封顶）。这是与 #19
> 不同的扩展维度：**实例数 × 规则数**（每实体每规则一份状态）。pool 模式 1000 sip
> → ~10 万实例，RSS 才 2.5GB。

### 内存扩展性（burst pool）

| 规则数 | RSS |
|---|---|
| 20 | 1.4GB |
| 40 | 1.9GB |
| **100** | **~2.5GB** |

共享解析后 RSS 亚线性增长（~15-20MB/规则，规则实例状态增量）。修复前每规则 ~0.7GB
重复解析 → 100 规则约 70GB，生产不可用；现在 100 规则 ~2.5GB。

### 吞吐

100 规则 EPS ~110k vs 20 规则 ~117k：事件解析共享后，规则数对吞吐的影响接近免费
（多出的成本是每事件的规则评估，非解析）。

## 200000 独立 sip 是什么水准

distinct 模式在一个 2 分钟窗口内出现 **200000 个独立源 IP（sip）**。这对应什么现实场景？

| 部署规模 | 单窗口内独立 sip | 100 规则实例数 |
|---|---|---|
| 企业内网（1-10 万内部 IP） | 几千 | ~50 万 |
| 大型企业 / 校园网 | 1-5 万 | ~500 万 |
| ISP / CDN 边缘 | 5-20 万 | 500-2000 万 |
| **攻击风暴（botnet 扫描 / 分布式探测）** | **可到 20 万+** | **2000 万+** |

- **pool 模式（1000 sip 循环复用）才是贴近真实的典型负载**——真实流量是长尾分布：
  少数 IP 是重头，绝大多数 IP 只出现一两次。这是场景默认模式。
- **distinct 模式（200000 唯一 sip）是刻意的最坏情况**：每个源 IP 只发一条连接，用于
  压测**实例 churn 上限**。它不代表"正常分析"，而代表**极端基数场景**（ISP 边缘、
  大规模扫描）下引擎的内存边界。
- **对生产的意义**：实例内存 = 窗口内独立实体 × 规则数。企业典型负载（几千实体 ×
  100 规则）完全无压力；只有高基数 + 多规则（如 ISP 攻击场景 200k × 100 = 2000 万
  实例）才会触及内存瓶颈——这正是 distinct 模式存在的意义。

## 前提

- `wfusion` / `wfgen` 在 PATH（或用 `WFUSION=/path WFGEN=/path`）。
- 二进制需含 wp-reactor#19 共享解析修复（aa5267e）与 #18 内容记账修复（228f441）。
- 200000 事件建议 ≥8GB 内存；`nc`、`python3`；端口 9800 空闲。
