# eps_throughput — 吞吐压力测试（目标 EPS ≥ 10000）

测量 wfusion 引擎在**复杂规则负载**下的输入处理吞吐（events/sec）。

## 实测结果（macOS, executor_parallelism=4, **release**）

| 模式 | 事件数 | 实测 EPS | 目标 |
|---|---|---|---|
| `burst`   | 200000 | **~250,000** | ≥ 10000 ✓ |
| `sustain` | 200000 | ~92,000 | ≥ 10000 ✓ |
| `burst distinct` | 200000 | ~150,000 | ≥ 10000 ✓ |

> **release vs debug**：默认用 `target/release`（`PROFILE=release`）；debug 约慢 4 倍，报告 EPS 用 release。

## 规则复杂度（6 条规则覆盖主要引擎路径）

`models/rules/throughput.wfl`：

| 规则 | 覆盖 | 触发 |
|---|---|---|
| `global_throughput` | 全局实例纯引擎路径 | 否（阈值 1 亿） |
| `per_sip_instances` | 每 sip 实例 map churn | 否 |
| `denied_probe` | guard 过滤 + `distinct` 聚合 | ✅（443 告警） |
| `login_brute` | 第二事件源 + `and close` close 路径 | ✅（50 告警） |
| `traffic_sum` | `sum(bytes)` 聚合 | ✅（375 告警） |
| `accu_tracker` | `on event<accu>` 窗口内累积（#65） | ✅（~16k 告警） |

## 数据多样性

`scripts/gen_events.py`（`--mode`：`global`/`pool`/`distinct`）：
- **两类事件**：~75% `conn_events`（网络流）+ ~25% `auth_events`（登录）。
- conn：端口 8 种、协议 4 种、动作 30% denied、字节/时长随机、时间抖动。
- auth：200 用户、30% failed（触发 login_brute）。

## 运行

```bash
./run.sh burst 200000 pool       # 峰值吞吐（默认，release）
./run.sh sustain 200000 pool     # 持续吞吐
./run.sh burst 200000 distinct   # 实例 churn 压力
PROFILE=debug ./run.sh burst 50000 pool   # debug 对比
```

## 指标

- **EPS** = 事件数 / 发送开始到引擎接收全部的时间；接收计数取 `receiver.rows_total` 累加。
- 告警分布看 `data/default.ndjson`（各规则 `__wfu_rule_name`）。

## 已知问题（已定位，见 wp-reactor#18）

- **结构化 `object` 字段（#64）在高压下批次被窗口内存驱逐丢弃**：
  Arrow IPC encode→decode 往返把批次 `get_array_memory_size` 膨胀 ~7 倍（12.4MB → 解码后 273MB），
  超过 conn_events 窗口 `max_window_bytes=256MB` → append 内存驱逐静默丢弃整批 → 规则不处理。
- wp-reactor 已加准确告警（`window \`conn_events\` dropped ... in memory eviction`）。
- 当前示例去掉 object 字段保持高吞吐；object 字段建议在中小规模使用（≤~9 万行/批）。
- 深层修复（窗口按实际内容记账 vs IPC 膨胀）见 wp-reactor#18。
