# eps_throughput_obj — 20 规则 object 字段高压回归场景（wp-reactor#18）

20 条规则 + 富数据类型 + `conn_info: object` 字段的综合压测场景。
用于回归验证 **#18：结构化 object 字段在高压下批次被窗口内存驱逐静默丢弃**，
同时覆盖主要引擎路径与数据类型。

## 根因（wp-labs/wp-reactor#18）

Arrow IPC 往返把批次 `get_array_memory_size` 膨胀 ~7x（12.4MB → 解码后 273MB）。
object 大批次（conn >~112000 行）膨胀后超过 conn_events 窗口 `max_window_bytes=256MB`，
append 内存驱逐把刚追加的整批 pop 掉——之前完全静默，表现为"object 字段把规则搞坏了"。

**修复（wp-reactor 228f441）**：窗口按 *内容字节* 记账（`wf_engine::window::content_bytes`），
不再按 IPC 膨胀后的分配字节。同一批次按内容 ~60MB < 256MB → 存活。

## 场景数据

- 三类事件：~75% `conn_events` + ~15% `auth_events` + ~10% `dns_events`。
- conn 富类型：`conn_info` object（嵌套 geo/vlan，每行 ~400B JSON，#18 负载）、
  `blocked` bool、`packet_rate` float、`app_id` chars、`tags` array/chars。
- 200000 事件中 conn 单批内容 ~60MB；修复前解码膨胀 >256MB 触发驱逐（conn 批被丢）。

## 20 条规则覆盖

| # | 规则 | 覆盖路径 | 触发 |
|---|---|---|---|
| 1 | global_throughput | 纯引擎路径 | 否（阈值 1 亿） |
| 2 | per_sip_instances | 每 sip 实例 churn | 否（阈值 1 亿） |
| 3 | denied_probe | guard + distinct 聚合 | ✅ |
| 4 | login_brute | 第二事件源 + and-close | ✅（close 路径） |
| 5 | traffic_sum | `sum(bytes)` 聚合 | ✅ |
| 6 | accu_tracker | `on event<accu>` 窗口内累积 (#65) | ✅ |
| 7 | dns_avg_tunnel | `avg()` 聚合（dns） | ✅ |
| 8 | max_bytes_spike | `max()` 聚合 | ✅ |
| 9 | min_duration_probe | `min()` 聚合 | ✅ |
| 10 | chain_attack | 多步序列（seq） | 否（需 syn/login_fail 数据） |
| 11 | port_scan_distinct | distinct dport 计数 | ✅ |
| 12 | high_packet_rate | float 字段 guard | ✅ |
| 13 | blocked_flag | bool 字段 guard | ✅ |
| 14 | object_nested_path | object 嵌套路径 (#64) | ✅ |
| 15 | array_tag_member | array 字段索引 | ✅ |
| 16 | hex_app_id | 字段等值匹配 | ✅ |
| 17 | string_func_guard | 字符串函数（indexof/startswith_any） | ✅ |
| 18 | math_func_guard | 数学函数（abs） | ✅ |
| 19 | close_threshold | and-close 累计阈值 | ✅（close 路径） |
| 20 | pipeline_aggregate | 两阶段 pipeline（fixed 桶） | ✅（close 路径） |

> 引擎加载 20 条规则编译为 21 个规则条目（pipeline 两阶段拆为 2 个内部规则）+ 5 个 schema
> （4 窗口 + pipeline 中间桶）。

## 运行

```bash
./run.sh                          # 默认 burst 200000 pool，EPS + #18 回归门禁
./run.sh sustain 200000 pool      # 持续吞吐
./run.sh burst 200000 distinct    # 实例 churn 压力
./run.sh burst 50000 pool         # 小规模快速验证
./validate.sh <wfusion> <wfgen> [N]   # A/B 驱动：对比修复前/后二进制行为
```

`run.sh` 结束时执行 #18 回归门禁：
- `in memory eviction` 告警数 = 0，**且**（pool/global 模式）conn_events 窗口规则告警 > 0
  （排除 auth 的 login_brute / dns 的 dns_avg_tunnel）→ PASS。
- 若 conn 大批次被内存驱逐丢弃，所有 conn 规则归零 → FAIL。
- `distinct` 模式每个 sip 仅 1 事件，规则阈值不触发属预期，门禁只查驱逐告警。

## 实测 A/B（200000 事件，release）

| 指标 | 修复前（git v0.4.0） | 修复后（228f441） |
|---|---|---|
| ingress 送达 | 200000 | 200000 |
| conn 规则告警 | **0**（conn 批被丢；仅 auth/dns 幸存） | **177886**（14 条 conn 规则触发） |
| conn 窗口记账 | 解码膨胀 >256MB → 批被驱逐 | 内容 ~60MB → 保留 |
| 驱逐告警 | 无（v0.4.0 无告警逻辑，静默丢） | 0 |
| daemon RSS 峰值 | ~112MB（conn 数据丢失） | ~1GB（conn 数据全处理） |

## 性能实测（修复后，200000 事件，release）

| 模式 | 送达 | EPS | #18 门禁 |
|---|---|---|---|
| `burst` pool | 200000 | **~70k-78k**（20 规则，vs 6 规则 ~192k） | PASS |
| `burst` distinct | 200000 | ~70k | PASS（0 驱逐，阈值不触发为预期） |
| `sustain` pool | ~187000* | ~4.4k* | PASS（conn 全处理） |

> \* sustain 顺序分片下 daemon `rx_rows` 停在 ~187000/200000，但 conn 规则告警数与 burst
> 一致——conn 事件全部处理。缺口指向 TCP 源/wfgen 顺序大帧路径，**与 #18 无关**，待引擎侧跟进。

20 条规则 vs 6 条规则：EPS 70k-78k vs 192k（约 3x 规则评估开销），仍远超 1W 目标。

## 内存观察（200000 事件，20 规则）

- daemon RSS 随规则数线性增长：~1GB 基线（解码批次）+ **~0.7GB/规则** → 20 规则 ≈ **15GB**。
- `window_bytes`（窗口内容记账，修复后）仅 ~67MB——RSS 大头在**各规则的保留状态**，
  不在窗口缓冲。告警量不驱动 RSS（855 告警仍有 3.5GB）。
- 建议 ≥16GB 内存运行 200000/20 规则；小规模用 `run.sh burst 50000 pool`。
- 每规则 ~0.7GB 的保留是引擎行为观察，是否应改为 Arc 共享而非逐规则拷贝待引擎侧评估。

## 前提

- `wfusion` / `wfgen` 在 PATH（或用 `WFUSION=/path WFGEN=/path`）。
- 二进制需含 wp-reactor#18 修复；修复前二进制可用 `validate.sh` 复现丢批。
- 200000 事件/20 规则建议 ≥16GB 内存；`nc`、`python3`；端口 9800 空闲。
