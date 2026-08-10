# eps_throughput_obj — object 字段高压回归场景（wp-reactor#18）

与 `eps_throughput` 相同负载，但 `conn_events` 额外携带 `conn_info: object` 字段。
用于回归验证 **#18：结构化 object 字段在高压下批次被窗口内存驱逐静默丢弃**。

## 根因（wp-labs/wp-reactor#18）

Arrow IPC 往返把批次 `get_array_memory_size` 膨胀 ~7x（12.4MB → 解码后 273MB）。
object 大批次（conn >~112000 行）膨胀后超过 conn_events 窗口 `max_window_bytes=256MB`，
append 内存驱逐把刚追加的整批 pop 掉——之前完全静默，表现为"object 字段把规则搞坏了"。

**修复（wp-reactor 228f441）**：窗口按 *内容字节* 记账（`wf_engine::window::content_bytes`），
不再按 IPC 膨胀后的分配字节。同一批次按内容 ~55MB < 256MB → 存活。

## 场景数据

- ~75% `conn_events`（150k 行，含 `conn_info` object 负载每行 ~370B）+ ~25% `auth_events`。
- `conn_info: object` 运行时为 JSON 编码的 Utf8 列（wfgen 与运行时均映射 Object → Utf8）。
- 200000 事件中 conn 单批内容 ~55MB；修复前解码膨胀 >256MB 触发驱逐。

## 运行

```bash
./run.sh                 # 默认 200000 事件 burst，EPS + #18 回归门禁
./run.sh 300000          # 更大规模
./validate.sh <wf> <wfg>  # A/B 驱动：对比修复前/后二进制行为
```

`run.sh` 结束时执行 #18 回归门禁：
- `in memory eviction` 告警数 = 0，**且** conn 规则告警 > 0（accu_tracker/denied_probe/traffic_sum）→ PASS。
- 若 object 批被丢，conn 规则 0 告警（只剩 auth 的 login_brute）→ FAIL。

## 实测 A/B（200000 事件，release）

| 指标 | 修复前（git v0.4.0） | 修复后（228f441） |
|---|---|---|
| ingress 送达 | 200000 | 200000 |
| conn 规则告警 | **0**（仅 login_brute=50） | **76619**（accu 75750 / denied 444 / traffic 375 / login 50） |
| conn 窗口记账 | 解码膨胀 >256MB → 批被驱逐 | 内容 ~55MB → 保留 |
| 驱逐告警 | 无（v0.4.0 无告警逻辑，静默丢） | 0 |
| daemon RSS 峰值 | 192MB（数据丢失） | 1029MB（数据全处理） |
| EPS | ~185k | ~187k |

标准场景（无 object）吞吐无回归：`eps_throughput` burst 200000 = **EPS 249486**（README 基线 ~250k）。

## 前提

- `wfusion` / `wfgen` 在 PATH（或用 `WFUSION=/path WFGEN=/path`）。
- 二进制需含 wp-reactor#18 修复；修复前二进制可用 `validate.sh` 复现丢批。
- `nc`、`python3`。端口 9800 空闲。
