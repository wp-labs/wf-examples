# memory_stability — 长时间运行内存稳定性验证

验证 CEP 引擎在**持续输入**与**输入下降**时的内存/实例行为：

| 阶段 | 行为 | 预期 |
|---|---|---|
| 1. burst（10000 个 distinct sip） | 每 key 一个实例，阈值 1M 不触发 → 实例存活 | instances ≈ 10000 |
| 2. 输入停止（窗口 TTL 到期） | 周期扫描按墙钟推进有效水位 | instances **释放到 0** |
| 3. trickle（推进 watermark） | 新事件推进 watermark → 旧实例 TTL 过期 | instances **释放** |

## 关键机制

- `match<sip:60s>` 高阈值聚合：实例 TTL = 距最后事件 60s。
- 实例过期由 `scan_expired` 驱动，水位 = `machine.watermark_nanos()`（最后事件时间）。
- **已修复**：周期 timeout 扫描按墙钟时间推进有效水位（`watermark + 距上次处理事件的墙钟时间`），
  因此输入完全静默时，实例也会按窗口 TTL（60s）自动过期释放，无需新事件。
- 窗口事件保留（`window.memory_bytes`）由 evictor 按 `time_first` 策略独立释放。

## 运行

```bash
./run.sh            # 完整验证（~3 分钟）
./run.sh --smoke    # 快速冒烟（~20 秒，仅验证配置加载 + 指标上报）
```

观察指标（`data/metrics.ndjson`）：`rule.instances`（实例数）、`window.memory_bytes`（窗口内存）。

## 前提

- `wfusion` / `wfgen` 在 PATH（或用 `WFUSION=/path/wfusion WFGEN=/path/wfgen`）。
- `nc`（macOS/Linux 自带）。
- 端口 9800 空闲。
