# memory_stability — 长时间运行内存稳定性验证

验证 CEP 引擎在**持续输入**与**输入静默**时的实例/内存行为，区分「真泄漏」与
「分配器不归还 OS / warm-up ratchet」。

| 阶段 | 行为 | 预期 |
|---|---|---|
| 1. burst（N 个 distinct sip） | 每 key 一个实例，阈值 1M 不触发 → 实例存活 | instances ≈ N |
| 2. 输入停止（> 窗口 TTL） | 周期 timeout 扫描把墙钟信用（每次 ≤1s）叠加到有效水位 | instances **释放到 0** |
| 3. trickle / 下一轮 burst | 新事件推进事件 watermark → 旧窗口行按 allowed_lateness 过期 | 窗口内存不随轮次累积 |

## 关键机制

- `match<sip:60s>` 高阈值聚合：实例 TTL = 距最后事件 60s。
- 实例过期由 `scan_timeouts` 驱动：有效水位 = 机器事件 watermark + **墙钟信用**
  （每次扫描消费 ≤ `timeout_scan_interval` 并累计，真实事件批清零）——输入完全
  静默时实例仍按 TTL 过期释放（wp-reactor `fix(wf-runtime)`：墙钟信用按扫描累计；
  旧实现累计被 min(总 idle, interval) 钉死，TTL>interval 的实例永不释放）。
- 泄漏判定用 **allocator 口径**（`alloc.current_commit_bytes` / `current_rss_bytes`，
  mimalloc 实占）而非 `ps rss`：逻辑释放后 commit/RSS 不回落 OS 是分配器复用行为；
  预热期 commit 会 ratchet 爬升到稳态平台（同样不是泄漏）。判定看两条：
  **末轮增量**（最后一轮 vs 倒数第二轮 commit ≤ tol）与**零输入 drain 增量**。
- 事件时间随轮次前移（`OFFSET_STEP`，须 > conn_events 的 `allowed_lateness=30m`），
  让上一轮窗口行真正过期——否则每轮打在同一固定基准时间，窗口只增不减，
  commit 增长是窗口保留而非分配器泄漏。

## 运行

```bash
./run.sh --smoke    # 快速冒烟（~20 秒，配置加载 + 指标上报）
./run.sh --demo     # A：单周期逻辑释放演示（~2.5 分钟）
./run.sh --leak     # B：多周期泄漏检测（默认 6 轮含预热，~10 分钟）
./run.sh            # A + B（~13 分钟）
```

环境变量：`N`（每轮事件数）、`CYCLES`、`CYCLE_IDLE`（默认 70s > TTL 60）、
`SETTLE`、`GROW_TOL_MB`（判定容忍，默认 8MB）、`IDLE_SEC`（demo 静默时长）、
`OFFSET_STEP`（每轮事件时间前移秒数，默认 7200 > allowed_lateness 1800）。

产物：`data/metrics.ndjson`（指标流）、`data/mem_samples.tsv`（1s 全时段轨迹）、
`data/wfusion.log`、`data/daemon.log`。

## 前提

- `wfusion` / `wfgen` 在 PATH（或用 `WFUSION=/path/wfusion WFGEN=/path/wfgen`）。
- `nc`（macOS/Linux 自带）。
- 端口 9800 空闲。
