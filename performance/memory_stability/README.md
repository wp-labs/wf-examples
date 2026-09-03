# memory_stability — 长时间运行内存稳定性验证

验证 CEP 引擎在**持续输入**与**输入静默**时的实例/内存行为，区分「真泄漏」与
「分配器不归还 OS / warm-up ratchet」。

| 阶段 | 行为 | 预期 |
|---|---|---|
| 1. burst（N 个 distinct sip） | 每 key 一个实例，阈值 1M 不触发 → 实例存活 | instances ≈ N |
| 2. 输入停止（> 窗口 TTL） | 周期 timeout 扫描把墙钟信用（每次 ≤ interval）累计叠加到有效水位 | instances **释放到 0** |
| 3. 下一轮 burst（事件时间前移） | 新事件推进事件 watermark → 旧窗口行按 allowed_lateness 过期 | 窗口内存不随轮次累积 |

## 关键机制

- `match<sip:60s>` 高阈值聚合：实例 TTL = 距最后事件 60s。
- 实例过期由 `scan_timeouts` 驱动：有效水位 = 机器事件 watermark + **墙钟信用**
  （`wall_advance_ns`：每次扫描把 `min(流逝, timeout_scan_interval)` 累加、真实事件批
  清零）——输入完全静默时实例仍按 TTL 过期释放。
  **依赖引擎**：wp-reactor `fix(wf-runtime)`「墙钟信用按扫描累计」（commit `727cbfe`；
  旧实现累计被 `min(总 idle, interval)` 钉死，TTL > interval 的实例在连续 daemon 下
  永不释放——本 case 最初把该伪泄漏误判为内存泄漏）。`wfusion` 二进制需含此修复
  （2026-09-02 21:03 后构建）。
- 泄漏判定用 **allocator 口径**（`alloc.current_commit_bytes` / `current_rss_bytes`，
  mimalloc 实占）而非 `ps rss`：
  - 逻辑释放后 commit/RSS 不回落 OS = 分配器复用行为，不是泄漏；
  - 预热期 commit 会 ratchet 爬升到稳态平台（实例每轮释放回 0 后 commit 仍逐轮
    爬升 ~10MB、RSS 增量收敛），同样不是泄漏；
  - **判定两条**（`run.sh` B 段）：① **alloc_rss 末轮增量** ≤ tol（末两轮 RSS 收敛，
    commit 末轮增量噪声大不作判据）；② **零输入 drain 增量** ≤ tol（drain 70s 内
    commit 不增长 = 无后台残留）。实例每轮须释放回 0（LOGICAL 门禁，引擎侧滞留
    会被抓到）。
- 事件时间随轮次前移（`OFFSET_STEP`，默认 7200s > conn_events `allowed_lateness=30m`），
  让上一轮窗口行真正过期——否则每轮打在同一固定基准时间，窗口只增不减，
  commit 增长是窗口保留而非分配器泄漏（wmem_conn 每轮翻倍是此症状）。

## 运行

```bash
./run.sh --smoke    # 快速冒烟（~20 秒，配置加载 + 指标上报）
./run.sh --demo     # A：单周期逻辑释放演示（实测 ~2.5 分钟）
./run.sh --leak     # B：多周期泄漏检测，默认 6 轮含预热（实测 ~9 分钟）
./run.sh            # A + B 全量（实测 ~11-12 分钟）
```

环境变量：`N`（每轮事件数）、`CYCLES`（默认 6）、`CYCLE_IDLE`（默认 70s > TTL 60）、
`SETTLE`（默认 3）、`GROW_TOL_MB`（判定容忍，默认 8MB）、`IDLE_SEC`（demo 静默时长，
默认 130）、`OFFSET_STEP`（每轮事件时间前移秒数，默认 7200 > allowed_lateness 1800）。

产物：`data/metrics.ndjson`（指标流）、`data/mem_samples.tsv`（1s 全时段轨迹：
epoch/ps_rss/alloc_rss/alloc_commit/instances/wmem_conn）、`data/wfusion.log`、
`data/daemon.log`。

## 实测参考（mac mini M4 24G，N=10000，TTL=60s，引擎含 727cbfe）

demo（A）：
```
burst 后 instances=10000  commit≈60MB
停止 130s 后 instances=0（t≈60s 开始释放，t≈70s 归零）commit 持平
OK: 逻辑释放验证通过（instances 10000 → 0）
```

leak（B，6 轮）：
```
轮次   instances   commit(MB)  wmem(MB)  alloc_rss(MB)
1/6       0         75          2          40
2/6       0         85          1          41
3/6       0         99          1          46
4/6       0        107          1          49
5/6       0        107          1          50
6/6       0        107          1          52
drain 后 instances=0 commit=107MB（零输入 70s 持平）
OK: 无泄漏迹象 —— RSS 收敛（末轮增量 ≤ tol）且零输入 drain commit 不增长
```

解读：commit 75→107MB 的 ratchet 是预热期分配器稳态爬升（第 4 轮起平台期，drain
持平），非泄漏；实例每轮释放回 0 = 引擎逻辑释放正常。修复前的引擎在同一场景下
`instances` 全程钉 10000、commit 直线不降（LOGICAL 红）。

## 前提

- `wfusion` / `wfgen` 在 PATH（或用 `WFUSION=/path/wfusion WFGEN=/path/wfgen`）。
- `nc`（macOS/Linux 自带）。
- 端口 9800 空闲。
