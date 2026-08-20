# nexmark_pk 测试方案（TEST PLAN）

> 用途：对 nexmark_pk 基准套件（Q1-Q9 共 7 条查询）做**正确性 + 性能**的系统性验证。
> 覆盖两层测试：**微基准**（单元级，定位改进逻辑逐操作耗时）与**端到端**（吞吐 + 输出正确性）。
> 本文沉淀 2026-08 以来的测量纪律与已知噪声边界；每个测试给可复制命令 + 验收标准。

## 0. 概览

| 层 | 工具 | 验证什么 | 耗时 |
|---|---|---|---|
| 微基准 | `cargo test --release -p wf-engine columnar_bench`（`#[ignore]`） | 列式读/emit/建 set/缓存的逐操作耗时 | ~1 min |
| 端到端正确性 | `bench.sh <q> cont 30m` + `verify_ground_truth.py` + `[clean]` + EMIT | 输出与确定性 ground truth 一致 | ~10 min/批 |
| 端到端吞吐 | `bench.sh all cont 10m/30m/100m` | EPS + RSS_peak，A/B 对比 | 10m ~7 min；30m ~20 min |
| 回归对比 | 新旧 wfusion 二进制同参数各跑一遍 | EMIT 一致 + EPS/RSS 变化 | 视规模 |

## 1. 环境与前置

```bash
cd wf-examples/performance/nexmark_pk
# 二进制：本地 warp-fusion release 或 WFUSION/WFGEN 覆盖
WFUSION=/path/to/wfusion WFGEN=/path/to/wfgen
# 需 wfusion/wfgen 含 gen-nexmark / dump-frames / send-arrow / stream 子命令
# 前置：nc、python3；端口 9800 空闲；数据帧 data/bench_<total>.frames（存在即复用）
```

**构建（改动后必须重建）**：
```bash
cargo build --release -p wfusion -p wfgen   # 在 warp-fusion workspace
```

## 2. 微基准（单元级，验证改进逻辑）

运行（release，`#[ignore]` 基准）：
```bash
cargo test --release -p wf-engine columnar_bench -- --ignored --nocapture
```

测量项（nexmark bid 形态 7 字段批，n=1M）与验收基准：

| 项 | 测量 | 方向 |
|---|---|---|
| 状态机字段读 `field_value` | eager Event vs 列式 ColumnarEvent | 列式应快（实测 4.8×） |
| emit trigger `to_event` | `Event::clone` vs 列式重建 | 列式重建慢 ~1.4×（已知代价） |
| join 行读 `field_value` | `HashMap::get` vs JoinRow 列式 | 列式应快（实测 4.7×） |
| `window.has` 建 set | `batch_to_events` vs 单列读 | 单列应快（实测 4.3×） |
| `window.has` 求值 | 冷扫描 vs 缓存命中 | 命中应 O(1)（实测 ~1380 万×） |

**语义自检**：每个基准断言列式与 eager 结果一致（field 值 / set 内容 / 命中行），不通过即红。

## 3. 端到端正确性（30M 规模，seed=1）

ground truth 由 `scripts/verify_ground_truth.py` 确定性模拟推算（Q2-Q9 期望 emitted）。

```bash
# 1) 跑 30M 全部查询，输出到 data/bench_<q>_cont.txt
WFUSION=... WFGEN=... ./bench.sh all cont 30m
# 2) 检查每个查询文件
for q in q1 q2 q3 q4 q5 q7 q9; do
  echo "== $q =="; grep -E "SUMMARY|EMIT" data/bench_${q}_cont.txt
done
```

**验收标准**：

| 项 | 通过条件 |
|---|---|
| `SUMMARY` | 全部 `clean`（serialize_failed / dropped_late / memory_evicted / cursor_gap = 0） |
| `appended` | 30M/30M（或 100M/100M） |
| EMIT 期望（30M seed=1，来自 README） | q2=224,289 · q3=1,800,000 · q4=27,600,000 · q5=1,712,532(±62 墙钟) · q7=10,350,961/34,578/0 · q9=1,800,000 |
| 逐 alert 对拍（可选，深验） | `python3 scripts/q5_diff_v2.py` 28k 探针全量吻合 |

> 30M 全量 ground truth 是权威；10m/100m 只做 EMIT 比例侧证 + 新旧一致（见 §6）。

## 4. 端到端吞吐（EPS + RSS）

```bash
# 单查询
WFUSION=... WFGEN=... ./bench.sh q2 cont 10m
# 全部（顺序跑，每查询独立 metrics）
WFUSION=... WFGEN=... ./bench.sh all cont 10m
```

**测量纪律（违反会得出假结论）**：
1. **计时口径 = append_total**（三输入流 append 求和追平 TOTAL），非 ingress 预读游标。
2. **A/B 必须不限速**：`RATE=10000000`（限速会把 EPS 封顶在 RATE）。
3. **同时段交错对比**：bench 机 EPS 与 RSS_peak 呈双峰相位相关（同配置 ±8%），
   A/B 结论必须按 RSS 相位配对；单轮数字只作量级参考。
4. **RSS 口径**：引用 RSS 须标注 `parse_buffer_bytes`（默认 128MB；吞吐场景调大到 2GB）。
5. **多次重跑取中位**：10m 短程 load 噪声大（Q5/Q7 曾见 load 4~40），EPS 结论至少 3 次。

## 5. 回归对比协议（新旧二进制）

目的：验证改动零正确性损失 + 量化 EPS/RSS 变化。

```bash
OLD=path/to/old-wfusion  NEW=path/to/new-wfusion
for q in q1 q2 q3 q4 q5 q7 q9; do
  for BIN in "$OLD" "$NEW"; do
    WFUSION=$BIN WFGEN=${BIN%wfusion}wfgen ./bench.sh $q cont 10m 2>&1 | grep "^$q/cont"
    grep -E "EMIT|SUMMARY" data/bench_${q}_cont.txt
  done
done
```

**验收标准**：
- **EMIT**：新旧一致。Q2/Q3/Q7/Q9 必须逐位相等；Q4（join）与 Q5（count）在**既存波动带**内
  （见 §8），判定为一致的标准是**区间重叠**而非单值相等。
- **`[clean]`**：双方都必须 clean。
- **RSS**：物化消除应带来下降（Q5 -36%、Q7 -18% 实测），不得上升。
- **EPS**：Q2/Q3/Q9 持平或升；Q7（emit 密集）允许 -8~15%（to_event 重建代价，已微基准定位）；
  Q5 持平。若超出，需定位（微基准锁热点）。

## 6. 本会话免物化改进的专项验证点

| 改进 | 专项验证 |
|---|---|
| 列式喂状态机（P3 FieldView） | 微基准 `field_value` 4.8×；Q2/Q5/Q7 EMIT 一致；Q5 RSS -36% |
| 放宽 defer 门槛（无 filter match） | Q5/Q7 走 deferred+columnar（RSS 下降即证）；EMIT 一致 |
| `window.has` 单列 + O(1) 缓存 | 微基准 4.3× / 1380 万×；`snapshot_field_values_*` 单测（含缓存失效刷新） |
| 列式 join（JoinRow） | Q9 EMIT 逐位一致；Q4 落在既存波动带；微基准 join 行读 4.7× |

单测兜底：`cargo test -p wf-engine -p wf-runtime -p wf-lang` 全绿 +
`cargo clippy --all-targets --all-features -- -D warnings` 干净。

## 7. 验收标准汇总（Checklist）

- [ ] wp-reactor 单测全绿 + clippy `-D warnings` 干净
- [ ] 微基准 `columnar_bench` 全部通过（含语义自检）
- [ ] 30M ground truth：EMIT 全部命中期望（q5 允许 ±62）
- [ ] 全部查询 `SUMMARY clean`、appended 100%
- [ ] 新旧 EMIT：Q2/Q3/Q7/Q9 逐位一致；Q4/Q5 区间重叠
- [ ] RSS 不上升（物化消除查询应显著下降）
- [ ] EPS 无未解释回退（Q7 允许 to_event 代价带）

## 8. 已知噪声 / 既存波动（诚实边界）

- **Q4**（snapshot join auction_events）：EMIT 在 ~7.3M ↔ 9.2M 间随 run 波动——
  **新旧二进制同样波动**，源是 auction_events 窗口保留量随管道时序变化（join 求值时刻
  窗口里的 auction 数），非正确性破坏。
- **Q5**（count≥10）：EMIT 波动 ±10（571,061~571,076），max_memory 驱逐时序 + 墙钟
  scan_timeouts 非确定性。
- **Q7**（emit 密集）：EPS -8~15% 是列式 emit 路径 `to_event` 重建（260.9ns vs clone
  183.4ns）的固有代价，已微基准锁定；RSS -18% 抵偿。
- **10m 短程 EPS**：load 噪声大，只作方向参考；结论用 30m/100m + 多次中位。

## 9. 产物位置

| 产物 | 路径 |
|---|---|
| 帧文件 | `data/bench_<total>[_mb<bytes>].frames`（复用） |
| 每查询结果 | `data/bench_<q>_cont.txt`（EPS/RSS/EMIT/SUMMARY） |
| 计数器流 | `data/metrics.ndjson`（`scripts/extract_emitted.py` 汇总） |
| 引擎/daemon 日志 | `data/wfusion.log` / `data/daemon.log` |
| 性能报告 | `PK_REPORT_MAC.md` / `PK_REPORT_LINUX.md` |
