# baseline — 在线行为基线 · 数据生产（step 1）

把 `baseline()` 从"占位能力"升级为**可信在线行为基线原语**的第一步：
用 stats 引擎把指标流收敛为可落盘的 `BaselineRecord`（`n/sum/sum_sq` 三元组），
后续步骤（消费侧 API / 持久化后端）都以本步产出的记录为输入。

> **业务示意（本 case 的可读包装）**：想象 5 条地铁线路（`1号线`~`5号线`）
> 每 15 秒上报一次客流强度（`metric=flow`，正常 ≈ 1000±25 人次/采样点）。
> 引擎把每条线路的客流按时间窗**结账归档**（收盘 → `n/sum/sum_sq` 三元组），
> 用三元组随时可推导该线路的 μ/σ —— 客流历史画像可长期压缩保存。
> `5号线` 每轮注入一次客流 9000（≈9× 异常大客流/事故前兆），两条判定通道
> 各自独立告警：**实时滚动基线 judge**（|z|>3，对单次尖峰最敏感）与 **全局周期基线 detect**
> （(v−μ)/μ>5，供给表经 knowdb 周期刷新）——而 1~4 号线永不误报。
> 闭环长跑同时验证：窗持续收盘不漏账、画像随新常态滚动更新、内存平台。

> 设计：`wp-reactor/docs/design/baseline-online-design.md`（§4 数据生产 / §5 消费 API）。
> 里程碑: §9 MVP 第一件。

## 形态

```
metrics_stream ──▶ stats<1m:fixed> group by (entity, metric)   # 每窗每键收盘
                      { count as n; sum(value) as s; sumsq(value) as ss }
               ──▶ yield baseline_out (entity, metric, win_start, win_end, n, sum, sum_sq)
               ──▶ file_json_sink ──▶ data/baseline/baseline.ndjson
```

每条输出记录 = 设计方案 §4.2 的 `BaselineRecord`：

| 字段 | 语义 |
|---|---|
| `entity` / `metric` | 隔离键（单体/群体由 group by 维度决定） |
| `win_start` / `win_end` | 窗口边界（`@window_start_time/@window_end_time`） |
| `n` / `sum` / `sum_sq` | 消费侧推导 `mean = sum/n`、`std = √(sum_sq/n − mean²)` |

**`sumsq`（∑v²）**是本仓库新加的 stats 聚合（wf-lang 全链 + wf-engine Classic
数值累加），与 `sum` 同门控（where 通过 + 数值行才累加）、`v·v` 饱和折叠；
含 `sumsq` 的计划落 Classic 数值路径（SoA 快路径资格仍只认
count/sum/avg/min/max）。

## 运行

前置：`wfgen`/`wfusion` 在 PATH，且为**包含本地 sumsq 改动的重建版**
（warp-fusion 以 path 依赖本地 wp-reactor，重建后刷 ~/bin）。

```bash
cd baseline && ./smoke.sh        # step1 生产（batch 对拍）
./scripts/run_m3a.sh             # S2-M3a：knowdb CSV 全局周期基线供给通道判定验证
./scripts/run_long.sh [rounds]   # daemon 长跑：窗推进 + 内存平台（默认 3 轮 ≈1 分钟）
./run.sh [--pg] [时长]           # 持续闭环长跑（--pg=PG 数据后端；Ctrl-C 或 5m 停止）
./scripts/run_loop.sh [rounds]   # 有界闭环校验（两条判定通道断言，默认 5 轮）
./scripts/run_phase.sh [rounds]  # 近端 B 相位同窗接线 e2e（默认 8 轮 ≈1.5 分钟）
./view.sh [--pg]                 # 结果看板 → http://localhost:8124/view/
```

### 近端 B 相位同窗（S2-M2-b，run_phase.sh）

近端 B judge 的相位同窗：store 键含相位桶 `(entity, metric, phase_bucket)`，
judge 按**事件时间**折叠相位桶、只与历史同期（同相位）比较——早高峰只跟早高峰比。
相位由 `[runtime]` 配置开启（`baseline_history_phase_period/bucket` 成对），随收盘
自然推进、无需外部刷新。精确语义（同相位过滤、相位下半衰期参照=4×period、事件缺
`event_time` 的全桶回退）由 `wf-cep baseline::` 单测锁定。

```bash
./scripts/run_phase.sh [rounds]  # 默认 8 轮 ≈1.5 分钟；逐轮断言 judge 恰 1 条/轮
```

场景（`scripts/gen_metrics_phase.py` + `conf/loop.phase.wfusion.toml`）：

- 相位周期 240s / 桶宽 15s（=收盘窗宽）；忙时格（8..15）水平 3000、闲时格（0..7）
  水平 1000；每轮事件时间 +120s = 半周期 → 忙/闲轮交替、相位位置每 2 轮复现；
- 每轮注入 1 个 5号线=9000 越界点。断言：judge **每轮恰 1 条**、仅 5号线、z>3，
  5 条线忙/闲双档零误报；首条 z≈10 作“相位已生效”canary（滚动形态实测 ≈21）；
  detect 通道 sanity（若告警仅 5号线 且 dev>5）。

> ⚠ 相位常量（conf `baseline_history_phase_*`）与 `gen_metrics_phase.py` 的
> `PERIOD_S/BUCKET_S` 是两处重复——改一处必须同步另一处，run_phase.sh 启动会用
> `--constants` 交叉校验拦截不一致。生成器自身属性可独立自检：
> `python3 scripts/gen_metrics_phase.py --selfcheck`。

> 实测注意（2026-09-08）：批量注入（wfgen send 整轮一帧）下事件判定滞后于收盘，
> 越界事件所在窗已先 append 进其自身相位桶 → z 被自窗摊薄（≈10 而非理论 400），
> 仍 ≫3 检出；该形态下滚动模式因“自窗+邻窗已入缓冲”同样每轮只告 1 条，故 daemon
> e2e 以**接线与稳定性**为断言口径（不凭计数断言隔离语义），隔离语义以单测为准。

### 长跑验证（run_long.sh）

daemon + TCP 注入（wfgen send），producer 专用 15s 窗（`models/rules-long/`，
生产形态仍 1h/1m）：每轮事件时间 +120s 前移驱动逐窗收盘，判据——

1. **窗推进不漏**：每轮 baseline.ndjson 单调 +6 窗×5 键 = 30 条（事件时间不前移
   会导致窗永不收盘的假绿）；
2. **内存平台**：stats 状态每窗 reset，末段两次 6s 采样 RSS/commit 增长 ≤
   `GROW_MB`（默认 80MB）。实测 3 轮：30→60→90 条，ps rss Δ<1MB PASS。

产物：`data/metrics.ndjson` / `data/long_samples.tsv` / `data/logs/wfusion_long.log`。

产物：

- `data/generated/metrics_baseline.jsonl` — 确定性输入事件（seed=42）
- `data/baseline/baseline.ndjson` — 基线记录（本步目标）
- `data/logs/wfusion.log` — 引擎日志

## 校验

`smoke.sh` 第 4 步把输入事件逐 `(entity, metric)` 聚合出 `(n, sum, sum_sq)`
总量，与 `baseline_out` 输出记录同键总量对拍（数值口径对齐引擎的 float→i128
截断），相等才通过。另外两个引擎内单测族保证聚合自身正确：

- wf-lang: `stats_sumsq_measure_parses` + 全链编译用例
  `compile_stats_baseline_producer_sumsq_and_window_times`（含窗口系统变量、float 赋值）
- wf-engine: `stats_exec_sumsq`（路由到 Classic / 行式↔列式 parity / SoA 跨路线
  parity）+ `sum_sq_domain`

## 参数调整

- 窗口长度：demo 用 `1m`（5m 数据 → 多窗口，验证跨窗一致性）；生产按设计用 `1h`，
  消费侧再按周期做同相位合并。
- 注入速率/时长：`models/scenarios/metrics_baseline.wfg`。

> 数据形态：默认 traffic 全随机 → 每个 (entity, metric) 恰 1 样本（n=1,
> sum_sq = sum² 恰好直接验证平方路径）；**多样本/跨窗求和**（如 [3,5,4]→
> sumsq 50）由引擎单测 `stats_exec_sumsq` 覆盖。若要让示例更像真实基线
> （同键多样本、同键跨多窗），需给 scenario 注入低基数字段池（datagen
> `use(field=…)` 目前与规则步绑定，后续可扩展）。
