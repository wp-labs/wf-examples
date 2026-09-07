# baseline — 在线行为基线 · 数据生产（step 1）

把 `baseline()` 从"占位能力"升级为**可信在线行为基线原语**的第一步：
用 stats 引擎把指标流收敛为可落盘的 `BaselineRecord`（`n/sum/sum_sq` 三元组），
后续步骤（消费侧 API / 持久化后端）都以本步产出的记录为输入。

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
cd baseline && ./smoke.sh
```

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
