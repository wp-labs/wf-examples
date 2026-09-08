# baseline() —— 在线行为基线：作用 · 构建 · 检测

> 本文说明 **baseline 能力本身**：它解决什么问题、基线数据如何构建、检测如何
> 判定（含两条判定通道与相位同窗）。落地示例（5 条地铁客流）与运行入口见
> [README.md](README.md)（案例操作手册）；逐条实现/测试见 §7 参考。

---

## 1. 作用：什么是"在线行为基线"

异常检测的前提是知道**什么是正常**。`baseline()` 把"正常"建成一条**可随时间
演进的基线画像**（某 `(entity, metric)` 的 μ/σ 及其历史形态），事件到达时与
画像比对得偏离度，越界即告警。它解决三个问题：

| 问题 | baseline 的答案 |
|---|---|
| **水平位移/突发**（值从 1000 跳到 9000） | 近端滚动基线：与本键最近 K 窗比，z 越界 |
| **周期内异常**（早高峰不该出现的低谷） | 同相位基线：只与"历史同期"（同相位位置）比 |
| **新常态自适应** | 半衰期加权：近期权重高、旧窗渐隐，基线跟随新常态 |
| **画像可解释/可复核** | 基线是看得见的记录（三元组），不是引擎黑盒状态 |

业务示意（案例）：5 条地铁线路客流 `flow≈1000±25`，`5号线` 某窗客流冲到 9000
（≈9×）→ 判定通道告警；`1~4号线` 永不误报。

---

## 2. 基线构建方案

### 2.1 基线的数据形态 = 可加三元组 `BaselineRecord`

```
BaselineRecord = (entity, metric, win_start, win_end, n, sum, sum_sq)
```

- `n / sum / sum_sq`（∑1 / ∑v / ∑v²）是**可加三元组**：跨窗口/跨层可直接相加后
  重推画像——`μ = sum/n`、`σ² = max(0, sum_sq/n − μ²)`；
- **只允许导出三元组，禁止只吐 μ/σ**（§11.3 契约底线）：μ/σ 不可跨层累加，
  三元组保证任意消费侧方法（mean / median / percentile…）可重新推导；
- 幂等：同 `(键, win_start)` 重放 → 替换，batch 重跑/分片合并不重复计数。

### 2.2 生成 = stats 窗口收盘（复用五原语，不新增状态机）

```
metrics_stream ─▶ stats<窗:fixed> group by (entity, metric)
                  { count as n; sum(v) as s; sumsq(v) as ss }
            ─▶ yield baseline_out(entity, metric, win_start, win_end, n, sum, sum_sq)
            ─▶ sink（file / PG / …）── 事实源
```

- 一条基线记录 = 一个已收盘窗口（`win_start` 自带相位信息，消费侧随时筛同相位）；
- `sumsq`（∑v²）是为基线新增的最小 stats 聚合（wf-lang 全链 + wf-engine 数值路径）；
  高吞吐 SoA 快路径仍只认 `count/sum/avg/min/max`——`sumsq` 落 Classic 数值路径，
  是"规则表达能力 vs 快路径资格"的显式取舍；
- 窗口粒度即画像粒度：demo 用 15s~1m 便于观察；生产按设计用 1h，跨窗合并由
  消费侧完成。

### 2.3 持久化 = 独立基线库（严禁复用窗口 spill）

收盘记录经规则级 yield→sink 落**外部持久库**
主键 `(entity, metric, win_start)`、追加幂等。**不存进窗口 spill**：基线是长存
画像资产，spill 是临时中间态，二者语义与生命周期不同。

### 2.4 分层记忆架构（读取侧：近 + 远 + 低频壳）

| 层 | 形态 | 语义 |
|---|---|---|
| **近端 B** | 规则级共享内存表 `BaselineStore`（收盘 append + 启动 warm） | 事件时间精确、每事件≈内存读；是检测热路径宿主 |
| **远端 A** | knowdb ProviderWindow（CSV 重载 / **PG 表级聚合**）周期供给 | 长留存/跨进程共享；接受处理时间近似 |

> external 壳 C（低频服务化出口）**已排除**：引擎内判定两档（B 热路径 / A 长程）已
> 闭环；“外部查询当前偏离度”属宿主产品 API 层职责（消费同一数据契约即可），不在
> 引擎内再造第三份状态与合并逻辑。见 wp-reactor 设计文档 §11.1/§11.4。

**数据血缘（PG 模式）**：`PG sink → baseline_records`（追加事实，唯一事实源）；
引擎每次装载/刷新对事实表执行聚合 SQL（`GROUP BY entity` 现算供给行），**无外部
中转表**；`baseline_ref.csv` 仅作看板镜像。

### 2.5 状态推进两种方式

- **收盘推进**：窗收盘自然产出新记录 → append/落库（近端 B 相位随收盘自动推进，
  无需外部刷新）；
- **刷新推进**：远端 A 由宿主启动 `RefreshService`（wp-knowledge），每表独立周期
  重载并**并发通知**宿主搬入边界。

---

## 3. 检测方案

### 3.1 判定函数与通道

| 通道 | 判定 | 公式（示例阈值） | 语义 |
|---|---|---|---|
| **judge（近端 B）** | 每事件、引擎内 `baseline_dev()` | `z = (v − μ)/σ`，`|z| > 3` | 单次尖峰/位移最敏感；σ≈0 不判离群（防除零误报） |
| **detect（远端 A）** | 每事件 join 供给表（2026-09-08 起按 `(entity, phase_bucket)` join 同相位基线行，不再整段退化） | `(v − μ)/μ > 5` | 量级相对偏离（9× 客流），供给周期装载/直聚合 |

无基线（键无历史）→ 判定为 None → 不告警（冷启动不误报）。

### 3.2 相位同窗（近端 B 扩展）

"早高峰只跟早高峰比"：store 按键的**相位桶**分存
`phase(ts) = (ts mod period) div bucket`（epoch 折叠、无时区），judge 按**事件
时间**折桶、只合并同相位历史（忙时格只与忙时格比）。要点：

- 配置成对：`baseline_history_phase_period/bucket`（`0 < bucket ≤ period`）；
- **桶宽 = 收盘窗宽**（或整数倍）语义才干净：每周期每桶恰 1 窗，跨期对比不混入
  当日多窗；
- 半衰期参照随序列重现间距：相位开 = `4×period`（保留 ~4 期同期画像），相位关 =
  `4×窗宽`（相邻窗，原语义不变）；
- 事件缺 `event_time` → 全桶并集（保守回退，仅诊断场景）。

### 3.3 合并数学：半衰期加权矩（消费侧）

`w = 0.5^(age/半衰期)`，以最新窗为参照：近期权重高、远期保留长程背景；方差由
**衰减加权矩**统一计算（各记录 `(n,sum,sum_sq)` 按权重累加后重推），无需存原始
样本。等权（`decay=false`）为确定对拍形态。

### 3.4 消费侧 API 演进（远期，§5）

`baseline(近期窗口, 周期, 方法, 值)`：`周期` 选同相位样本集（日/周/月或 none），
`方法` 决定"当前窗口聚合法 + 历史合并法"必须同一统计量（mean / median /
percentile(pN) / ewma）——当前实现以 `baseline_dev`（z）为近端 B 判定原语，
EWMA/median/percentile 是同一合并函数上的方法参数化演进。

---

## 4. 性能与可扩展性（实测）

`BaselineStore` 主逻辑基准（`cargo test --release -p wf-cep baseline_bench`，
release，K=8）：

| 形态 | append | deviation_at | 吞吐 |
|---|---|---|---|
| 1 实体 | ~70ns | ~86ns | ~1100万判定/s |
| 1 万实体 | ~70ns | ~86ns（与规模**解耦**） | ~1100万判定/s |

- `append`（收盘）与实体规模无关（哈希定位稳定）；
- `deviation_at`（每事件判定）经 store 改为 `entity→metric→相位桶` 三层嵌套后
  与共享实体数解耦（旧全表扫描在 1 万实体下 7.9µs → 86ns，≈92×）；
- 结论：单/大实体集群判定都在 ~100ns 级，热路径不随实体空间线性退化。

---

## 5. 案例落地（wf-examples/baseline）

案例把上述方案落到**可跑、可断言、可看板**的示例：5 条地铁线路客流 + 5号线
9× 越界。入口映射：

| 能力验证点 | 入口 | 断言 |
|---|---|---|
| 数据生产（收盘 → 三元组对拍） | `./smoke.sh` | 输入/输出逐键总量一致 |
| 近端 B judge（warm + z 判定） | `./scripts/run_m2.sh` | 仅 5号线 z≈500+ |
| 远端 A detect（供给 join） | `./scripts/run_m3a.sh` | 仅 5号线 dev≈8 |
| 全链路闭环（默认形态回归） | `./scripts/run_loop.sh` | judge 每轮 1 条；detect 同相位复现后每轮告警（累计 ≥ r−2）；CSV 按 (实体,桶) 聚合、内存平台 |
| 相位同窗（接线 e2e） | `./scripts/run_phase.sh` | judge 每轮 1 条；detect 忙轮 spike（3×<5×）正确不告警、闲轮复现告警 |
| 供给刷新（CSV / PG 直聚合） | `scripts/run_refresh.sh` / `scripts/run_pg_refresh.sh` | 改供给 μ→5 后同批事件仅 5号线 dev≈199 |
| 持续运行 + 看板 | `./run.sh [--pg]` + `./view.sh` | 实时产物/图表 |

运行细节、产物与排障见 [README.md](README.md)。

---

## 6. 方案决策速查（D1–D6 / S2）

| 决策 | 内容 |
|---|---|
| 半衰期 | `dur`=半衰期，权重 `0.5^(age/dur)` 平滑从不为 0（非硬截断） |
| 存储 | 独立持久基线库（PG/Doris/本地 redb），**严禁复用窗口 spill** |
| 生成 | 复用 stats 五原语 + `sumsq`，无新状态机；相位靠 `win_start` 消费侧筛选 |
| 周期 | 一等公民：同相位窗口集参与对比 |
| 判定分层 | 近端 B 精确（事件时间）· 远端 A 近似（处理时间，提前刷新）；external 壳 C 已排除 |
| 事实源 | 外部 sink=唯一事实源；引擎内不持有另一份权威态 |
| 一致性 | B 收盘 append 幂等；A 供给刷新幂等（主键 upsert）；存储不可用降级仅失长程 |

---

## 7. 参考

- 设计文档：`wp-reactor/docs/design/baseline-online-design.md`（§4 构建 / §5 消费
  API / §6 内联热路径 / §11 分层记忆 / §11.7 相位同窗）
- 引擎实现（相对各自仓库根；wp-reactor 为工作区兄弟仓库）：
  - `wp-reactor/crates/wf-cep/src/baseline.rs`——BaselineStore（三元组/相位桶/
    半衰期/幂等）
  - `wp-reactor/crates/wf-cep/src/cep/eval/funcs_baseline.rs`——`baseline_dev`
  - `wp-reactor/crates/wf-runtime/src/lifecycle/bootstrap.rs`——相位解析安装/warm
  - `wp-reactor/crates/wf-cep/src/baseline_bench.rs`——性能基准
- 供给刷新服务：`wp-knowledge`（RefreshService / loader 单表重载 /
  `init_postgres_provider_named_uri`）
