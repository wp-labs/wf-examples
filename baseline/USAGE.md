# baseline 使用指南（在线行为基线 · 地铁客流案例）

> 本文是 **baseline 案例的独立使用指南**：运行模式、判定通道与数据契约、产物、
> 校验体系、配置参数与排障。案例入口（业务简介 / 快速上手）见
> [README.md](README.md)；架构设计见 `wp-reactor/docs/design/baseline-online-design.md`
> （§4 数据生产 / §5 消费 API / §11 分层记忆架构 / §11.7 相位同窗）。

---

## 1. 快速开始

**前置**：`wfusion` / `wfgen` 在 PATH（若引擎有本地改动：warp-fusion 以 path 依赖
本地 wp-reactor，`cargo build --release -p wfusion -p wfgen` 后刷 `~/bin/`）；
`python3`；`--pg` 模式另需 docker。

```bash
cd baseline
./smoke.sh                     # ① 一次性数据生产 + 对拍（~秒级）
./scripts/run_loop.sh          # ② 有界闭环校验：两条判定通道断言（默认 5 轮 ≈1 分钟）
./view.sh                      # ③ 看板 → http://localhost:8124/view/（另开终端）
```

| 入口 | 模式 | 做什么 | 期望 |
|---|---|---|---|
| `./smoke.sh` | 生产(step1) | batch 回放 producer + 输入/输出总量对拍 | `n/sum/sum_sq` 逐键一致 |
| `./scripts/run_m2.sh` | S2-M2 最小闭环 | warm 历史 + judge z 判定对拍 | 仅 5号线 告警（z≈500+） |
| `./scripts/run_m3a.sh` | S2-M3a 最小验证 | knowdb CSV 供给 → detect join 判定 | 仅 5号线（dev≈8）告警 |
| `./scripts/run_long.sh [n]` | 长跑验证 | 窗推进不漏 + 内存平台（默认 3 轮） | 每轮 +30 行、RSS 平台 |
| `./scripts/run_loop.sh [n]` | 有界闭环 | daemon 全链路：收盘→judge/detect→refresh 断言 | 每轮 1 条 5号线 告警 |
| `./scripts/run_refresh.sh` | refresh 实证 | CSV 覆盖后 3 tick 内判定读到新供给 | 仅 5号线 告警（dev≈199） |
| `./scripts/run_pg_refresh.sh` | PG 供给实证 | PG 事实库 UPDATE 后判定读到新聚合 | 仅 5号线 告警（dev≈199） |
| `./scripts/run_phase.sh [n]` | 相位同窗 e2e | 忙/闲双档 × 相位配置接线验证 | 每轮恰 1 条、首条 z≈10 |
| `./run.sh [--pg] [时长]` | 持续闭环 | 长跑 + 注入循环（Ctrl-C / 时长停） | 看板实时增长 |
| `./view.sh [--pg]` | 看板 | 只读静态页（默认 :8124） | 页面 2s 刷新 |

---

## 2. 判定通道与数据契约（先看懂再跑）

```
metrics_stream ─▶ producer stats<窗>: group by (entity, metric)
                  { count as n; sum(v) as s; sumsq(v) as ss }
            ─▶ yield baseline_out(entity, metric, win_start, win_end, n, sum, sum_sq)
            ─▶ 落盘 baseline.ndjson（实时追加）
                 ├─▶ 近端 B：收盘 append 共享 BaselineStore（内存）
                 │      └─ judge 规则：baseline_dev() |z|>3 → judge.ndjson
                 ├─▶ 导出聚合 → baseline_ref 供给（CSV 重载 或 PG 直聚合）
                 │      └─ detect 规则：(v−μ)/μ > 5 → alerts.ndjson
                 └─（PG 模式）postgres sink → baseline_records 事实表
```

| 概念 | 含义 |
|---|---|
| `entity`/`metric` | 隔离键（线路 / 客流） |
| `win_start`/`win_end` | 窗口边界（epoch 纳秒） |
| `n`/`sum`/`sum_sq` | 可加三元组：`mean = sum/n`、`σ = √(sum_sq/n − mean²)`。**契约底线**——各层必须保留三元组（可加、方法可重推导），`mu/sigma` 只是派生消费列 |
| 近端 B · judge | 事件级在线判定（共享内存 store，相位开启=同相位历史比较） |
| 远端 A · detect | 全局周期基线供给判定（knowdb provider 周期装载） |
| `mu`/`sigma` | `μ=sum/n`、`σ=√(max(0,sum_sq/n−μ²))`——供给行的画像列 |

**数据血缘（PG 模式）**：PG sink → `baseline_records`（追加事实，唯一事实源）；
引擎每次装载/刷新对事实表**表级 query 直聚合**成供给行（无外部中转表）；
`data/detect/baseline_ref.csv` 只是每轮导出的**看板镜像**。file 模式该 CSV 即
knowdb 供给源。详见 [pg/](pg/baseline_records.sql)。

---

## 3. 运行模式详解

### 3.1 数据生产对拍（smoke.sh）
确定性生成 `metrics_stream` 事件 → batch 回放 `baseline_producer`（1m 固定窗 +
sumsq）→ `scripts/verify_baseline.py` 把输入事件逐键总量与输出对拍（对齐引擎 float→i128
截断口径）。产物 `data/baseline/baseline.ndjson`。

### 3.2 近端 B 最小闭环（run_m2.sh）
5 条线路受控历史（4 窗 × 60 样本）+ 5号线 9× 越界 → producer 收盘 →
`scripts/export_baseline_history.py` 导出历史 CSV → `runtime.baseline_history` warm 共享
store → judge（`|baseline_dev|>3`）→ 断言仅 5号线（z≈500+）。

### 3.3 远端 A 最小验证（run_m3a.sh）
同受控数据 → producer → `scripts/export_baseline_ref.py` 聚合 provider CSV（三元组 +
mu/sigma）→ knowdb CSV 装载 → detect join → 断言仅 5号线（dev≈8）。

### 3.4 有界闭环校验（run_loop.sh）★ 最常用
单 daemon 全链路（`conf/loop.wfusion.toml`：producer_long + judge + detect）：
每轮事件时间 +120s 驱动 15s 窗持续收盘，注入 1 个 5号线=9000。逐轮断言
baseline 增长、两条通道各 +1；终态校验 judge 仅 5号线 z>3、detect dev≈8、
CSV μ≈1000、refresh 日志次数、末段内存平台。**相位关闭形态的默认回归**。

### 3.5 持续闭环（run.sh [--pg] [时长]）
长时间运行的注入循环（`INJ_INTERVAL` 默认 8s 一轮），配合看板实时观察。
`--pg`：docker postgres + `pg/baseline_records.sql` 建事实表 + postgres sink 双写 +
引擎 PG 直聚合供给（boot 以 `win_start='seed'` 占位 5 行，首轮收盘后删除）。
Ctrl-C（或时长到）自动恢复 conf、清理后台。

### 3.6 近端 B 相位同窗（run_phase.sh）
judge 按**事件时间**折相位桶、只与**历史同期**比较（早高峰只跟早高峰比）；相位随
收盘自然推进、无需外部刷新。配置：`[runtime]` 成对字段
`baseline_history_phase_period/bucket`。用例 `conf/loop.phase.wfusion.toml`：
period=240s / bucket=15s（=窗宽），忙时格(8..15) 3000、闲时格(0..7) 1000，奇偶轮
交替忙/闲。断言 judge 每轮恰 1 条（仅 5号线、z>3、首条 z≈10 作相位生效 canary）。
精确隔离语义（同相位过滤/半衰期参照=4×period/缺 event_time 全桶回退）由
`wf-cep baseline::` 单测锁定，本 e2e 防全引擎接线回归。

> ⚠ **相位常量同步**：conf 与 `scripts/gen_metrics_phase.py` 的 `PERIOD_S/BUCKET_S` 是两处
> 重复，run_phase.sh 启动会用 `--constants` 交叉校验拦截错位；生成器属性可自检：
> `python3 scripts/gen_metrics_phase.py --selfcheck`。
> 批量注入下事件判定滞后于收盘 → 越界自窗先入其相位桶，z 被摊薄（≈10 而非理论
> 400）仍 ≫3——daemon 级断言口径是"接线与稳定性"，隔离语义以单测为准。

### 3.7 refresh / PG 供给实证（run_refresh.sh / run_pg_refresh.sh）
验证供给**运行期可刷新**：同一批事件在"旧供给(μ≈1000)"下不告警；把供给改到
μ=5（CSV 覆盖 或 PG UPDATE）后等 ≥3 个刷新 tick，同批事件再注入 → 仅 5号线
告警 dev≈199；日志锚点 `provider refresh loaded table=baseline_ref`。

### 3.8 长跑内存平台（run_long.sh）
窗推进不漏（每轮 +6 窗×5 键 = 30 行）+ stats 状态随窗重置的内存平台（末段
6s×2 采样 RSS/commit 增长 ≤ `GROW_MB` 默认 80MB）。产物
`data/long_samples.tsv`。

### 3.9 看板（view.sh）
只读静态页，2s 轮询 `data/` 产物。区块：
总览 / 逐窗均值漂移 / **实时滚动基线 · judge**（近端 B 告警，本键最近 K 窗，相位
开启=同相位历史）/ **全局周期基线告警 · detect**（远端 A 告警）/ **detect 判定
依据 · 基线画像 baseline_ref**（供给 μ/σ，告警 mu 的来源）/ 内存平台曲线。
`--pg` 仅提示用途（页面读同一批引擎产物文件）。

---

## 4. 产物与数据文件

| 文件 | 内容 |
|---|---|
| `data/baseline/baseline.ndjson` | 收盘基线记录（每 15s 窗 × 线路） |
| `data/detect/judge.ndjson` | judge z 越界告警（entity/alert_type/value/z） |
| `data/detect/alerts.ndjson` | detect 偏离告警（…/mu/deviation） |
| `data/detect/baseline_ref.csv` | 供给镜像（n/sum/sum_sq/mu/sigma） |
| `data/metrics.ndjson` | 注入事件流 |
| `data/logs/wfusion*.log` | 引擎日志（refresh 锚点） |
| `data/loop_samples.tsv` | 内存/RSS/行数采样 |

---

## 5. 配置与参数

- **conf**：`conf/wfusion.toml`（smoke）· `conf/loop.wfusion.toml`（闭环）·
  `conf/loop.phase.wfusion.toml`（相位）· `conf/loop.pg.wfusion.toml`（PG）·
  `conf/refresh.wfusion.toml`（refresh 实证）· `conf/long.wfusion.toml`（长跑）。
- **规则**：`models/rules-loop/*.wfl`（producer_long + judge + detect）等，见各目录。
- **窗口长度**：demo 15s~1m；生产按设计 1h，消费侧按周期同相位合并。
- **注入**：`scripts/gen_metrics_live.py`（闭环）/ `scripts/gen_metrics_phase.py`（相位）/
  scenario `models/scenarios/metrics_baseline.wfg`。
- **近端 B runtime**：`baseline_history`（warm CSV）· `baseline_history_k`（默认 8）
  · `baseline_history_decay`（默认 true）· `baseline_history_phase_period/bucket`
  （成对开启相位；0 < bucket ≤ period）。

---

## 6. 校验体系

| 层 | 位置 | 断言 |
|---|---|---|
| 脚本断言 | `scripts/run_*.sh`（见 §3） | 各通道计数/数值/单调/内存 |
| 对拍脚本 | `scripts/verify_baseline.py` `scripts/verify_m2.py` `scripts/verify_detect.py` | 总量对拍 / 实体与 z / 契约自洽与 dev |
| 生成器自检 | `scripts/gen_metrics_phase.py --selfcheck` | 单调/均衡/忙闲纯度/spike/周期复现 |
| 引擎单测 | `wf-cep baseline::` `wf-runtime baseline_warm_tests` | 分桶/同相位过滤/decay/幂等/裁剪/并发/配置校验 |
| 基准 | `cargo test --release -p wf-cep baseline_bench -- --ignored --nocapture` | append/deviation_at 每 op 纳秒（规模缩放） |

---

## 7. 约定与排障

- **端口**：9800 = TCP 注入（各 case 共享——运行前脚本会 `lsof` 清理残留 daemon）；
  8124 = 看板；9901 = metrics exporter。
- **残留进程**：`lsof -ti:9800 | xargs kill` 是既存约定；跑新 case 前先清场。
- **二进制**：引擎/示例代码改动后需重建并刷 `~/bin/{wfusion,wfgen}`
  （warp-fusion release：`cargo build --release -p wfusion -p wfgen`）。
- **PG**：`docker compose up -d postgres` 起库；`pg/baseline_records.sql` 为事实表
  DDL；`./run.sh`/`scripts/run_pg_refresh.sh` 退出会自动把 knowdb 配置恢复 CSV 变体（强杀会
  残留，可用 `git checkout -- baseline/models/schemas/knowdb.toml` 还原）。
- **相位配置解析**：`scripts/run_phase.sh` 交叉校验 conf 与生成器常量，错位即 FAIL。

---

## 8. 参考

- 设计文档：`wp-reactor/docs/design/baseline-online-design.md`（以下相对各自仓库根；wp-reactor 为工作区兄弟仓库）
- 引擎实现：`wp-reactor/crates/wf-cep/src/baseline.rs`（store）·
  `crates/wf-cep/src/cep/eval/funcs_baseline.rs`（baseline_dev）·
  `crates/wf-runtime/src/lifecycle/bootstrap.rs`（相位安装/warm）·
  `crates/wf-runtime/src/lifecycle/baseline_warm_tests.rs`（warm/配置测试）·
  `crates/wf-cep/src/baseline_bench.rs`（性能基准）
- 供给刷新服务：`wp-knowledge`（RefreshService / loader 单表重载）
