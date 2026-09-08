# baseline —— 在线行为基线案例（地铁客流）

> 把 `baseline()` 从"占位能力"升级为**可信在线行为基线原语**的端到端示例：
> 客流指标按窗收盘成 `n/sum/sum_sq` 可加三元组 → 近端 **judge**（实时滚动 / 相位
> 同窗 z 判定）+ 远端 **detect**（全局周期基线供给判定）→ 看板可视化。
> 📖 **baseline 能力方案见 [USAGE.md](USAGE.md)**（作用 / 基线构建 / 检测方案）；本文是案例操作手册。

**业务示意**：5 条地铁线路（`1号线`~`5号线`）每 15s 上报客流强度
（`metric=flow`，正常 ≈1000±25）。引擎把每条线路客流按窗**结账归档**成三元组，
随时可推导线路画像 μ/σ。`5号线` 每轮注入一次客流 9000（≈9× 异常大客流），两条
判定通道独立告警；`1~4号线` 永不误报。

```
metrics_stream ─▶ producer stats 窗 group by (entity, metric)
                  { count as n; sum(v) as s; sumsq(v) as ss }
            ─▶ baseline_out ─▶ baseline.ndjson
                  ├▶ 近端 B · judge   （共享内存 store，|z|>3）
                  ├▶ 远端 A · detect  （knowdb 供给，(v−μ)/μ>5）
                  └（PG 模式）postgres sink → baseline_records 事实表
```

## 快速开始

前置：`wfusion` / `wfgen` 在 PATH（本地引擎改动后需重建刷 `~/bin/`，见
[README 排障](#约定与排障)）。

```bash
cd baseline
./smoke.sh                     # ① 数据生产 + 对拍（秒级）
./scripts/run_loop.sh          # ② 有界闭环校验：两条判定通道断言（≈1 分钟）
./view.sh                      # ③ 看板 → http://localhost:8124/view/
```

全部 10 个入口的用途/期望断言/产物见下表与 **[USAGE.md §5 案例落地](USAGE.md#5-案例落地-wf-examplesbaseline)**。

## 目录导览

| 路径 | 内容 |
|---|---|
| `USAGE.md` | baseline 能力方案（作用/构建/检测/决策/参考） |
| `run.sh` · `view.sh` · `smoke.sh` | 持续闭环 / 看板 / 生产对拍 |
| `scripts/` | 各验证入口（`scripts/run_loop.sh` / `run_phase.sh` / `run_long.sh` / `run_m2.sh` / `run_m3a.sh` / `run_refresh.sh` / `run_pg_refresh.sh`）与生成/导出/校验脚本 |
| `conf/` · `models/` · `topology/` | 引擎配置（含相位档 `conf/loop.phase.wfusion.toml`）/ 规则与 schema / 连接器 |
| `pg/` | PG 事实表 DDL（`pg/baseline_records.sql`） |
| `view/` | 看板页面 |

## 约定与排障

- **端口**：9800 = TCP 注入（各 case 共享，脚本启动前会清场并等端口释放）；
  8124 = 看板；9901 = metrics exporter。
- **残留进程**：`lsof -ti:9800 \| xargs kill` 后重试（偶发首轮 rx=0 多为残留
  daemon 抢占）。
- **看板数据**：先跑一次 `./run.sh` 或 `./scripts/run_loop.sh` 生成闭环产物；
  只跑 `smoke.sh` 后直接开看板会看到不完整数据。
- **二进制**：引擎改动后重建刷 `~/bin/{wfusion,wfgen}`（warp-fusion release）。
- **PG conf 恢复**：`run.sh --pg`/`run_pg_refresh.sh` 强杀会残留 PG 变体
  `models/schemas/knowdb.toml`，可用 `git checkout -- models/schemas/knowdb.toml` 还原。

## 参考

- 设计文档：`wp-reactor/docs/design/baseline-online-design.md`
- 引擎实现：`wp-reactor/crates/wf-cep/src/baseline.rs` ·
  `wp-reactor/crates/wf-cep/src/cep/eval/funcs_baseline.rs` ·
  `wp-reactor/crates/wf-runtime/src/lifecycle/bootstrap.rs` ·
  `wp-reactor/crates/wf-cep/src/baseline_bench.rs`
  （以下相对各自仓库根；wp-reactor 为工作区兄弟仓库）
