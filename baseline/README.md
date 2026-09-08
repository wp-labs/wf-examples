# baseline —— 在线行为基线案例（地铁客流）

> 把 `baseline()` 从"占位能力"升级为**可信在线行为基线原语**的端到端示例：
> 客流指标按窗收盘成 `n/sum/sum_sq` 可加三元组 → 近端 **judge**（实时滚动 / 相位
> 同窗 z 判定）+ 远端 **detect**（全局周期基线供给判定）→ 看板可视化。
> 📖 **详细使用指南见 [USAGE.md](USAGE.md)**（运行模式/契约/产物/校验/排障）。

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
[USAGE.md §7](USAGE.md#7-约定与排障)）。

```bash
cd baseline
./smoke.sh                     # ① 数据生产 + 对拍（秒级）
./scripts/run_loop.sh          # ② 有界闭环校验：两条判定通道断言（≈1 分钟）
./view.sh                      # ③ 看板 → http://localhost:8124/view/
```

全部 10 个入口（m2/m3a/long/loop/refresh/pg_refresh/phase/run --pg/view）的
用途、期望断言与产物见 **[USAGE.md §1/§3](USAGE.md#1-快速开始)**。

## 目录导览

| 路径 | 内容 |
|---|---|
| `USAGE.md` | 完整使用指南（模式/契约/产物/校验/排障/参考） |
| `run.sh` · `view.sh` · `smoke.sh` | 持续闭环 / 看板 / 生产对拍 |
| `scripts/` | 各验证入口（run_loop / run_phase / run_long / run_m2 / m3a / refresh / pg_refresh）与生成/导出/校验脚本 |
| `conf/` · `models/` · `topology/` | 引擎配置（含相位档 `conf/loop.phase.wfusion.toml`）/ 规则与 schema / 连接器 |
| `pg/` | PG 事实表 DDL（`pg/baseline_records.sql`） |
| `view/` | 看板页面 |

## 参考

- 设计文档：`wp-reactor/docs/design/baseline-online-design.md`
- 引擎实现：`wp-reactor/crates/wf-cep/src/baseline.rs` ·
  `wp-reactor/crates/wf-cep/src/cep/eval/funcs_baseline.rs` ·
  `wp-reactor/crates/wf-runtime/src/lifecycle/bootstrap.rs` ·
  `wp-reactor/crates/wf-cep/src/baseline_bench.rs`
  （以下相对各自仓库根；wp-reactor 为工作区兄弟仓库）
