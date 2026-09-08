# models —— 规则 / schema / 场景

## rules 三目录（都是必需，形态不同）

| 目录 | 内容 | 被谁引用 | 形态 |
|---|---|---|---|
| `rules/` | `baseline_producer`(1m 窗) · `baseline_judge` · `baseline_detect` | `conf/wfusion.toml`（smoke）、`conf/refresh.wfusion.toml`、`test/*.batch.toml`（m2/m3a） | **canonical 源**：生产/最小验证形态 |
| `rules-long/` | `baseline_producer_long`(15s 窗) | `conf/long.wfusion.toml`（run_long.sh） | **canonical 源**：长跑专用 producer |
| `rules-loop/` | `producer_long` + `judge` + `detect` | `conf/loop*.toml`（run_loop / run.sh / run_phase / --pg） | **组合快照**：闭环 conf 的 `rules` 是单 glob，三条规则必须同目录 |

## ⚠ 组合快照同步（canonical → 副本）

引擎 conf 的 `rules` 是**单个 glob**，闭环形态需要 judge+detect+producer_long 落于
同一目录，因此 `rules-loop/` 是复制出来的组合快照。下列三对文件必须**逐字一致**：

- `rules/baseline_judge.wfl` ⇔ `rules-loop/baseline_judge.wfl`
- `rules/baseline_detect.wfl` ⇔ `rules-loop/baseline_detect.wfl`
- `rules-long/baseline_producer_long.wfl` ⇔ `rules-loop/baseline_producer_long.wfl`

**改动 canonical 源后必须同步副本**，否则闭环形态与最小验证形态行为漂移。
一致性由 `scripts/check_rules_sync.sh` 守卫（挂在 run_loop.sh / run_phase.sh 启动时，
也可单独运行：`./scripts/check_rules_sync.sh`）。

## 其它

- `schemas/`：窗口/流 schema（`metrics.wfs` / `baseline_ref.wfs` / `windows.toml`）与
  knowdb 供给配置（`knowdb.toml`=CSV 变体 / `knowdb.pg.toml`=PG 变体，运行期二选一）。
- `scenarios/`：`metrics_baseline.wfg`——确定性注入场景（seed=42）。
