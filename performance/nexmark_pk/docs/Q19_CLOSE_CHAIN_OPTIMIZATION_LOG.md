# Q19 Close 输出链性能优化记录（2026-08-25 ~ 08-26）

> 完整过程归档：方法、定位数据、优化实施、验证、教训。
> 数字口径与 `BENCH_RESULTS.md` / `TEST_PLAN.md` 一致；原始结果以 `data/*.txt` 为准。
> 环境：Mac mini 10 核 / 16GB，常载开发机（load 4~8，读数偏保守）。

---

## 1. 背景与目标

- **目标查询**：q19（`stats<10m:fixed> group by (b.auction) { b | top(10, b.price) }`）——per-auction top-10，30M 事件输出 **13.2M 条 close**（EMIT 实测，语义正确：每 auction 10 条 × ~26.5 万 auction × 5 窗口）。
- **起点**：diag 墙梯 full 档 306.9 ns/evt（增量 +197.6），daemon 采样热点 = `close_current_window` + `execute_close_direct_batch_columnar`。
- **目标**：定位 close 输出链瓶颈并优化，收益需可度量、字节一致、跨路径可复用。

---

## 2. 方法论：先定位，再优化（二分屏蔽法）

三条铁律（本 case 反复验证有效）：

1. **先定位再优化**：任何优化前必须量化成本归属（墙梯 / 二分屏蔽 / 微基准）。
2. **屏蔽用最简单的注释法**：逐段 `if false { ... }` / 早退 `continue` 屏蔽，隔离测量；**不能用整段 `/* */` 包裹**（循环体内有嵌套 `/* */` 会坏语法）。
3. **隔离微基准是最可信的度量**：`cargo test --release -p wf-engine nexmark_hotpath_bench -- --ignored --nocapture`——不受生产负载影响；生产 bench 单次噪声 ±15~20%，只作参考。

---

## 3. 定位过程

### 3.1 墙梯：full 档是主墙（q19 30M，优化前）

| 档 | ns/evt | 增量 | CPU% |
|---|---|---|---|
| rules | 109.2 | +12.2 | 467 |
| **full**（+序列化+sink 写） | **306.9** | **+197.6（64%）** | 600 |

墙判定：主墙 = full 档，CPU 600%（忙墙）。close 输出链（构建+序列化+写）占全链 64%。

### 3.2 二分屏蔽：full 增量归因（三分定位）

在 `stats_task.rs::close_current_window` 输出链上逐刀屏蔽（改代码 + 重编 + diag）：

| 刀 | 屏蔽段 | full 增量 | 段成本 |
|---|---|---|---|
| 基准 | 无 | +135~166 | — |
| 第一刀 | `builder.finish()` + `dispatch_columns`（投递） | +144.7 | **投递 ≈ 0**（blackhole payload_blind + parallel=8 异步稀释） |
| 第二刀 | + `execute_close_direct_batch_columnar`（列式 close+落列） | +32.9 | **execute ≈ 102-117 ns（~75%）** |
| 第三刀 | + `build_stats_close_output` row 字段注入 | +150.3（对照 166） | **row 注入 ≈ 16 ns** |

**归因结论**：

| 段 | ns/evt | 占比 |
|---|---|---|
| `execute_close_direct_batch_columnar` + 落列 | ~102-117 | **~75%** |
| CloseOutput 构建（row 注入 ~16 + 结构 ~17） | ~33 | ~25% |
| finish + dispatch 投递 | ~0 | ~0% |

### 3.3 微基准内部归因（close 链 553 ns/entry 构成）

| 子段 | ns/entry | 优化 |
|---|---|---|
| entity + wfx_id + summary | ~106-141 | wfx 前缀缓存 / entity 连续缓存 / `{:.1}` 快路径 |
| yield 字段循环 | ~91 | 批级 cvec（prepare）+ Lit 跳过 |
| close_batch_prepare（物化+fmt eval） | ~160 | 批级列式（层 1） |
| commit 落列 | ~20 | — |
| 系统列/遍历 | ~22（Arc 分配，已修） | OriginArcs 预建 |

**SHIELD-D 陷阱记录**：屏蔽系统列 push 后测出 163ns「系统列成本」，实为 **DCE 污染**——push 被屏蔽后 wfx_id/entity/summary 计算结果无人消费，LLVM 整段删除，测量高估 7 倍。用「只改 Arc::from → 预建 clone、其余原样」的 TEMP-VERIFY 交叉验证，真实成本仅 **22 ns**（每 close 2 次 Arc 堆分配）。

---

## 4. 优化实施（全部字节一致，测试锁定）

| # | 优化 | 位置 | 收益 |
|---|---|---|---|
| P1 | `write_fixed1`：summary 的 `{:.1}` 整数快路径（fract==0 且 \|v\|≤2^53 → itoa+".0"，-0.0 特判，非整数回退 std） | `alert.rs::build_summary_iter` | q19 close 链 -63~86 ns/entry；q6 match -69 ns/evt |
| P2 | `WfxPrefixCache`：wfx_id 前缀 FNV state 缓存（同桶 top-10 共享 rule/scope/fired_at/labels，只续 hash measure+origin） | `alert.rs` + `close_exec.rs` | 免每 close 重 hash 前缀 |
| P3 | `EntityIdCache`：相邻同 scope_key 复用 entity_id（close + match 双路径） | `alert.rs` + `close_exec.rs` + `match_exec.rs` | 免每行 resolve + value_to_string |
| P4 | fired_at 窗级缓存 / summary split 免 combine 深克隆 / 批级 fmt cvec（层 1，前序） | `close_exec.rs` | — |
| P5 | `OriginArcs`：origin/reason 静态字符串 Arc 预建（3 close reason），循环内 Arc::clone | `alert.rs` + `close_exec.rs` | ~22 ns/entry（TEMP-VERIFY 干净对照） |

**review 修正（5 轮）**：`WfxPrefixCache` 初版用 FNV-64 判定 labels 相同（`labels_hash`），review 发现 **2^-64 碰撞会静默产出与全量 build 不一致的 wfx_id**，破坏字节一致硬保证 → 改为 build 克隆一次 + `prefix_matches` 逐 label **借用**比较（零碰撞、零分配、可提前短路）；同步修正 `write_fixed1` doc 的错误数字（并行/隔离混算伪差）。

**可复用性收敛**：
- `write_fixed1` 在 `build_summary_iter` **单点收敛** → close 列式 / match 列式 / 逐条 close·match 全受益（each 的 summary 是 plan 常量，不命中）。
- `EntityIdCache` close + match 统一调用；each 不适用（每事件 key 可变，miss 率 100%）。
- `WfxPrefixCache` / `OriginArcs` 仅 close（match/each 的 fired_at 每事件变 / 已用 statics 预建 Arc）。

---

## 5. 验证数据

### 5.1 微基准（隔离，最可信）

| 项 | 优化前 | 优化后 | 变化 |
|---|---|---|---|
| q19 close 链 full(fmt) | 1534 ns/entry | **~545 ns/entry** | **-64%** |
| q6 match emit（全路径） | 744 ns/evt | 375 ns/evt | **-50%** |
| q16/q17/q18 close | 2036/2087/3295 | 908/1141/1339 | -55%/-45%/-59% |

测试：wf-engine **1170** / wf-lang 964 / wf-runtime 559 全绿（含 `write_fixed1_matches_std_fixed1` 1.5 万值逐值对拍、`wfx_prefix_matches_rejects_label_mismatch`、`origin_arcs_match_as_str`）。

### 5.2 diag 墙梯（同口径前后对比）

| 档 | 优化前 ns/evt | 优化后 | 变化 |
|---|---|---|---|
| rules | 109.2 | 106.2 | ~0 |
| **full** | **306.9** | **241.4** | **-21%** |
| full 增量 | +197.6 | +135.2 | **-32%** |
| full CPU 总工作量 | 5.52 核·s | 4.45 核·s | **-19%** |

### 5.3 生产 bench（rp=10，load 波动大只作参考）

q19 replay 30m：EPS 2.88~3.48M（load 4.2~6.6）。**结论：生产 EPS 受 load 主导，close 链优化的吞吐收益在 bench 口径不可见**（CPU 未饱和，瓶颈不在 close 链计算）。

### 5.4 并行度调优（rule_parallelism 10→20）

q19 是 keyed stats → `shardable`（`shard_count = rule_parallelism`）→ 按 auction 分片多 task，**close 已 10 路并行**（并非单线程！）。

| 指标 | rp=10 | **rp=20** |
|---|---|---|
| diag full 增量 | +135~166 | **+98.0（-28~41%）** |
| bench EPS | 4.12M（load 4.5） | **4.72 / 4.99M（load 3.4/3.6，+15~21%）** |
| EMIT | 13,243,649 | **13,243,649（逐位一致）** |
| RSS | 5.76-6.27GB | 5.32-5.41GB |

解释：close 是窗口边界突发（burst 时 10 路并行打满、burst 间 CPU 低 → bench CPU 45%）；20 路更细分片 → 每 task 突发块更小、分配/队列争用更分散。**结论：close 突发并行度不足，rp=20 有效**（配置调优，已恢复 rp=10 待决策）。

---

## 6. 关键教训

1. **二进制陈旧（最大坑）**：warp-fusion 用 git 依赖时，改 wp-reactor 不编进 wfusion。必须确认 `warp-fusion/Cargo.toml` 是 **path 依赖**（local_reactor 块），改后 `cargo build --release -p wfusion -p wfgen` 重编才生效。二进制 mtime/大小可核对。
2. **DCE 陷阱**：屏蔽「结果消费点」（如 vec push）后，前面的计算可能被编译器整段删除 → 测量高估。必须用「只改一个环节、结果仍被消费」的对照（TEMP-VERIFY）交叉验证。
3. **字节一致是硬保证**：哈希近似判定（FNV-64 比较 labels）在 2^-64 概率下静默产出错误 wfx_id——不可接受；精确借用比较零成本。
4. **并行/串行认知**：q19 的 close 已是 10 路并行（keyed 分片，`shard_count=rule_parallelism`）——「单线程串行」的说法对单 task 内成立、对整体不成立。定位前先读 spawn 装配代码。
5. **测量噪声**：并行测试套件的微基准数字受争用污染（同 run 内相对值可信、绝对值不可跨 run 比）；生产 bench 单次 ±15-20%（load 主导），多次取中位 + 同 load 对比才有意义。
6. **注释屏蔽**：整段 `/* */` 包裹遇到体内嵌套 `/* */` 会坏语法；循环体开头早退（`continue`）最稳。
7. **负增量档（decode）**：墙梯系统偏差大于该段真实成本时不可结论——不强行解读。

---

## 7. 遗留与建议

- **rp=20 调优**：收益已验证（diag -28~41%、bench +15~21%、正确性逐位一致），正式环境如需采用，先验证 q1/q5/q6 等其它查询无退化（配置共享）。
- **生产 bench 口径**：close 链优化是 CPU 侧收益（CPU 未饱和时吞吐不可见）；要看吞吐提升需配合 rp 调优或定位前级（注入/并行度）瓶颈。
- **工作区未提交**：wp-reactor 11 文件（P1-P5 + 测试）+ warp-fusion Cargo.toml/Cargo.lock（path 依赖，开发期配置，提交需谨慎）。
- **Q19 语义确认**：13.2M EMIT 是 per-auction top-10 的正确输出量（auction 域 ~26.5 万 × 10 × 5 窗）；若要全局 top-10 需去掉 `group by`（空键 stats），输出降 3 个量级、性能墙自然消失——取决于业务语义。

---

## 8. 机制类改进建议（流程/工具层面）

> 本 case 暴露的**流程/机制**问题（非代码 bug），按价值排序。

### M1. 实验配置用环境变量覆盖，不改配置文件 ✅ 已验证可用

- `bench.sh` / `diag.sh` 已支持 `RULE_PARALLELISM` / `PARSE_PARALLELISM` 环境变量（L101-102 读取，`write_conf` 用 sed 注入临时 conf）。
- **本次 rp=20 实验改 `conf/wfusion.toml` + 手动恢复是绕弯路**——有遗忘恢复污染后续实验的风险（`/tmp/wfusion.toml.bak` 手动回滚）。
- 正确姿势：`RULE_PARALLELISM=20 ./bench.sh q19 replay 30m` / `RULE_PARALLELISM=20 ./diag.sh q19 30m`，零残留。
- **行动**：后续一切并行度/帧参数实验走环境变量；确需改配置时实验结束立即恢复（本次已恢复）。

### M2. 二进制新鲜度自检（最大坑的机制化）✅ 已落地（2026-08-26）

- 现象：warp-fusion 用 git 依赖时，改 wp-reactor **不编进 wfusion**，浪费大量时间（本会话最大坑）。
- 落地：`bench.sh` / `diag.sh` 启动即校验（`check_binary_freshness`）：① `warp-fusion/Cargo.toml` 是否 path 依赖；② `find $WP_REACTOR/crates -name '*.rs' -newer $WFUSION` 是否存在比二进制新的源码（显示具体文件）。失败 → 警告（默认不阻塞）；`BIN_CHECK_STRICT=1` 拒绝；`SKIP_BIN_CHECK=1` 跳过。
- 验证：touch 源码 → 检出 + STRICT 拒绝（显示 `wf-engine/.../mod.rs`）✓；恢复 mtime → 无警告正常跑 ✓；`bash -n` 语法 ✓。

### M3. 微基准隔离运行（测量可信度）

- 现象：`cargo test` 并行线程争用污染数字（q19 close 链并行 1289 vs 隔离 553，伪差 2.3×；`{:.1}` 快路径并行口径误算为 683ns、隔离实测 63-86ns）。
- **行动**：性能 benchmark 用例跑隔离（单测试过滤）；文档明示「绝对值只信隔离口径，并行套件内只信同 run 相对值」。

### M4. 屏蔽实验的 DCE 防护

- 现象：屏蔽「结果消费点」（vec push）后，wfx_id/entity/summary 计算被 LLVM 整段删除 → 系统列段误测 163ns，TEMP-VERIFY（只改单环节、结果仍被消费）交叉验证真实仅 22ns，**高估 7 倍**。
- **行动**：屏蔽实验必须保留结果消费（`black_box` / 「只改一个环节」的对照设计）；把本节补进 `PERF_BISECTION_METHOD.md` 的屏蔽法章节。

### M5. 墙梯 emit 档不可用（cut_serialize 卡死）

- 现象：`perf-diag-wall.toml` 插入 `cut_serialize=true` 的 emit 档后档位切换挂死（AlertBatch 到 sink 即丢，投递链路 ack/背压断裂，哨兵等待永不满足）。
- **行动**：修复 dispatch 早退的 ack 协议，或从墙梯模板移除 emit 档并在注释标注「不可用」；本 case 已改走代码级屏蔽（M4）。

### M6. 性能断言测试 flaky ✅ 已落地（2026-08-26）

- 现象：`columnar_output_func_cell_beats_per_row` / `columnar_regex_match_overhead_bounded` 等比例式性能断言在全量并发时偶发失败（`columnar_regex_match_overhead_bounded` 实测 2.51x 失败 / 隔离 0.52x；`beats_per_row` ignored 集合并行 1.39x / 单独 0.75x——**同进程内相对比例都会被争用反转**）。
- 落地：`columnar_bench.rs` 全部 5 个比例式断言加 `#[ignore]`（含前序已加的 `columnar_not_overhead_bounded`，本次补 `cidr/regex/str_search/beats_per_row` 4 个）。
- 正确跑法：`cargo test --release -p wf-engine columnar_bench -- --ignored --nocapture --test-threads=1`（**必须串行**——ignored 集合内并行同样失真；串行 14 个全过，57s）。

### M7. 多视角 review 流程

- 现象：5 轮 review（正确性/性能/状态/边界/文档）才挖出 P0（WfxPrefixCache 的 FNV-64 labels 比较 → 2^-64 静默 wfx_id 错误）与 P1（doc 错误数字）。
- **行动**：性能优化合入前按 5 视角各查一遍（可固化为 checklist）：字节一致性 / 分配与缓存 / 缓存生命周期 / 边界特殊值 / 注释与测试。

### M8. 优化合入的三层证据 + 接入点清单

- 本 case 的验证分层：微基准（函数级）→ diag 墙梯（段级）→ 生产 bench（系统级），每级有独立陷阱（噪声 / DCE / load）。
- **行动**：优化合入需附三层证据；可复用优化附带「接入点清单」（哪些路径复用、哪些不适用及原因——见 §4 可复用性收敛）。
