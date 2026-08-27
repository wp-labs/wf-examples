# Q5 规则求值墙 CPU 采样定位（2026-08-26）

> 承接 `data/diag_q5_30m.txt` 墙梯结论的下一步：rules 主墙（+161.7 ns/evt，占全链 82.0%，
> CPU 831% 忙墙）→ 对 daemon 做 CPU 采样，定位段内热点函数。
> 环境：Mac mini 10 核 / 16GB（与墙梯同机）。原始数据：`data/sample_q5_30m_rules.txt`
> （macOS `/usr/bin/sample`，8s @20ms），分析器：`diag_analyze_sample.py`（自耗时段为本次新增）。

---

## 1. 采样方法

- **墙梯裁剪**：`STAGES=rules`（`./diag_sample.sh q5 30m rules`）——墙梯只剩
  `warmup + rules` 两档。warmup（全链路，丢弃不进结论）后 rules 档保持墙表同款门控
  （`cut_output=true`：规则求值全量跑，仅输出链直通）。
- **采样窗口**：`/usr/bin/sample <pid> 8s @20ms`，从 wfgen 打出 `stage 1 [rules] applied`
  起采（= daemon 已切好 rules 档门控，数据尚未发送），覆盖发送突发 + 处理稳态 + 尾部空闲。
- **本次 run 的 rules 档**：30M @ 4.17M EPS（240.0 ns/evt，CPU 723% avg）——与 6 档墙梯
  的 rules 档（5.07M EPS / 195.6 ns/evt / 831%）同形态、同忙墙结论；双档 run 无前序档
  预热分配器，绝对 EPS 略低属预期。
- **样本总量**：5520 线程根样本；其中空闲等待（cvwait 2508 + 信号量 322 + kevent 177 +
  swtch 47 ≈ 3054，55%）来自采样窗口尾部的档内收尾段——**忙侧有效样本 ≈ 2466**，
  下文占比按忙侧计。

## 2. 热点定位（自耗时 = 栈顶归属，段内最直接证据）

### 2.1 自耗时 top（忙侧占比，总忙侧 2466）

| 自耗时函数 | 样本 | 忙侧% | 归属段 |
|---|---|---|---|
| `CepStateMachine::advance_window`（自） | 291 | 11.8% | **事件路径 · hop 扇入 ×5** |
| `ScopeKey::from_value` | 59 | 2.4% | 键构建 |
| `close::evaluate_close` | 53 | 2.1% | close/conv 段 |
| `ScopeKey::eq`（PartialEq） | 52 | 2.1% | 键比较（HashMap 探测） |
| `ScopeKey::hash`（FoldHasher） | 50 | 2.0% | 键哈希 |
| `scan_expired_at_impl`（自） | 46 | 1.9% | **事件路径 · 过期扫描** |
| `BinaryHeap<InstanceKey>::pop` | 45 | 1.8% | 过期堆 |
| `hash_one::<InstanceKey>`（foldhash） | 44 | 1.8% | 键哈希 |
| `ScopeKey` drop_glue（37+20） | 57 | 2.3% | 键释放 |
| `Vec<StepState>::drop` | 37 | 1.5% | close 段状态清理 |
| `SipHasher::write` / `RandomState::hash_one` | 36+35 | 2.9% | 键哈希（std HashMap 侧） |
| `advance_at_with_diagnostics`（自） | 35 | 1.4% | 事件路径 · 键提取+hop 循环 |
| `scope_key_from_values` | 35 | 1.4% | 键构建 |
| `hashbrown remove_entry`（InstanceKey→Instance） | 34 | 1.4% | 实例表 |
| `smol_str::Repr::new` | 34 | 1.4% | 字符串构建 |
| `Vec<Value>::clone` | 34 | 1.4% | 键克隆（hop 扇入） |
| `event_bridge::extract_value` | 32 | 1.3% | 键提取（arrow 字段读） |
| `RuleTask::process_batch`（自） | 29 | 1.2% | rule_task 行循环 |
| `BinaryHeap::sift_up` | 29 | 1.2% | 过期堆 |

> 环境开销：mimalloc 系（`mi_*`）合计 ≈ 250+（~10%）、`memmove` 174（7.1%）、
> `memcmp` 51（2.1%）、`mach_absolute_time` 109 + `clock_gettime` 45（6.2%）——
> 分配/拷贝/时钟大部分是键 churn 与 close 构建的副产品，单独列账便于二分时对照。

### 2.2 inclusive 视角（引擎侧，占全样本含空闲）

| 函数 | 样本 | 全样本% |
|---|---|---|
| `RuleTask::process_batch` | 2232 | 40.4% |
| `run_rule_task` / `pull_and_advance` | 2098 / 2081 | 38.0 / 37.7% |
| `advance_at_with_diagnostics` | 1431 | 25.9% |
| `advance_window` | 1189 | 21.5% |
| `scan_expired_at_impl` | 591 | 10.7% |
| `evaluate_close` | 307 | 5.6% |
| `ConvStageTask::on_batch` / `try_seal` | 269 / 257 | 4.9 / 4.7% |
| `conv::apply_conv` + `precompute_sort_keys`（两处展开） | 264 + ~247 | 4.8 + ~4.5% |

## 3. 归因分账（忙侧）

```
事件路径（每事件一次，hop 扇入 ×5）
  advance_window 自       291  11.8%
  键构建 (from_value/from_values/extract/field_value)  ~156   6.3%
  键哈希 (ScopeKey hash/InstanceKey hash_one/Sip/Random) 165   6.7%
  键比较 (eq/Ord::cmp)                                ~72    2.9%
  键克隆+释放 (Value clone/ScopeKey drop/Value drop)   ~148   6.0%
  实例表 (remove_entry/insert/reserve_rehash)          ~72    2.9%
  其它 advance 附属 (check_threshold/new_at/StepState drop…) ~101  4.1%
                                      ──────────────────────────
  小计 ≈ 1005                                     ≈ 40.7%
过期扫描（每事件一次）
  scan_expired_at_impl 46 + heap pop 45 + sift_up 29  120    4.9%
close + conv 段（每 2s slide 收口一批，1500 批）
  evaluate_close 53 + conv（sort/top_ties/context 构建）≈ 470（inc.） ≈ 19%
分配器/内存搬运（mimalloc + memmove/memcmp/bzero）      ≈ 490    19.9%
时钟（mach_absolute_time + clock_gettime）              154     6.2%
```

**主墙判定（段内）**：q5 规则求值墙 = **每事件 hop 扇入 ×5 的 scope-key 构建/哈希/比较/
克隆/释放 churn（≈40%）+ 每事件过期扫描（≈5%）+ 每窗收口批的 close 求值与 conv 排序
（≈19%）**。分配器与 memmove 的近 20% 主要是前三者 churn 的付现（每事件 5 次 key 往返的
alloc/free + SmolStr/Value 拷贝）。

## 4. 为什么是「键 churn」——q5 形状核对（wf-engine/src/match_engine/mod.rs）

q5：`match<auction:hop(10s,2s)>`，key = `entity(digit, b.auction)` 单 Int 字段。
每事件在 `advance_at_with_diagnostics`（mod.rs L410-433）按 `k_min..=k_max` 扇入
**5 个覆盖窗口**，每个窗口各执行一遍：

1. `scope_key.clone()`（Vec<Value> 分配）→ 2. `scope_key_from_values` 重建
   `ScopeKey` → 3. `InstanceKey::fixed` 再 `scope_key.clone()`
   → 4. `instances.contains_key` + `entry` 探测（哈希 ×2 + eq）→ 5. 新实例时
   `push_expiry_candidate`（堆 push）。

**H1（§8.1）已把 1-3 的重复构建消除**：skey 建一次、非命中窗口零分配，
`advance_window` 每窗口成本 105→86 ns。

close 侧：1500 个 slide 收口批（每批 ~1.2 万 auction），conv `sort(-n)|top_ties(1)`
对整批做 **O(n log n) 全量排序**，且 `precompute_sort_keys` 每元素建 eval context
（`HashMap<SmolStr,Value>` 分配）——两个独立样本簇（`precompute_sort_keys` 125+122、
`build_eval_context` 的 HashMap insert/reserve_rehash）证实。

## 5. 优化候选（下一轮二分屏蔽的起点，按预期收益排序）

| # | 候选 | 位置 | 预期形态 | 语义风险 |
|---|---|---|---|---|
| H1 | **hop 扇入批级键预解析**：把 `scope_key.clone` → `scope_key_from_values` → `InstanceKey::fixed` 的每窗口 3 次键构建合并为一次组合键构建（单 Int 键时组合键 = `(i64, k*slide_ns)` 直接内联哈希，免 enum 分发） | `mod.rs::advance_at_with_diagnostics` / `key.rs` | 事件路径 ≈40% 中砍掉大部分构建/克隆/释放 | 需对拍列式 `scope_key_columnar` 的配对序（注释已点名） |
| H2 | **单键 ScopeKey 特化**：`ScopeKey::Int` 已 8 字节，但 hash/eq/clone/drop 走 enum 分发 + drop_glue；常见单 Int 键可走专用内联 key（对齐 `ValueKey` 的 dispatch 思路） | `key.rs` | 哈希/比较/释放簇（~9%） | 哈希序需与 foldhash 现状一致（对拍锁定） |
| H3 | **过期扫描批量水位化**：当前每事件 `scan_expired_at_impl` + 堆 peek；事件时间单调推进时过期只发生在 slide 边界——可 slide 粒度批量收口 | `mod.rs::scan_expired_at_impl` + `rule_task.rs` 行循环 | 每事件 4.9% → 摊销 | hop 无界预算语义（q16 修过的尾部桶）须保住 |
| H4 | **conv `sort(-n)\|top_ties(1)` 单趟**：只为 top-1 时全量排序是 O(n log n)；改单趟找最大值 + 保留并列（O(n)，语义等价——并列全输出是权威 JOIN 语义，必须保留） | `conv.rs::apply_chain` | conv 段（~19% inc.） | top_ties 并列语义由现有测试锁定 |
| H5 | **conv 排序键预提取免 context**：单 sort key（`n`）直读 `close_step_data` 的 measure，免每元素建 HashMap context | `conv.rs::precompute_sort_keys` / `build_eval_context` | `precompute_sort_keys` 簇 | 多 key/表达式场景需回退通用路径 |

---

## 8. 二分屏蔽量化结果（2026-08-26，隔离微基准为主）

> 方法（Q19 日志铁律）：隔离微基准最可信（不受开发机负载干扰）；墙梯级
> 前后对比受 load 主导（本次 08-26_21:00 前后 load 3.7→7.1，单次 ±15-20%），
> 只作参考。微基准：`cargo test --release -p wf-engine --lib hop_bench -- --ignored --nocapture --test-threads=1`。

### 8.1 H1（hop 键 churn）——已实施，微基准验证生效，生产效应在噪声内

| 口径 | 未优化 | H1 后 | 变化 |
|---|---|---|---|
| 隔离键路径（键构建+实例表探测，60k 键） | 221.8 ns/evt | 128.6 ns/evt | **-93.2 ns/evt（-42%）** |
| 机器内 hop(10,2) advance（含每事件 scan，200k） | 585 ns/evt | **494 ns/evt** | **-91 ns/evt（-15.6%）** |
| hop(10,2) 每窗口成本 | 105 ns/window | 86 ns/window | -19 ns/window |
| hop(10,10) / fixed（单窗口参照） | 162 / 154 | 151 / 150 | ~0（单窗口路径无回归） |

**实施**（`wf-engine/src/match_engine/match_engine/mod.rs`）：typed `skey` 提升到
窗口扇出循环外——hop 每事件 5 窗口原先各自 `scope_key_from_values` 重建 +
`scope_key.clone()`（Vec<Value> 堆分配）；现在 skey 只建一次，`advance_window`
改收 `&[Value]` + `&ScopeKey`，非命中窗口零分配（ctx 仅命中时 `to_vec()`，与旧
每次 clone 成本持平、非命中纯省）。语义不变（`scope_key_from_values` 纯函数，
对拍测试锁定）。

**正确性**：wf-engine 1183 + wf-runtime 581 + wfgen 172 全绿；hop/deferred 集成
通过。q5.wfl 测试块（`q5_top1` 等）失败为**既有问题**（stash 对照确认 H1 前后
同样 `hits==1 got 5`）：hop 每事件 5 覆盖窗各命中一次，测试块按 fixed-10s 期望
写的，未随 hop 化更新——与本次改动无关。

**生产墙梯**（08-26_21:04，load=5.4）：rules 5.00M EPS / 199.9 ns/evt，对照原报告
（load=4.3）5.11M / 195.6——整档在噪声内，机器路径 -15.6% 折算到规则求值段
（占墙 ~40%）≈ -6%，低于本机单次噪声。

**生产对照协议（08-26_21:19，背靠背同口径）**：EPS 受负载/帧页缓存干扰
（第二次跑时帧文件被页缓存命中，前序档恢复到 ~30M——证实早前 runs 的 I/O
污染），**但 rules 段 CPU 工作量（CPU%×时长）是负载稳健口径**：

| 口径 | H1 | 无 H1（本次） | 无 H1（原报告） |
|---|---|---|---|
| rules EPS | 5.01M | 4.75M | 5.11M |
| rules 时长 | 5.99s | 6.32s | 5.87s |
| rules CPU% avg | 772% | 806% | 831% |
| **rules CPU 工作量** | **1.54 µs/evt** | 1.70 µs/evt | 1.62 µs/evt |

**H1 生产效应 = rules 段 CPU 工作量 -7~9%**（对照本次无 H1 1.70 为 -9.4%；对照
原报告 1.62 为 -4.9%）。EPS 未等比提升因为 rules 段非纯 CPU 约束（I/O + 调度
噪声），且单次 run ±3%。结论：H1 是真实但温和的生产优化，机器路径 -15.6%
、rules 段 CPU -7~9%。

### 8.2 H4（conv sort 本体）——量化：单趟节约 63-66%，但 conv 非墙主段

| 对照（同克隆口径，10k 批） | sort+top_ties | 单趟 max+tie | 节约 |
|---|---|---|---|
| 无并列 | 217 ns/row | 81 ns/row | 63% |
| 中等并列（tie_every=100） | 236 ns/row | 81 ns/row | 66% |
| 高并列（tie_every=5000） | 220 ns/row | 82 ns/row | 63% |

（含克隆口径 81 ns/row 中 ~78 ns 是基准内 clone；生产 `process_bucket` 的 closes
是 move 进来，真实成本 ≈ sort 140 ns/row vs 单趟 ~3 ns/row。）

**墙归因（干净 Cut-C，08-26_22:01，load 1.96 起步）**：屏蔽 apply_conv 后 rules
CPU 工作量 45.3 vs 基准 46.2 核·s——**conv 变换只占墙 ~2%**（微基准的 140 ns/row
× 18M 行估算被高估：生产批更小/排序更快）。**H4 不做**（收益 ~1-2% 墙，不入主路径）。

### 8.3 H2（单 Int 键特化）——量化后否决（机器路径无效果）

| 口径（foldhash，隔离探测 60k 键 × 5 窗/事件） | ns/evt |
|---|---|
| 派生 Hash 枚举键 `(ScopeKey, i64)` | 43.8~65.2（run 间波动 ±20%） |
| 平铺 Hash newtype（`FlatIntKey`，16B 键） | ~11-13 |
| 单切片 write 真枚举（`SliceKey`，H2 实现形态） | ~14 |
| 组合键 `(i64, i64)`（16B） | ~11-13 |

实现候选 = 给 `ScopeKey` 写手动 `impl Hash`（tag+payload 一次 `write`）。已实现并
验证：隔离探测确实从 ~65 降到 ~49 ns/evt（跨 run 对照，非同日），但**机器 hop
advance 路径 A/B 无差异**——H2 版 505/501/518 vs H1-only 543/507/504（中位
~505 vs ~507，纯噪声）。已回退（`git stash drop`）。

**教训**：隔离键探测基准（cache-hot 60k 小值表）高估了哈希函数成本——真实机器
探测被 40B InstanceKey 尺寸 + Instance 大值内联 + 缓存行为主导，哈希计算占比
极小（每 5 窗事件差 2-4 ns，埋没在 500 ns 机器路径 ±3% 噪声里）。键形状特化
（16B 内联键）才能拿全差量，但需并行 map 类型（侵入），收益 ~3-4% 墙。

### 8.4 并行度实验（rp=20）——无收益，不做

Q19 日志 §5.4 的 rp=20 收益（close 突发规则 +15~21%）**不迁移到 q5**：

| 口径 | rp=10（H1） | **rp=20** |
|---|---|---|
| rules EPS | 5.01M | 4.88M |
| rules CPU 工作量 | 46.2 核·s（1.54 µs/evt） | 47.9 核·s（1.60 µs/evt） |

q5 的墙是**稳态每事件 advance**（非 q19 的窗口边界 close 突发）——20 task 超订
10 核只加同步/上下文切换开销，CPU 工作量反升 +3.7%。配置保持 rp=10。

### 8.5 结论与建议

- **H1 已落地**（安全、全绿、机器路径 -15.6%、生产 rules 段 CPU -7~9%）。
- **H2 量化后否决**（手动 Hash 机器路径无效果，已回退）；键形状特化需并行 map，
  收益 ~3-4% 墙，暂不做。
- **H4 量化后否决**（干净 Cut-C：conv 只占墙 ~2%）。
- **rp=20 实验无收益**（稳态 advance 非突发 close，CPU 工作量反升）。
- **H3（过期扫描）**：hop 微基准显示每事件 scan 摊还 ≈ 0（heap peek 空时廉价），
  优先级下调。
- **剩余候选（各 ~5% 墙，复杂度递增）**：
  1. ~~双探测合并~~（`contains_key`+`entry` 每窗两次哈希探测，合并需重构 limits
     驱逐与 entry 借用；~5% 墙，中风险）
  2. **批级临时 Vec 消减 ✅ 已实施**（见 §8.6）
  3. conv 单趟（~1-2% 墙）留待 100m/更高并行度再评估。
- **结构性现实**：q5 的 5× hop 扇入是语义固有（Flink 同形状）；已吃光廉价收益，
  剩余为增量优化。若目标是整体吞吐，q1/q2 等无状态查询墙结构不同、回报更大。

### 8.6 批级临时 Vec 消减（2026-08-26，已实施）

**改动**（`rule_task.rs`）：批处理行域从每批 `Vec<usize>`（分片批 =
`shard_rows` 的 u32→usize 转换，q5 10k 行/批 × 300 批 × 10 shard ≈ 240MB
分配+转换 churn；未分片 = 恒等 `(0..n)` 纯浪费）改为借用枚举 `RowDomain`
（`Sharded(&[u32])` / `Full(n)` + `row_at(i)` 索引）；key_join 规则（q4/q6）
才 `to_vec()` 物化。另：`times` 从 `vec![0;n]` 零填+覆盖双写改为 `with_capacity`
+ push（事件时间列存在时免一次零填）。

**验证**：wf-runtime 581 + deferred 集成 30 + wf-engine 1183 全绿（一次 flaky 为
EOS 竞态复现测试的既有异步抖动，隔离复跑通过）。

**生产测量**：rules CPU 工作量 48.4 vs H1 基线 46.2 核·s——~1% 预期效果埋在
指标 ±5% 方差里（同配置历史 45.3~50.4，随负载漂移），本机分辨不了。改动按
「严格减分配 + 全绿 + 语义不变」保留，非按实测收益。

### 8.7 实现 review（5 视角，M7 checklist）

| 视角 | 结论 |
|---|---|
| 字节一致性 | ✅ skey 纯函数提升同输入同输出；MatchedContext 值相同（to_vec vs 旧 clone/move）；列式/行式 hash 契约保持（构建函数未变）；RowDomain 迭代/to_vec/times 序列逐位一致（新单元测试锁定） |
| 分配与缓存 | ⚠→✅ q5 每事件 6→1 alloc；**发现**：fixed/sliding 单窗口每事件 match 规则 +1 to_vec alloc/事件（理论）→ 实测 **0 回归**（374 vs 374 ns/evt，hop_bench `bench_fixed_match_every_event` A/B）→ 接受 |
| 借用/生命周期 | ✅ skey/RowDomain 借用均限单函数作用域；limits 驱逐（map 变更）在 owned key 上，无借用冲突 |
| 边界特殊值 | ✅ 空键 Empty / 多键 Pair / key_join override / 空批 / 空分片 / 乱序 hop 范围 / accu 规则逐项核对 |
| 注释与测试 | ✅ 修：`too_many_arguments` 注释 7→8 组参数；补：`row_domain_tests`（Sharded/Full 等价、to_vec 逐位一致、空域）3 例 + `l3::hop` H1 专项（hop/fixed 命中 ctx.scope_key 逐位一致、AND+close 事件路径不命中）3 例 → wf-engine 1186 / wf-runtime 584 全绿 |

## 6. 复现与工具

```bash
# 采样（泛化后 diag_sample.sh 支持任意墙梯档；默认 full 保持旧行为）
./diag_sample.sh q5 30m rules          # → data/sample_q5_30m_rules.txt + 分析输出
./diag_sample.sh q19 30m               # 旧行为不变（full 档，12s）

# 分析（diag_analyze_sample.py 本次新增「Sort by top of stack」自耗时段）
python3 diag_analyze_sample.py data/sample_q5_30m_rules.txt 40
```

工具变更（本次）：`diag_sample.sh` 泛化（第 3 参 = 墙梯档名、第 4 参 = 采样秒数、
输出名带档后缀、分析收敛到共享分析器）；`diag_analyze_sample.py` 新增自耗时段。

## 7. 备注

- **采样窗口含 idle 尾部**：8s 采样覆盖 ~5.7s 忙 + ~2.3s 收尾空闲，忙侧占比按 2466 样本
  归一；如需更纯的忙窗口可把 SECS 收到 6s（`./diag_sample.sh q5 30m rules 6`）。
- **双档 run 与 6 档墙梯的 EPS 差**（4.17M vs 5.07M）：无前序档预热分配器 + 常载机
  load 波动，墙形态与忙墙判定一致，热点归属不受影响。
- **本报告 = 定位 + H1 已实施**：H1 落地（wp-reactor `mod.rs` + 微基准），H2~H5 为
  下一轮候选，按 `PERF_BISECTION_METHOD.md` 纪律（先量化、注释屏蔽、隔离微基准、
  字节一致对拍）逐项验证。本次机器负载（load 3.7→7.1）使墙梯前后对比不可信，
  生产 EPS 验证待安静机/多次取中位。
