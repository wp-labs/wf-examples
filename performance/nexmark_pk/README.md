# nexmark_pk — NEXMark 基准：吞吐 PK + 正确性验证（对齐 Flink 官方基线）

本目录是 wfusion 引擎的 **NEXMark 基准套件**：同一份确定性基准数据（Q1~Q22 全量查询），
对照阿里 Nexmark 白皮书的 OSS Flink / VVR 基线做吞吐 PK，并用真实 WFL 规则引擎 ground truth
验证输出正确性。四个核心工具：

| 工具 | 回答的问题 |
|---|---|
| `bench.sh` | **吞吐/内存是多少**（EPS / RSS / CPU，对 Flink PK） |
| `diag.sh` | **墙在管线哪一段**（性能墙定位） |
| `verify_daemon.sh` | **输出是否正确**（daemon+TCP 路径 vs 期望对拍） |
| `verify_wfg.sh` | **规则/语料本身对不对**（五层：L0 静态校验 · L0' 规则内联用例 · L1 注入断言 hit/near_miss/miss · L2 期望文件 · **L3 引擎级对拍**——L3 默认开） |

背景（事件模型 / 查询语义 / 正确性标准）见 [`docs/NEXMARK.md`](docs/NEXMARK.md)；
查询覆盖判定见 [`docs/CAPABILITY_GAP_MATRIX.md`](docs/CAPABILITY_GAP_MATRIX.md)；
实测结果归档见 [`docs/BENCH_RESULTS.md`](docs/BENCH_RESULTS.md)。

## 快速开始

```bash
./bench.sh q1 replay 10m        # 性能测试：q1 单查询，10M 数据重放
./bench.sh all replay 30m       # 全量 22 查询吞吐 PK（all=逐个单规则，不含 q6，见下）
./bench.sh mix replay 10m       # 混跑：全部规则一个 daemon 同时跑（多规则同跑，对照 all）
./verify_daemon.sh all 1m       # 正确性验证：daemon+TCP 路径全量对拍（~2-4 分钟）
./verify_wfg.sh all --lint-only # L0 + L0' 静态层（22 查询，秒级；含规则内联用例）
./verify_wfg.sh q1 q2 --duration 10s # 按 .wfg 语料验证规则语义（注入断言）
./verify_wfg.sh all --with-engine   # 全量 22 查询 + 引擎级对拍（用各语料自带的 #[duration]）
./verify_wfg.sh all --no-engine      # 只要期望级（L0/L0'/L1/L2），不跑真引擎
./diag.sh q5 10m                # 性能诊断：定位 q5 的墙在哪一段
```

## 1. 性能测试：bench.sh

```bash
./bench.sh [query=all|mix|q1..q22] [feed=replay|stream] [total=100m|30m|10m|1m]
WARMUP=1 ./bench.sh all replay 30m     # 预热一轮再测（stash 重建后首跑偏低，剔除）
RULE_PARALLELISM=6 ./bench.sh q1 replay 10m   # 调 rule_shards（parse 已无池，见下）
CONNECTIONS=4 SHARD_KEYS="bid_events:auction,..." ./bench.sh q2 replay 30m  # 键闭包分片
./bench.sh mix replay 30m      # 混跑：全部规则同时加载进一个 daemon（测多规则同跑）
```

- **feed=replay**（默认，PK 口径）：预编码 Arrow 帧线速重放，**测引擎峰值持续吞吐**。
  事件按 30s 桶序、事件时间固定 100µs/事件；帧缓存 `data/bench_<total>_v5.frames`
  跨查询复用（存在即不重生成，`DATA_VER` 指纹防旧缓存静默复用）。
- **feed=stream**：wfgen 实时生成按 `RATE` 注入（客户端编码上限 ~760k/s，**非引擎能力**，
  EPS 不可比）。只用于：① 真实时间窗口语义（watermark/驱逐按真实时间发生）；② 长时稳定性
  内存有界（看 RSS 是否泄漏）；③ 生产形态模拟（late / `allowed_lateness`）。**不用于**吞吐对比。
- **all 与 mix 的区别**：`all` = 每个查询**单独**一个 daemon 跑（单规则集，规则互不干扰），
  输出每查询一行；`mix` = 全部规则**同时**加载进一个 daemon 混跑（共享 parse/rule 并行度，
  规则间资源竞争真实可见），只输出一行合并吞吐。`all` 用来逐查询横向对比，`mix` 用来
  看规则叠加时的真实合并吞吐（可与 all 的均值对照出同跑开销）。
- **mix 的规模（2026-08-30 修复后）**：曾因 join 索引单 key 独占冻结在 30M
  （q8 按 seller / q20 按 id 共窗，后注册者回退全窗扫描 → q8 deferred join
  O(全窗)×pending 卡死）——wp-reactor 改**多 key join 索引**后 30M 已正常
  （EPS ~760K、~105s、clean；1m/10m 同理）。100M 未验证：30M RSS≈9.3GB
  线性外推 ~30GB，需大内存机。
- **all/mix 均不含 q6**：join-then-key 单线程 + 逐事件 sliding 状态机，架构性慢，单跑研究用
  `./bench.sh q6 ...`。

### 输出行怎么读

```
q1/replay: EPS=12,881,009 · RSS_peak=3,571MB · CPU 240%avg/382%max · evict=39
           · appended=30,000,000/30,000,000 · eps_mode=sentinel · conns=1
           · [clean] p=10 r=10 c=1 frame_mb=8 load=1.6 · 08-30_00:42:10
```

| 列 | 含义 |
|---|---|
| `EPS` | 引擎消化速率 = 哨兵窗 Σn/(max_emit−min_start)（整轮均值，非峰值） |
| `RSS_peak` | 全生命周期驻留峰值（100ms 采样） |
| `CPU avg/max` | **引擎活跃窗**内核占数（多核可 >100%，100% ≈ 1 核满） |
| `evict` | 窗口驱逐数（有值属正常窗口关闭） |
| `appended` | 追平 = 数据完整性无丢失（旁证） |
| `eps_mode` | `sentinel`=精确口径；`metrics-append`/`⚠TIMEOUT`=兑底值，只作量级参考 |
| `[clean]` | 致命计数器（append_failed/dropped_late/cursor_gap/...）全零 = 测量可信 |
| `p/r/c/frame_mb` | parse/rule 并行度、连接数、帧 cap（引用数字时必须带上） |

结果写 `data/bench_<q>_<feed>.txt`；哨兵流 `data/perf_sentinel.ndjson`、计数器流
`data/metrics.ndjson`、引擎日志 `data/{wfusion,daemon}.log`。

### 测量纪律（违反会得出假结论）

1. **先看 `eps_mode=`**：非 sentinel 的 EPS/CPU 只作量级参考。
2. **预热轮**：stash 重建后首跑系统性偏低（曾三次复现），`WARMUP=1` 剔除。
3. **A/B 必须不限速**：`RATE` 会把 EPS 封顶（限速 = 测供给不是引擎）。
4. **同时段交错对比**：EPS 与 RSS_peak 双峰相位强相关（同配置差 ±8%），结论按 RSS 相位配对；
   单轮数字只作量级参考。
5. **引用 RSS 必须标注 `window_buffer_bytes`**（入流背压预算，默认 64MiB；旧 `parse_buffer_bytes`
   已于 2026-08-31 decode-route-merge 移除，引擎不再使用）——预算口径不同的 EPS/RSS 不可直接对等。
6. **CPU 是活跃窗口径**（哨兵 start/emit ± 0.5s，100ms cputime 差分）：全生命周期统计会把
   亚秒级突发（q2/q8 ≈ 0.4s）稀释成 0% 假象；短跑（<2s）读数只宜作量级参考。

完整度量口径（哨兵链路 / 兑底 / 采样）见 `docs/NEXMARK.md` §7。

## 2. 正确性验证：怎么是正确的

### 正确性标准

**正确 = 两条同时成立**：

1. **数据完整性无丢失** → 结果行 `[clean]`：`appended` 追平（如 30M/30M）且致命计数器
   （append_failed / dropped_late / cursor_gap / channel_full / sink_dispatch_failed）全零。
2. **输出与确定性 ground truth 一致** → 每规则 EMIT 计数与期望逐规则相等。

**ground truth 从哪来**：`wfgen verify-nexmark` 用**真实 WFL 规则引擎**（非手写模拟器）处理
与引擎**同一份确定性数据**（同 count+seed 字节级确定）+ **同一套 .wfl 规则**，逐规则算出期望
`emitted_total`——保证「比的是同一个查询、同一份数据」。对拍是 git-diff 同款分层（L1 哈希 →
L2 Myers → L3 明细），退出码 0=一致 / 1=有差异。**期望的完整定义（处理流程/三档验证
层级/排除与边界）见 [`docs/EXPECTATION_VERIFY.md`](docs/EXPECTATION_VERIFY.md)。**

**判定层级**（验证输出逐查询）：

| 结果 | 含义 | 处理 |
|---|---|---|
| `PASS` | 与期望精确一致 | ✅ |
| `FAIL` | 有差异（期望 diff） | 看 diff 明细：引擎 bug 待修 / 已知 flaky |
| `DIRTY` | 致命计数器非零 | 测量作废，重跑 |
| ⚠ known-diff | 已知差异（如 q12 fixed+close 尾桶收口） | 不判失败 |

**当前已知 FAIL**：无——22 查询全 PASS，但注意 **q12 是豁免放行而非一致**（引擎多收尾部桶，
1M 实测 27,446 vs 期望 10,240，+168%；由 verify-nexmark 内置 known 列表处理不判失败）。其余 21
个真一致（L1+L2+L3 全过，含 stats 的 q4b/q15-q19 值级对拍）。历史 FAIL（q3/q5/q7）已修复：q7/q5 =
close_all 尾桶收口语义，q3 = join 索引与提交前沿竞态。**每个查询「验证正确」的判定逻辑
（正确语义 + 断言什么 + 覆盖层 + 状态）见 [`docs/QUERY_VERIFY_LOGIC.md`](docs/QUERY_VERIFY_LOGIC.md)。**

**规模口径**：
- **30M**：逐位对拍（权威）；**100M**：EMIT 与 30M 同比例侧证 + `[clean]`（期望工作集
  ~19GB，不跑 100M 对拍）；**特殊口径查询**（q11/q12/q13）：多轮端到端 EMIT 确定性 + `[clean]`。
- **防误判**：`max_memory` 超限会**静默丢弃事件**（不报错、`[clean]` 照常）→ EMIT 变少，极易
  误判成「引擎正确、对拍基准错」——配置必须按公式预留（见 `docs/NEXMARK.md` §5.6）；多规则
  同跑存在规则间交互差异（q8/q11 曾数量级异常）→ 单规则/多规则分路径验证。

### 验证路径

基于同一 ground truth，深度验证走 daemon+TCP 路径（唯一全深度脚本），`bench.sh --verify`
做性能跑批的浅回归：

| 路径 | 命令 | 验证对象 | 深度 |
|---|---|---|---|
| daemon+TCP（深） | `./verify_daemon.sh [query] [total]` | 生产形态：TCP 注入 + 常驻 + SIGTERM flush 收口 | L1+L2+L3 |
| daemon+TCP（浅） | `./bench.sh <q> replay 30m --verify` | 同上，但 blackhole sink 只对拍 EMIT 计数 | 仅 L1 |

**推荐**：`verify_daemon.sh` 做全深度正确性验证；`bench.sh --verify` 用于性能跑批的顺带回归
（只查计数）。（batch 文件源路径曾由 verify_file.sh 覆盖，2026-08-30 起并入 daemon 验证，已移除。）

### verify_daemon.sh（daemon 路径）

```bash
./verify_daemon.sh all 1m      # 默认 all + 1M 快验（~2-4 分钟）
./verify_daemon.sh q3 1m       # 单查询
```

- **注入/收口形态**：`wfusion daemon` TCP 监听 → `send-arrow` 推帧 → metrics 追平
  （appended ≥ N 且 acked_lag == 0，所有被消费窗口消费完）→ SIGTERM flush 尾批收口落盘
  `data/alerts/benchmark.ndjson` → L1/L2/L3 三层对拍。
- **逐查询单跑**（每查询 rules = 该查询 .wfl）：多规则同跑存在规则间交互差异，单规则保真。
- **双口径交叉**：`metrics.ndjson` 的 `emitted_total`（权威引擎计数）+ 输出文件
  `data/alerts/benchmark.ndjson` 逐行计数，再与期望对拍；致命计数器非零 → `[dirty]` 作废。
- **指标口径脏检测（2026-08-30 加固）**：残留 wfusion 进程可能往 metrics.ndjson 写外来 label
  → 循环前清残留进程 + 校验 emitted_total label 恰为当前 query 规则集合，脏则自动重跑一次。
- 覆盖 batch 文件源路径跑不到的注入/收口形态：TCP 注入 + 常驻进程 + 关机 flush 尾批收口
  （bench.sh `--verify` 因 blackhole sink 只对拍 L1 计数，本脚本补 L2/L3 深度）。
- 不注册哨兵窗（哨兵 alert 会污染落盘对拍，完成信号用 metrics 追平——bench.sh 同款兑底口径）。
- **已知尾批丢失/竞态均已修复**（wp-reactor 2026-08-28~30）：on-each 关机尾批、q13 中间管道
  竞态、q6/q20 snapshot join 竞态、q8/q11/q7 多规则交互。
- **当前状态**：22/22 显示 PASS，但 q12 为**豁免放行**（fixed+close 收口多收尾部桶，
  1M 引擎 27,446 vs 期望 10,240，+168%，known 列表剔除不判失败）；其余 21 个 L1+L2+L3
  真一致（历史 q3/q5/q7 FAIL 已修复：close_all 尾桶收口语义 + join 索引/提交前沿竞态），
  如实记录于 docs/EXPECTATION_VERIFY.md 与 docs/QUERY_VERIFY_LOGIC.md。

### bench.sh --verify（daemon 路径，仅 L1）

```bash
./bench.sh q9 replay 30m --verify    # 单查询 daemon 对拍（只对拍 EMIT 计数）
```

30M 全量多规则对拍（q6=872,913 / q20=196,517）已 4/4 轮精确。

### verify_wfg.sh（`.wfg` 语料，L0/L0'/L1/L2 + L3 默认开）

上面两条路径验证的是「同一份 benchmark 数据下引擎输出对不对」；`verify_wfg.sh` 验证的是
**规则 + 语料本身**——用定向构造的实体跑 wfgen 场景，把规则语义压在硬断言下：

```bash
./verify_wfg.sh all --lint-only     # L0 静态校验 + L0' 规则内联用例（快、不落数据）
./verify_wfg.sh q1 q2 --duration 10s # 单/多查询：L0 + L0' + L1 注入断言 + L2 期望文件（+ L3 引擎级）
./verify_wfg.sh all                 # 全量（有语料走 curated，没有自动落 smoke）
./verify_wfg.sh q3 --scaffold       # 为缺语料的查询**写出** scenarios/q3_verify.wfg 骨架
./verify_wfg.sh q1 --no-engine      # 关掉 L3（L3 **默认开**；--lint-only 也自动关）
```

**五个层次（各答不同的问题）**：

| 层 | 做什么 | 答的是 |
|---|---|---|
| L0 | `.wfg` 静态校验（LN*/VN*） | 语料写对了吗 |
| **L0'** | **规则内联手写用例（`test` 块，跑真引擎 match-engine）** | **规则本身的语义/几何** |
| L1 | 注入断言 INJ1/INJ2（hit 必报 / near_miss·miss 必不报） | 语料的意图实现了吗 |
| L2 | 期望文件（`.except.jsonl` + meta） | 给出了可对拍的期望 |
| L3 | 引擎输出 vs 期望逐条全等（**默认开**，`--no-engine` 关） | 两套实现是否一致 |

**L0'（规则内联手写用例）**：本仓 10 个规则文件带 `test` 块（共 21 条：19 条可跑 + q13 的 2 条
harness 刻意拒绝）、`verify_wfg.sh` 会逐个跑。它不需要生成数据，`--lint-only` 下也跑；缺 `wfl`
二进制时启动会响亮提醒（`WFL=/path/to/wfl` 可指定）。

断言里**算出来的值**才算真锚（字面量只能防字段丢失）：本仓已钉住 q1 的 `0.908 × price`、
**score 的 `clamp(0,100)` 语义**（price=100 → 90.8；price=200 → 181.6 被截到 **100**）、
q7/q11/q14 的 `detail`、q13a 的 `mod_key`、q21/q22 的解包结果。

**两级语料**：

| 档 | 来源 | 证明了什么 |
|---|---|---|
| `curated` | `scenarios/<q>_verify.wfg`（人工写） | 有语义断言：`hit` 必报、`near_miss`/`miss` 必不报（`gen` 内置 **INJ1/INJ2** 硬断言） |
| `smoke` | 自动生成背景-only 场景（跑完删除） | 仅「规则/schema 未漂移」，**不含语义断言** |
| `GAP` | 规则不可注入时的 curated | 只能背景-only；**不是**通过，也不是失败——原因写在文件头（现在只剩 q13 这类需要 provider 的形态） |

- **curated 必须是真语料**：只有 `background`、没有 `inject` 的 curated 文件会被判 **`NOINJ` 失败**
  （跑了但什么都没验证 = 静默失效）；想要只测漂移就删掉该文件回落 `smoke`。
- **源流自动推导**：从规则的 `events { alias : WINDOW }` ∩ schema 里声明了 `stream_tag` 的源流
  （链式查询的中间窗会被自然过滤）。
- **现有语料**（22 条 curated——每一个查询都有真语料；每个文件头都写了判定量、构造依据与注意事项）：

| 语料 | 规则形态 | 能构造的用例 | L3 引擎级对拍 |
|---|---|---|---|
| `q1_verify.wfg` | `on each` 纯投影（`score = 0.908 × price`） | 仅 hit | ✅ 61000/61000 |
| `q2_verify.wfg` | `on each` + bind filter（`% 123 == 0`） | hit / near_miss / miss | ✅ 474/474 |
| `q3_verify.wfg` | snapshot join + join 后 `where` | hit / near_miss / miss | ✅ 1/1 |
| `q4_verify.wfg` | 链式（内层 deferred reduce join） | hit / miss（**无** near_miss：无阈值） | ⚠ 已知差异（2 条，见 `KNOWN_DIFF`） |
| `q5_verify.wfg` | hop(10s,2s) + `and close` + `conv top_ties(1)` | hit / near_miss（差 1 票）/ miss | ✅ 115/115（语料**钉** `#[duration=20s]`） |
| `q6_verify.wfg` | **join-then-key**（`match<seller>` + snapshot join） | hit / near_miss（均价差 1）/ miss（join miss） | ✅ 1/1 |
| `q7_verify.wfg` | `10s:fixed` + `and close` + `conv top_ties(1)` | hit / near_miss（名次差 1）/ miss | ✅ 2/2（**钉** `#[duration=18s]`） |
| `q8_verify.wfg` | `on each` + deferred join（`bucket_end`） | hit / miss | ✅ 1/1 |
| `q9_verify.wfg` | `on each` + deferred `reduce maxrow` | hit / miss | ✅ 1/1 |
| `q10_verify.wfg` | `on each` 纯投影 | 仅 hit | ✅ 60003/60003 |
| `q11_verify.wfg` | `session(10s)` + `and close` | 仅 hit（会话内计数是二元结论） | ✅ 849/849 |
| `q12_verify.wfg` | `10s:fixed` + `and close` | 仅 hit | ✅ 2856/2856（**钉** `#[duration=55s]`） |
| `q13_verify.wfg` | **双规则链**：`on each` → 中间窗 `bid_mod` → `on each` + provider snapshot join | hit（两段都是二元存在性） | ✅ 12001/12001（比对的是 q13b；q13a 的中间行被剔除） |
| `q14_verify.wfg` | 价格区间过滤（开区间 `0.908×price ∈ (1e6, 5e7)`） | hit（界内）/ near_miss（界外差 1 元）/ miss | ✅ 3/3 |
| `q15_verify.wfg` | `stats<1d:fixed>` 空键全局桶 + 12 度量 | 仅 hit（常量实体，见下） | ✅ 1/1 |
| `q16_verify.wfg` | `stats<1d:fixed> group by (channel)` | 仅 hit（常量实体） | ✅ 5/5 |
| `q17_verify.wfg` | `stats<1d:fixed> group by (auction)` | 仅 hit（聚合无阈值） | ✅ 943/943 |
| `q18_verify.wfg` | `stats<1d:fixed> group by (bidder,auction)` + `last` | 仅 hit | ✅ 11978/11978 |
| `q19_verify.wfg` | `stats<10m:fixed> group by (auction)` + `top(10, price)` | 仅 hit（顺带钉住 **top-10 截断**：每桶 12 条 → 恰 10 条告警） | ✅ 4413/4413 |
| `q20_verify.wfg` | `on each` + snapshot join + `where category == 10` | hit / near_miss（category 差 1）/ miss（join miss） | ✅ 1/1 |
| `q21_verify.wfg` | `on each` + `channel_id != ""` | hit / miss（非空过滤，无 near_miss） | ✅ 60002/60002 |
| `q22_verify.wfg` | `on each` 纯投影（`split(url,/')` 解包） | 仅 hit | ✅ 60003/60003 |

  22 条 curated 跑引擎：**21 条精确配对**（`missing`/`unexpected`/`field_mismatch` 均为 0）+ q4 一条已知差异。
  窗口/桶切分敏感的语料都把 `#[duration]` 钉住并在文件头说明理由。

  ⚠ **q15/q16 的注入断言是空转的**（脚本会明确标出 `inject: N/A（注入断言空转…）`）：这两条规则的
  实体是**常量** `entity(digit, 1)`，而注入断言要求实体标识是 `entity(...)` 的**单字段** →
  生成器把它们计入 `unasserted`（`Warning: N inject entities skipped by the mode assertion`）。
  因此它们的证据**只到 L3**（两侧独立算出同一行），**不含逐实体 hit/miss 断言**——
  要补 L1 得把规则改成字段实体（那会改基准语义，不建议）。

**已知差异：只剩 q4 一条**（引擎侧待查，不计失败）：

- q4（双规则链 q4a→`auction_finals`→q4b）的期望里有一条 **q4b 的 1d 桶收口告警**
  （从 relay 进去的中间窗行算出），而**引擎一条都没出** → `missing`。
  待查方向：引擎是否把 relay 的中间窗行喂给了绑定该窗的 stats task（期望侧会喂），
  以及 shutdown flush 对**由 relay 供数**的 1d 桶是否收口。
  （q15 这类由**源流**供数的 stats 桶在 shutdown flush 下确实会收口——已实测 PASS。）

**之前的两条链式查询缺口已排查并修复**（改动在 wfgen，不是 sink 配置）：

- 引擎 `emit()` 对**中间管道输出**（yield target 被下游规则 bind 的窗，如 `auction_finals`/
  `bid_mod`）提前 `return`：只回灌窗口给下游规则、**不落 sink**（`windows = ["*"]` 这样
  的全量 sink 组也拿不到）——改 sink 配置无解。
- 而期望侧原有两个**作用域错误**（都在 `crates/wfgen/src/oracle/mod.rs`）：
  ① 「中间窗」集合按 `injected_rules` 过滤 ⇒ 只给 q4a 写用例时，`auction_finals` 被当成
  **最终告警**写进期望 → 永远 `missing`（q4 的旧 known-diff 根源）；
  ② 期望侧只评估**被注入的规则** ⇒ 链式查询里下游规则（q13b）的 sink 可见告警变成
  `unexpected`（实测：引擎 12001 条、期望侧 0 条）。
  现两者均与引擎对齐（都在**全部已加载规则**上算），且 `wfgen verify` 剔除带
  `intermediate: true` 的期望行 → **q13 已有真语料且 12001/12001 精确配对**。
- **`--duration` 会覆盖语料自带的 `#[duration]`**，而窗口切分敏感的语料靠这个值成立：`q5` 的
  `near_miss` 是「差 1 票」构造（`top_ties(1)` 对**并列最高**全输出），只在原切分下严格成立。
  实测把 q5 的 20s 覆盖成 1m：簇起点从 9.0s 抬到 29.0s，切片把 20 条/19 条重新切开 →
  `INJ2`（near_miss 反而报警）。所以语料把 duration **钉住**；gen 失败时脚本会提醒这一点，
  `wfgen gen` 自身也会打 `Duration override: 20s -> 60s`。
- 退出码：`0` 全通过（已登记的「已知差异」不计失败）/ `1` 有失败（lint · 规则内联用例 · gen · NOINJ · engine）/ `2` 用法或环境错误（含显式 `--with-engine` 但跑不了）。

**为什么还需要 L0'（内联用例）**：`--with-engine` 只证明「同一份规则的**两套实现**一致」，
看不到规则本身偏离权威语义（两套实现都读同一份规则）。实测（2026-09-20）：

| 变异（相对权威 SQL） | L0'（内联用例） | L3（引擎级对拍） |
|---|---|---|
| `top_ties(1)` → `top_ties(2)` | ❌ 抓到（INJ2 也抓到） | 不适用（两侧同源） |
| `hop(10s,2s)` → `hop(20s,2s)`（size 10s → 20s） | ❌ 抓到：`expected hits == 5, got 7` | ✅ **全绿放行**（期望与引擎输出逐字节相同，115 条） |
| `hop(10s,4s)`（非法：size 非 slide 整数倍） | — | — （**编译器直接拒绝**，不是可用的变异） |

即：**扇出/几何量这类可观察量只有内联用例钉得住**（q5 的 `hits == size/slide`）；
L1 的 hit/near_miss/miss 只钉得动「会改二元结论」的偏离。所以 `test` 块不是可选项，
而是规则自己的**规范锚**；`verify_wfg.sh` 把它们跑起来（实测 10 个规则文件 / 19 条可跑用例全绿）。

**L3（引擎级对拍，默认开）**：把证据从「期望级」升到「引擎级」——

默认就跑 L3；`--no-engine` 关掉只跑期望级（L1 比的是「作者意图 vs 期望求值」，**不碰真引擎**）：

```
gen 产物 JSONL --wfgen dump-frames--> events.arrow_framed
  → wfusion batch（mode="batch" + 文件源，跑完输入自动退出）
  → wfgen verify --expected/--actual/--meta 对拍
```

- **file + batch 形态**（与仓库自己的 gen↔engine 对拍 `crates/wfgen/tests/*` 同形）：不起 daemon、
  不占端口、不发 SIGTERM，也不需要「追平启发式」——`wfusion batch` 跑完输入即退出。
  全量 22 查询（22 条 curated，无 smoke）实测 **~22s**（按各语料声明的 `#[duration]`；
  用 `--duration 10s` 压时长可更快），退出码 0。
- ⚠ **只对含 `inject` 的语料有意义**：`gen` 的期望由注入用例驱动 —— 场景里没有
  `inject` 时 `Expected: 0`（期望文件为空），无可比对。实测：同一个场景加一条 `inject` 后
  `Expected` 从 `0` 变 `5001`。所以 smoke 档一律记 **N/A**（不是通过），也不会白跑引擎。
- 两侧都是 0 条时也会记 N/A（**空对空不算证据**）；裁定不看 `status` 字段，而是按
  `missing` / `unexpected` / `field_mismatch` 全为 0 从计数重算。
- 需要 `wfusion` / `python3`（起 L3 时）；不需要 `nc`，也不需要 daemon。缺 `wfusion`
  时**响亮降级**（默认开着但跑不了：警告 + 汇总里也如实说），只有显式 `--with-engine` 才硬失败。
  报告落 `<out>/<q>/engine_verify.json`，引擎日志落 `<out>/<q>/engine_batch.log`（单查询 batch 上限 `ENGINE_TIMEOUT`s，默认 300）。
- 输出形态：**一行一查询**（`Q KIND LINT GEN UNIT L3 NOTE`），问题细节不在表格中间打断、
  集中到表末的「需关注」块；L3 列取值 `OK` 精确配对 / `KNOWN` 已知差异 / `N-A` 无可比对 /
  `FAIL` 失败 / `-` 未跑。
- **已知差异**（`KNOWN_DIFF`，现仅 `q4`）：已定位且已记录的「期望 ↔ 引擎」模型差异，L3 列记
  `KNOWN`（不计失败）并计入汇总，同时在表末「需关注」块里写明原因；未登记的查询 L3 `FAIL`
  一律判失败——不允许把差异悄悄变成通过。差异被修好后脚本会提示把它从表里删掉。

## 3. 性能诊断：diag.sh

`bench.sh` 回答「吞吐是多少」，`diag.sh` 回答「**墙在管线哪一段**」——基于引擎内置
perf-diag 三档墙梯，单 daemon 不重启逐段切除，每段增量成本 = 相对上一档。

```bash
./diag.sh q5 10m                   # 默认预热档开（消除首档冷分配偏差）
WARMUP=0 ./diag.sh q1 10m          # 关预热（仅粗看方向时用）
N_LIST=1m,10m ./diag.sh q1 10m     # 多个 N（每个 N 重启一套完整墙梯）
STAGES=floor,full ./diag.sh q1 10m # 自定义墙梯（跳过中间档）
GEN_FRAMES=1 ./diag.sh q1 10m      # 帧缺失时自动生成（与 bench.sh 共享缓存）
```

| 档 | 切什么 | 测得 |
|---|---|---|
| `floor` | 切规则求值 + 切输出链 | 注入 + 解码 + 窗口 append（管道净段） |
| `rules` | 切输出链 | + 规则求值 → **增量 = 规则墙** |
| `full` | 不切 | + 输出链 → **增量 = 输出墙** |

输出 `data/diag_<q>_<total>.txt`：每档 EPS/耗时/每事件 ns/增量成本/占全链/CPU%/RSS +
**墙判定**（主墙 = 增量最大段，附**基线占比**「墙前基线占全链多少」；CPU 占核比 >50% = 忙墙
→ 下一步 CPU 采样定位热点；<15% = 等/供给墙，RSS 逐档上涌 → 窗口/join 容量，平稳 → 供给侧）。

### 性能门禁（L4）：墙梯后自动断言，防静默退化

diag.sh 内置 wfgen perf 门禁（`docs/useage/ai_native_rule_dev_loop.md` §2.5）：

```bash
# 1) 首次校准：留存本机同 total 基线墙表（本机/规则集变更后重录）
RECORD_BASELINE=data/perf_wall.q1.baseline.txt ./diag.sh q1 10m
# 2) 门禁：墙梯测量后自动断言（绝对兜底 + 相对防回归），任一 FAIL → 退出码 1
GATE=conf/perf-gate.toml ./diag.sh q1 10m
```

`GATE` 指向 conf/perf-gate.toml（模板见 `conf/perf-gate.example.toml`，阈值按机器实测填）：
绝对断言用 `rules_eps_min`（整集下限）或 `per_rule_ns_max`（单规则成本，需 `rule_count`）；
相对断言用 `baseline` + `max_regression_pct`（跨机器稳健）。门禁取每档最大 N 行，基线须同
`total` 录制。两个 env 互斥（先录后门，见 wfgen 报错）。

### 诊断纪律（违反会得出假结论）

1. **预热档默认开，别关**：首档独自承担窗口冷分配/page fault，实测不预热时 floor 反而慢于
   rules 25%，偏差大于信号；脚本在负增量时报警。
2. **帧文件必须与当前 schema 同版本**：旧版帧会整批丢弃 → 50M EPS 级假象；脚本强制校验
   `appended = N × 档数`，不追平即判失败。
3. **`--n-list` 一次只能给一个 N**：哨兵驱动切换在每档首个哨兵后即发生，多 N 会吃到下一档门控。
4. **事件时间跨度 ≤ allowed_lateness**：同一份数据发 N 次，重发即迟到；超限被 drop → rules/full
   档实际无数据。脚本自动放宽并拦截超 `over=1h` 的规模。
5. **RSS 是档内峰值、随墙梯累积**，不是该段内存成本；内存分析用 `bench.sh`。
6. **on-each 查询的输出墙口径**：`cut_output` 在 on-each 直投路径上于 OutputRecord 构造之前
   返回，q1 类查询的 `rules→full` 增量含「构造 + append + 通道 + sink 物化」；match 类计入 rules 档。

## 4. 目录结构

```
bench.sh                 # 基准驱动（生成/复用帧 → 起 daemon → send-arrow 回放 → 采样）
diag.sh                  # 性能墙定位驱动（perf-diag 三档墙梯 → 每段增量成本 + 墙判定）
verify_daemon.sh         # 正确性验证（daemon TCP 注入 → benchmark.ndjson → 期望对拍）
verify_wfg.sh            # 规则/语料验证（.wfg 语料 → wfgen lint/gen 注入断言）
conf/wfusion.toml        # daemon 配置（parse/rule 并行度等）
conf/perf-diag.toml      # 诊断模式·无档（bench.sh：哨兵精确 EPS 口径）
conf/perf-diag-wall.toml # 诊断模式·三档墙梯（diag.sh）
conf/perf-gate.example.toml  # L4 门禁配置模板（diag.sh GATE= 引用，按机器填阈值）
models/queries/qN.wfl    # 查询定义（唯一权威来源，每文件一组同族规则）
models/schemas/          # 事件 schema（nexmark.wfs）+ windows.toml + knowdb.toml（q13 侧输入表）
topology/                # source/sink 拓扑（send-arrow 源、blackhole 汇；sinks_file/ 为验证落盘用）
scripts/                 # metrics 工具（extract_emitted / read_metrics / compare-metrics / verify_file_lib）
side_input/              # q13 有界侧输入 CSV（models/schemas/knowdb.toml 引用）
scenarios/nexmark.wfg    # 数据生成场景定义
scenarios/qN_verify.wfg  # 逐查询验证语料（hit/near_miss/miss，verify_wfg.sh 的 curated 档）
docs/                    # 背景/口径/结果归档（NEXMARK、BENCH_RESULTS、CAPABILITY_GAP、EXPECTATION_VERIFY 等 10 篇）
data/                    # 运行产物（gitignore）：帧文件、bench 结果、metrics、ground truth
```

## 5. 背景与参考文档

- **数据模型**：三流 person 2% / auction 6% / bid 92%，事件时间固定 100µs/事件、严格递增，
  生成语义严格对齐 Flink 官方（价格对数均匀 / hot 分布 / 引用窗口），同 count+seed **字节级确定**
  ——正确性可验证的前提。详见 [`docs/NEXMARK.md`](docs/NEXMARK.md) §1~§3 与
  [`docs/NEXMARK_CONFORMANCE.md`](docs/NEXMARK_CONFORMANCE.md)。
- **查询**：Q1~Q22 全实现；逐条能力/语义判定见
  [`docs/CAPABILITY_GAP_MATRIX.md`](docs/CAPABILITY_GAP_MATRIX.md)，语义对齐状态表与执行器
  矩阵见 [`docs/SEMANTIC_ALIGNMENT.md`](docs/SEMANTIC_ALIGNMENT.md)（§4 / §8），权威 SQL 原文
  见 [`docs/NEXMARK_AUTHORITATIVE_SEMANTICS.md`](docs/NEXMARK_AUTHORITATIVE_SEMANTICS.md)。
- **结果归档**：[`docs/BENCH_RESULTS.md`](docs/BENCH_RESULTS.md)（按跑批日期分节，含 Linux）；
  OSS/VVR 白皮书基线见 [`docs/OSS_VVR_BASELINE.md`](docs/OSS_VVR_BASELINE.md)。

## 前提

- `wfusion` / `wfgen` 在 PATH 或 `WFUSION=/path WFGEN=/path`，需含 `gen-nexmark`、
  `dump-frames`/`send-arrow`、`stream` 子命令。
- `nc`、`python3`；端口 9800 空闲。
- 正确性验证另需 sink 输出（file_json_sink 的 alerts.ndjson），吞吐 PK 用 blackhole。
- 数据量对应帧缓存可复用；清理生成产物用 `./bench.sh clean [cache|all]`。
