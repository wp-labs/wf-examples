# nexmark_pk — NEXMark 基准：吞吐 PK + 正确性验证（对齐 Flink 官方基线）

本目录是 wfusion 引擎的 **NEXMark 基准套件**：同一份确定性基准数据（Q1~Q22 全量查询），
对照阿里 Nexmark 白皮书的 OSS Flink / VVR 基线做吞吐 PK，并用真实 WFL 规则引擎 ground truth
验证输出正确性。三个核心工具：

| 工具 | 回答的问题 |
|---|---|
| `bench.sh` | **吞吐/内存是多少**（EPS / RSS / CPU，对 Flink PK） |
| `diag.sh` | **墙在管线哪一段**（性能墙定位） |
| `verify_file.sh` | **输出是否正确**（文件源路径 vs oracle 对拍） |

背景（事件模型 / 查询语义 / 正确性标准）见 [`docs/NEXMARK.md`](docs/NEXMARK.md)；
查询覆盖判定见 [`docs/CAPABILITY_GAP_MATRIX.md`](docs/CAPABILITY_GAP_MATRIX.md)；
实测结果归档见 [`docs/BENCH_RESULTS.md`](docs/BENCH_RESULTS.md)。

## 快速开始

```bash
./bench.sh q1 replay 10m        # 性能测试：q1 单查询，10M 数据重放
./bench.sh all replay 30m       # 全量 22 查询吞吐 PK（all 不含 q6，见下）
./verify_file.sh all 1m         # 正确性验证：文件源路径全量对拍（~2-4 分钟）
./diag.sh q5 10m                # 性能诊断：定位 q5 的墙在哪一段
```

## 1. 性能测试：bench.sh

```bash
./bench.sh [query=all|q1..q22] [feed=replay|stream] [total=100m|30m|10m|1m]
WARMUP=1 ./bench.sh all replay 30m     # 预热一轮再测（stash 重建后首跑偏低，剔除）
PARSE_PARALLELISM=6 RULE_PARALLELISM=6 ./bench.sh q1 replay 10m   # 调并行度
CONNECTIONS=4 SHARD_KEYS="bid_events:auction,..." ./bench.sh q2 replay 30m  # 键闭包分片
```

- **feed=replay**（默认，PK 口径）：预编码 Arrow 帧线速重放，**测引擎峰值持续吞吐**。
  事件按 30s 桶序、事件时间固定 100µs/事件；帧缓存 `data/bench_<total>_v5.frames`
  跨查询复用（存在即不重生成，`DATA_VER` 指纹防旧缓存静默复用）。
- **feed=stream**：wfgen 实时生成按 `RATE` 注入（客户端编码上限 ~760k/s，**非引擎能力**，
  EPS 不可比）。只用于：① 真实时间窗口语义（watermark/驱逐按真实时间发生）；② 长时稳定性
  内存有界（看 RSS 是否泄漏）；③ 生产形态模拟（late / `allowed_lateness`）。**不用于**吞吐对比。
- **all 模式不含 q6**：join-then-key 单线程 + 逐事件 sliding 状态机，架构性慢，单跑研究用
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
5. **引用 RSS 必须标注 `parse_buffer_bytes`**：128MB 与 2GB 预算的 EPS/RSS 不可直接对等。
6. **CPU 是活跃窗口径**（哨兵 start/emit ± 0.5s，100ms cputime 差分）：全生命周期统计会把
   亚秒级突发（q2/q8 ≈ 0.4s）稀释成 0% 假象；短跑（<2s）读数只宜作量级参考。

完整度量口径（哨兵链路 / 兑底 / 采样）见 `docs/NEXMARK.md` §7。

## 2. 正确性验证：怎么是正确的

### 正确性标准

**正确 = 两条同时成立**：

1. **数据完整性无丢失** → 结果行 `[clean]`：`appended` 追平（如 30M/30M）且致命计数器
   （append_failed / dropped_late / cursor_gap / channel_full / sink_dispatch_failed）全零。
2. **输出与确定性 ground truth 一致** → 每规则 EMIT 计数与 oracle 期望逐规则相等。

**ground truth 从哪来**：`wfgen verify-nexmark` 用**真实 WFL 规则引擎**（非手写模拟器）处理
与引擎**同一份确定性数据**（同 count+seed 字节级确定）+ **同一套 .wfl 规则**，逐规则算出期望
`emitted_total`——保证「比的是同一个查询、同一份数据」。对拍是 git-diff 同款分层（L1 哈希 →
L2 Myers → L3 明细），退出码 0=一致 / 1=有差异。**oracle 的完整定义（处理流程/三档验证
层级/排除与边界）见 [`docs/ORACLE_VERIFY.md`](docs/ORACLE_VERIFY.md)。**

**判定层级**（验证输出逐查询）：

| 结果 | 含义 | 处理 |
|---|---|---|
| `PASS` | 与 oracle 精确一致 | ✅ |
| `FAIL` | 有差异（oracle diff） | 看 diff 明细：引擎 bug 待修 / 已知 flaky |
| `DIRTY` | 致命计数器非零 | 测量作废，重跑 |
| ⚠ known-diff | 已知差异（如 q12 fixed+close 尾桶收口） | 不判失败 |

**当前已知 FAIL**：无——22 查询全 PASS，但注意 **q12 是豁免放行而非一致**（引擎多收尾部桶，
1M 实测 27,446 vs oracle 10,240，+168%；由 verify-nexmark 内置 known 列表处理不判失败）。其余 21
个真一致（L1+L2+L3 全过，含 stats 的 q4b/q15-q19 值级对拍）。历史 FAIL（q3/q5/q7）已修复：q7/q5 =
close_all 尾桶收口语义，q3 = join 索引与提交前沿竞态。**每个查询「验证正确」的判定逻辑
（正确语义 + 断言什么 + 覆盖层 + 状态）见 [`docs/QUERY_VERIFY_LOGIC.md`](docs/QUERY_VERIFY_LOGIC.md)。**

**规模口径**：
- **30M**：逐位对拍（权威）；**100M**：EMIT 与 30M 同比例侧证 + `[clean]`（oracle 工作集
  ~19GB，不跑 100M 对拍）；**特殊口径查询**（q11/q12/q13）：多轮端到端 EMIT 确定性 + `[clean]`。
- **防误判**：`max_memory` 超限会**静默丢弃事件**（不报错、`[clean]` 照常）→ EMIT 变少，极易
  误判成「引擎正确、对拍基准错」——配置必须按公式预留（见 `docs/NEXMARK.md` §5.6）；多规则
  同跑存在规则间交互差异（q8/q11 曾数量级异常）→ 单规则/多规则分路径验证。

### 两条验证路径

基于同一 ground truth，两条互补路径覆盖两种形态：

| 路径 | 命令 | 验证对象 | 深度 |
|---|---|---|---|
| 文件源 batch | `./verify_file.sh [query] [total]` | `wfusion batch` 直读帧 + 自动关机 flush | L1+L2+L3 |
| daemon+TCP | `./verify_daemon.sh [query] [total]` | 生产形态：TCP 注入 + 常驻 + SIGTERM flush 收口 | L1+L2+L3 |
| daemon+TCP（浅） | `./bench.sh <q> replay 30m --verify` | 同上，但 blackhole sink 只对拍 EMIT 计数 | 仅 L1 |

**推荐**：`verify_file.sh`（batch，确定性最高）+ `verify_daemon.sh`（daemon，覆盖 TCP/常驻/flush
收口路径）双跑；`bench.sh --verify` 用于性能跑批的顺带回归（只查计数）。

### verify_file.sh（文件源路径）

```bash
./verify_file.sh all 1m        # 默认 all + 1M 快验（~2-4 分钟）
./verify_file.sh q13 1m        # 单查询
```

- **逐查询单跑**（每查询 rules = 该查询 .wfl）：多规则同跑存在规则间交互差异，单规则保真。
- **双口径交叉**：`metrics.ndjson` 的 `emitted_total`（权威引擎计数）+ 输出文件
  `data/alerts/benchmark.ndjson` 逐行计数，再与 oracle 对拍；致命计数器非零 → `[dirty]` 作废。
- **指标口径脏检测（2026-08-30 加固）**：残留 wfusion 进程可能往 metrics.ndjson 写外来 label
  → 循环前清残留进程 + 校验 emitted_total label 恰为当前 query 规则集合，脏则自动重跑一次。
- **已知尾批丢失/竞态均已修复**（wp-reactor 2026-08-28~30）：on-each 关机尾批、q13 中间管道
  竞态、q6/q20 snapshot join 竞态、q8/q11/q7 多规则交互。
- **当前状态**：22/22 显示 PASS，但 q12 为**豁免放行**（fixed+close 收口多收尾部桶，
  1M 引擎 27,446 vs oracle 10,240，+168%，known 列表剔除不判失败）；其余 21 个 L1+L2+L3
  真一致（历史 q3/q5/q7 FAIL 已修复：close_all 尾桶收口语义 + join 索引/提交前沿竞态），
  如实记录于 docs/ORACLE_VERIFY.md 与 docs/QUERY_VERIFY_LOGIC.md。

### verify_daemon.sh（daemon 路径，2026-08-30 新增）

```bash
./verify_daemon.sh all 1m      # 默认 all + 1M 快验
./verify_daemon.sh q3 1m       # 单查询
```

- **注入/收口形态**：`wfusion daemon` TCP 监听 → `send-arrow` 推帧 → metrics 追平
  （appended ≥ N 且 acked_lag == 0，所有被消费窗口消费完）→ SIGTERM flush 尾批收口落盘
  `data/alerts/benchmark.ndjson` → 与 verify_file.sh 相同的 L1/L2/L3 三层对拍。
- 覆盖 batch 路径跑不到的：TCP 注入 + 常驻进程 + 关机 flush 尾批收口（bench.sh `--verify`
  因 blackhole sink 只对拍 L1 计数，本脚本补 L2/L3 深度）。
- 逐查询单跑（同 verify_file.sh）；不注册哨兵窗（哨兵 alert 会污染落盘对拍，完成信号用
  metrics 追平——bench.sh 同款兑底口径）。
- **当前状态**：与 batch 路径一致（22/22 显示 PASS，q12 同款豁免，详见上）。

### bench.sh --verify（daemon 路径，仅 L1）

```bash
./bench.sh q9 replay 30m --verify    # 单查询 daemon 对拍（只对拍 EMIT 计数）
```

30M 全量多规则对拍（q6=872,913 / q20=196,517）已 4/4 轮精确。

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
verify_file.sh           # 文件源正确性验证（batch 模式 → benchmark.ndjson → oracle 对拍）
conf/wfusion.toml        # daemon 配置（parse/rule 并行度等）
conf/wfusion_file.toml   # 文件源 batch 配置（verify_file.sh 基线）
conf/perf-diag.toml      # 诊断模式·无档（bench.sh：哨兵精确 EPS 口径）
conf/perf-diag-wall.toml # 诊断模式·三档墙梯（diag.sh）
models/queries/qN.wfl    # 查询定义（唯一权威来源，每文件一组同族规则）
models/schemas/          # 事件 schema（nexmark.wfs）+ windows.toml + knowdb.toml（q13 侧输入表）
topology/                # source/sink 拓扑（send-arrow 源、blackhole 汇；sinks_file/ 为文件源用）
scripts/                 # metrics 工具（extract_emitted / read_metrics / compare-metrics / verify_file_lib）
side_input/              # q13 有界侧输入 CSV（models/schemas/knowdb.toml 引用）
scenarios/nexmark.wfg    # 数据生成场景定义
docs/                    # 背景/口径/结果归档（NEXMARK、BENCH_RESULTS、CAPABILITY_GAP、ORACLE_VERIFY 等 10 篇）
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
