# nexmark_pk — NEXMark 基准：吞吐 PK + 正确性验证（对齐 Flink 官方基线）

与 Flink 对齐的 PK case：**同一份权威基准数据（NEXMark）+ 同一批查询（Q1~Q22）+ 同输出
口径（blackhole 丢弃）**，跑引擎实测吞吐与输出正确性，对照阿里 Nexmark 白皮书发布的
OSS Flink / VVR 基线。查询覆盖与语义对齐判定见 `docs/CAPABILITY_GAP_MATRIX.md`（22 条全实现）；
**性能数字以当次跑批 `data/bench_*_replay.txt` 为准**（历史实测报告已清理，见 git 历史）。
**度量口径（EPS 哨兵机制 / RSS / CPU 活跃窗 / 正确性对拍）见 `docs/TEST_PLAN.md`；
实测结果归档（含 Linux 跑批）见 `docs/BENCH_RESULTS.md`。**

## 目录结构

```
bench.sh                 # 基准驱动（生成/复用帧 → 起 daemon → send-arrow 回放 → 采样）
diag.sh                  # 性能墙定位驱动（perf-diag 三档墙梯 → 每段增量成本 + 墙判定）
verify_file.sh           # 文件源正确性验证（batch 模式 → benchmark.ndjson → oracle 对拍）
conf/wfusion.toml        # daemon 配置（parse/rule 并行度等）
conf/wfusion_file.toml   # 文件源 batch 配置（verify_file.sh 基线，rules 需指向 models/queries）
conf/perf-diag.toml      # 诊断模式·无档（bench.sh 用：只要哨兵精确 EPS 口径）
conf/perf-diag-wall.toml # 诊断模式·三档墙梯（diag.sh 用：floor → rules → full）
models/queries/qN.wfl    # 查询定义（唯一权威来源，每文件一组同族规则）
models/schemas/          # 事件 schema（nexmark.wfs）
topology/                # source/sink 拓扑（send-arrow 源、blackhole 汇；sinks_file/ 为文件源用）
scripts/                 # 数据生成 + 正确性验证工具（见下文）
data/                    # 运行产物（gitignore）：帧文件、bench 结果、metrics、ground truth
```

## 数据（NEXMark 事件模型）

`wfgen gen-nexmark <count> [--seed N]`（默认 seed=1，StdRng，确定性、流式、内存有界）：

| 流 | 占比 | 说明 |
|---|---|---|
| person_events | 2% | 30M 总量 = 600k person |
| auction_events | 6% | 30M 总量 = 1.8M auction |
| bid_events | 92% | 30M 总量 = 27.6M bid |

事件时间线性映射、严格递增（固定 100µs/事件，30M → 3000s，等价 Flink `outOfOrderGroupSize=1`）；生成语义
**严格对齐 Flink 官方** `nexmark/nexmark` 默认配置：价格对数均匀 `round(10^(6u)×100)`
（[100, 1e8)）、hot auction 50% / hot seller·bidder 75%（最近 100 人批次）、bid 引用最近
`numInFlightAuctions=100` 个 auction ± 10 lead、seller/bidder 引用最近 `numActivePeople=1000`
人 ± 10 lead、auction 有效期 = 1+[0,2×horizon) ms、category 10..14、channel 50% 热门
4 通道 + 50% channel-N、city/state 10 城/6 州、name/email/creditCard/itemName/description
随机生成（官方数组与 nextString）、extra 补齐到 avgByteSize（200/500/100）。
**同一 count + seed 的生成结果字节级确定**（`wfgen gen-nexmark`，确定性已验证）。
与 Flink 官方定义的**逐项对照（含残余差异说明）**见
[`NEXMARK_CONFORMANCE.md`](./docs/NEXMARK_CONFORMANCE.md)；`gen-nexmark --check` 与
`verify-nexmark` 会在报告尾部自动输出符合性摘要。

## 查询：与 Flink Nexmark 测试集的逻辑匹配度

Flink 参照系 = 官方 `nexmark/nexmark` 测试集 **Q1~Q22 全部 22 条查询**（`qN.sql`，
权威原文见 `docs/NEXMARK_AUTHORITATIVE_SEMANTICS.md`）。22 条已全部实现（`models/queries/`），
逐条判定（18 已有 / Q12 待补强 / Q6·Q11·Q13 特殊口径）见
[`CAPABILITY_GAP_MATRIX.md`](./docs/CAPABILITY_GAP_MATRIX.md)，复核见
[`REVIEW_FLINK_CONFORMANCE_2026-08-23.md`](./docs/REVIEW_FLINK_CONFORMANCE_2026-08-23.md)，
各查询语义对齐细节见 `docs/SEMANTIC_ALIGNMENT.md` / `docs/SEMANTIC_SUPPORT_MATRIX.md`。

### 正确性验证（30M replay，seed=1）

期望值由 `wfgen verify-nexmark`（真实 WFL 规则引擎）对同一份确定性数据逐规则算出：
`bench.sh <q> replay 30m --verify` 在 wfgen 内与引擎实际 EMIT 计数对拍
（git-diff 同款分层：L1 哈希 → L2 Myers → L3 明细，退出码 0=一致 / 1=有差异）。

- **全量 30M replay**：22 查询全部 `[clean]`（appended 30M/30M + 致命计数器归零），
  登记见 `docs/CAPABILITY_GAP_MATRIX.md` §一。
- **`--verify` oracle 对拍**：Q8 已修复并对拍一致（10M = 82,446 identical；三处根因：
  到期 miss 的 join 目标 append 滞后 → EOS 重试补出、shutdown flush 的 EMIT 指标尾部导出、
  flush 按最终事件水位收口不误扫尾部桶）；Q9 同口径一致；其余查询的 daemon 级对拍
  **待跑**（Q19 stats oracle 未接入 = known-diff，标 ⚠ 不判失败）。
- 逐 alert 明细对拍（旧 28k 探针 `alerts.ndjson` 方案）随引擎 sink 改造已由计数级对拍替代。
- 各查询 EMIT 期望值 / 已知波动 / 特殊口径（Q11 分片、Q12 处理时间近似、Q13 形状对齐）
  见 `docs/CAPABILITY_GAP_MATRIX.md` §一·§二 与 `docs/SEMANTIC_ALIGNMENT.md` §5~§6。
- **文件源路径验证**：`./verify_file.sh`（1M 数据 batch 模式，逐查询对拍；含已知尾批
  丢失与引擎快速重放非确定的如实报告）——见下文「文件源验证工具：verify_file.sh」。

> **100M 吞吐跑批的正确性侧证（2026-08-17 记录）**：当时 Q1-Q9 全 clean、
> q2=747,816（0.8129%）、q9=6,000,000（记录见 git 历史）。

## 基准工具：bench.sh

```bash
./bench.sh [query=all|q1..q22] [feed=replay|stream] [total=100m|30m|10m]
MAX_FRAME_BYTES=1048576 ./bench.sh all replay 30m    # 指定帧 cap（默认 8MiB）
```

- **feed=replay**（默认，PK 口径）：gen-nexmark → dump-frames 预编码 Arrow 帧 →
  send-arrow 重放（默认 **单连接**整文件推，`CONNECTIONS=1`、`SHARD_KEYS` 空；
  多连接仅在有状态负载需要键闭包分片时用）。事件按 30s 桶序（v2 排序数据），
  帧/分片缓存带 `DATA_VER`（默认 v2）指纹，`data/bench_<total>_v2.frames`
  跨查询复用，存在即不重生成。**测引擎峰值持续吞吐（性能基准口径）**。
- **feed=stream**：wfgen 实时生成按 RATE 注入（事件时间随墙钟推进，客户端编码
  上限 ~760k/s——**非引擎能力**，EPS 不可比）。**使用场景**：
  ① 真实时间窗口语义（`over=10m` 时间驱逐/watermark 按真实时间实时发生，而非
  replay 的追赶式）；② 长时稳定性/内存有界（中低速持续跑几十分钟~几小时，看
  RSS 是否有界、不泄漏）；③ 生产形态模拟（事件时间=现在，验证 late/`allowed_lateness`
  行为）。**不用于**吞吐对比与短时跑批。
- 输出每查询 `data/bench_<q>_<feed>.txt`：EPS + RSS 峰值 + 驱逐数 + 口径标注
  （`eps_mode=sentinel|metrics-append`）；`data/perf_sentinel.ndjson` 为哨兵
  四元组流（完成信号 + EPS 数据源），`data/metrics.ndjson` 为计数器流，
  `data/{wfusion,daemon}.log` 为引擎日志。

### 测量纪律（违反会得出假结论）

1. **计时口径 = 哨兵四元组**（2026-08-24 起）：daemon 以 `--perf-diag
   conf/perf-diag.toml` 启动（无档 = 门控全 false，性能零影响，仅注册
   `__wf_sentinel` 窗口），`send-arrow/stream --sentinel` 启用**分连接哨兵**：
   每条连接 copy 完自己的数据后追加哨兵帧（round=连接号，单连接 1 条 round=0）；
   引擎等**数据窗排空**后写 `{round,n,start_ns,emit_ns}`，EPS =
   Σn/(max emit_ns − min start_ns)——无 metrics 轮询（±200ms）粒度误差，短跑
   读数同样可信，且多连接时各连接 dt 可对比（连接均衡/慢连接诊断）。
   哨兵超时退回 append_total 轮询兑底（`eps_mode=metrics-append`，TIMEOUT 标注）。
   （旧口径：metrics 三输入流 append 计数器求和追平 TOTAL 的时刻。）
   完整链路（哨兵帧生成/引擎落盘/聚合/兑底）见 `docs/TEST_PLAN.md` §1。
2. **RSS 口径（2026-08-17 起）**：`parse_buffer_bytes` 默认值已改为 128MB
   （P0-② content 记账，18 槽）——q1 100M ≈ 6.1M / RSS ~5.9GB，旧默认
   （256MB 解码记账）为 5.93M / 4.4GB。吞吐优先场景显式调大预算（bench 默认
   2GB：q1 7.5M+，RSS 随吞吐升至 12-14GB）。引用 RSS 数字时必须标注所用
   `parse_buffer_bytes`，旧口径数字（“100M 6.8GB”等）与现默认不直接对等。
3. **A/B 必须不限速**：`RATE=10000000`（限速会把 EPS 封顶在 RATE）。
4. **同时段交错对比**：bench 机 EPS 与 RSS_peak 呈双峰相位强相关（同配置差 ±8%），
   结论必须按 RSS 相位配对；单轮数字只能作量级参考。
5. **CPU 口径（2026-08-24 起）**：`CPU X%avg/Y%max` 是**引擎活跃窗**（哨兵
   start_ns/emit_ns ± 0.5s）内的核占数（多核可 >100%），100ms 采样；采样器先
   产出 cputime 差分基线才启动客户端（防亚秒突发在首个差分前烧完 → 假 0%）。
   此前 1s 粗采样 + 全生命周期统计会把亚秒级突发（如 q2 26M EPS ≈ 0.4s）稀释/
   漏采成 0%（实测假象）——新口径下 0% 才可信；短跑（<2s）读数仍只宜作量级参考。
6. 消费侧计数器提取：`python3 scripts/extract_emitted.py data/metrics.ndjson`
   （counter 跨 1s 区间求和；gauge 取峰值，不可混用）。

## 文件源验证工具：verify_file.sh

`bench.sh --verify` 验证的是 daemon+TCP 注入路径；`verify_file.sh` 验证**文件源路径**
（`conf/wfusion_file.toml` 同款 batch 配置）：`wfusion batch` 直读预编码帧，规则 EMIT 落到
`topology/sinks_file/business.d/benchmark.toml` 指定的 `data/alerts/benchmark.ndjson`，逐查询与
`wfgen verify-nexmark` ground truth 对拍。

```bash
./verify_file.sh [query=q1..q22|all] [total=1m|10m|30m|100m]   # 默认 all 1m，~2-4 分钟
```

- **逐查询单跑**（每查询 rules = 该查询 .wfl）：多规则同跑存在规则间交互差异（实测 all 跑：
  q8 7565→1、q11 17081→118234），单规则保真。
- **双口径**：oracle 对拍以 `metrics.ndjson` 的 `emitted_total` 为权威引擎计数（规则任务
  join 后导出，与 bench.sh 一致）；`benchmark.ndjson` 文件计数作一致性交叉检查。
- **尾批丢失已修复（wp-reactor 2026-08-28）**：on-each 规则关机不再丢最后未满批
  （`flush()` 补 flush_alerts）+ q13 中间管道消费竞态修复（push loop cancel 改为 1s
  截止时间驱动）——修复后 q1/q10/q14/q21/q22 文件计数与指标完全一致；交叉检查保留作
  回归守卫（缺额 >1% 标 ⚠⚠）。
- **引擎多规则正确性修复（wp-reactor 3fda01c）**：多规则同跑（生产形态）三个数量级
  异常已修复——q8（join 索引 key 覆盖，7565→1）、q11（分片 key 覆盖，session 切碎
  118234）、q7（单 worker 缺 conv stage，54 vs 10）；q5/q7 多规则单 worker 模式已与
  oracle 一致（51/10）。
- **引擎快速重放非确定（剩余已知项）**：q3（尾 close 0~7 条）、q5/q7 单规则 sharded
  尾桶差 1、q6/q20（snapshot join 流式竞态 3~8%，低负载单规则时与 oracle 一致）、
  q13（管道竞态残留偶发）——如实报 FAIL，属引擎待修项，非规则逻辑错。

## 性能墙定位工具：diag.sh

`bench.sh` 回答「吞吐是多少」，`diag.sh` 回答「**墙在管线哪一段**」——基于引擎内置的
性能诊断模式（perf-diag）三档墙梯，单 daemon 不重启逐段切除：

```bash
./diag.sh [query=q1|q1,q5,q9|all] [total=1m|10m|30m]
./diag.sh q5 10m                   # 预热档默认开（消除首档冷分配偏差）
WARMUP=0 ./diag.sh q1 10m          # 关预热（省一档时间/内存，仅粗看方向时用）
N_LIST=1m,10m ./diag.sh q1 10m     # 多个 N（每个 N 重启一套完整墙梯）
STAGES=floor,full ./diag.sh q1 10m # 自定义墙梯（跳过中间档）
GEN_FRAMES=1 ./diag.sh q1 10m      # 帧缺失时自动生成（与 bench.sh 共享缓存）
```

| 档 | 切什么 | 测得 |
|---|---|---|
| `floor` | 切规则求值 + 切输出链 | 注入 + 解码 + 窗口 append（管道净段） |
| `rules` | 切输出链 | + 规则求值 → **增量 = 规则墙** |
| `full` | 不切 | + 输出链 → **增量 = 输出墙** |

输出 `data/diag_<q>_<total>.txt`：每档 EPS/耗时/每事件 ns/**增量成本**/**占全链**/CPU%/RSS +
墙判定（主墙 = 增量最大段，附**基线占比**「墙前基线占全链多少」；CPU 占核比 >50% = 忙墙、
<15% = 等/供给墙并细分：RSS 逐档上涌 → 窗口/join 容量，RSS 平稳 → 供给侧）+ 健康校验
（`appended` 追平、致命计数器、`emitted_total`）。预热档**只占一行占位、不显示任何数字**——
它跑在「窗口全空 + 冷启动」的特殊状态，与后续档不可比（q1 无状态查询它反而慢、q9 join 查询它虚高）。

### 实测墙表（2026-08-24，M3 Max 12 核，N=10M，预热档开）

| 查询 | floor | rules | full | 主墙（占全链） | CPU 占用 | 判定 |
|---|---|---|---|---|---|---|
| q1（无状态投影） | 31.8M | 33.0M（−1.2ns） | 18.8M（+22.9ns） | 输出链 43% | 53% | 忙墙 → 采样定位段内热点 |
| q5（滑窗 top-N） | 31.7M | 939k（+1032.8ns） | 918k（+24.7ns） | 规则求值 95% | 9% | 等/供给墙 → 查串行/同步，profiler 采到的是等待栈 |
| q9（deferred join） | 0.99M | 678k（+470.5ns） | 704k（−56ns） | 规则段 33%（**基线 floor 已占 71%**） | 9% | 等墙细分：RSS 1.4× 温和增长 → 查规则段等待/join 串行，而非供给侧 |

> **q9 的读法**：主墙增量（+470.5ns）只占全链 33%，墙前基线（floor 1005.3ns）自己就占 71%
> ——所以 q9 的真瓶颈是**窗口/join 侧整体成本**（三流 join 的窗口维护被计在 floor 段），规则
> 段增量是第二道墙。单看「增量最大段」会漏掉这个结构，报告因此同时给出基线占比。

> 两次独立运行的 `floor` 一致（31.7 / 31.8M）——管道净段与查询无关，可作口径自校验。

### 诊断纪律（违反会得出假结论）

1. **预热档默认开，别关**（`WARMUP=0` 才关）：墙梯在单 daemon 内顺序跑，**首档独自承担窗口
   冷分配/page fault**。实测 q1 10m 不预热时 `floor`(21.2M) 反而慢于 `rules`(26.6M) 25%，
   偏差大于信号（RSS 逐档 1.98→3.10→4.07GB 即痕迹）；预热后 `floor` 升到 31.8M、墙梯
   恢复单调。脚本会在出现负增量时报警并根据预热是否已开给不同处理建议。
2. **帧文件必须与当前 schema 同版本**：旧版本帧（缺 `channel_id`/多 `wp_src_ip` 等）会
   在 window actor 报 `schema mismatch` 后**整批丢弃**，只剩哨兵被处理 → 墙表出现
   50M EPS 级假象。`diag.sh` 因此**不自动复用其它 `DATA_VER` 的帧**，且强制校验
   `appended = N × 档数`（不追平即判失败并给出根因）。
3. **`--n-list` 一次只能给一个 N**：哨兵驱动的切换在每档**首个**哨兵后即发生，同档的
   第 2 个 N/第 2 轮会吃到下一档门控。`diag.sh` 因此把 `N_LIST` 拆成外层循环（每个 N
   重启一套墙梯），而不是交给 `wfgen perf-diag --n-list` 一次跑多值。
4. **事件时间跨度 ≤ `allowed_lateness`**：墙梯把同一份数据发 N 次，事件时间不随重发前进，
   重发数据的迟到量 = 数据跨度（v5 = 100µs/事件，30M → 3000s > 默认 1800s）。超出即被
   `late_policy=drop` 丢弃 → rules/full 档实际无数据。脚本默认自动放宽
   （`LATENESS_FIX=1`，只影响迟到判定、不影响吞吐口径），并拦截跨度超窗口 `over=1h` 的规模。
5. **RSS 列是档内峰值、随墙梯累积**（同一份数据发多次），不是该段的内存成本；内存分析用 `bench.sh`。
6. **on-each 类查询的输出墙口径**：`cut_output` 在 on-each 直投路径上于 `OutputRecord`
   构造**之前**返回（该路径的 emitted 计数与 append 耦合、无法保留），所以 q1 这类查询的
   `rules → full` 增量包含「构造 + append（record→列）+ 通道 + sink 物化/序列化/写」；
   match 类查询的构造成本已计入 `rules` 档。

## scripts/ 工具清单

| 脚本 | 用途 |
|---|---|
| `wfgen verify-nexmark [--query qN] [--engine-emit data]` | 真实规则引擎 ground truth + 引擎 EMIT 对拍（git-diff 同款分层） |
| `extract_emitted.py` | metrics.ndjson → emitted/append/正确性计数器汇总 |
| `read_metrics.py` | metrics NDJSON 指定 stage/name/label 最新值查询 |
| `bench_lib.py` | bench.sh / diag.sh（两 case 共享，位于 `../scripts/`）的度量工具库（comma/parse-n/EPS/引擎游标/哨兵汇总/告警摘要/正确性摘要/CPU·RSS 采样），纯标准库，子命令分派可单独验证：`python3 ../scripts/bench_lib.py --help` |
| `diag_analyze.py` | diag.sh（两 case 共享，位于 `../scripts/`）的墙表/墙判定/健康分析器（哨兵四元组 × CPU·RSS 采样 × metrics），输入走环境变量 |

## 前提

- `wfusion` / `wfgen` 在 PATH 或 `WFUSION=/path WFGEN=/path`，需含 `gen-nexmark`、
  `dump-frames`/`send-arrow`、`stream` 子命令。
- `nc`、`python3`；端口 9800 空闲。
- 正确性验证另需 sink 输出（file_json_sink 的 alerts.ndjson），吞吐 PK 用 blackhole。
