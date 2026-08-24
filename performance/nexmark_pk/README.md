# nexmark_pk — NEXMark 基准：吞吐 PK + 正确性验证（对齐 Flink 官方基线）

与 Flink 对齐的 PK case：**同一份权威基准数据（NEXMark）+ 同一批查询（Q1~Q22）+ 同输出
口径（blackhole 丢弃）**，跑引擎实测吞吐与输出正确性，对照阿里 Nexmark 白皮书发布的
OSS Flink / VVR 基线。查询覆盖与语义对齐判定见 `docs/CAPABILITY_GAP_MATRIX.md`（22 条全实现）；
**性能数字以当次跑批 `data/bench_*_replay.txt` 为准**（历史实测报告已清理，见 git 历史）。

## 目录结构

```
bench.sh                 # 基准驱动（生成/复用帧 → 起 daemon → send-arrow 回放 → 采样）
conf/wfusion.toml        # daemon 配置（parse/rule 并行度等）
models/queries/qN.wfl    # 查询定义（唯一权威来源，每文件一组同族规则）
models/schemas/          # 事件 schema（nexmark.wfs）
topology/                # source/sink 拓扑（send-arrow 源、blackhole 汇）
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
   `__wf_sentinel` 窗口），`send-arrow/stream --sentinel <n>` 在数据末尾追加
   哨兵帧；引擎等**数据窗排空**后写 `{round,n,start_ns,emit_ns}`，EPS =
   n/(emit_ns−start_ns)——无 metrics 轮询（±200ms）粒度误差，短跑读数同样可信。
   哨兵超时退回 append_total 轮询兑底（`eps_mode=metrics-append`，TIMEOUT 标注）。
   （旧口径：metrics 三输入流 append 计数器求和追平 TOTAL 的时刻。）
2. **RSS 口径（2026-08-17 起）**：`parse_buffer_bytes` 默认值已改为 128MB
   （P0-② content 记账，18 槽）——q1 100M ≈ 6.1M / RSS ~5.9GB，旧默认
   （256MB 解码记账）为 5.93M / 4.4GB。吞吐优先场景显式调大预算（bench 默认
   2GB：q1 7.5M+，RSS 随吞吐升至 12-14GB）。引用 RSS 数字时必须标注所用
   `parse_buffer_bytes`，旧口径数字（“100M 6.8GB”等）与现默认不直接对等。
3. **A/B 必须不限速**：`RATE=10000000`（限速会把 EPS 封顶在 RATE）。
4. **同时段交错对比**：bench 机 EPS 与 RSS_peak 呈双峰相位强相关（同配置差 ±8%），
   结论必须按 RSS 相位配对；单轮数字只能作量级参考。
5. 消费侧计数器提取：`python3 scripts/extract_emitted.py data/metrics.ndjson`
   （counter 跨 1s 区间求和；gauge 取峰值，不可混用）。

## scripts/ 工具清单

| 脚本 | 用途 |
|---|---|
| `wfgen verify-nexmark [--query qN] [--engine-emit data]` | 真实规则引擎 ground truth + 引擎 EMIT 对拍（git-diff 同款分层） |
| `extract_emitted.py` | metrics.ndjson → emitted/append/正确性计数器汇总 |
| `read_metrics.py` | metrics NDJSON 指定 stage/name/label 最新值查询 |

## 前提

- `wfusion` / `wfgen` 在 PATH 或 `WFUSION=/path WFGEN=/path`，需含 `gen-nexmark`、
  `dump-frames`/`send-arrow`、`stream` 子命令。
- `nc`、`python3`；端口 9800 空闲。
- 正确性验证另需 sink 输出（file_json_sink 的 alerts.ndjson），吞吐 PK 用 blackhole。
