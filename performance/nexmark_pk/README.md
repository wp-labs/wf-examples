# nexmark_pk — NEXMark 基准：吞吐 PK + 正确性验证（对齐 Flink 官方基线）

与 Flink 对齐的 PK case：**同一份权威基准数据（NEXMark）+ 同一批查询（Q1/Q2/Q3/Q4/Q5/Q7/Q9）
+ 同输出口径（blackhole 丢弃）**，跑引擎实测吞吐与输出正确性，对照阿里 Nexmark 白皮书发布的
OSS Flink / VVR 基线。性能结论与诚实边界见 `PK_REPORT_MAC.md`（姊妹文档）。

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

事件时间 ~30 分钟，hot 分布（50% hot auction / 25% hot bidder / 25% hot seller）。
**同一 count + seed 的生成结果字节级确定**（确定性已验证，脚本可重复复现）。
`scripts/gen_nexmark.py` 保留为算法参考实现。

## 查询：与 Flink Nexmark 测试集的逻辑匹配度

Flink 参照系 = [Alibaba Nexmark 白皮书](https://help.aliyun.com/en/flink/realtime-flink/support/nexmark-performance-testing)
测试集内的 7 条查询（Q1/Q2/Q3/Q4/Q5/Q7/Q9；**Q6、Q8 等其余 12 条未实现**）。
每条按「语义对齐程度」分三档：

| Query | Flink 语义 | wfusion 实现（`models/queries/`） | 匹配度 |
|---|---|---|---|
| Q1 | 无状态投影：每 bid 输出一行 | `on each` 每 bid 一条告警（无 match 状态机——keyed 版会让 6M 实例表击穿 CPU 缓存，非对等工作负载） | **精确**（工作负载等价，输出 schema 不同） |
| Q2 | `WHERE MOD(auction,123)=0` 过滤 | `events { b && b.auction % 123 == 0 }` + count≥1 fire | **精确**（谓词同构，选中 ~0.81% bids） |
| Q3 | person⋈auction（seller=id）投影卖家信息 | auction 驱动 + `join person_events snapshot`，count≥1 每 auction 一条 | **精确**（join 语义同构；输出 seller id 而非 name/city/state 投影） |
| Q4 | bid⋈auction 后按 category 均价 | bid 驱动 + `join auction_events snapshot`（92M bids 进管道）+ 窗口 count | **工作负载等价**（join + 窗口聚合成本同构；聚合面是 count 而非 category 均价——**输出语义近似**） |
| Q5 | 滑窗计数面（Top-N 的 counting 面） | `match<auction:10m>` count≥{10,50,100} 三阈值 | **状态语义精确**（滑动计数 + reset-on-fire；非完整 Top-N 输出） |
| Q7 | 每 auction 滑窗最高出价 | `match<auction:10m>` max(price)≥{200,500,1000} 三阈值 | **状态语义精确**（滑窗 MAX；输出为阈值告警非 max 值投影） |
| Q9 | person⋈auction 按 seller 分组计数 | auction 驱动 + snapshot join + `match<seller:10m>` count≥1 | **精确**（join + 分组计数同构） |

**匹配度小结**：Q1/Q2/Q3/Q9 精确；Q5/Q7 窗口状态语义精确（输出为告警阈值面）；Q4 是唯一
工作负载等价但输出语义近似（count ≠ category 均价）的查询。全部 7 条的白皮书测试集内对齐
已覆盖（7/19 条 NEXMark 全集）。

### 正确性验证（30M cont，seed=1）

期望值由确定性模拟器 `scripts/verify_ground_truth.py` 独立推算（Python 重放 JSONL，精确镜像
引擎 match 语义，含 pending_expiry 每 key 单条目去重、fire/reset 保留实例 created_at=fire
时间等细节）：

| 规则 | 期望 | 引擎实测 | 结果 |
|---|---|---|---|
| q2_mod_123 | 224,289 | 224,289 | ✅ |
| q3_auction_seller | 1,800,000 | 1,800,000 | ✅ |
| q4_real_avg_100 | 27,600,000 | 27,600,000 | ✅ |
| q5_bidcount_10 | 1,712,532 | 1,712,470 | ✅（差 62 = 0.0036%，scan_timeouts 墙钟非确定性） |
| q5_bidcount_50 / 100 | 0 / 0 | 0 / 0 | ✅ |
| q7_maxbid_200/500/1000 | 10,350,961 / 34,578 / 0 | 同左 | ✅ |
| q9_seller_count | 1,800,000 | 1,800,000 | ✅ |

28k 事件探针逐 alert 对拍 **2,679/2,679 全量精确吻合**（含 fire 时刻）。q1 为无状态路径
（输出=输入 bid 数，无状态机语义）。全部跑批 appended 30M/30M，
serialize_failed/dropped_late/cursor_gap/memory_evicted = 0。

**100M 吞吐跑批的正确性侧证（2026-08-17，P0-② content 记账 2GB）**：全量 Q1-Q9
（q1/q2/q3/q4/q5/q7/q9）SUMMARY 全 clean（serialize_failed / dropped_late /
memory_evicted / cursor_gap = 0），appended=100M/100M。输出计数：q1=92,000,000
（=输入 bid 数）、**q2=747,816（占比 0.8129%，与 30M 的 0.8127% 精确吻合）**、
q9=6,000,000（=30M 期望 1.8M × 100/30）。q3/q5/q7 的 EMIT 在 run 间有驱逐时序
波动（非正确性破坏，见 PK_REPORT_MAC 测量说明）。

## 基准工具：bench.sh

```bash
./bench.sh [query=all|q1|q2|q3|q4|q5|q7|q9] [feed=cont|stream] [total=100m|30m|10m]
MAX_FRAME_BYTES=1048576 ./bench.sh all cont 30m    # 指定帧 cap（默认 8MiB）
```

- **feed=cont**（默认，PK 口径）：gen-nexmark → dump-frames 预编码 Arrow 帧 →
  `shard-frames`（默认 4 分片、`SHARD_KEYS=bid_events:auction` 键闭包）→
  `send-arrow --shard-files` 并发推（默认 `CONNECTIONS=4`，唯一数据分片）。
  帧文件 `data/bench_<total>[_mb<bytes>].frames` 跨查询复用，存在即不重生成。
- **feed=stream**：实时生成按 RATE 注入（客户端编码上限 ~760k/s，**非引擎能力**；
  用于正确性/长稳，不用于 PK）。
- 输出每查询 `data/bench_<q>_<feed>.txt`：EPS + RSS 峰值 + 驱逐数；
  `data/metrics.ndjson` 为计数器流，`data/{wfusion,daemon}.log` 为引擎日志。

### 测量纪律（违反会得出假结论）

1. **计时口径 = append_total**：metrics 三输入流 append 计数器求和追平 TOTAL 的时刻
   （旧 ingress 预读游标口径已作废；PK_REPORT_MAC §4.1 已按新口径更新）。
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
| `verify_ground_truth.py` | 确定性 ground truth 模拟器（stdin JSONL → Q2-Q9 期望 emitted 数），语义依据见文件头 |
| `extract_emitted.py` | metrics.ndjson → emitted/append/正确性计数器汇总 |
| `q5_diff_v2.py` | 引擎 alerts.ndjson 逐 alert 对拍（回归验证入口） |
| `q5_trace_auc.py` | 单 auction 逐事件 trace（分歧定位） |
| `gen_nexmark.py` | 生成算法参考实现 |

## 前提

- `wfusion` / `wfgen` 在 PATH 或 `WFUSION=/path WFGEN=/path`，需含 `gen-nexmark`、
  `dump-frames`/`send-arrow`、`stream` 子命令。
- `nc`、`python3`；端口 9800 空闲。
- 正确性验证另需 sink 输出（file_json_sink 的 alerts.ndjson），吞吐 PK 用 blackhole。
