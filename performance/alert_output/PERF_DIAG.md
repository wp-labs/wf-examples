# wfusion_new 性能墙定位

`diag.sh` 参考 `performance/nexmark_pk/diag.sh` 的 perf-diag 思路，为当前
`sdm_event -> sdm_alert` 管道提供分段定位。它把同一批预编码 Arrow frame 在
一个 daemon 内按档位重放：

| 档位 | 保留的链路 | 主要回答的问题 |
| --- | --- | --- |
| `recv` | TCP 收帧，跳过普通帧解码 | 注入/接收是否成为供给墙 |
| `decode` | 收帧 + 解码，不 append 窗口 | 解码成本是否明显 |
| `floor` | 收帧 + 解码 + 窗口 append，切规则/输出 | 窗口和 fanout 成本 |
| `rules` | 再加规则求值，切输出 | 规则执行成本 |
| `emit` | 再加输出构建/投递，切 sink 消费 | 列式输出构建成本 |
| `full` | 完整业务输出链 | 序列化、sink 写和背压成本 |

默认墙梯不包含 `emit`，因为 `rules -> full` 已经能看到完整输出增量；需要
拆开时使用 `--stages floor,rules,emit,full`。默认业务 sink 改为
`blackhole_sink`，这样 full 档不被磁盘速度主导；使用 `--sink file` 才会把
`file_json_sink` 的序列化和写盘纳入测量。

## 运行

```bash
cd performance/wfusion_new
./diag.sh 100k
./diag.sh 1m --sink blackhole
./diag.sh 200k --stages floor,rules,emit,full --rule-shards 4
# 9800 已有进程时换一个本机端口
./diag.sh 100k --addr 127.0.0.1:19800
```

脚本优先使用仓库根目录下 `../../warp-fusion/target/release/{wfusion,wfgen}`，再回退到
`/Users/dy_xuyuhao/bin` 和 `PATH`。也可以显式指定：

```bash
./diag.sh 1m --wfusion-bin /path/to/wfusion --wfgen-bin /path/to/wfgen
```

事件由 `scripts/wfusion_perf_diag_events.py` 从 `models/samples/ngsoc_alert_23.json`
逐行生成；每条记录含 23 个 NGSOC 业务字段（3 个 `_stream`/`_window`/`_timestamp`
路由元数据不计入字段数）。这是输入字段数，输出字段仍由 `alert.wfl` 的 `yield` 决定。
`wfgen dump-frames`
预编码后才开始墙梯。为让预编码帧直接进入引擎的 Arrow 解码路径，运行副本会把
TCP source 改为 `arrow_framed`/`len` 并清空固定 `stream_tag`，同时改写端口、业务
sink 和 metrics 报告周期；原始 `conf/`、`models/`、`topology/` 不会被覆盖。

## 结果和判读

每次运行生成一个唯一目录 `data/perf_diag_runs/<run-id>/`，其中包括：

- `data/perf_sentinel.ndjson`：每档的 `{round,n,start_ns,emit_ns}` 完成信号；
- `data/perf_diag_wall.txt`：`wfgen` 原始墙表；
- `data/out_dat/metrics.ndjson`：窗口、规则、输出和错误计数器；
- `data/metrics_snapshot.ndjson`：停止 daemon 前冻结的 metrics，分析器读取此快照，
  避免旧 connector 在 shutdown 时产生的连接关闭告警污染健康度；
- `data/samples.tsv`：daemon 的 CPU/RSS 采样；
- `logs/`：daemon、帧编码和墙梯日志。

可读报告和原始墙表也会复制到 `data/perf_diag_<N>_<run-id>.txt` 与
`data/perf_diag_wall_<N>_<run-id>.txt`。分析器用末档的 `ns/事件` 做统一分母，
把相邻档的差作为增量，并在墙表中列出每档 CPU 平均/峰值和 RSS 峰值；它同时检查 sentinel 完成数、窗口 append 数、规则事件、
emitted/dispatch 计数和错误/丢弃计数。`memory_evicted_total`、`time_evicted_total`
和 `channel_full_total` 会作为非致命运行计数器展示：前两者是窗口策略动作，后者
表示生产者曾等待 sink 消费，不等于已经丢数据。

读墙表时，`decode - recv` 近似解码增量，`floor - decode` 是窗口/fanout，
`rules - floor` 是规则求值，`full - rules` 是输出链；其中增量最大的正值是主墙。
这些差分必须在墙梯单调时才有意义；报告遇到大幅负增量会明确标记为不可结论。

若某段出现大幅负增量，报告会写明墙梯不满足单调性并且不输出主墙。这通常表示
缓存、调度、背压、窗口容量或单轮测量噪声大于真实段成本；应增大 `N`，再用
相同参数交错重跑。报告中的 `appended` 未达到期望值或出现错误计数时，EPS 也
只能作为方向参考。

`wfusion_new` 的窗口配置有总容量和迟到策略。大事件量会让同一批数据在多个档位
重复进入窗口，建议先用 `100k` 验证链路，再用 `1m` 或更大规模定位；如需控制
容量，可传 `--max-total-bytes 8GB` 或 `--max-total-bytes 0`，实际值会写入报告
口径行。`--cleanup` 可在分析完成后删除本次临时运行目录，默认保留证据。
`--shutdown-secs` 可单独调整 daemon 的优雅停止宽限；File sink 或大批量规则在
停止时仍有尾部 drain 时，应保持默认值或显式增大它。
