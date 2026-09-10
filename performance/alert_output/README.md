# wfusion_new — 当前规则性能 case

这个目录把当前 `wfusion_new` 工程中的规则、schema、窗口、TCP source、sink 和诊断脚本
作为一个独立 performance case 归档。三个入口是直接放在本目录的 `nexmark_pk` 风格脚本，
不会再调用 `scripts/wfusion_eps_bench.sh`。生成的 `data/`、`tmp/` 和运行时文件由仓库忽略。

三个入口使用同一套 `models/rules/alert.wfl` 和 `sdm_event` 工程配置：

```bash
cd performance/wfusion_new

# 端到端吞吐；replay 先用当前生成器做 Arrow frame，再直接 send-arrow
./bench.sh alert replay 1m --mode blackhole

# 统计第一条发送到最后一批完成的时间和 EPS（参数原样传给 bench.sh）
./first_last_eps.sh mix replay 3m --mode file

# 直接监控业务输出文件，固定测试 mix/replay 300 万事件、File sink
./time_file_3m.sh

# 包含 JSON 序列化和 File sink 的完整链路
./bench.sh alert replay 1m --mode file --trials 3

# 也可以用 wfgen stream 做实时生成
./bench.sh alert stream 1m --mode blackhole --rate 300000

# 性能墙定位；按 recv/decode/floor/rules/emit/full 分段
./diag.sh 1m --sink blackhole --rule-shards 4 \
  --max-total-bytes 1536MB --stages recv,decode,floor,rules,emit,full

# daemon + TCP + File 输出的一对一正确性验收
./verify_daemon.sh alert 100k --rule-shards 4 --sink-parallel 2
```

`bench.sh` 遵循 Nexmark 脚本的 `query feed total` 入口，当前 `alert`、`sdm_alert`、
`mix`、`all` 都映射到本目录唯一的 `sdm_alert` 规则；调优参数使用实际值，默认是
`rule_shards=4`、`sink_parallel=2`、`max_total_bytes=1536MB`、`sdm_event` 窗口
`512MB`、`allowed_lateness=30s`；`diag.sh` 默认也使用 `rule_shards=4` 和
`max_total_bytes=1536MB`。`verify_daemon.sh` 固定 File sink，要求 appended、
`emitted_total`、输出条数和唯一 `alert_id` 均与输入事件数一致；`diag.sh` 使用同一规则工程，
在临时副本中预编码 Arrow frame，再按 recv/decode/floor/rules/emit/full 墙梯定位成本。
压测结果写到 `data/bench_<query>_<feed>_<sink>.txt`，验证摘要写到
`data/verify_daemon_all.txt`；诊断报告沿用 `data/perf_diag_*` 命名。

`first_last_eps.sh` 是 `bench.sh` 的计时包装器。它读取本轮
`data/perf_sentinel.ndjson` 中最早的 `start_ns`（发送起点）和最晚的 `emit_ns`（业务数据
排空后的完成时刻），计算 `EPS = 事件数 / ((emit_ns - start_ns) / 1e9)`。帧预编码和
daemon 启动不计入该区间；`--trials` 大于 1 时只报告最后一轮。

`time_file_3m.sh` 直接执行 `./bench.sh mix replay 3m --mode file`，后台逐块读取
`data/out_dat/sdm_alert.json`，记录读到首个和最后一个换行符的时间，并输出
`first_to_last_seconds`、`file_eps` 和报告路径。这个计时不读取 sentinel；它反映业务文件从
第一条记录可见到最后一条记录可见的区间，文件行数不足 3,000,000 时会失败。

压测输入统一由 `scripts/wfusion_perf_diag_events.py` 生成 NGSOC 告警风格的
`sdm_event`。每条事件包含之前 NGSOC 样本的 23 个业务字段：

```text
tenant_id, log_id, event_id, occur_time, parse_time, schema_version,
mapping_id, log_type, log_name, severity, source_ip, target_ip,
observer_vendor, observer_product, source_finding_title,
source_finding_severity, source_finding_category, source_original_event_id,
data_src_product, ingest_time, record_kind, source_finding_obj, extensions_obj
```

其中 `log_type=ngsoc_alert_info`；`_stream`、`_window`、`_timestamp` 只是
Arrow/TCP 路由元数据，不计入 23 个字段。这样 `bench.sh` 的 replay、`diag.sh` 和
`scripts/wfusion_eps_bench.sh` 使用同一输入形状，便于直接比较结果；`bench.sh` 的
stream 模式仍由 WFG 场景实时造数。这里的 23 个字段是输入事件字段数，WFL 输出字段数
仍由 `models/rules/alert.wfl` 中的 `yield` 定义。

二进制默认从 PATH 查找。也可以显式指定：

```bash
WFUSION=/absolute/path/to/wfusion ./bench.sh alert replay 1m --mode blackhole
./diag.sh 1m --wfusion-bin /absolute/path/to/wfusion \
  --wfgen-bin /absolute/path/to/wfgen
```

本地仓库布局下，诊断脚本也会尝试使用 `../../warp-fusion/target/release/` 中的 release
二进制；如果本地二进制较旧，先重新构建 `wfusion` 和 `wfgen` 再测试。
