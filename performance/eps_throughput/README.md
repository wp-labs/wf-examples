# eps_throughput — 吞吐压力测试（目标 EPS ≥ 10000）

测量 wfusion 引擎的**输入处理吞吐**（events/sec），验证单机单实例能持续处理 **1W 事件/秒**以上。

## 实测结果（macOS, executor_parallelism=4, **release** 二进制）

| 模式 | 事件数 | 实测 EPS | 目标 |
|---|---|---|---|
| `burst`   | 200000 | **~150,000** | ≥ 10000 ✓ |
| `sustain` | 200000 | **~92,000** | ≥ 10000 ✓ |
| `burst distinct` | 200000 | **~153,000** | ≥ 10000 ✓ |

> **release vs debug**：压测默认用 `target/release`（`PROFILE=release`，run.sh 自动解析）。
> debug 构建显著偏慢（实测 ~40k EPS）—— 报告 EPS 请始终用 release。
> 不同机器/CPU 会有差异；daemon 关闭时 res 汇总表的 `row/s max` 是引擎真实接收峰值。

## 三种规则压力面（`--mode` 参数）

| generator 模式 | 压力特点 |
|---|---|
| `global`   | 所有事件一个实例，最纯的引擎处理路径 |
| `pool`（默认）| 固定 1000 个 sip 复用实例（贴近真实） |
| `distinct` | 每事件 distinct sip，实例 map churn 最大压力 |

## 运行

```bash
./run.sh burst 200000 pool      # 峰值吞吐（默认，release）
./run.sh sustain 200000 pool    # 持续吞吐
./run.sh burst 200000 distinct  # 实例 churn 压力
PROFILE=debug ./run.sh burst 50000 pool   # 显式 debug（对比用）
```

依赖：默认用 `REPO_ROOT/target/{release,debug}`（REPO_ROOT=../warp-fusion），找不到回退 PATH
（或用 `WFUSION=/path WFGEN=/path` 显式覆盖）、`nc`。

## 指标说明

- **EPS** = 事件数 / 从发送开始到引擎接收全部事件的时间。
- 接收计数取 `receiver.rows_total`（`report_interval=1s`，**区间计数需累加** = 累计接收）。
- 引擎侧确认：daemon 关闭时的 res 汇总表 `total rows = N`、`row/s max`（峰值）。

## 规则

`models/rules/throughput.wfl`：
- `global_throughput` — `match<:1m>` 全局实例，每事件 count 累加（阈值 1 亿不触发，无告警干扰）。
- `per_sip_instances` — `match<sip:1m>` 每 sip 实例，压实例 map 的 get/insert/update。

## 调优（若未达 1W EPS）

- `conf/wfusion.toml` 增大 `executor_parallelism`（按核数）。
- 减小 `[metrics] report_interval`（更细的采样，不影响吞吐）。
- 检查 `data/daemon.log` 的 res 汇总表 `row/s max` —— 那是引擎真实接收峰值。
