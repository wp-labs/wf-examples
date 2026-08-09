# eps_throughput — 吞吐压力测试（目标 EPS ≥ 10000）

测量 wfusion 引擎的**输入处理吞吐**（events/sec），验证单机单实例能持续处理 **1W 事件/秒**以上。

## 实测结果（macOS, executor_parallelism=4）

| 模式 | 事件数 | 实测 EPS | 目标 |
|---|---|---|---|
| `burst`   | 100000 | **~40000** | ≥ 10000 ✓ |
| `sustain` | 60000  | **~34000** | ≥ 10000 ✓ |

（不同机器/CPU 会有差异；`row/s` 峰值实测约 5 万/s。）

## 三种规则压力面（`--mode` 参数）

| generator 模式 | 压力特点 |
|---|---|
| `global`   | 所有事件一个实例，最纯的引擎处理路径 |
| `pool`（默认）| 固定 1000 个 sip 复用实例（贴近真实） |
| `distinct` | 每事件 distinct sip，实例 map churn 最大压力 |

## 运行

```bash
./run.sh burst 100000 pool    # 峰值吞吐（默认）
./run.sh sustain 60000 pool   # 持续吞吐
./run.sh burst 100000 distinct # 实例 churn 压力
```

依赖：`wfusion` / `wfgen` 在 PATH（或用 `WFUSION=/path WFGEN=/path` 覆盖）、`nc`。

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
