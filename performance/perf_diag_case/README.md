# perf_diag_case — 性能诊断机制（perf-diag）独立验证 case

> 目的：**验证诊断模式机制的正确性**（同时提供有区分度的墙梯）。机制设计见
> `wp-reactor/docs/design/perf-diag-mode-design.md`。独立于 nexmark_pk/qradar_pk，
> 不与基准数据混在一起。

## 验证什么（机制五要素）

| # | 要素 | 验证方式 | 验收标准 |
|---|---|---|---|
| 1 | 诊断模式进入 | `wfusion daemon --perf-diag conf/perf-diag.toml` | 启动成功，`__wf_sentinel` 窗口注册 |
| 2 | 门控生效 | `cut_rules` 档（floor） | 规则求值直通（emit=0），append 追平（管道照跑） |
| 3 | 哨兵记录 | 每批帧尾追加 sentinel | `data/perf_sentinel.ndjson` 每档一条，四元组 `{round, n, start_ns, emit_ns}` 完整 |
| 4 | 状态机自切换 | sentinel 处理后自动推进 | `stage{current=k+1}` 记录出现（先于 sentinel 记录），无外部控制指令 |
| 5 | 墙梯形状 | floor → rules → full 三档 | 每档 EPS 可算且单调 `floor ≥ rules ≥ full`（相对增量有意义） |

## 结构与数据

- **1 个流** `evt_events`（sip/action/code/blocked/bytes，确定性 seed=42）——
  事件时间 **1ms 步进**：1000 个 sip 轮转 → 同 sip 事件间隔 1s，2m 窗口内每个
  sip 积累 ~120 条 → count/guard/distinct 规则**真实触发**，规则墙可见
  （早期 0.6s 步进版本窗口内计数上不去，floor≈full，墙梯无区分度）；
- **21 条规则**（3 类 × 7）：count（`c_*`）/ guard（`g_*`）/ distinct（`d_*`）——
  三类在规则墙上成本不同，验证墙梯能区分；yield 到 `network_alerts`；
- **sink**：`business.d/sentinel.toml` 把哨兵记录落盘
  `data/perf_sentinel.ndjson`；`business.d/rules.toml` 把规则输出 blackhole
  （只测吞吐，对齐 Flink discarding sink 口径）。

```
perf_diag_case/
├── conf/
│   ├── wfusion.toml        # 最小 daemon 配置（rule_shards=1：规则墙可见）
│   └── perf-diag.toml      # 诊断档：floor → rules → full
├── models/
│   ├── schemas/evt.wfs     # 1 流 schema（evt_events + network_alerts）
│   └── schemas/windows.toml
│   └── rules/basic.wfl     # scripts/gen_rules.py 生成
├── scenarios/evt.wfg       # dump-frames 场景
├── scripts/
│   ├── gen_events.py       # 确定性事件 → JSONL
│   └── gen_rules.py        # 21 条规则 → basic.wfl
├── topology/
│   ├── sources/ingress.toml    # TCP 源（9800）
│   └── sinks/business.d/       # sentinel（哨兵落盘）+ rules（blackhole）
└── verify.sh               # 验证驱动（已实现，可直接运行）
```

## 验证流程

```bash
./verify.sh 100000        # 快速机制验证（默认 N=100k）
./verify.sh 1000000       # 质量门禁：N≥1M 时断言 floor 档 EPS ≥ 10M
```

## 实测墙表（2026-08-24，本机）

N=1M（`--rounds 1`，机制验证 + 10M 门禁）：

| 档 | EPS | 说明 |
|---|---|---|
| floor（管道净段） | ~45M | 注入+解码+窗口（≥10M 门禁 ✓） |
| rules（+规则求值） | ~540k | 21 条触发中的 match 规则是主墙 |
| full（+输出链） | ~550k | blackhole 输出 ≈ 规则档（输出墙在此规模不显著） |

## 验收清单（verify.sh 断言）

- [x] daemon 以 `--perf-diag` 启动成功，日志无 sentinel 相关报错；
- [x] `perf_sentinel.ndjson` 行数 = 诊断档数 × 轮数，每行四元组
      `round/n/start_ns/emit_ns` 齐全且 `emit_ns > start_ns`；
- [x] `floor` 档规则 emit = 0（cut_rules 生效），append 追平（管道未断）；
- [x] 每档完成信号（`stage{current=k}`）出现后推进下一档（无超时卡死）；
- [x] 墙表单调：`floor ≥ rules ≥ full`——规则墙（floor→rules，~60×）严格断言
      ±10%；输出墙（rules→full，blackhole 近无成本）容差 ±20%（噪声内）；
- [x] 全程单 daemon 未重启（pid 不变）；
- [x] `N ≥ 1M` 时 floor 档 EPS ≥ 10M。

## 质量门禁（实现验收）

1. **新增代码单测覆盖率 ≥ 90%**（`cargo llvm-cov` 行覆盖率口径，实测）：
   - wf-config `PerfConfig` 解析：**100%**；
   - wf-runtime `perf_diag.rs`（门控/哨兵/状态机/排空等待/投递）：**99.2%**；
   - wfgen `cmd_perf_diag.rs`（EPS 计算/哨兵文件读取/帧前缀）：单测齐备；
2. **case 下性能 ≥ 10M EPS**：`floor` 档用 **N ≥ 1M** 验收（实测 ~45M）。
