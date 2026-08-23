# perf_diag_case — 性能诊断机制（perf-diag）独立验证 case

> 目的：**验证诊断模式机制的正确性**（不是测性能数字）。机制设计见
> `wp-reactor/docs/design/perf-diag-mode-design.md`。独立于 nexmark_pk/qradar_pk，
> 不与基准数据混在一起。

## 验证什么（机制五要素）

| # | 要素 | 验证方式 | 验收标准 |
|---|---|---|---|
| 1 | 诊断模式进入 | `wfusion daemon --perf-diag conf/perf-diag.toml` | 启动成功，`__perf_sentinel` 窗口注册 |
| 2 | 门控生效 | `cut_rules` 档 | 规则 emit = 0，append 正常（管道照跑） |
| 3 | 哨兵记录 | 每批帧尾追加 sentinel | `data/perf_sentinel.ndjson` 每点一条，四元组 `{round, n, start_ns, emit_ns}` 完整 |
| 4 | 状态机自切换 | sentinel emit 后自动推进 | `perf_point{current=k+1}` 指标出现，无外部控制指令 |
| 5 | 墙梯形状 | floor → rules → full 三点 | 每档 EPS 可算且单调 `floor ≥ rules ≥ full`（相对增量有意义） |

## 结构与数据

- **1 个流** `evt_events`（sip/action/code/blocked/bytes，100k 事件，确定性 seed）——
  最小但覆盖规则墙；
- **21 条规则**（3 类 × 7）：count（`c_*`）/ guard（`g_*`）/ distinct（`d_*`）——
  三类在规则墙上成本不同，验证墙梯能区分；
- **sink**：`infra.d/perf_sentinel.toml` 把哨兵告警落盘 `data/perf_sentinel.ndjson`。

```
perf_diag_case/
├── conf/
│   ├── wfusion.toml        # 最小 daemon 配置（metrics 100ms）
│   └── perf-diag.toml      # 诊断点：floor → rules → full
├── models/
│   ├── schemas/evt.wfs     # 1 流 schema
│   └── schemas/windows.toml
│   └── rules/basic.wfl     # scripts/gen_rules.py 生成
├── scripts/
│   ├── gen_events.py       # 100k 确定性事件 → JSONL
│   └── gen_rules.py        # ~24 条规则 → basic.wfl
├── topology/
│   ├── sources/ingress.toml    # TCP 源（9800）
│   └── sinks/infra.d/perf_sentinel.toml   # 哨兵记录 sink
└── verify.sh               # 验证驱动（引擎实现后可用）
```

## 验证流程（引擎落地后执行）

```bash
# 1. 生成数据 + 规则
python3 scripts/gen_events.py 100000 > data/evt.jsonl
python3 scripts/gen_rules.py > models/rules/basic.wfl

# 2. 起诊断 daemon（启动参数进入，不带即全关）
../../../warp-fusion/target/release/wfusion daemon --perf-diag conf/perf-diag.toml \
  --config conf/wfusion.toml --work-dir . &

# 3. dump 帧（schema 握手）
../../../warp-fusion/target/release/wfgen dump-frames --scenario ... --input data/evt.jsonl \
  --addr 127.0.0.1:9800 --ws models/schemas/evt.wfs --output data/evt.frames

# 4. 驱动诊断（3 点 = 3 轮）
../../../warp-fusion/target/release/wfgen perf-diag --diag conf/perf-diag.toml \
  --frames data/evt.frames --addr 127.0.0.1:9800 --n-list "100k"

# 5. 验收检查
cat data/perf_sentinel.ndjson          # 每点一条四元组
cat data/perf_diag_wall.txt            # 墙表：EPS 单调 + 增量成本
```

## 验收清单（verify.sh 断言）

- [ ] daemon 以 `--perf-diag` 启动成功，日志无 sentinel 相关报错；
- [ ] `perf_sentinel.ndjson` 行数 = 诊断点数 × 轮数，每行四元组 `round/n/start_ns/emit_ns` 齐全且 `emit_ns > start_ns`；
- [ ] `floor` 档规则 emit = 0（cut_rules 生效），但 append 追平（管道未断）；
- [ ] 每档完成信号（sentinel emit）出现后 `perf_point` 推进到下一档（无超时卡死）；
- [ ] 墙表三档 EPS 单调 `floor ≥ rules ≥ full`，增量成本非负（噪声容差 ±10%）；
- [ ] 全程单 daemon 未重启（pid 不变）。

## 质量门禁（实现验收）

1. **新增代码单测覆盖率 ≥ 90%**：`PerfConfig` 解析、门控切口（cut_rules/cut_output）、
   内置哨兵窗口/规则/指标、诊断点状态机、`wfgen perf-diag`（EPS 计算与哨兵读取逻辑
   抽成库函数以便单测）——按 `cargo llvm-cov` 行覆盖率口径；
2. **case 下性能 ≥ 10M EPS**：`floor` 档（管道净段）用 **N ≥ 1M** 验收（100k 太小受
   固定开销影响）——单流小字段的管道吞吐应高于 qradar 6 流 floor（9.7M），目标 10M+。
   verify.sh 增断言：`--n-list "100k,1m"` 的 1m floor EPS ≥ 10M。
