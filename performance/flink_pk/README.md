# flink_pk — PK-Flink 专用：250 条 CEP 风格规则吞吐

专门为与 **Flink CEP** 对比而建的 case，不与 `eps_throughput_rules100`（#18 吞吐/内存回归）
混在一起。目标：引擎在**模式密集**规则（seq/close/multi，即 Flink CEP 最贵的 NFA 工作负载）
下的单节点吞吐，直接对标公开锚点。

## 对标锚点

| 锚 | 负载 | EPS | 来源 |
|---|---|---|---|
| **PatternStudio**（Flink 运行时 CEP） | 250 条 pattern，单节点普通硬件 | **~47.9k** | [PatternStudio](https://www.scilit.com/publications/1d420299589ce81c7ac91e74e7b132c1) |
| Flink 单查询窗口聚合（Nexmark，8 CU） | 单条窗口聚合查询 | ~80k–270k/查询 | [Nexmark 白皮书](https://help.aliyun.com/en/flink/realtime-flink/support/nexmark-performance-testing) |

PatternStudio 是最贴近的锚：单节点、~250 条、CEP pattern（NFA partial-match，模式密集）。

## 规则（250 条，pattern 占 ~86%）

`scripts/gen_rules.py` 生成 `models/rules/pk.wfl`（250 条，引擎加载 260 条规则条目，
10 条 pipeline 各拆 2 阶段）：

| 类别 | 数量 | 覆盖 |
|---|---|---|
| seq 2 步（seq2_） | 96 | conn 动作对 × 阈值对 × key(sip/dip) |
| seq 3 步（seq3_） | 24 | 三步有序 |
| close（and-close，close_） | 40 | conn/proxy/firewall 窗口闭合 |
| multi 多事件关联（m_） | 36 | conn+dns/proxy/firewall、proxy/firewall+dns |
| pipeline（pipe_） | 10 | 两阶段 fixed 桶 |
| guard + count（g_） | 44 | bool/float/object 嵌套/数组/字符串/数学函数 |

实体统一 ip，match key 独立变化（sip/dip）。重新生成：`python3 scripts/gen_rules.py > models/rules/pk.wfl`。
事件复用 `eps_throughput_rules100` 的 6 类事件源（conn/auth/dns/proxy/firewall/file），
由 `scripts/gen_events.py` 生成。

## 运行

```bash
./run.sh                          # 默认 stream 200000 normal（单连接流式持续）
./run.sh peak 200000 normal       # 峰值突发
./run.sh stream 1000000 normal    # 长跑（100 万事件）
CHUNK=1000 RATE_MS=50 ./run.sh stream 200000 normal  # 受控持续入流速率
```

健康检查：驱逐告警 = 0 且总告警 > 0（本 case 不做 #18 门禁，只测吞吐）。

## 实测（200000 事件，release，2026-08-11，单 Mac）

EPS 用 **send 墙钟**计时（同 rules100 口径）。

| 模式 | 送达 | EPS | 驱逐 | 告警 |
|---|---|---|---|---|
| `stream` normal | 200000 | **~166k–172k** | 0 | 105k–301k |
| `peak` normal | 200000 | **~145k** | 0 | ~105k |

引擎加载 260 规则条目，6 类事件源，2m/5m 有状态窗口，实体实例（normal 模式 1000 sip）。

## 对比结论

**同口径（单节点、~250 规则、pattern 密集）：我们 ~145–172k EPS vs PatternStudio ~47.9k → 约 3–3.5×。**

- 关键：即使换成 pattern 密集规则（seq/close/multi 占 86%），引擎吞吐**不随模式形状坍缩**——
  和 300 条混合规则（rules100，~114–165k）基本持平。Flink CEP 的吞吐被 NFA partial-match
  状态卡死，规则越模式化越慢；我们的引擎按"每事件一条"做共享解析 + 逐规则状态，规则数/形状
  对吞吐影响小（#19 共享解析）。

## 诚实边界

1. **非全等对比**：250 条里实测 ~81 条触发告警（close/pipe 及高阈值 seq/multi 未触发）——
   吞吐反映**全部 250 条**的评估/状态成本（引擎为每条建实例），但触发面不是 100%。
2. **规则性质不同**：我们的 seq/close/multi 是状态计数 + 排序语义；PatternStudio 是 NFA
   pattern（partial-match 组合状态）。方向可比、机制不同。
3. **硬件不同**：M 系 Mac vs 普通 x86，均为单节点。
4. **锚本身是研究框架**（neuro-symbolic），非生产级 Flink，EPS 可能偏低。

## 前提

- `wfusion` / `wfgen` 在 PATH（或用 `WFUSION=/path WFGEN=/path`），wfgen 需含
  `--chunk/--rate-ms` 单连接流式支持（warp-fusion 84333b5）。
- `nc`、`python3`；端口 9800 空闲。
