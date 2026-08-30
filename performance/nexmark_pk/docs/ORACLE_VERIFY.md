# oracle 验证：定义、口径与边界（wfgen verify-nexmark）

> 本文回答三个问题：**oracle 是什么？它验证什么、怎么验证？哪些内容它不验证/不能验证？**
> 配套：`README.md`（工具用法）、`NEXMARK.md`（正确性标准）、`CAPABILITY_GAP_MATRIX.md`（查询语义）。

## 1. oracle 是什么

oracle（`wfgen verify-nexmark`）是 NEXMark 正确性验证的**权威参考实现**：用**真实 WFL 规则引擎**
（`wf_engine`，与 daemon 同一套引擎代码）处理与引擎**同一份确定性数据**、**同一套 `.wfl` 规则**，
逐条算出「引擎应该输出什么」。它不是手写模拟器、不是独立理想实现——它和引擎跑同一个
`RuleExecutor`/`StatsExecutor`，因此「oracle 与引擎一致」=「引擎与自身权威语义一致」，
任何差异都直接指向引擎实现缺陷。

**定义的三个锚点**：

| 锚点 | 来源 | 保证 |
|---|---|---|
| 数据 | `wfgen gen-nexmark`（同 count+seed **字节级确定**） | 两边处理完全同一份输入 |
| 规则 | `models/queries/*.wfl`（唯一权威来源） | 两边编译同一套语义 |
| 执行器 | `wf_engine`（CEP `CepStateMachine`+`RuleExecutor` / `StatsExecutor`） | oracle 与引擎同源，非平行实现 |

## 2. 处理流程

```
wfgen gen-nexmark <N> --seed 1
  └─ 30s 桶序事件流（与 daemon 收到的帧序一致）
       └─ 按规则 yield-bind 依赖并查集分组（每组一线程，独立吃完整事件流）
            ├─ CEP 规则（on-each / match / deferred / join）→ RuleEngine
            │    └─ 事件时间序喂入 CepStateMachine → OutputRecord → OracleAlert
            └─ stats 规则（q4b/q15~q19）→ StatsOracleEngine
                 └─ StatsExecutor 逐事件驱动，fixed 窗口 bucket 对齐推进
                      → 跨边界 close → OracleAlert
```

- **事件时间语义**：oracle 用事件时间驱动窗口推进/过期（确定性），引擎 replay 同口径；
  **不推进 EOS**（`close_at_eos=false`，与引擎 replay 对拍）——尾部未收口窗口不 close，
  对齐引擎行为（实证：q9/q16 若 close_all 会多出尾部窗口 EMIT）。
- **join**：join 目标窗口状态按 schemas 的 `over` 维护（预加载 + 事件时间过期），
  与引擎窗口可见性对齐。
- **stats**：仅 **fixed 窗口**接入 oracle（bucket 对齐推进）；session/sliding stats 跳过。

## 3. 验证层级（三档）

| 层 | 机制 | 验证粒度 | 覆盖 |
|---|---|---|---|
| L1 计数对拍 | `--engine-emit`：`规则名 计数` 文本行 diff | 每规则输出**条数** | 全部规则 |
| L2 内容断言 | `verify_daemon.sh` → `verify_file_lib.py content` | 每条 alert 的字段**形状/约束**（CHECKS 表 + 通用断言） | 全部规则 |
| L3 字段级明细对拍 | `--detail-diff`：oracle 逐条 yield 字段值 vs 引擎 `benchmark.ndjson` | **每条 alert 的每个字段值** | 已求值 yield 的规则（CEP/on-each/match/deferred） |

三档递进：L1 保证「数量对」，L2 保证「字段形状对」，L3 保证「字段值对」。
L3 是 2026-08-30 新增（本文件即其定义）。**每个查询的判定逻辑（正确语义 + 具体断言 + 当前
状态）见 `QUERY_VERIFY_LOGIC.md`。**

## 4. 字段级明细对拍（L3，`--detail-diff`）的口径

### 4.1 规范化行

oracle 每条 alert 与引擎 `benchmark.ndjson` 每行都规范化为同格式：

```
规则名 \t entity_id \t 字段名=值;字段名=值;...
```

- 字段 = yield 定义的业务字段（`id`/`alert_type`/`detail`/`request_count`）；引擎侧排除
  `__wfu_*` 系统字段后即 yield 字段（同源同名）。
- 字段按**名排序**（两侧一致，不依赖 yield 定义序）；`entity_id` = 引擎 `__wfu_entity_id`。
- 值格式化：数字整数精度打印整数（`1007.0 → "1007"`）、字符串原样、bool `true/false`——
  与引擎 file_json_sink 输出对齐。

### 4.2 比较方式：multiset（多重数）

两侧行集**排序后逐行比较会因重复行错位**（如 q6 每事件命中、同 id 多条完全相同的 alert）。
因此按**多重数比较**：每行内容在两侧出现的次数相等即一致。差异输出差集
（`- oracle 独有 / + 引擎独有`，最多 20 行样例）。

### 4.3 参与规则

oracle 侧只对**已求值 yield 字段**的 alert 产出明细行；以下规则自动排除（保持 L1 计数 + L2 内容断言覆盖）：

| 排除类别 | 规则 | 原因 |
|---|---|---|
| stats 规则 | q4b_category_avg、q15~q19 | oracle 的 stats close 未求值 yield 字段（待接入） |
| 中间管道输出 | q4a_auction_finals、q13a_bid_mod | yield 到中间窗（auction_finals/bid_mod），不写引擎 benchmark.ndjson |
| known 差异 | q12_bidder_10s_window_count | fixed+close 收口非确定（oracle 理想值，见 §6） |
| 工具级例外 | q6（join 可见性非确定）、q13b（provider 静态表） | 见 §6，由 verify_daemon.sh 跳过 `--detail-diff` |

## 5. 输出

- `--engine-emit <dir|file>`：L1 计数对拍（git-diff 同款分层），退出码 0=一致 / 1=有差异。
- `--detail-diff <benchmark.ndjson>`：L3 明细对拍，退出码同。可与 `--engine-emit` 同时用
  （一次 oracle 跑两档）。
- `WFGEN_VERIFY_DETAIL_DEBUG=1`：打印各规则有/无字段的 alert 数（诊断）。

## 6. 已知差异与边界（oracle 不判失败 / 不验证）

### known 差异（L1/L3 都跳过，单独报告 ⚠ 不判失败）

- **q12_bidder_10s_window_count**：fixed+close 收口非确定——引擎 fixed 收口预算
  （`MAX_EXPIRY_SCAN_BUDGET`）+ scan_timeouts 墙钟推进，快速 replay 可能多收/漏收尾部桶
  （1M 实测 oracle=10,240 vs 引擎=27,446，多 ~168%；10M 实测 oracle=102,400 vs 引擎=282,514）；
  oracle 事件时间到末尾即止，为理想值。**q12 是豁免放行而非验证一致，引擎待修项。**

### 工具级边界（verify_daemon.sh 跳过 L3）

- **q13（provider 静态表 join）**：q13b 的 `side_input` 是 knowdb 静态表，oracle 不加载
  knowdb → join 富化字段（`detail=side_input.value`）oracle 为空，无法对拍。由 L2 CHECKS
  （alert_type 恒定）+ L1 覆盖。
- **q6（join-then-key）**：join 可见性非确定——引擎 replay 的 join 目标窗口 append/evict
  时序影响逐 bid 是否计入 avg，oracle（预加载 + 事件时间过期）为理想值；计数一致但内容
  分布不同；且 q6 无权威基线（Flink 官方未实现）。由 L1 + L2 覆盖。

### 引擎待修项（L3 如实报 FAIL，属引擎缺陷非 oracle 误差）

- ~~**q3 / q5 / q7**~~：明细对拍曾暴露**引擎文件比 oracle 少行**（q3 6060 vs
  6051 等 flaky 波动）——2026-08-30 已修复：q7/q5 = close_all 尾桶收口语义
  （窗口终点按窗起点算 + 水位对齐到桶边界；hop 用 slide 粒度 + 真 ceil），
  q3 = join 索引与提交前沿竞态（frontier 回退不再领先索引内容 + eager gate
  冷启动不 bail）。当前 22 查询全 PASS：**21 个 L1+L2+L3 真一致**（含 stats 的
  q4b/q15-q19 值级对拍），**q12 为 known 豁免放行**（见上，+168% 差异，待修）。

## 7. 使用

```bash
# L1 + L3 一次跑（verify_daemon.sh 单查询内部即此调用）
wfgen verify-nexmark 1000000 --query q1 --engine-emit data/verify_daemon_emit_q1.txt \
  --detail-diff data/alerts/benchmark.ndjson

# 全量（verify_daemon.sh all 1m 内部逐查询执行）
./verify_daemon.sh all 1m
```

oracle 输出的 JSON 明细（纯 oracle 模式，无 --engine-emit）：
```json
{"q1_bid_passthrough": 920000, "_counts": {"persons": 20000, "auctions": 60000, "bids": 920000}}
```
