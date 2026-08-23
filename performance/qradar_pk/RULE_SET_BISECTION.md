# qradar_pk 规则集成本二分分析（2026-08-23）

> 背景：当前引擎（wp-reactor `feat/columnar-execution`）下 qradar 1M 稳态 EPS ~35k，
> 对比 2026-08-17 基线 150-162k（**-4.3×**，负载特定回归——同机 nexmark q1 30m 9.42M
> 正常）。本文用**规则集二分**（PERF_BISECTION_METHOD 方法论在规则维度的应用）定位
> 影响性能的规则子集，并用 stats<> 对照实验反证了一个直觉假设。

---

## 1. 方法

- **数据**：`gen_events.py` 300k 事件，`dump-frames` 预编码为帧一次，跨轮复用
  （dump-frames 需连接 daemon 做 schema 握手，见 `run.sh`）。
- **测量**：每轮把规则子集写入临时 `wfl` → 临时 config（去 `max_ingest_rate`）→
  起 daemon → `send-arrow` → 等 **append_total 追平 N 且 acked_lag 归零** →
  EPS = N / 全墙钟。重复 2 轮取最大 EPS（降机器负载噪声，本机 load 5-6）。
- **二分**：规则名按文件序（= 生成器类别序）切半，测两半 EPS；
  更贵（EPS 更低）且差异 >15% 才递归，否则判"成本均匀"停止。
- 脚本：`data/rule_bisect.sh`（运行产物，data/ 已 gitignore）。

## 2. 二分收敛路径

```
全部 450 (36.0k EPS)
└→ 前 225 (61.2k) ── 更贵
   └→ 前 112 (77.7k) ── 更贵
      └→ 深度3 两半相当（56 条：147.5k vs 136.7k）→ 停止
```

收敛保留的 112 条 = **conn 主族**：`c_sip/c_dip/c_dport/c_protocol/c_duration/c_bytes`
× 阈值网格（count）+ `s_*` sum + avg/min/max + dist + accu + `g_*` guard（geo/tag/app/str/math）。

## 3. 类别成本表

成本 = N/EPS(子集) − N/EPS(空规则)，即超出管道 floor 的规则时间（近似可加）。
管道 floor（0 规则）= **350.7k EPS**。

| 类别 | 规则数 | EPS | 成本(300k) | 占比 |
|---|---|---|---|---|
| **`c_*` conn count** | 125 | 97.4k | **2.23s** | **~30%** |
| **`g_*` conn guard** | 45 | 147.3k | **1.18s** | **~16%** |
| `s_*` sum | 18 | 264k | 0.28s | |
| `avg_*` | 13 | 274k | 0.24s | |
| pipe（`__wf_pipe_*`） | 3 | 277k | 0.23s | |
| `dist_*` | 17 | 284k | 0.20s | |
| max / fw / min / pr | 11/40/7/47 | ~294-296k | 0.16s 各 | |
| accu / multi / dns / auth / close / chain / fl | 7/12/28/31/7/5/34 | 310k+ | ≤0.11s | |

> 剩余 ~1.9s 未归因（74 条 count 补齐 + 交叉项 + 测量噪声），不影响结论。

## 4. 结论一：成本结构

1. **规则求值占 ~90% 墙钟**：floor 350k → 全规则 36k。
2. **成本集中在 conn count（~30%）+ guard（~16%）**；其余 15 类单类 ≤0.3s。
3. **`c_*` 成本 = 单规则成本线性叠加**：125 条 ≈ 2.23s / 300k / 125 ≈ **60ns/事件/规则**
   （per-key 实例查找 + 计数）。6 条 `c_sip_*` 实测 345-352k（≈floor），即单条 count
   规则近乎免费，量变（125 条）才是质变。

## 5. 结论二：stats<> 反证（A/B 实测）

**假设**（基于 nexmark Q15-18 CEP→stats 提速 2-4× 的经验）：纯 count 聚合可用
`stats<>` 列式路径加速。

**实测**（同 300k 帧）：

| 方案 | 规则数 | EPS | emitted |
|---|---|---|---|
| CEP `c_sip_*`（`match<sip:2m>` count≥N） | 6 | **345-352k** | 74,196 |
| stats `stats<2m:fixed> group by (sip) { count }` | 1 | **258-260k** | 6,258 |

**结果与假设相反：1 条 stats 比 6 条 CEP 更慢。** 原因：

- stats group-by 在**高键数 churn**（1000+ sip × 2m 桶）下每事件哈希表键查找/插入
  ≈ **1.3µs/事件**，≈ 20× CEP 单规则成本；
- stats 的优势区是 **低键数 × 大窗口 × 批量收口**（nexmark Q15-18：全局单实例 /
  4 channel / 100 auction × 1d 桶）——qradar 的高基数短窗恰在劣势区；
- 语义上 stats 也表达不了阈值规则：`stats` 的 `where` 只过滤**输入行**（度量级），
  无聚合后输出过滤（`yield ... where` / 规则级 `where stat.value(...)` 均解析失败）；
  `match<sip:2m>` 滑动窗口 vs stats `fixed/session` 固定桶。

## 6. 对回归排查的指向

- **不是 stats-vs-CEP 选型问题**——qradar 的 count 规则形态 CEP 已是最优表达。
- 问题在 **CEP 单规则求值路径**（实例查找/计数/guard 富类型访问）在当前引擎变慢
  （08-17 单事件 ~90µs → 当前 ~368µs，5× 量级，且 q1 等轻查询不受影响）。

## 7. 引擎侧定位（2026-08-23，PERF_BISECTION_METHOD 应用）

### 7.1 段内热点（`sample` 活跃期采样，c_* 家族 1M 事件）

活跃样本 27.6k，idle 线程已排除：

| 热点簇 | 活跃占比 | 来源 |
|---|---|---|
| **计时开销** | **~7.6%** | `process_batch` 每行 5 对 `Instant::now()+elapsed`（profile 计时器） |
| 状态机 advance | ~6% | `CepStateMachine::advance_*` + `scan_expired` + `evaluate_step` |
| 分配/拷贝 | ~5% | `smol_str::Repr::new`、`mi_malloc`、`_platform_memmove/memcmp`（key 路径 String 克隆） |
| 字段访问 | ~4.4% | `ColumnarEvent::field_value/value_at`、`extract_value`、`ColumnExpr::eval_vec` |
| 实例表哈希 | ~3% | `foldhash` + `hashbrown` + `take_instance`（3 次哈希：contains+take+put） |
| process_batch 自身 | ~4% | 循环/调度 |

### 7.2 改进与验证

**✅ 计时门控（已提交 wp-reactor `3beb41c`）**：规则相位计时器只为 1s 节流的
`dump_profiling` 日志服务，却每行做 5 对时钟调用。新增 `set_rule_profiling`
（`WF_RULE_PROFILING=0` 关闭，默认开启保持兼容）。采样验证（§7.3）：
计时簇 **7.6% → 0%**（clock_gettime 全部消失），进程热路径零时钟开销。
EPS 级 A/B 受本机负载噪声（±60%）阻挡无法分辨（max 158.5k vs 157.3k），
以采样验证为准。

**✖ take-early 尝试后回退**：把 `contains_key+take+put`（3 次哈希）提前 take
（2 次）。1050 测试全过后采样显示实例表哈希只占活跃 ~2-3%，收益 <1%；
改动触及正确性最关键的实例表访问（DropOldest 驱逐语义），性价比差，回退。

### 7.3 剩余热点（改进方向）

- **key 路径分配**：每事件每规则 2 次 String 克隆（`field_value` → `ScopeKey::from_value`）
  + Vec 分配 + smol_str 构造 ≈ 活跃 5%——单 key 规则可直读列构 `ScopeKey` 免中间 Value。
- **状态机 advance 内部**（~6%）：`evaluate_step`/`scan_expired` 每事件开销。
- EPS 级测量需安静机器或更稳的测量协议（当前 load 5-6 + Zed 占 2 核，±60% 噪声
  淹没 <10% 的改动）。

---

## 附录：测量注意

- 完成判定用 `append_total`（全文件求和）追平 + `acked_lag` 归零；旧 `emitted` 停滞
  判定在区间差值指标下失效（见 run.sh 修复记录）。
- 本机负载 5-6（Zed 占 ~2 核），EPS 绝对值为量级参考；A/B 相对差可信
  （每方案 2 轮一致：CEP 345/352k，stats 259/260k）。
- 子集窗口可能缺失部分流 → 完成判定兜底：append 停滞 6 轮 + lag 归零。
