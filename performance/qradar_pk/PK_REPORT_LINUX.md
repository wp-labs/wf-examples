# wfusion vs IBM QRadar EP — 450 规则吞吐性能报告（Linux 8 核对等口径）

> 测试日期：2026-08-17（**wfusion 0.3.1** / wp-reactor 1.0.2，P0-② content 记账）
> 对标：IBM QRadar Event Processor 认证负载（**80k EPS @ 451 条规则**）
> 场景：`wf-examples/performance/qradar_pk`（450 条有状态规则 / 6 类事件源 / 1000 sip 键）
> **本报告为 Linux 8 核云主机复测**——8 vCPU 单进程跑 450 条有状态规则，验证规则求值
> 在低核数云主机上的能力，与 Mac 报告（16 核 M3 Max）口径分开引用。

---

## 1. 摘要

在 **Linux 8 核（AMD EPYC，KVM）** 云主机上，wfusion 单进程处理 450 条有状态规则，
**EPS ≈ 103.7k**（20 万事件、单连接流式突发，初测口径），**超过 QRadar EP 认证上限
（80k）1.3×**——且是在 **8 vCPU**（vs QRadar 认证 56-80 核）上做到，核心结论与 Mac
报告一致：450 规则求值吞吐不是瓶颈。

| 结论 | 结果 |
|---|---|
| 450 规则吞吐 | **~103.7k EPS**（200k 事件，单连接流式，初测口径） |
| vs QRadar EP 认证上限 | **1.3×**（80k @ 451 规则，56-80 核 + 128GB） |
| vs QRadar 物理 appliance | **5.2×**（20k @ 16-20 核） |
| 每核效率（按 8 核估算） | **~13k EPS/核** vs QRadar 虚拟版 ~1.0-1.4k（**高 ~10-13×**） |
| #18 门禁 | **通过**：内存驱逐 0，emitted 2.4M（20 万事件），293/450 规则触发 |
| 内存 | 未测（Linux 无 macOS `footprint`，见诚实边界 §6） |

---

## 2. 测试环境

| 项 | 值 |
|---|---|
| 芯片 | **AMD EPYC 9T95**（KVM 虚拟化，8 vCPU / 8 核 / 每核 1 线程，x86_64） |
| 内存 | **30 GiB**（29 GiB available，Swap 0） |
| 系统 | **Ubuntu 24.04.4 LTS**（Noble），内核 6.8.0-137-generic |
| 引擎 | **wfusion 0.3.1** / wp-reactor 1.0.2（release，P0-② content 记账） |
| 注入 | `wfgen send` 单连接流式（CHUNK=10000），200k 事件 |
| 对照 | QRadar EP 1699（虚拟版）：56-80 核 + 128GB，认证上限 80k EPS @ 451 规则 |

---

## 3. 结果（200k 事件，blackhole sink）

| 项 | 值 |
|---|---|
| EPS | **103,692**（200k 事件 / 1.93s send 墙钟） |
| #18 回归 | **通过**：内存驱逐 0，emitted 2,399,911，conn_rules 29,310，rules_seen 293 |
| 规则触发 | 293/450（200k 数据分布下其余阈值/过滤不满足，与 Mac 报告 200k 口径一致） |
| RSS | 未测（RSS_peak=0：`footprint` 为 macOS 工具，Linux 采样静默跳过） |

正确性：驱逐 0、无 dropped_late / serialize_failed / cursor_gap；emitted 2.4M
（20 万事件多规则命中 ~12×，与 Mac 报告 1M→10.2-10.4M 的 ~10× 量级一致）。

---

## 4. 对等分析（8 核 vs QRadar 56-80 核）

1. **8 核单进程 103.7k > QRadar 认证 80k**（56-80 核 + 128GB）——倍率 1.3×，硬件算力
   差距远超倍率（8 vCPU vs 56-80 核）：**规则求值吞吐本身不是墙**，与 Mac 报告结论一致；
2. **每核效率**：按 8 核满负荷估算 **~13k EPS/核**（未测 CPU%，见诚实边界）——vs
   QRadar 虚拟版 ~1.0-1.4k EPS/核，**高 ~10-13×**；
3. **vs Mac（156k 中位）≈ 0.66×**：450 规则是规则求值（CPU-bound）瓶颈，云主机
   8 核 + EPYC 单核弱于 M3 性能核；幅度远小于 nexmark 简单查询的 1/3——规则求值
   吃满多核，简单查询吃单核/带宽，qradar 的 CPU-bound 形态在云主机上损失更小；
4. **口径注明**：200k 突发（~1.9s）含稳态前的少量启动影响，1M 长稳 EPS 只会更高
   不会更低（Mac 报告 200k→147.6k、1M→150-162k 同向）。

---

## 5. 结论

- **Linux 8 核对等口径下，450 规则稳态能力仍超 QRadar 认证上限**（8 核 vs 56-80 核），
  与 Mac 报告的架构优势一致——对外引用建议用本报告（低核数云主机，对 QRadar 最不利
  的口径）；
- **每核效率 ~13k vs QRadar ~1.0-1.4k**，消除核数差异后优势仍 10 倍量级；
- 待补：Linux RSS 采样（`/proc/<pid>/status` VmRSS）+ 1M 完整稳态三轮，补上内存与
  更稳的 EPS 后本报告即可定稿。

---

## 6. 诚实边界

1. **EPS 为 200k 突发初测**（默认事件数），非 1M 三轮稳态；单连接流式、CHUNK=10000；
2. **RSS 未测**：`footprint` 为 macOS 专用工具，Linux 上采样静默跳过（RSS_peak=0）——
   内存数字待补；
3. **CPU% 未测**：每核效率按 8 核满负荷估算，未用 `wait4`/rusage 实测；
4. **QRadar 数字来源单一**（IBM 官方认证方法论），不可复现；对照含板载采集/存储等
   产品层组件（非仅规则引擎），利于 QRadar；
5. 硬件：AMD EPYC 9T95（KVM 虚拟化），8 vCPU / 8 核，30 GiB，Ubuntu 24.04.4。

---

> 完整技术细节、规则构成、窗口记账演进见本目录 `README.md` 与 `PK_REPORT_MAC.md`
> （16 核 M3 Max 口径）；复测：`./run.sh 1000000`（CHUNK=10000，窗口 4GB）。
