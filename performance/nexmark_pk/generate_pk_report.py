#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
生成 NEXMark PK 报告（warp-fusion vs Flink OSS vs Flink VVR）的 MD 与 HTML 双格式。

数据来源（项目内权威文档，已核实）：
- docs/OSS_VVR_BASELINE.md  —— 阿里白皮书 OSS Flink / VVR 基线（q0~q22，100M 条，固定值）
- docs/BENCH_RESULTS.md     —— wfusion 实测归档
- 主 PK 表采用「2026-08-27 Linux 100M · 21/21 clean · v2.0.7」最新权威跑批
  （与白皮书同规模 100M，最对等；q6 已移出 all 套件，仅单跑保留，无白皮书基线）
- KPI / 摘要中的关键数字（最弱项、最高 EPS/RSS、倍数区间）由 BASE 数据自动计算，避免硬编码漂移。
"""
import os
import math

OUT_DIR = os.path.dirname(os.path.abspath(__file__))

# ----------------------------------------------------------------------------
# 1. PK 主表：2026-08-27 Linux 100M（21 查询，v2.0.7，21/21 clean）
#    (q, 语义, wf_eps, oss_rps, vvr_rps, vs_oss, vs_vvr, rss_mb)
#    vs_oss / vs_vvr 取自 BENCH_RESULTS.md §「2026-08-27 Linux 100M」PK 对照表
#    oss_rps / vvr_rps 取自 OSS_VVR_BASELINE.md 白皮书固定基线（与轮次无关）
#    q13 白皮书未发布基线 -> None；q6 已移出 all 套件，不计入主表
# ----------------------------------------------------------------------------
BASE = [
    ("q1",  "无状态投影（货币换算 0.908×price + 过滤）", 15621205, 1753002, 4381353, 8.91, 3.57, 4190),
    ("q2",  "选择（MOD(auction,123)=0 的 bid）",          28345809, 1927154, 6568576, 14.71, 4.32, 4300),
    ("q3",  "按州过滤（IN OR/ID/CA）",                     21320217, 1176664, 4638649, 18.12, 4.60, 5239),
    ("q4",  "分类均价（累积窗口 deferred reduce + stats）", 3754758, 180693, 636468, 20.78, 5.90, 13696),
    ("q5",  "热门新商品（HOP 10s/2s + top_ties(1)）",      2305542, 273496, 279684, 8.43, 8.24, 13577),
    ("q7",  "时段最高出价（match<auction:10s> + top_ties）", 9522424, 79526, 299547, 119.74, 31.79, 4203),
    ("q8",  "新用户 + 其拍卖（TUMBLE deferred join）",      29553370, 1253321, 3340125, 23.58, 8.85, 5221),
    ("q9",  "中标出价（asof deferred reduce join）",        7774233, 43020, 375146, 180.71, 20.72, 7354),
    ("q10", "全量 bid 按时间分区落盘（每 bid 一行）",       17379758, 526357, 1953049, 33.02, 8.90, 4219),
    ("q11", "用户会话统计（session 窗口 + 分片）",          10420884, 244868, 685011, 42.56, 15.21, 4080),
    ("q12", "每 bidder × 10s 处理时间窗计数",              10315778, 822680, 2703360, 12.54, 3.82, 4109),
    ("q13", "有界侧输入 join（snapshot join，重写后 O(1)）", 8165854, None, None, None, None, 5091),
    ("q14", "时间戳换算 + 价格过滤（Calculation）",         8867337, 1451316, 4997002, 6.11, 1.77, 4213),
    ("q15", "日历天出价统计（stats + 1d 桶）",              6568803, 544339, 2340057, 12.07, 2.81, 5646),
    ("q16", "日历天渠道统计（stats + 1d 桶）",              4243485, 108980, 296478, 38.94, 14.31, 7607),
    ("q17", "日历天拍卖统计（stats + 1d 桶）",              4127814, 972318, 3693308, 4.25, 1.12, 19361),
    ("q18", "每 (bidder,auction) 最后一条 bid（stats last）", 2902069, 173928, 1038044, 16.69, 2.80, 30055),
    ("q19", "拍卖 Top-10 价格（stats<> top-N）",           7293694, 170565, 1051293, 42.76, 6.94, 6421),
    ("q20", "展开 bid 关联 auction（snapshot join + where）", 17098156, 74591, 431999, 229.23, 39.58, 8187),
    ("q21", "附加 channel id（热通道映射 + cold url）",     13884112, 786850, 2519336, 17.65, 5.51, 4293),
    ("q22", "URL 目录投影",                                 10242382, 1054519, 3202254, 9.71, 3.20, 4500),
]

# 自动计算的关键数字（避免硬编码漂移）
_base_with = [b for b in BASE if b[5] is not None]
CALC = {
    "oss_min": min(b[5] for b in _base_with), "oss_max": max(b[5] for b in _base_with),
    "vvr_min": min(b[6] for b in _base_with), "vvr_max": max(b[6] for b in _base_with),
    "vvr_weak_q": min(_base_with, key=lambda b: b[6])[0],
    "vvr_weak_v": min(b[6] for b in _base_with),
    "eps_hi_q": max(BASE, key=lambda b: b[2])[0],
    "eps_hi_v": max(b[2] for b in BASE),
    "rss_hi_q": max(BASE, key=lambda b: b[7])[0],
    "rss_hi_v": max(b[7] for b in BASE),
    "n_base": len(_base_with),
    "n_total": len(BASE),
}

# ----------------------------------------------------------------------------
# 2. 同机规模缩放（2026-08-27 Linux 同机 30M vs 100M，21 查询）
#    (q, eps_30m) —— 用于 §5 规模缩放观测（比值 = 100M / 30M）
# ----------------------------------------------------------------------------
SCALE30 = {
    "q1": 12633621, "q2": 29503249, "q3": 22750096, "q4": 3990030, "q5": 3433159,
    "q7": 9028437, "q8": 29333603, "q9": 9328400, "q10": 17401438, "q11": 10691342,
    "q12": 10833174, "q13": 8121986, "q14": 8847693, "q15": 6647697, "q16": 4454014,
    "q17": 6274551, "q18": 9086465, "q19": 8225134, "q20": 17770339, "q21": 14008303,
    "q22": 10299910,
}

# ----------------------------------------------------------------------------
# 3. 跨机一致性验证（2026-08-25 Mac 10 核常载，100M 无状态查询子集，7 查询）
#    (q, wf_eps, oss_rps, vvr_rps, vs_oss, vs_vvr)
# ----------------------------------------------------------------------------
MAC100 = [
    ("q1",  9203143, 1753002, 4381353, 5.25, 2.10),
    ("q2",  11241238, 1927154, 6568576, 5.83, 1.71),
    ("q3",  9082100, 1176664, 4638649, 7.72, 1.96),
    ("q10", 9722892, 526357, 1953049, 18.47, 4.98),
    ("q14", 5994744, 1451316, 4997002, 4.13, 1.20),
    ("q21", 9150195, 786850, 2519336, 11.63, 3.63),
    ("q22", 5153313, 1054519, 3202254, 4.89, 1.61),
]

# 历史跑批汇总（附录 B）已按需求移除：不呈现内部跑批演进史。

def fnum(n):
    return f"{n:,}" if n is not None else "—"

def fxf(x):
    return f"{x:.2f}×" if x is not None else "N/A"

def fmt_eps(n):
    return f"{n/1_000_000:.2f}M" if n is not None else "—"

def fmt_gb(mb):
    return f"{mb/1024:.1f}GB"

# ============================================================================
#  MD 渲染
# ============================================================================
def render_md():
    c = CALC
    L = []
    L.append("# warp-fusion NEXMark 性能 PK 报告：对比 Flink OSS 与 Flink VVR\n")
    L.append("> **范围**：warp-fusion（wp-reactor 引擎）在 NEXMark 上的吞吐表现，"
             "对照阿里云《Nexmark 性能白皮书》发布的 **OSS Flink** 与 **实时计算 Flink（VVR）** 基线。\n")
    L.append("> **数据截止**：PK 主表采用 **2026-08-27 Linux 100M 权威跑批**（21/21 clean，v2.0.7，哨兵 EPS 口径），"
             "与白皮书**同规模（100M 条）**——最对等的对照。q6 已移出 `all` 套件（架构性慢，单跑保留，无白皮书基线）。\n")
    L.append("---\n")

    # 摘要
    L.append("## 0. 摘要\n")
    L.append(f"- **相对 OSS Flink**：{c['n_base']}/{c['n_base']} 有基线查询 **{c['oss_min']:.2f}×–{c['oss_max']:.2f}× 全面领先**。\n")
    L.append(f"- **相对 VVR**：{c['n_base']}/{c['n_base']} 有基线查询 **全部达 VVR**，倍数 **{c['vvr_min']:.2f}×–{c['vvr_max']:.2f}×**。\n")
    L.append(f"- **⚠ 口径声明**：wfusion 与 VVR 使用**相同型号云服务器**，残余不对等仅剩计算资源计量口径（8 核 vs 8CU，OSS 3×12vCPU）；数据规模已对齐 100M，结论作**量级参照**。详见 §2。\n")

    # 背景
    L.append("\n## 1. 背景与基准对象\n")
    L.append("**NEXMark** 是流处理领域社区公认的基准（源自 Apache Calcite / Nexmark，"
             "被 Flink 官方 `nexmark` 测试集采纳），由三流事件构成：\n")
    L.append("| 流 | 占比 | 说明（100M 总量） |\n|---|---|---|\n")
    L.append("| person_events | 2% | 2M person |\n")
    L.append("| auction_events | 6% | 6M auction |\n")
    L.append("| bid_events | 92% | 92M bid |\n")
    L.append("\nwfusion 生成器 `wfgen gen-nexmark` 按官方默认配置确定性生成（价格对数均匀、hot auction/seller/bidder、"
             "事件时间严格递增），**同一 count + seed 字节级确定**，与 Flink 官方定义逐项对齐"
             "（见 `docs/NEXMARK_CONFORMANCE.md`）。\n")
    L.append("\n查询集 **Q1–Q22 共 22 条**，覆盖无状态投影/过滤、join（TUMBLE/asof/snapshot）、"
             "滑窗/跳窗/top-N、会话窗口、stats 聚合等流处理典型形态。阿里白皮书发布其中 **q0–q22（23 条）** 的"
             "OSS/VVR 吞吐——本报告对比 q1–q22（q0 为计数投影，白皮书有基线但非 NEXMark 标准查询集，本套件未纳入）。"
             "**当前自动化套件 21 条**（Q6 因架构性慢移出 `all`、单跑保留；Q6 亦无白皮书基线）。\n")

    # 口径
    L.append("\n## 2. 三方配置与度量口径（公平性核心）\n")
    L.append("| 维度 | warp-fusion | OSS Flink | VVR（实时计算 Flink） |\n|---|---|---|---|\n")
    L.append("| 引擎版本 | warp-fusion（wp-reactor）v2.0.7 | 1.20.4 | vvr-11.5-jdk11-flink-1.20 |\n")
    L.append("| 数据规模 | **100,000,000**（PK 主表） | 100,000,000 | 100,000,000 |\n")
    L.append("| 计算资源 | Linux 8 核（实际可用 ≥10，与 VVR 同型号云服务器） | 3×ecs.g6a.xlarge = 12 vCPU / 48GiB | 8 CU ≈ 8 vCPU / 32 GiB（托管分布式集群，总资源=8C/32G，与 wfusion 同型号云服务器） |\n")
    L.append("| sink | 本地文件（blackhole 等价） | Blackhole | Blackhole |\n")
    L.append("| 指标 | EPS = Σn / (max emit − min start)（哨兵） | RPS = 输入量 / 用时 | RPS = 输入量 / 用时 |\n")
    L.append("| 来源 | 本仓库 `bench.sh` | 阿里白皮书 | 阿里白皮书 |\n")
    L.append("\n**度量口径说明**：\n")
    L.append("1. wfusion 的 **哨兵 EPS** 与白皮书的 **RPS** 思路同源——都是「消化（或处理）掉的记录数 ÷ 耗时」。"
             "哨兵机制从数据窗排空后取首尾时间戳，消除轮询粒度误差，短跑读数同样可信。\n")
    L.append("2. **规模已对齐（100M × 100M）**：本版 PK 主表与白皮书同为 1 亿条；"
             "**wfusion 与 VVR 使用相同型号云服务器**，残余不对等仅剩计算资源计量口径（8 核 vs 8CU，OSS 为 3×12vCPU），结论仍作量级参照。\n")
    L.append("3. 单轮数字存在 ±8% 相位噪声（bench 机 EPS 与 RSS 双峰相位强相关），结论已按 RSS 相位配对。\n")
    L.append("4. 引用 wfusion RSS 须标注 `parse_buffer_bytes`（默认 128MB；吞吐优先 2GB）。\n")
    L.append("5. **WFL 原语分工**：wfusion 规则以 **`match`**（序列/状态机检测）与 **`stats`**（列式统计聚合）两类一等原语表达；"
             "PK 边缘项 q14/q17（vs VVR 最弱）与 q18（RSS 内存问题）均落在 **`stats` 路径**，非 `match`。\n")

    # 主表
    L.append("\n## 3. 性能 PK 主表（100M Linux · 2026-08-27 权威跑批 · v2.0.7）\n")
    L.append("> EPS/RPS 单位：条/秒。倍数 = wfusion EPS ÷ 对应基线 RPS。q13 白皮书未发布基线（重写后 O(1) snapshot join，已实测 8.17M），标 N/A。\n")
    L.append("| Query | 语义 | wfusion EPS | OSS RPS | VVR RPS | vs OSS | vs VVR | RSS(MB) |\n|---|---|---:|---:|---:|---:|---:|---:|\n")
    for q, sem, wf, oss, vvr, vo, vv, rss in BASE:
        L.append(f"| {q} | {sem} | {fmt_eps(wf)} | {fmt_eps(oss)} | {fmt_eps(vvr)} | {fxf(vo)} | {fxf(vv)} | {rss:,} |\n")
    L.append(f"\n**结论**：vs OSS **{c['oss_min']:.2f}×~{c['oss_max']:.2f}× 全面领先**（{c['n_base']}/{c['n_base']}）；"
             f"vs VVR **{c['vvr_min']:.2f}×~{c['vvr_max']:.2f}×，{c['n_base']}/{c['n_base']} 全部达 VVR**。\n")
    L.append(f"边缘项：**{c['vvr_weak_q']} vs VVR {c['vvr_min']:.2f}×**（stats 1d 桶，状态窗随 100M 规模增长、RSS 19.4GB，为 vs VVR 最弱项）；"
             "q14 1.77×、q22 3.20×。**q13 白皮书无基线**。⚠ **q18 RSS 30GB** 为已知内存问题（100M 状态窗线性增长），建议跟进。\n")

    # 规模缩放
    L.append("\n## 4. 规模缩放观测（同机 30M vs 100M）\n")
    L.append("> 同一台 Linux 8 核机、同一 v2.0.7，30M 与 100M 背靠背跑批。比值 = 100M EPS ÷ 30M EPS；"
             "<0.8 标记为规模退化（状态窗/状态随数据量增长）。\n")
    L.append("| Query | 30M EPS | 100M EPS | 100M/30M | 类型 |\n|---|---:|---:|---:|---|\n")
    for q, sem, wf, oss, vvr, vo, vv, rss in BASE:
        e30 = SCALE30.get(q)
        if e30:
            ratio = wf / e30
            rtxt = f"{ratio:.2f}×"
            typ = "状态型退化" if ratio < 0.8 else "近线性/无退化"
            L.append(f"| {q} | {fmt_eps(e30)} | {fmt_eps(wf)} | {rtxt} | {typ} |\n")
        else:
            L.append(f"| {q} | — | {fmt_eps(wf)} | — | — |\n")
    L.append("\n**结论**：无状态/轻量查询（q1/q2/q3/q7/q8/q10/q11/q12/q14/q15/q16/q19/q20/q21/q22）规模因子 0.94~1.24×，"
             "基本不随规模退化（部分 100M 略快属机器相位）；**状态型退化集中在 q5 0.67×、q17 0.66×、q18 0.32×**"
             "——均由窗口状态随数据量增长驱动（RSS 翻倍）。q18 0.32× 伴随 30GB RSS，为已知内存问题。\n")

    # 跨机验证
    L.append("\n## 5. 跨机一致性验证（Mac 10 核常载 · 100M 无状态子集）\n")
    L.append("> 白皮书基线本就是 **100M 条**——这是与白皮书**同规模**的跨机验证。仅无状态查询"
             "（状态重查询 50GB+ 未在本机跑）。Mac 为 10 核常载开发机（load 4.4~7.9），读数偏保守。\n")
    L.append("| Query | wfusion EPS | OSS RPS | VVR RPS | vs OSS | vs VVR |\n|---|---:|---:|---:|---:|---:|\n")
    for q, wf, oss, vvr, vo, vv in MAC100:
        L.append(f"| {q} | {fmt_eps(wf)} | {fmt_eps(oss)} | {fmt_eps(vvr)} | {fxf(vo)} | {fxf(vv)} |\n")
    L.append("\n**结论**：vs OSS **4.13×~18.47× 全面领先**（7/7）；vs VVR **1.20×~4.98× 全部达 VVR**（7/7）。"
             "同规模下 q14 vs VVR 仍最弱（1.20×）。跨机结论与 Linux 主表方向一致。\n")

    # 分析
    L.append("\n## 6. 结果分析\n")
    L.append("### 6.1 按查询形态分类\n")
    L.append("- **无状态投影/过滤（q1/q2/q3/q8/q10/q14/q21/q22）**：EPS 5~30M，受单连接读链限制，吞吐最高。\n")
    L.append("- **窗口/聚合/join（q4/q5/q7/q9/q11/q12/q15–q18/q19/q20）**：中高 4~15M，受窗口状态与 join 维护成本主导。\n")
    L.append("- **状态重查询（q13/q18）**：q13 重写后 8.17M（snapshot join O(1)，取消 2d 窗口全保留）；q18 仅 2.90M 且 RSS 30GB（已知内存问题）。\n")
    L.append("### 6.2 边缘项：vs VVR 最弱是 q17（1.12×），非 q14\n")
    L.append("新版 100M 数据下，**相对 VVR 最弱项是 q17（1.12×）**——stats 1d 桶，状态窗随 100M 规模增长、RSS 19.4GB。"
             "q14（Calculation，`0.908×price` 过滤 + `HOUR` 分型 + `count_char` UDF，无状态投影）在 100M 下为 **1.77×**，"
             "每命中事件需构建 detail 字符串，属 Flink 语义固有成本；diag 墙表显示主墙 = **输出链**（+54.6ns）与规则段（+42.1ns），"
             "差距本质来自**引擎行式 cell 求值**，非查询写法问题；改进需进 wp-reactor 做列式字符串/过滤求值（通用能力，建议独立立项）。\n")
    L.append("### 6.3 已知问题与口径说明\n")
    L.append("- **q18 内存（30GB）🔴**：100M 状态窗线性增长，监视器曾测 ~60GB；建议内存归因跟进。\n")
    L.append("- **q5/q17 规模退化（0.67×/0.66×）**：窗口状态随数据量增长，RSS 翻倍，属预期内状态型退化，非 bug。\n")
    L.append("- **q13 无白皮书基线**：snapshot join（重写后 O(1)）实测 8.17M，Flink 官方未发布对应档，不参与 vs 基线倍数计算。\n")

    # 正确性
    L.append("\n## 7. 正确性与查询覆盖\n")
    L.append(f"- **覆盖**：查询集 22 条，自动化套件 **{c['n_total']}/21 已完成 100M clean 跑批**（Q6 移出 `all`、单跑保留）。")
    L.append("能力判定：**20 已有 · 1 待补强（Q12 处理时间窗）· 1 特殊口径（Q6 移出套件）**。\n")
    L.append("- **oracle 对拍（30M）**：Q4/Q9 **identical**（D4 保留 pin：join 目标窗字节上限驱逐不再静默丢行）；"
             "剩余已知差异：Q12 fixed+close 尾桶 known-diff、Q19 stats oracle 未接入（7 个 stats 规则组被 oracle 跳过）。\n")
    L.append("- **验证工具**：`wfgen verify-nexmark` 用真实 WFL 规则引擎对同一份确定性数据逐规则算出期望值，"
             "与引擎 EMIT 计数 git-diff 同款分层对拍（L1 哈希→L2 Myers→L3 明细）。\n")

    # 资源
    L.append("\n## 8. 资源消耗（RSS / CPU）\n")
    L.append(f"- **RSS 峰值（100M）**：无状态查询 4.0~5.6GB；状态重查询 **q18 {fmt_gb(30055)}**、q17 19.4GB、q4 13.7GB、q9 7.4GB。"
             "q18 30GB 为已知内存问题（状态窗随数据量线性增长）。其余随消费速度饱和、30M 后基本有界。\n")
    L.append("- **CPU（活跃窗核占）**：无状态查询受单连接读链限制 ~100% avg（满核但单连接供给瓶颈）；"
             "重查询 avg 400%+（多核充分占用）。CPU 0% 旧口径假象已修复（亚秒突发在采样器首个差分前烧完），新口径下 0% 才可信。\n")

    # 结论
    L.append("\n## 9. 结论\n")
    L.append(f"warp-fusion 在 NEXMark 全查询上相对 OSS Flink **量级领先 {c['oss_min']:.2f}×–{c['oss_max']:.2f}×**，"
             f"相对 VVR（实时计算 Flink）**全面达到且多数显著超出（{c['vvr_min']:.2f}×–{c['vvr_max']:.2f}×）**，"
             "正确性已达生产可用基线，21/21 套件查询端到端可运行（100M clean）。"
             "短板集中在：① q14/q17 类 Calculation / stats 的行式 cell 求值（通用引擎级优化）；"
             "② q18 状态窗内存（100M 30GB，已知问题）；③ Q12 处理时间窗（事件时间引擎固有，replay 下等价）。\n")

    # 附录
    L.append("\n## 附录 A：口径与引用\n")
    L.append("- **EPS 哨兵口径**：`EPS = Σn / (max emit_ns − min start_ns)`，数据窗排空后取首尾时间戳，无轮询粒度误差。\n")
    L.append("- **RSS 口径**：须标注 `parse_buffer_bytes`（默认 128MB，吞吐优先 2GB）。\n")
    L.append("- **A/B 纪律**：不限速（RATE=10000000）、同时段交错、按 RSS 相位配对、单轮 ±8% 噪声。\n")
    L.append("- **来源**：阿里云《性能白皮书（Nexmark 性能测试）》"
             "https://help.aliyun.com/zh/flink/realtime-flink/support/nexmark-performance-testing ；"
             "本仓库 `docs/OSS_VVR_BASELINE.md`、`docs/BENCH_RESULTS.md`、`docs/CAPABILITY_GAP_MATRIX.md`。\n")

    return "".join(L)

# ============================================================================
#  HTML 渲染（自包含，浅色主题，CSS 柱状图，无外部依赖）
# ============================================================================
def bar(val, vmax, color, q, display, log=False):
    if log and val and vmax:
        pct = max(2.0, (math.log10(val) - math.log10(1.0)) / (math.log10(vmax) - math.log10(1.0)) * 100.0)
    else:
        pct = max(2.0, val / vmax * 100.0) if val and vmax else 0
    return (f'<div class="brow"><span class="blab">{q}</span>'
            f'<div class="btrack"><div class="bfill" style="width:{pct:.1f}%;background:{color}"></div></div>'
            f'<span class="bval">{display}</span></div>')

def render_html():
    c = CALC
    # 计算 vs OSS / vs VVR 最大值用于条形比例
    voss_vals = [b[5] for b in BASE if b[5] is not None]
    vvvr_vals = [b[6] for b in BASE if b[6] is not None]
    voss_max = max(voss_vals)
    vvvr_max = max(vvvr_vals)
    eps_max = max(b[2] for b in BASE)

    def heat(x, xmax):
        t = min(1.0, x / xmax) if xmax else 0
        if t < 0.33:
            return "#2f9e8f"
        elif t < 0.66:
            return "#e0922f"
        return "#c0392b"

    chart_vvr = "".join(
        bar(vv, vvvr_max, heat(vv, vvvr_max), q, f"{vv:.2f}×", log=True)
        for q, sem, wf, oss, vvr, vo, vv, rss in BASE if vv is not None)
    chart_voss = "".join(
        bar(vo, voss_max, heat(vo, voss_max), q, f"{vo:.2f}×", log=True)
        for q, sem, wf, oss, vvr, vo, vv, rss in BASE if vo is not None)
    chart_eps = "".join(
        bar(wf, eps_max, "#1769c2", q, fmt_eps(wf))
        for q, sem, wf, oss, vvr, vo, vv, rss in BASE)

    # 主表行
    rows = []
    for q, sem, wf, oss, vvr, vo, vv, rss in BASE:
        cls = ' class="na"' if oss is None else ""
        rows.append(
            f"<tr{cls}><td class='q'>{q}</td><td class='sem'>{sem}</td>"
            f"<td class='num'>{fmt_eps(wf)}</td><td class='num'>{fmt_eps(oss)}</td>"
            f"<td class='num'>{fmt_eps(vvr)}</td>"
            f"<td class='num strong'>{fxf(vo)}</td><td class='num strong'>{fxf(vv)}</td>"
            f"<td class='num'>{rss:,}</td></tr>")
    main_rows = "\n".join(rows)

    # 规模缩放行
    srows = []
    for q, sem, wf, oss, vvr, vo, vv, rss in BASE:
        e30 = SCALE30.get(q)
        if e30:
            ratio = wf / e30
            rtxt = f"{ratio:.2f}×"
            typ = "状态型退化" if ratio < 0.8 else "近线性/无退化"
            cls = ' class="degrade"' if ratio < 0.8 else ""
        else:
            rtxt, typ, cls = "—", "—", ""
        srows.append(f"<tr{cls}><td class='q'>{q}</td><td class='num'>{fmt_eps(e30)}</td>"
                     f"<td class='num'>{fmt_eps(wf)}</td><td class='num'>{rtxt}</td>"
                     f"<td class='sem'>{typ}</td></tr>")
    scale_rows = "\n".join(srows)

    # 跨机验证行
    mrows = [f"<tr><td class='q'>{q}</td><td class='num'>{fmt_eps(wf)}</td>"
             f"<td class='num'>{fmt_eps(oss)}</td><td class='num'>{fmt_eps(vvr)}</td>"
             f"<td class='num strong'>{fxf(vo)}</td><td class='num strong'>{fxf(vv)}</td></tr>"
             for q, wf, oss, vvr, vo, vv in MAC100]
    mac_rows = "\n".join(mrows)

    html = f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>warp-fusion NEXMark 性能 PK 报告 · Flink OSS / VVR</title>
<style>
  :root {{
    color-scheme: light;
    --bg:#f4f7fb; --surface:#fff; --surface2:#f8fbff; --text:#172235; --muted:#65758a;
    --border:#dbe4ee; --border2:#9fb3c8; --accent:#1769c2; --accentsoft:#eaf4ff;
    --oss:#9b59b6; --vvr:#e0922f; --wf:#1769c2; --good:#087e6d; --warn:#a26018; --danger:#b53c36;
    --shadow:0 8px 24px rgba(36,35,30,.06); --radius:10px;
  }}
  * {{ box-sizing:border-box; }}
  body {{ margin:0; background:var(--bg); color:var(--text);
    font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif;
    line-height:1.65; font-size:15px; }}
  a {{ color:var(--accent); text-decoration:none; }}
  a:hover {{ text-decoration:underline; }}
  code {{ background:var(--surface2); padding:1px 6px; border-radius:4px; font-size:.88em;
    font-family:SFMono-Regular,Consolas,"Liberation Mono",monospace; }}
  .wrap {{ width:min(1180px, calc(100% - 36px)); margin:0 auto; padding:28px 0 64px; }}
  header.hero {{ background:linear-gradient(135deg,#1c3a5e,#1769c2); color:#fff; border-radius:var(--radius);
    padding:30px 32px; box-shadow:var(--shadow); }}
  header.hero h1 {{ margin:0 0 8px; font-size:25px; letter-spacing:-.01em; }}
  header.hero p.lead {{ margin:0; color:#dbe9f7; max-width:780px; }}
  .badges {{ margin-top:14px; display:flex; gap:8px; flex-wrap:wrap; }}
  .badge {{ background:rgba(255,255,255,.16); color:#fff; border:1px solid rgba(255,255,255,.28);
    padding:3px 10px; border-radius:999px; font-size:12.5px; }}
  .cards {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:14px; margin:22px 0; }}
  .card {{ background:var(--surface); border:1px solid var(--border); border-radius:var(--radius);
    padding:16px 18px; box-shadow:var(--shadow); }}
  .card .k {{ font-size:12px; color:var(--muted); }}
  .card .v {{ font-size:22px; font-weight:760; margin-top:4px; color:var(--accent); }}
  .card .v small {{ font-size:13px; color:var(--muted); font-weight:500; }}
  .card--good .v {{ color:var(--good); }}
  .card--warn .v {{ color:var(--warn); }}
  .card--danger .v {{ color:var(--danger); }}
  .card--accent .v {{ color:var(--accent); }}
  section {{ background:var(--surface); border:1px solid var(--border); border-radius:var(--radius);
    padding:22px 26px; margin:18px 0; box-shadow:var(--shadow); }}
  section h2 {{ font-size:19px; margin:0 0 6px; }}
  section h3 {{ font-size:15.5px; margin:18px 0 6px; color:#23344c; }}
  section .sub {{ color:var(--muted); font-size:13.5px; margin:0 0 14px; }}
  table {{ border-collapse:collapse; width:100%; font-size:13.5px; margin:10px 0; }}
  th,td {{ border:1px solid var(--border); padding:7px 10px; text-align:left; vertical-align:top; }}
  th {{ background:var(--surface2); color:#2a3b52; font-weight:680; }}
  td.num,th.num {{ text-align:right; font-variant-numeric:tabular-nums; white-space:nowrap; }}
  td.q {{ font-weight:700; color:var(--accent); white-space:nowrap; }}
  td.sem {{ color:#42566e; }}
  td.strong {{ font-weight:720; color:var(--good); }}
  tr.na td {{ color:#9aa7b6; font-style:italic; background:#fafbfc; }}
  tr.degrade td {{ background:#fff7ec; }}
  table tbody tr:nth-child(even):not(.na):not(.degrade) td {{ background:var(--surface2); }}
  .tbl-scroll {{ overflow-x:auto; -webkit-overflow-scrolling:touch; }}
  .chart {{ margin:6px 0 4px; }}
  .brow {{ display:flex; align-items:center; gap:10px; margin:3px 0; }}
  .blab {{ width:34px; font-weight:700; color:#2a3b52; text-align:right; font-size:13px; }}
  .btrack {{ position:relative; flex:1; background:var(--surface2); border-radius:6px; height:22px; overflow:hidden;
    border:1px solid var(--border); }}
  .bfill {{ position:absolute; left:0; top:0; bottom:0; border-radius:6px 0 0 6px; }}
  .bval {{ margin-left:8px; min-width:54px; font-size:12.5px; font-weight:700; color:#1c2533; white-space:nowrap; }}
  .legend {{ display:flex; gap:16px; font-size:12.5px; color:var(--muted); margin:4px 0 10px; flex-wrap:wrap; }}
  .legend i {{ display:inline-block; width:11px; height:11px; border-radius:3px; margin-right:5px; vertical-align:-1px; }}
  .note {{ border-left:3px solid var(--warn); background:#fff7ec; padding:10px 14px; border-radius:0 8px 8px 0;
    font-size:13.5px; color:#5b431a; margin:12px 0; }}
  .ok {{ border-left-color:var(--good); background:#eef9f5; color:#1d4d43; }}
  .grid2 {{ display:grid; grid-template-columns:1fr 1fr; gap:22px; }}
  @media(max-width:760px) {{ .grid2 {{ grid-template-columns:1fr; }} }}
  footer {{ color:var(--muted); font-size:12.5px; text-align:center; margin-top:26px; }}
  .toc {{ columns:2; font-size:13.5px; }}
  .toc a {{ display:block; padding:2px 0; }}
</style>
</head>
<body>
<div class="wrap">

<header class="hero">
  <h1>warp-fusion NEXMark 性能 PK 报告</h1>
  <p class="lead">warp-fusion（wp-reactor 引擎）在 NEXMark 上的吞吐表现，对照阿里云《Nexmark 性能白皮书》
  发布的 <b>OSS Flink</b> 与 <b>实时计算 Flink（VVR）</b> 基线。同一份权威基准数据 + 同一组查询 + 同输出口径（blackhole 丢弃）
  + <b>同规模 100M 条</b> 对照。</p>
  <div class="badges">
    <span class="badge">PK 主表：2026-08-27 Linux 100M（21/21 clean）</span>
    <span class="badge">基线：阿里白皮书 OSS/VVR（100M）</span>
    <span class="badge">引擎 v2.0.7 + H1/P</span>
    <span class="badge">数据截止 2026-08-27</span>
  </div>
</header>

<div class="cards">
  <div class="card card--good"><div class="k">vs OSS Flink（{c['n_base']}/{c['n_base']}）</div><div class="v">{c['oss_min']:.2f}×–{c['oss_max']:.2f}×</div></div>
  <div class="card card--good"><div class="k">vs VVR（{c['n_base']}/{c['n_base']} 达 VVR）</div><div class="v">{c['vvr_min']:.2f}×–{c['vvr_max']:.2f}×</div></div>
  <div class="card card--accent"><div class="k">查询覆盖</div><div class="v">21/21 <small>clean</small></div></div>
</div>

<section>
  <h2>0. 摘要</h2>
  <p class="sub">warp-fusion 在 NEXMark 全查询上相对 OSS Flink <b>量级领先 {c['oss_min']:.2f}×–{c['oss_max']:.2f}×</b>，相对 VVR <b>全面达 VVR（{c['vvr_min']:.2f}×–{c['vvr_max']:.2f}×）</b>，与白皮书同规模（100M）对照。</p>
  <ul>
    <li><b>vs OSS Flink</b>：{c['n_base']}/{c['n_base']} 有基线查询 <b>{c['oss_min']:.2f}×–{c['oss_max']:.2f}× 全面领先</b>。</li>
    <li><b>vs VVR</b>：{c['n_base']}/{c['n_base']} 有基线查询 <b>全部达 VVR</b>，倍数 {c['vvr_min']:.2f}×–{c['vvr_max']:.2f}×。</li>
  </ul>
  <div class="note">⚠ <b>口径声明</b>：wfusion 与 VVR 使用<b>相同型号云服务器</b>，残余不对等仅剩计算资源计量口径（8 核 vs 8CU，OSS 3×12vCPU）；数据规模已对齐（100M × 100M），结论作<b>量级参照</b>，非逐位比较。详见 §2。</div>
</section>

<section>
  <h2>1. 背景与基准对象</h2>
  <p><b>NEXMark</b> 是流处理领域社区公认的基准（源自 Apache Calcite / Nexmark，被 Flink 官方采纳），由三流事件构成：</p>
  <div class="tbl-scroll"><table>
    <tr><th>流</th><th>占比</th><th>说明（100M 总量）</th></tr>
    <tr><td>person_events</td><td>2%</td><td>2M person</td></tr>
    <tr><td>auction_events</td><td>6%</td><td>6M auction</td></tr>
    <tr><td>bid_events</td><td>92%</td><td>92M bid</td></tr>
  </table></div>
  <p>wfusion 生成器 <code>wfgen gen-nexmark</code> 按官方默认配置<b>确定性生成</b>（价格对数均匀、hot auction/seller/bidder、事件时间严格递增），与 Flink 官方定义逐项对齐。查询集 <b>Q1–Q22 共 22 条</b>，覆盖无状态投影/过滤、join（TUMBLE/asof/snapshot）、滑窗/跳窗/top-N、会话窗口、stats 聚合等典型形态——阿里白皮书发布 q0–q22 的 OSS/VVR 吞吐，本报告对比 q1–q22。<b>当前自动化套件 21 条</b>（Q6 移出 <code>all</code>）。</p>
</section>

<section>
  <h2>2. 三方配置与度量口径（公平性核心）</h2>
  <div class="tbl-scroll"><table>
    <tr><th>维度</th><th>warp-fusion</th><th>OSS Flink</th><th>VVR（实时计算 Flink）</th></tr>
    <tr><td>引擎版本</td><td>warp-fusion（wp-reactor）v2.0.7</td><td>1.20.4</td><td>vvr-11.5-jdk11-flink-1.20</td></tr>
    <tr><td>数据规模</td><td>100,000,000（PK 主表）</td><td>100,000,000</td><td>100,000,000</td></tr>
    <tr><td>计算资源</td><td>Linux 8 核（实际≥10，与 VVR 同型号云服务器）</td><td>3×ecs.g6a.xlarge = 12 vCPU/48GiB</td><td>8 CU ≈ 8 vCPU / 32 GiB（托管分布式集群，总资源=8C/32G，与 wfusion 同型号云服务器）</td></tr>
    <tr><td>sink</td><td>本地文件（blackhole 等价）</td><td>Blackhole</td><td>Blackhole</td></tr>
    <tr><td>指标</td><td>EPS = Σn/(max emit−min start)（哨兵）</td><td>RPS = 输入量/用时</td><td>RPS = 输入量/用时</td></tr>
    <tr><td>来源</td><td>本仓库 <code>bench.sh</code></td><td>阿里白皮书</td><td>阿里白皮书</td></tr>
  </table></div>
  <div class="note ok">本版 PK 主表与白皮书<b>同规模（100M × 100M）</b>；wfusion 与 VVR 使用<b>相同型号云服务器</b>，残余不对等仅剩<b>计算资源计量口径</b>（8 核 vs 8CU，OSS 为 3×12vCPU），结论作量级参照。wfusion 哨兵 EPS 与白皮书 RPS 思路同源（消化记录数 ÷ 耗时）。</div>
</section>

<section>
  <h2>3. 性能 PK 主表（100M Linux · 2026-08-27 权威跑批 · v2.0.7）</h2>
  <p class="sub">EPS/RPS 单位：条/秒。倍数 = wfusion EPS ÷ 对应基线 RPS。q13 白皮书未发布基线（重写后 O(1) snapshot join，已实测 8.17M），标 N/A。</p>
  <div class="tbl-scroll"><table>
    <tr><th>Query</th><th>语义</th><th>wfusion EPS</th><th>OSS RPS</th><th>VVR RPS</th><th>vs OSS</th><th>vs VVR</th><th>RSS(MB)</th></tr>
    {main_rows}
  </table></div>
  <div class="note">结论：vs OSS <b>{c['oss_min']:.2f}×~{c['oss_max']:.2f}× 全面领先</b>（{c['n_base']}/{c['n_base']}）；vs VVR <b>{c['vvr_min']:.2f}×~{c['vvr_max']:.2f}×，{c['n_base']}/{c['n_base']} 全部达 VVR</b>。
  边缘项：<b>{c['vvr_weak_q']} vs VVR {c['vvr_min']:.2f}×</b>（stats 1d 桶，状态窗随规模增长）；q14 1.77×、q22 3.20×。q13 白皮书无基线。⚠ <b>q18 RSS 30GB</b> 为已知内存问题。</div>
  <div class="note">WFL 原语分工：wfusion 规则以 <code>match</code>（序列/状态机检测）与 <code>stats</code>（列式统计聚合）两类一等原语表达；PK 边缘项 <b>q14 / q17（vs VVR 最弱）与 q18（RSS 内存问题）均落在 <code>stats</code> 路径</b>，非 <code>match</code>。</div>
</section>

<section>
  <h2>4. 可视化：领先倍数</h2>
  <div class="grid2">
    <div>
      <h3>相对 VVR 的倍数 · 对数座标（{c['vvr_min']:.2f}× – {c['vvr_max']:.2f}×）</h3>
      <div class="legend"><span><i style="background:#2f9e8f"></i>低（≤11×）</span><span><i style="background:#e0922f"></i>中</span><span><i style="background:#c0392b"></i>高（≥25×）</span></div>
      <div class="chart">{chart_vvr}</div>
    </div>
    <div>
      <h3>相对 OSS Flink 的倍数 · 对数座标（{c['oss_min']:.2f}× – {c['oss_max']:.2f}×）</h3>
      <div class="legend"><span><i style="background:#2f9e8f"></i>低（≤60×）</span><span><i style="background:#e0922f"></i>中</span><span><i style="background:#c0392b"></i>高（≥120×）</span></div>
      <div class="chart">{chart_voss}</div>
    </div>
  </div>
  <p class="sub" style="margin-top:2px">倍数条形采用<b>对数座标</b>（跨度 1.1×–229×，<b>1× = 与基线持平</b>基线）。条长表示相对量级而非线性比例；颜色仍按倍数大小分级（绿=低 / 橙=中 / 红=高）。</p>
  <h3>wfusion 100M 绝对吞吐（EPS）</h3>
  <div class="chart">{chart_eps}</div>
</section>

<section>
  <h2>5. 规模缩放观测（同机 30M vs 100M）</h2>
  <p class="sub">同一台 Linux 8 核机、同一 v2.0.7，30M 与 100M 背靠背跑批。比值 = 100M EPS ÷ 30M EPS；琥珀底行标记规模退化（&lt;0.8）。</p>
  <div class="tbl-scroll"><table>
    <tr><th>Query</th><th>30M EPS</th><th>100M EPS</th><th>100M/30M</th><th>类型</th></tr>
    {scale_rows}
  </table></div>
  <div class="note">无状态/轻量查询规模因子 0.94~1.24×，基本不随规模退化；<b>状态型退化集中在 q5 0.67×、q17 0.66×、q18 0.32×</b>（窗口状态随数据量增长，RSS 翻倍）。q18 0.32× 伴随 30GB RSS，为已知内存问题。</div>
</section>

<section>
  <h2>6. 跨机一致性验证（Mac 10 核常载 · 100M 无状态子集）</h2>
  <p class="sub">白皮书基线本就是 100M 条——与白皮书同规模的跨机验证。仅无状态查询（状态重查询 50GB+ 未在本机跑）。Mac 为常载开发机，读数偏保守。</p>
  <div class="tbl-scroll"><table>
    <tr><th>Query</th><th>wfusion EPS</th><th>OSS RPS</th><th>VVR RPS</th><th>vs OSS</th><th>vs VVR</th></tr>
    {mac_rows}
  </table></div>
  <div class="note">结论：vs OSS <b>4.13×~18.47× 全面领先</b>（7/7）；vs VVR <b>1.20×~4.98× 全部达 VVR</b>（7/7）。同规模下 q14 vs VVR 仍最弱（1.20×）。跨机结论与 Linux 主表方向一致。</div>
</section>

<section>
  <h2>7. 结果分析</h2>
  <h3>7.1 按查询形态分类</h3>
  <ul>
    <li><b>无状态投影/过滤</b>（q1/q2/q3/q8/q10/q14/q21/q22）：EPS 5~30M，受单连接读链限制，吞吐最高。</li>
    <li><b>窗口/聚合/join</b>（q4/q5/q7/q9/q11/q12/q15–q18/q19/q20）：中高 4~15M，受窗口状态与 join 维护成本主导。</li>
    <li><b>状态重查询</b>（q13/q18）：q13 重写后 8.17M（snapshot join O(1)）；q18 仅 2.90M 且 RSS 30GB（已知内存问题）。</li>
  </ul>
  <h3>7.2 边缘项：vs VVR 最弱是 q17（1.12×），非 q14</h3>
  <p>新版 100M 数据下，<b>相对 VVR 最弱项是 q17（1.12×）</b>——stats 1d 桶，状态窗随 100M 规模增长、RSS 19.4GB。q14（Calculation，<code>0.908×price</code> 过滤 + <code>HOUR</code> 分型 + <code>count_char</code> UDF，无状态投影）在 100M 下为 <b>1.77×</b>，每命中事件需构建 detail 字符串，属 Flink 语义固有成本。diag 墙表显示主墙 = <b>输出链</b>（+54.6ns）与规则段（+42.1ns），差距本质来自<b>引擎行式 cell 求值</b>，非查询写法问题；改进需进 wp-reactor 做列式字符串/过滤求值（通用能力，建议独立立项）。</p>
  <h3>7.3 已知问题与口径说明</h3>
  <ul>
    <li><b>q18 内存（30GB）🔴</b>：100M 状态窗线性增长，监视器曾测 ~60GB；建议内存归因跟进。</li>
    <li><b>q5/q17 规模退化（0.67×/0.66×）</b>：窗口状态随数据量增长，RSS 翻倍，属预期内状态型退化，非 bug。</li>
    <li><b>q13 无白皮书基线</b>：snapshot join（重写后 O(1)）实测 8.17M，Flink 官方未发布对应档，不参与 vs 基线倍数计算。</li>
  </ul>
</section>

<section>
  <h2>8. 正确性与查询覆盖</h2>
  <ul>
    <li><b>覆盖</b>：查询集 22 条，自动化套件 <b>21/21 已完成 100M clean 跑批</b>（Q6 移出 <code>all</code>、单跑保留）。能力判定：<b>20 已有 · 1 待补强（Q12 处理时间窗）· 1 特殊口径（Q6 移出套件）</b>。</li>
    <li><b>oracle 对拍（30M）</b>：Q4/Q9 <b>identical</b>（D4 保留 pin：join 目标窗字节上限驱逐不再静默丢行）；剩余差异：Q12 fixed+close 尾桶 known-diff、Q19 stats oracle 未接入（7 个 stats 规则组被 oracle 跳过）。</li>
    <li><b>验证工具</b>：<code>wfgen verify-nexmark</code> 用真实 WFL 规则引擎对同一份确定性数据逐规则算出期望值，与引擎 EMIT 计数 git-diff 同款分层对拍。</li>
  </ul>
</section>

<section>
  <h2>9. 资源消耗（RSS / CPU）</h2>
  <ul>
    <li><b>RSS 峰值（100M）</b>：无状态查询 4.0~5.6GB；状态重查询 <b>q18 {fmt_gb(30055)}</b>、q17 19.4GB、q4 13.7GB、q9 7.4GB。q18 30GB 为已知内存问题（状态窗随数据量线性增长）。其余随消费速度饱和、30M 后基本有界。</li>
    <li><b>CPU（活跃窗核占）</b>：无状态查询受单连接读链限制 ~100% avg（满核但供给瓶颈）；重查询 avg 400%+。CPU 0% 旧口径假象已修复，新口径下 0% 才可信。</li>
  </ul>
</section>

<section>
  <h2>10. 结论</h2>
  <p>warp-fusion 在 NEXMark 全查询上相对 OSS Flink <b>量级领先 {c['oss_min']:.2f}×–{c['oss_max']:.2f}×</b>，相对 VVR <b>全面达到且多数显著超出（{c['vvr_min']:.2f}×–{c['vvr_max']:.2f}×）</b>，正确性已达生产可用基线，21/21 套件查询端到端可运行（100M clean）。短板集中在：① q14/q17 类 Calculation / stats 的行式 cell 求值（通用引擎级优化）；② q18 状态窗内存（100M 30GB，已知问题）；③ Q12 处理时间窗（事件时间引擎固有，replay 下等价）。</p>
</section>

<section>
  <h2>附录 A：口径与引用</h2>
  <ul>
    <li><b>EPS 哨兵口径</b>：<code>EPS = Σn / (max emit_ns − min start_ns)</code>，数据窗排空后取首尾时间戳，无轮询粒度误差。</li>
    <li><b>RSS 口径</b>：须标注 <code>parse_buffer_bytes</code>（默认 128MB，吞吐优先 2GB）。</li>
    <li><b>A/B 纪律</b>：不限速（RATE=10000000）、同时段交错、按 RSS 相位配对、单轮 ±8% 噪声。</li>
    <li><b>来源</b>：阿里云《性能白皮书（Nexmark 性能测试）》<a href="https://help.aliyun.com/zh/flink/realtime-flink/support/nexmark-performance-testing">help.aliyun.com</a>；
    本仓库 <code>docs/OSS_VVR_BASELINE.md</code>、<code>docs/BENCH_RESULTS.md</code>、<code>docs/CAPABILITY_GAP_MATRIX.md</code>。</li>
  </ul>
</section>

<footer>范围：wf-examples/performance/nexmark_pk · PK 主表采用 Linux 100M 权威跑批（v2.0.7，21/21 clean）</footer>

</div>
</body>
</html>"""
    return html

def main():
    md = render_md()
    html = render_html()
    md_path = os.path.join(OUT_DIR, "NEXMARK_PK_REPORT.md")
    html_path = os.path.join(OUT_DIR, "NEXMARK_PK_REPORT.html")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(md)
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    print("written:", md_path, f"({len(md)} bytes)")
    print("written:", html_path, f"({len(html)} bytes)")

if __name__ == "__main__":
    main()
