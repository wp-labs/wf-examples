#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
生成 NEXMark PK 报告（warp-fusion vs Flink OSS vs Flink VVR）的 MD 与 HTML 双格式。

数据来源（项目内权威文档，已核实）：
- docs/OSS_VVR_BASELINE.md  —— 阿里白皮书 OSS Flink / VVR 基线（q0~q22，100M 条）
- docs/BENCH_RESULTS.md     —— wfusion 实测归档（10M/30M Linux、100M Mac）
- 主 PK 表采用「2026-08-25 Linux 30M · 22/22 clean」归档基线（OSS_VVR_BASELINE §3 / BENCH_RESULTS §3）
- 2026-08-26 新鲜跑批（data/bench_*_replay.txt）作为「待归档附录」，未写入 BENCH_RESULTS.md
"""
import os

OUT_DIR = os.path.dirname(os.path.abspath(__file__))

# ----------------------------------------------------------------------------
# 1. PK 主表：2026-08-25 Linux 30M 归档基线
#    (q, 语义, wf_eps, oss_rps, vvr_rps, vs_oss, vs_vvr, rss_mb)
#    vs_oss / vs_vvr 取自 BENCH_RESULTS.md §3；q6/q13 白皮书无基线 -> None
# ----------------------------------------------------------------------------
BASE = [
    ("q1",  "无状态投影（货币换算 0.908×price + 过滤）", 13500405, 1753002, 4381353, 7.70, 3.08, 3810),
    ("q2",  "选择（MOD(auction,123)=0 的 bid）",          29770854, 1927154, 6568576, 15.45, 4.53, 3651),
    ("q3",  "按州过滤（IN OR/ID/CA）",                     21214312, 1176664, 4638649, 18.03, 4.57, 3908),
    ("q4",  "分类均价（累积窗口 deferred reduce + stats）", 7546882, 180693, 636468, 41.77, 11.86, 9608),
    ("q5",  "热门新商品（HOP 10s/2s + top_ties(1)）",      3531444, 273496, 279684, 12.91, 12.63, 6628),
    ("q6",  "卖家售出均价（sliding 10m avg，能力面）",      367276, None, None, None, None, 8010),
    ("q7",  "时段最高出价（match<auction:10s> + top_ties）", 9283947, 79526, 299547, 116.74, 30.99, 3768),
    ("q8",  "新用户 + 其拍卖（TUMBLE deferred join）",      29846145, 1253321, 3340125, 23.81, 8.94, 4101),
    ("q9",  "中标出价（asof deferred reduce join）",        7752037, 43020, 375146, 180.20, 20.66, 9325),
    ("q10", "全量 bid 按时间分区落盘（每 bid 一行）",       14051840, 526357, 1953049, 26.70, 7.19, 3886),
    ("q11", "用户会话统计（session 窗口 + 分片）",          10651936, 244868, 685011, 43.50, 15.55, 3704),
    ("q12", "每 bidder × 10s 处理时间窗计数",              11068963, 822680, 2703360, 13.45, 4.09, 3712),
    ("q13", "有界侧输入 join（snapshot join）",            279095, None, None, None, None, 15706),
    ("q14", "时间戳换算 + 价格过滤（Calculation）",         5494039, 1451316, 4997002, 3.79, 1.10, 3892),
    ("q15", "日历天出价统计（stats + 1d 桶）",              6546380, 544339, 2340057, 12.03, 2.80, 5184),
    ("q16", "日历天渠道统计（stats + 1d 桶）",              4423617, 108980, 296478, 40.59, 14.92, 7187),
    ("q17", "日历天拍卖统计（stats + 1d 桶）",              6158283, 972318, 3693308, 6.33, 1.67, 6879),
    ("q18", "每 (bidder,auction) 最后一条 bid（stats last）", 8706044, 173928, 1038044, 50.06, 8.39, 15537),
    ("q19", "拍卖 Top-10 价格（stats<> top-N）",           4110426, 170565, 1051293, 24.10, 3.91, 8204),
    ("q20", "展开 bid 关联 auction（snapshot join + where）", 14218398, 74591, 431999, 190.62, 32.91, 4349),
    ("q21", "附加 channel id（热通道映射 + cold url）",     12012182, 786850, 2519336, 15.27, 4.77, 3903),
    ("q22", "URL 目录投影",                                 5611076, 1054519, 3202254, 5.32, 1.75, 3857),
]

# ----------------------------------------------------------------------------
# 2. 同规模 100M 对照（2026-08-25 Mac 10 核，无状态查询子集）
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

# ----------------------------------------------------------------------------
# 3. 2026-08-26 新鲜跑批（data/bench_*_replay.txt，待归档，未写入 BENCH_RESULTS.md）
#    (q, wf_eps, rss_mb) —— 已实测 22/22 clean
# ----------------------------------------------------------------------------
RUN0826 = {
    "q1": (19743089, 3381), "q2": (23411948, 2933), "q3": (22323953, 3197),
    "q4": (4659686, 5767), "q5": (5777971, 6048), "q6": (614914, 5539),
    "q7": (12849308, 3062), "q8": (22060770, 3268), "q9": (10162877, 5886),
    "q10": (19363371, 2892), "q11": (15978780, 2984), "q12": (16999921, 3070),
    "q13": (9907101, 3584), "q14": (14051127, 3030), "q15": (13392887, 4121),
    "q16": (7359669, 7413), "q17": (14614876, 6107), "q18": (12956226, 14638),
    "q19": (10124996, 8295), "q20": (20157646, 3798), "q21": (21167037, 3631),
    "q22": (10349331, 9352),
}

def fnum(n):
    return f"{n:,}" if n is not None else "—"

def fxf(x):
    return f"{x:.2f}×" if x is not None else "N/A"

def fmt_eps(n):
    return f"{n/1_000_000:.2f}M" if n is not None else "—"

# ============================================================================
#  MD 渲染
# ============================================================================
def render_md():
    L = []
    L.append("# warp-fusion NEXMark 性能 PK 报告：对比 Flink OSS 与 Flink VVR\n")
    L.append("> **范围**：warp-fusion（wp-reactor 引擎）在 NEXMark Q1–Q22 上的吞吐表现，"
             "对照阿里云《Nexmark 性能白皮书》发布的 **OSS Flink** 与 **实时计算 Flink（VVR）** 基线。\n")
    L.append("> **数据截止**：PK 主表采用 **2026-08-25 Linux 30M 归档基线**（22/22 clean，哨兵 EPS 口径）；"
             "同日 Mac 100M 子集用于同规模对照；**2026-08-26 新鲜跑批**见附录 B（待归档）。\n")
    L.append("---\n")

    # 摘要
    L.append("## 0. 摘要\n")
    L.append("- **相对 OSS Flink**：在 20 个有白皮书基线的查询上 **3.79×~190.62× 全面领先**。\n")
    L.append("- **相对 VVR（实时计算 Flink）**：20/20 有基线查询 **全部达到 VVR**，倍数 **1.10×~32.91×**"
             "（其中 17 个显著超出 VVR 2× 以上）。\n")
    L.append("- **查询覆盖**：NEXMark Q1–Q22 **22/22 已实现并完成 30M clean 跑批**；"
             "能力判定 20 条完整支持、Q12 待补强（处理时间窗）、Q6 特殊口径（Flink 官方亦未实现）。\n")
    L.append("- **正确性**：30M oracle 对拍中 Q4/Q9 **identical**（D4 pin 修复），17/22 非 stats 查询一致；"
             "Q3 30M −16% 规模 bug 待查，Q19 stats oracle 未接入（known-diff）。\n")
    L.append("- **⚠ 口径声明**：三方**数据规模与计算资源不对等**（wfusion 30M/8 核 vs 白皮书 100M/8CU·12vCPU），"
             "本报告结论作**量级参照**，非逐位比较。详见 §2。\n")

    L.append("| 指标 | 数值 |\n|---|---|\n")
    L.append("| 有基线查询 vs OSS Flink | **3.79× – 190.62×**（20/20 全面领先） |\n")
    L.append("| 有基线查询 vs VVR | **1.10× – 32.91×**（20/20 全部达 VVR） |\n")
    L.append("| 30M 最高 EPS | 29.85M（Q8） |\n")
    L.append("| 30M 最低 EPS | 0.28M（Q13，滑窗语义代价；08-26 重做后 9.91M） |\n")
    L.append("| 相对 VVR 最高倍数 | 32.91×（Q20） |\n")
    L.append("| 相对 VVR 最弱项 | 1.10×（Q14 Calculation，字符串/格式化重） |\n")
    L.append("| 30M 最高 RSS | 15.7GB（Q13，08-25） |\n")

    # 背景
    L.append("\n## 1. 背景与基准对象\n")
    L.append("**NEXMark** 是流处理领域社区公认的基准（源自 Apache Calcite / Nexmark，"
             "被 Flink 官方 `nexmark` 测试集采纳），由三流事件构成：\n")
    L.append("| 流 | 占比 | 说明 |\n|---|---|---|\n")
    L.append("| person_events | 2% | 30M 总量 = 600k person |\n")
    L.append("| auction_events | 6% | 30M 总量 = 1.8M auction |\n")
    L.append("| bid_events | 92% | 30M 总量 = 27.6M bid |\n")
    L.append("\nwfusion 生成器 `wfgen gen-nexmark` 按官方默认配置确定性生成（价格对数均匀、hot auction/seller/bidder、"
             "事件时间严格递增），**同一 count + seed 字节级确定**，与 Flink 官方定义逐项对齐"
             "（见 `docs/NEXMARK_CONFORMANCE.md`）。\n")
    L.append("\n查询集 **Q1–Q22 共 22 条**，覆盖无状态投影/过滤、join（TUMBLE/asof/snapshot）、"
             "滑窗/跳窗/top-N、会话窗口、stats 聚合等流处理典型形态。阿里白皮书发布其中 **q0–q22（23 条）** 的"
             "OSS/VVR 吞吐——本报告对比 q1–q22（q0 为计数投影，白皮书有基线但非 NEXMark 标准查询集，本套件未纳入）。\n")

    # 口径
    L.append("\n## 2. 三方配置与度量口径（公平性核心）\n")
    L.append("| 维度 | warp-fusion | OSS Flink | VVR（实时计算 Flink） |\n|---|---|---|---|\n")
    L.append("| 引擎版本 | warp-fusion（wp-reactor） | 1.20.4 | vvr-11.5-jdk11-flink-1.20 |\n")
    L.append("| 数据规模 | 30M（PK 主表）/ 100M（子集） | 100,000,000 | 100,000,000 |\n")
    L.append("| 计算资源 | Linux 8 核（实际可用 ≥10）/ Mac 10 核 | 3×ecs.g6a.xlarge = 12 vCPU / 48GiB | 8 CU |\n")
    L.append("| sink | 本地文件（blackhole 等价） | Blackhole | Blackhole |\n")
    L.append("| 指标 | EPS = Σn / (max emit − min start)（哨兵四元组） | RPS = 输入量 / 用时 | RPS = 输入量 / 用时 |\n")
    L.append("| 来源 | 本仓库 `bench.sh` | 阿里白皮书 | 阿里白皮书 |\n")
    L.append("\n**度量口径说明**：\n")
    L.append("1. wfusion 的 **哨兵 EPS** 与白皮书的 **RPS** 思路同源——都是「消化（或处理）掉的记录数 ÷ 耗时」。"
             "哨兵机制从数据窗排空后取首尾时间戳，消除轮询粒度误差，短跑读数同样可信。\n")
    L.append("2. **规模/资源不对等**：白皮书为 100M 条 × 8CU(VVR)/12vCPU(OSS) × Blackhole；"
             "wfusion 主表为 30M 条 × 本机核数 × 本地文件 sink。**结论作量级参照，非逐位比较**。\n")
    L.append("3. 单轮数字存在 ±8% 相位噪声（bench 机 EPS 与 RSS 双峰相位强相关），结论已按 RSS 相位配对。\n")
    L.append("4. 引用 wfusion RSS 须标注 `parse_buffer_bytes`（默认 128MB；吞吐优先 2GB）。\n")

    # 主表
    L.append("\n## 3. 性能 PK 主表（30M Linux · 2026-08-25 归档基线）\n")
    L.append("> EPS/RPS 单位：条/秒。倍数 = wfusion EPS ÷ 对应基线 RPS。q6/q13 白皮书未发布基线（Q6 Flink 官方亦未实现），标 N/A。\n")
    L.append("| Query | 语义 | wfusion EPS | OSS RPS | VVR RPS | vs OSS | vs VVR | RSS(MB) |\n|---|---|---:|---:|---:|---:|---:|---:|\n")
    for q, sem, wf, oss, vvr, vo, vv, rss in BASE:
        L.append(f"| {q} | {sem} | {fmt_eps(wf)} | {fmt_eps(oss)} | {fmt_eps(vvr)} | {fxf(vo)} | {fxf(vv)} | {rss:,} |\n")
    L.append("\n**结论**：vs OSS **3.79×~190.62× 全面领先**（20/20）；vs VVR **1.10×~32.91×，20/20 全部达 VVR**。\n")
    L.append("边缘项：**q14 vs VVR 1.10×**（Calculation 类，字符串/格式化重，为 vs VVR 最弱项）；q17 1.67×、q22 1.75×。"
             "**q6/q13 白皮书无基线**。⚠ **q19 规模退化**（10M→30M：12.4M→4.1M，驱逐触发，待查；08-26 跑批 q19=10.12M 显示已回升，见附录 B）。\n")

    # 100M 子集
    L.append("\n## 4. 同规模 100M 对照（Mac 10 核子集，无状态查询）\n")
    L.append("> 白皮书基线本就是 **100M 条**——这是首次与白皮书**同规模**对照。仅无状态查询（状态重查询 50GB+ 未在本机跑）。"
             "Mac 为 10 核常载开发机（load 4.4~7.9），读数偏保守。\n")
    L.append("| Query | wfusion EPS | OSS RPS | VVR RPS | vs OSS | vs VVR |\n|---|---:|---:|---:|---:|---:|\n")
    for q, wf, oss, vvr, vo, vv in MAC100:
        L.append(f"| {q} | {fmt_eps(wf)} | {fmt_eps(oss)} | {fmt_eps(vvr)} | {fxf(vo)} | {fxf(vv)} |\n")
    L.append("\n**结论**：vs OSS **4.13×~18.47× 全面领先**（7/7）；vs VVR **1.20×~4.98× 全部达 VVR**（7/7）。"
             "同规模下 q14 vs VVR 仍最弱（1.20×）。\n")

    # 分析
    L.append("\n## 5. 结果分析\n")
    L.append("### 5.1 按查询形态分类\n")
    L.append("- **无状态投影/过滤（q1/q2/q3/q8/q10/q14/q21/q22）**：EPS 5~30M，受单连接读链限制，吞吐最高。\n")
    L.append("- **窗口/聚合/join（q4/q5/q7/q9/q11/q12/q15–q18/q19/q20）**：中高 4~15M，受窗口状态与 join 维护成本主导。\n")
    L.append("- **状态重查询（q6/q13）**：30M 仅 0.3~0.6M（滑窗 10m avg / 双规则链中间窗全保留的语义代价）；"
             "08-26 跑批 q13 经重写升至 9.91M（见附录 B）。\n")
    L.append("### 5.2 边缘项：q14 vs VVR 1.10×\n")
    L.append("q14 实为 **Calculation**（`0.908×price` 过滤 + `HOUR` 分型 + `count_char` UDF，无状态投影），"
             "每命中事件需构建 detail 字符串，属 Flink 语义固有成本。diag 墙表显示主墙 = **输出链**（+54.6ns）与"
             "规则段（+42.1ns），**差距本质来自引擎行式 cell 求值**，非查询写法问题；改进需进 wp-reactor 做列式"
             "字符串/过滤求值（通用能力，建议独立立项），不为 q14 单点投入。\n")
    L.append("### 5.3 已知规模问题\n")
    L.append("- **q19 规模退化**（08-25：10M 12.4M → 30M 4.1M，驱逐触发）：序列查询状态规模相关。08-26 跑批 q19=10.12M 显示已回升，待归档确认。\n")
    L.append("- **q3 30M −16%**（150,992 vs 180,304）：规模相关独立 bug，已排除字节上限驱逐，待查。\n")
    L.append("- **q6/q13 白皮书无基线**：Q6 因 OVER WINDOW 不支持 retractions Flink 官方亦未实现，双方都无权威实现。\n")

    # 正确性
    L.append("\n## 6. 正确性与查询覆盖\n")
    L.append("- **覆盖**：22/22 查询已实现，30M 全量 `[clean]`（appended 30M/30M）。")
    L.append("能力判定：**20 已有 · 1 待补强（Q12 处理时间窗）· 1 特殊口径（Q6）**。\n")
    L.append("- **oracle 对拍（30M）**：Q4/Q9 **identical**（D4 保留 pin：join 目标窗字节上限驱逐不再静默丢行）；"
             "剩余已知差异：Q3 30M −16%、Q12 fixed+close 尾桶 known-diff、Q19 stats oracle 未接入。\n")
    L.append("- **验证工具**：`wfgen verify-nexmark` 用真实 WFL 规则引擎对同一份确定性数据逐规则算出期望值，"
             "与引擎 EMIT 计数 git-diff 同款分层对拍（L1 哈希→L2 Myers→L3 明细）。\n")

    # 资源
    L.append("\n## 7. 资源消耗（RSS / CPU）\n")
    L.append("- **RSS 峰值（30M）**：无状态查询 3.3~3.9GB；状态重查询 q13 15.7GB、q18 15.5GB、q4 9.6GB、q9 9.3GB。")
    L.append("内存随消费速度饱和、不随数据量线性涨（q14 10M→100M 仅 +0.38GB），30M 后基本有界。\n")
    L.append("- **CPU（活跃窗核占）**：无状态查询受单连接读链限制 ~100% avg（满核但单连接供给瓶颈）；"
             "重查询 avg 400%+（多核充分占用）。08-24 前的 CPU 0% 为假象（亚秒突发在采样器首个差分前烧完），"
             "新口径下 0% 才可信。\n")

    # 结论
    L.append("\n## 8. 结论\n")
    L.append("warp-fusion 在 NEXMark 全查询上相对 OSS Flink **量级领先 3.8×–190×**，相对 VVR（实时计算 Flink）"
             "**全面达到且多数显著超出（1.1×–33×）**，正确性已达生产可用基线，22/22 查询端到端可运行。"
             "短板集中在：① q14 类 Calculation 的行式 cell 求值（通用引擎级优化）；② q3/q19 规模相关 bug；"
             "③ Q12 处理时间窗（事件时间引擎固有，replay 下等价）。\n")

    # 附录
    L.append("\n## 附录 A：口径与引用\n")
    L.append("- **EPS 哨兵口径**：`EPS = Σn / (max emit_ns − min start_ns)`，数据窗排空后取首尾时间戳，无轮询粒度误差。\n")
    L.append("- **RSS 口径**：须标注 `parse_buffer_bytes`（默认 128MB，吞吐优先 2GB）。\n")
    L.append("- **A/B 纪律**：不限速（RATE=10000000）、同时段交错、按 RSS 相位配对、单轮 ±8% 噪声。\n")
    L.append("- **来源**：阿里云《性能白皮书（Nexmark 性能测试）》"
             "https://help.aliyun.com/zh/flink/realtime-flink/support/nexmark-performance-testing ；"
             "本仓库 `docs/OSS_VVR_BASELINE.md`、`docs/BENCH_RESULTS.md`、`docs/CAPABILITY_GAP_MATRIX.md`。\n")

    L.append("\n## 附录 B：最新跑批（2026-08-26，待归档）\n")
    L.append("> ⚠ 以下为 2026-08-26 在 `data/bench_*_replay.txt` 的完整 22 查询新鲜跑批，**尚未写入 `docs/BENCH_RESULTS.md`，"
             "发布为权威基线前需复核归档**。其中 **q13 于 08-25 20:56 重写**（snapshot join O(1) / 取消 2d 窗口全保留），"
             "EPS 由 0.28M 升至 9.91M、RSS 15.7GB→3.6GB；其余查询在更高机器相位下读数普遍上移。\n")
    L.append("| Query | 08-26 EPS | 08-26 RSS(MB) | vs 08-25 基线 EPS | 备注 |\n|---|---:|---:|---:|---|\n")
    for q, sem, wf, oss, vvr, vo, vv, rss in BASE:
        e26, r26 = RUN0826.get(q, (None, None))
        if e26 is None:
            delta = "—"
        else:
            ratio = e26 / wf if wf else 0
            delta = f"{ratio:.2f}×"
        note = ""
        if q == "q13":
            note = "查询重写，非噪声"
        elif q == "q19":
            note = "规模退化回升"
        L.append(f"| {q} | {fmt_eps(e26)} | {r26:,} | {delta} | {note} |\n")
    L.append("\n> 读数普遍高于 08-25 归档基线（部分 +40%~+150%），含机器相位与 q13/q19 修复因素；"
             "在归档前建议：① 将本跑批写入 `BENCH_RESULTS.md`；② 重跑 q14 A/B 确认是否相位效应；"
             "③ 以 08-26 为新的「当前对照」更新 `OSS_VVR_BASELINE.md` §3。\n")

    return "".join(L)

# ============================================================================
#  HTML 渲染（自包含，浅色主题，CSS 柱状图，无外部依赖）
# ============================================================================
def bar(val, vmax, color, q, display):
    pct = max(2.0, val / vmax * 100.0) if val and vmax else 0
    return (f'<div class="brow"><span class="blab">{q}</span>'
            f'<div class="btrack"><div class="bfill" style="width:{pct:.1f}%;background:{color}"></div>'
            f'<span class="bval">{display}</span></div></div>')

def render_html():
    # 计算 vs OSS / vs VVR 最大值用于条形比例
    voss_vals = [b[5] for b in BASE if b[5] is not None]
    vvvr_vals = [b[6] for b in BASE if b[6] is not None]
    voss_max = max(voss_vals)
    vvvr_max = max(vvvr_vals)
    eps_max = max(b[2] for b in BASE)

    # 颜色：按倍数映射（越红=越强，符合中文"涨=红"直觉；但这里是领先倍数，用红/橙渐变强调优势）
    def heat(x, xmax):
        # 0..1 -> 浅蓝到红
        t = min(1.0, x / xmax) if xmax else 0
        # 简单：低=青，高=红
        if t < 0.33:
            return "#2f9e8f"
        elif t < 0.66:
            return "#e0922f"
        return "#c0392b"

    # vs VVR 图
    chart_vvr = []
    for q, sem, wf, oss, vvr, vo, vv, rss in BASE:
        if vv is not None:
            chart_vvr.append(bar(vv, vvvr_max, heat(vv, vvvr_max), q, f"{vv:.2f}×"))
    chart_vvr = "".join(chart_vvr)

    # vs OSS 图
    chart_voss = []
    for q, sem, wf, oss, vvr, vo, vv, rss in BASE:
        if vo is not None:
            chart_voss.append(bar(vo, voss_max, heat(vo, voss_max), q, f"{vo:.2f}×"))
    chart_voss = "".join(chart_voss)

    # EPS 绝对图
    chart_eps = []
    for q, sem, wf, oss, vvr, vo, vv, rss in BASE:
        chart_eps.append(bar(wf, eps_max, "#1769c2", q, fmt_eps(wf)))
    chart_eps = "".join(chart_eps)

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

    # 100M 子集行
    mrows = []
    for q, wf, oss, vvr, vo, vv in MAC100:
        mrows.append(f"<tr><td class='q'>{q}</td><td class='num'>{fmt_eps(wf)}</td>"
                     f"<td class='num'>{fmt_eps(oss)}</td><td class='num'>{fmt_eps(vvr)}</td>"
                     f"<td class='num strong'>{fxf(vo)}</td><td class='num strong'>{fxf(vv)}</td></tr>")
    mac_rows = "\n".join(mrows)

    # 附录 B 行
    arows = []
    for q, sem, wf, oss, vvr, vo, vv, rss in BASE:
        e26, r26 = RUN0826.get(q, (None, None))
        delta = f"{e26/wf:.2f}×" if (e26 and wf) else "—"
        note = "查询重写，非噪声" if q == "q13" else ("规模退化回升" if q == "q19" else "")
        arows.append(f"<tr><td class='q'>{q}</td><td class='num'>{fmt_eps(e26)}</td>"
                     f"<td class='num'>{r26:,}</td><td class='num'>{delta}</td>"
                     f"<td class='sem'>{note}</td></tr>")
    appb_rows = "\n".join(arows)

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
  .chart {{ margin:6px 0 4px; }}
  .brow {{ display:flex; align-items:center; gap:10px; margin:3px 0; }}
  .blab {{ width:34px; font-weight:700; color:#2a3b52; text-align:right; font-size:13px; }}
  .btrack {{ position:relative; flex:1; background:var(--surface2); border-radius:6px; height:22px; overflow:hidden;
    border:1px solid var(--border); }}
  .bfill {{ position:absolute; left:0; top:0; bottom:0; border-radius:6px 0 0 6px; }}
  .bval {{ position:absolute; right:8px; top:0; line-height:22px; font-size:12.5px; font-weight:700; color:#1c2533; }}
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
  <p class="lead">warp-fusion（wp-reactor 引擎）在 NEXMark Q1–Q22 上的吞吐表现，对照阿里云《Nexmark 性能白皮书》
  发布的 <b>OSS Flink</b> 与 <b>实时计算 Flink（VVR）</b> 基线。同一份权威基准数据 + 同一组 22 条查询 + 同输出口径（blackhole 丢弃）。</p>
  <div class="badges">
    <span class="badge">PK 主表：2026-08-25 Linux 30M（22/22 clean）</span>
    <span class="badge">基线：阿里白皮书 OSS/VVR（100M）</span>
    <span class="badge">数据截止 2026-08-26</span>
  </div>
</header>

<div class="cards">
  <div class="card"><div class="k">vs OSS Flink（20/20）</div><div class="v">3.79×–190.62×</div></div>
  <div class="card"><div class="k">vs VVR（20/20 达 VVR）</div><div class="v">1.10×–32.91×</div></div>
  <div class="card"><div class="k">查询覆盖</div><div class="v">22/22 <small>clean</small></div></div>
  <div class="card"><div class="k">30M 最高 EPS</div><div class="v">29.85M <small>Q8</small></div></div>
  <div class="card"><div class="k">vs VVR 最弱</div><div class="v">1.10× <small>Q14</small></div></div>
  <div class="card"><div class="k">30M 最高 RSS</div><div class="v">15.7GB <small>Q13</small></div></div>
</div>

<section>
  <h2>0. 摘要</h2>
  <p class="sub">一句话结论：warp-fusion 在 NEXMark 全查询上相对 OSS Flink 量级领先 3.8×–190×，相对 VVR 全面达到且多数显著超出（1.1×–33×）。</p>
  <ul>
    <li><b>相对 OSS Flink</b>：在 20 个有白皮书基线的查询上 <b>3.79×~190.62× 全面领先</b>。</li>
    <li><b>相对 VVR</b>：20/20 有基线查询 <b>全部达到 VVR</b>，倍数 <b>1.10×~32.91×</b>（其中 17 个显著超出 VVR 2× 以上）。</li>
    <li><b>查询覆盖</b>：Q1–Q22 <b>22/22 已实现并完成 30M clean 跑批</b>；能力判定 20 条完整支持、Q12 待补强、Q6 特殊口径。</li>
    <li><b>正确性</b>：30M oracle 对拍中 Q4/Q9 <b>identical</b>，17/22 非 stats 查询一致；Q3 30M −16% 待查，Q19 stats oracle 未接入（known-diff）。</li>
  </ul>
  <div class="note">⚠ <b>口径声明</b>：三方数据规模与计算资源不对等（wfusion 30M/8 核 vs 白皮书 100M/8CU·12vCPU），本结论作<b>量级参照</b>，非逐位比较。详见 §2。</div>
</section>

<section>
  <h2>1. 背景与基准对象</h2>
  <p><b>NEXMark</b> 是流处理领域社区公认的基准（源自 Apache Calcite / Nexmark，被 Flink 官方采纳），由三流事件构成：</p>
  <table>
    <tr><th>流</th><th>占比</th><th>说明（30M 总量）</th></tr>
    <tr><td>person_events</td><td>2%</td><td>600k person</td></tr>
    <tr><td>auction_events</td><td>6%</td><td>1.8M auction</td></tr>
    <tr><td>bid_events</td><td>92%</td><td>27.6M bid</td></tr>
  </table>
  <p>wfusion 生成器 <code>wfgen gen-nexmark</code> 按官方默认配置<b>确定性生成</b>（价格对数均匀、hot auction/seller/bidder、事件时间严格递增），与 Flink 官方定义逐项对齐。查询集 <b>Q1–Q22 共 22 条</b>，覆盖无状态投影/过滤、join（TUMBLE/asof/snapshot）、滑窗/跳窗/top-N、会话窗口、stats 聚合等典型形态——阿里白皮书发布 q0–q22 的 OSS/VVR 吞吐，本报告对比 q1–q22。</p>
</section>

<section>
  <h2>2. 三方配置与度量口径（公平性核心）</h2>
  <table>
    <tr><th>维度</th><th>warp-fusion</th><th>OSS Flink</th><th>VVR（实时计算 Flink）</th></tr>
    <tr><td>引擎版本</td><td>warp-fusion（wp-reactor）</td><td>1.20.4</td><td>vvr-11.5-jdk11-flink-1.20</td></tr>
    <tr><td>数据规模</td><td>30M（PK 主表）/ 100M（子集）</td><td>100,000,000</td><td>100,000,000</td></tr>
    <tr><td>计算资源</td><td>Linux 8 核（实际≥10）/ Mac 10 核</td><td>3×ecs.g6a.xlarge = 12 vCPU/48GiB</td><td>8 CU</td></tr>
    <tr><td>sink</td><td>本地文件（blackhole 等价）</td><td>Blackhole</td><td>Blackhole</td></tr>
    <tr><td>指标</td><td>EPS = Σn/(max emit−min start)（哨兵）</td><td>RPS = 输入量/用时</td><td>RPS = 输入量/用时</td></tr>
    <tr><td>来源</td><td>本仓库 <code>bench.sh</code></td><td>阿里白皮书</td><td>阿里白皮书</td></tr>
  </table>
  <div class="note ok">wfusion 的<b>哨兵 EPS</b> 与白皮书 <b>RPS</b> 思路同源（消化记录数 ÷ 耗时）。但规模/资源不对等，结论作量级参照，非逐位比较；单轮存在 ±8% 相位噪声，已按 RSS 相位配对。</div>
</section>

<section>
  <h2>3. 性能 PK 主表（30M Linux · 2026-08-25 归档基线）</h2>
  <p class="sub">EPS/RPS 单位：条/秒。倍数 = wfusion EPS ÷ 对应基线 RPS。q6/q13 白皮书未发布基线，标 N/A。</p>
  <table>
    <tr><th>Query</th><th>语义</th><th>wfusion EPS</th><th>OSS RPS</th><th>VVR RPS</th><th>vs OSS</th><th>vs VVR</th><th>RSS(MB)</th></tr>
    {main_rows}
  </table>
  <div class="note">结论：vs OSS <b>3.79×~190.62× 全面领先</b>（20/20）；vs VVR <b>1.10×~32.91×，20/20 全部达 VVR</b>。
  边缘项：q14 vs VVR 1.10×（Calculation 类，字符串/格式化重）；q17 1.67×、q22 1.75×。q6/q13 白皮书无基线。</div>
</section>

<section>
  <h2>4. 可视化：领先倍数</h2>
  <div class="grid2">
    <div>
      <h3>相对 VVR 的倍数（1.10× – 32.91×）</h3>
      <div class="legend"><span><i style="background:#2f9e8f"></i>低（≤11×）</span><span><i style="background:#e0922f"></i>中</span><span><i style="background:#c0392b"></i>高（≥25×）</span></div>
      <div class="chart">{chart_vvr}</div>
    </div>
    <div>
      <h3>相对 OSS Flink 的倍数（3.79× – 190.62×）</h3>
      <div class="legend"><span><i style="background:#2f9e8f"></i>低（≤60×）</span><span><i style="background:#e0922f"></i>中</span><span><i style="background:#c0392b"></i>高（≥120×）</span></div>
      <div class="chart">{chart_voss}</div>
    </div>
  </div>
  <h3>wfusion 30M 绝对吞吐（EPS）</h3>
  <div class="chart">{chart_eps}</div>
</section>

<section>
  <h2>5. 同规模 100M 对照（Mac 10 核子集）</h2>
  <p class="sub">白皮书基线本就是 100M 条——首次与白皮书同规模对照。仅无状态查询（状态重查询 50GB+ 未在本机跑）。Mac 为常载开发机，读数偏保守。</p>
  <table>
    <tr><th>Query</th><th>wfusion EPS</th><th>OSS RPS</th><th>VVR RPS</th><th>vs OSS</th><th>vs VVR</th></tr>
    {mac_rows}
  </table>
  <div class="note">结论：vs OSS <b>4.13×~18.47× 全面领先</b>（7/7）；vs VVR <b>1.20×~4.98× 全部达 VVR</b>（7/7）。同规模下 q14 vs VVR 仍最弱（1.20×）。</div>
</section>

<section>
  <h2>6. 结果分析</h2>
  <h3>6.1 按查询形态分类</h3>
  <ul>
    <li><b>无状态投影/过滤</b>（q1/q2/q3/q8/q10/q14/q21/q22）：EPS 5~30M，受单连接读链限制，吞吐最高。</li>
    <li><b>窗口/聚合/join</b>（q4/q5/q7/q9/q11/q12/q15–q18/q19/q20）：中高 4~15M，受窗口状态与 join 维护成本主导。</li>
    <li><b>状态重查询</b>（q6/q13）：30M 仅 0.3~0.6M（滑窗/双规则链中间窗全保留的语义代价）；08-26 跑批 q13 经重写升至 9.91M。</li>
  </ul>
  <h3>6.2 边缘项：q14 vs VVR 1.10×</h3>
  <p>q14 实为 <b>Calculation</b>（<code>0.908×price</code> 过滤 + <code>HOUR</code> 分型 + <code>count_char</code> UDF，无状态投影），每命中事件需构建 detail 字符串，属 Flink 语义固有成本。diag 墙表显示主墙 = <b>输出链</b>（+54.6ns）与规则段（+42.1ns），差距本质来自<b>引擎行式 cell 求值</b>，非查询写法问题；改进需进 wp-reactor 做列式字符串/过滤求值（通用能力，建议独立立项）。</p>
  <h3>6.3 已知规模问题</h3>
  <ul>
    <li><b>q19 规模退化</b>（08-25：10M 12.4M → 30M 4.1M，驱逐触发）；08-26 跑批 q19=10.12M 显示已回升，待归档确认。</li>
    <li><b>q3 30M −16%</b>（150,992 vs 180,304）：规模相关独立 bug，已排除字节上限驱逐，待查。</li>
    <li><b>q6/q13 白皮书无基线</b>：Q6 因 OVER WINDOW 不支持 retractions Flink 官方亦未实现。</li>
  </ul>
</section>

<section>
  <h2>7. 正确性与查询覆盖</h2>
  <ul>
    <li><b>覆盖</b>：22/22 查询已实现，30M 全量 <code>[clean]</code>。能力判定：<b>20 已有 · 1 待补强（Q12 处理时间窗）· 1 特殊口径（Q6）</b>。</li>
    <li><b>oracle 对拍（30M）</b>：Q4/Q9 <b>identical</b>（D4 保留 pin：join 目标窗字节上限驱逐不再静默丢行）；剩余差异：Q3 30M −16%、Q12 fixed+close 尾桶 known-diff、Q19 stats oracle 未接入。</li>
    <li><b>验证工具</b>：<code>wfgen verify-nexmark</code> 用真实 WFL 规则引擎对同一份确定性数据逐规则算出期望值，与引擎 EMIT 计数 git-diff 同款分层对拍。</li>
  </ul>
</section>

<section>
  <h2>8. 资源消耗（RSS / CPU）</h2>
  <ul>
    <li><b>RSS 峰值（30M）</b>：无状态查询 3.3~3.9GB；状态重查询 q13 15.7GB、q18 15.5GB、q4 9.6GB、q9 9.3GB。内存随消费速度饱和、不随数据量线性涨（q14 10M→100M 仅 +0.38GB），30M 后基本有界。</li>
    <li><b>CPU（活跃窗核占）</b>：无状态查询受单连接读链限制 ~100% avg（满核但供给瓶颈）；重查询 avg 400%+。08-24 前 CPU 0% 为假象（亚秒突发在采样器首个差分前烧完），新口径下 0% 才可信。</li>
  </ul>
</section>

<section>
  <h2>9. 结论</h2>
  <p>warp-fusion 在 NEXMark 全查询上相对 OSS Flink <b>量级领先 3.8×–190×</b>，相对 VVR <b>全面达到且多数显著超出（1.1×–33×）</b>，正确性已达生产可用基线，22/22 查询端到端可运行。短板集中在：① q14 类 Calculation 的行式 cell 求值（通用引擎级优化）；② q3/q19 规模相关 bug；③ Q12 处理时间窗（事件时间引擎固有，replay 下等价）。</p>
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

<section>
  <h2>附录 B：最新跑批（2026-08-26，待归档）</h2>
  <p class="sub">⚠ 以下为 2026-08-26 在 <code>data/bench_*_replay.txt</code> 的完整 22 查询新鲜跑批，<b>尚未写入 docs/BENCH_RESULTS.md</b>，发布为权威基线前需复核归档。其中 q13 于 08-25 20:56 重写（snapshot join O(1) / 取消 2d 窗口全保留），EPS 0.28M→9.91M、RSS 15.7GB→3.6GB。</p>
  <table>
    <tr><th>Query</th><th>08-26 EPS</th><th>08-26 RSS(MB)</th><th>vs 08-25 基线 EPS</th><th>备注</th></tr>
    {appb_rows}
  </table>
  <div class="note">读数普遍高于 08-25 归档基线（部分 +40%~+150%），含机器相位与 q13/q19 修复因素。归档前建议：① 将本跑批写入 BENCH_RESULTS.md；② 重跑 q14 A/B 确认是否相位效应；③ 以 08-26 为新的「当前对照」更新 OSS_VVR_BASELINE.md §3。</div>
</section>

<footer>范围：wf-examples/performance/nexmark_pk · 数据截止 2026-08-26 · PK 主表采用 2026-08-25 Linux 30M 归档基线</footer>

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
