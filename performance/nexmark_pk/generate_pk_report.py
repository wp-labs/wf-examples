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
#    wf_eps / rss_mb 取自 BENCH_RESULTS.md §「2026-08-27 Linux 100M 复跑（晚间，负载干净）」
#    —— 早间轮后段 load 爬升（q18 时 58.2）压低 q15/q16/q17，文档明示「以复跑为准」，
#       故 PK 主表采用复跑（负载干净）数字。
#    vs_oss / vs_vvr 由 wf_eps ÷ 白皮书基线计算（OSS_VVR_BASELINE.md 固定值，与轮次无关）
#    q13 白皮书未发布基线 -> None；q6 已移出 all 套件，不计入主表
# ----------------------------------------------------------------------------
BASE = [
    ("q1",  "无状态投影（货币换算 0.908×price + 过滤）", 16850363, 1753002, 4381353, 9.62, 3.85, 4185),
    ("q2",  "选择（MOD(auction,123)=0 的 bid）",          31078008, 1927154, 6568576, 16.13, 4.73, 4285),
    ("q3",  "按州过滤（IN OR/ID/CA）",                     22183478, 1176664, 4638649, 18.84, 4.79, 5219),
    ("q4",  "分类均价（累积窗口 deferred reduce + stats）", 3738638, 180693, 636468, 20.69, 5.87, 18459),
    ("q5",  "热门新商品（HOP 10s/2s + top_ties(1)）",      1998737, 273496, 279684, 7.31, 7.15, 13546),
    ("q7",  "时段最高出价（match<auction:10s> + top_ties）", 9293974, 79526, 299547, 116.74, 31.03, 4067),
    ("q8",  "新用户 + 其拍卖（TUMBLE deferred join）",      28969548, 1253321, 3340125, 23.12, 8.67, 5264),
    ("q9",  "中标出价（asof deferred reduce join）",        7962629, 43020, 375146, 185.08, 21.22, 7208),
    ("q10", "全量 bid 按时间分区落盘（每 bid 一行）",       17299653, 526357, 1953049, 32.82, 8.85, 4245),
    ("q11", "用户会话统计（session 窗口 + 分片）",          10854288, 244868, 685011, 44.36, 15.84, 4085),
    ("q12", "每 bidder × 10s 处理时间窗计数",              10892485, 822680, 2703360, 13.23, 4.03, 4108),
    ("q13", "有界侧输入 join（snapshot join，重写后 O(1)）", 8202970, None, None, None, None, 4912),
    ("q14", "时间戳换算 + 价格过滤（Calculation）",         8798983, 1451316, 4997002, 6.06, 1.76, 3994),
    ("q15", "日历天出价统计（stats + 1d 桶）",              7385812, 544339, 2340057, 13.57, 3.15, 5561),
    ("q16", "日历天渠道统计（stats + 1d 桶）",              5924391, 108980, 296478, 54.36, 19.96, 7470),
    ("q17", "日历天拍卖统计（stats + 1d 桶）",              8363724, 972318, 3693308, 8.60, 2.26, 18068),
    ("q18", "每 (bidder,auction) 最后一条 bid（stats last）", 3092075, 173928, 1038044, 17.78, 2.98, 28614),
    ("q19", "拍卖 Top-10 价格（stats<> top-N）",           7859815, 170565, 1051293, 46.08, 7.48, 6620),
    ("q20", "展开 bid 关联 auction（snapshot join + where）", 17272687, 74591, 431999, 231.49, 39.98, 8046),
    ("q21", "附加 channel id（热通道映射 + cold url）",     13666841, 786850, 2519336, 17.38, 5.43, 4219),
    ("q22", "URL 目录投影",                                 10328808, 1054519, 3202254, 9.79, 3.23, 4305),
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
    # VVR 相对 OSS 的平均倍数（每查询 vvr_rps/oss_rps）
    "vvr_over_oss_avg": sum(b[4] / b[3] for b in _base_with) / len(_base_with),
    "vvr_over_oss_geo": __import__("math").exp(
        sum(__import__("math").log(b[4] / b[3]) for b in _base_with) / len(_base_with)),
    # wfusion 相对 OSS / VVR 的平均倍数（每查询 vs_oss / vs_vvr）
    "wf_over_oss_avg": sum(b[5] for b in _base_with) / len(_base_with),
    "wf_over_oss_geo": __import__("math").exp(
        sum(__import__("math").log(b[5]) for b in _base_with) / len(_base_with)),
    "wf_over_vvr_avg": sum(b[6] for b in _base_with) / len(_base_with),
    "wf_over_vvr_geo": __import__("math").exp(
        sum(__import__("math").log(b[6]) for b in _base_with) / len(_base_with)),
}

# ----------------------------------------------------------------------------
# 2. 同机规模缩放（2026-08-27 Linux 同机 30M vs 100M，21 查询）
#    (q, eps_30m) —— 用于 §5(MD)/§6(HTML) 规模缩放观测（比值 = 100M / 30M）
# ----------------------------------------------------------------------------
SCALE30 = {
    "q1": 12633621, "q2": 29503249, "q3": 22750096, "q4": 3990030, "q5": 3433159,
    "q7": 9028437, "q8": 29333603, "q9": 9328400, "q10": 17401438, "q11": 10691342,
    "q12": 10833174, "q13": 8121986, "q14": 8847693, "q15": 6647697, "q16": 4454014,
    "q17": 6274551, "q18": 9086465, "q19": 8225134, "q20": 17770339, "q21": 14008303,
    "q22": 10299910,
}

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
    L.append(f"- **⚠ 口径声明**：wfusion 与 VVR 使用**相同型号云服务器**，残余不对等仅剩计算资源计量口径（8 核 vs 8CU，OSS 3×12vCPU）；数据规模已对齐 100M，结论作**量级参照**。详见 §3。\n")

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
    L.append("\n## 2. Query Case 覆盖场景与应用范围\n")
    L.append("NEXMark 的 **Q1–Q22** 是流处理社区定义的 22 条标准查询，覆盖从最简单无状态投影到复杂状态聚合、"
             "多流 join、会话窗口、Top-N 的完整原语谱系。wfusion 以 WFL 的 **`match`（序列/状态机检测）** 与 "
             "**`stats`（列式统计聚合）** 两类一等原语表达全部查询。下面按「场景类别」归类，并映射到真实 "
             "**实时数仓 / ETL、SIEM 安全检测、IoT 监控、风控** 四类应用范围。\n")
    L.append("\n### 2.1 场景类别与覆盖 Query\n")
    L.append("| 场景类别 | 覆盖 Query | 流处理形态 | 典型应用范围 |\n|---|---|---|---|\n")
    L.append("| 无状态投影 / 标量计算 | q1, q2, q3, q14, q22 | 字段变换、货币换算、条件选择、URL 解析 | 事件字段归一化与富化、黑白名单命中、日志标准化（ECS/CEF） |\n")
    L.append("| 全量落盘 / 热冷分流 | q10, q21 | 每条事件一行输出、热通道映射 + 冷 URL | 原始事件归档、取证留痕、热冷数据分层 |\n")
    L.append("| 时间窗口统计（stats） | q4, q15, q16, q17, q18, q19 | 累积/日历天/会话窗口 + count/min/max/avg/sum/top-N/last | 指标基线、行为统计、Top-N 异常排行、按天聚合看板 |\n")
    L.append("| 滑窗 / 跳窗 / 会话窗 | q5, q11 | HOP 跳窗、session 会话窗 | 滑动时间窗热门/突增检测、用户会话行为分析 |\n")
    L.append("| 序列检测 / 状态机（match） | q7 | match<key:dur> + top_ties | 攻击链序列匹配（暴力破解→提权→外联）、多步异常 |\n")
    L.append("| 多流 Join | q8, q9, q13, q20 | TUMBLE/asof/snapshot deferred join、展开关联 | 上下文富化（IP→资产、用户→部门）、关联告警归并、维表/快照关联 |\n")
    L.append("| 处理时间窗 | q12 | 每 bidder × 10s 处理时间窗计数 | 基于处理时间的限流/频控/去重 |\n")
    L.append("\n### 2.2 应用范围映射\n")
    L.append("- **实时数仓 / ETL**：q1/q2/q3/q10/q14/q21/q22 的投影、过滤、富化、落盘即典型流 ETL。\n")
    L.append("- **SIEM / 安全检测**：q7（序列匹配）、q4/q15–q19（行为统计与 Top-N）、q8/q9/q13/q20（上下文关联）对应检测规则、告警归并与实体行为分析。\n")
    L.append("- **IoT / 监控**：q5/q11（滑窗/会话突增）、q12（处理时间频控）对应指标异常与限流。\n")
    L.append("- **风控**：q18（每实体最后状态）、q19（Top-N）对应实时名单与排行风控。\n")
    L.append("\n> 各 Query 的具体流处理形态见 §4 性能 PK 主表「语义」列；q6 因架构性慢移出 `all` 套件（单跑保留），不计入主表。\n")

    L.append("\n## 3. 三方配置与度量口径（公平性核心）\n")
    L.append("| 维度 | warp-fusion | OSS Flink | VVR（实时计算 Flink） |\n|---|---|---|---|\n")
    L.append("| 引擎版本 | warp-fusion（wp-reactor）v2.0.7 | 1.20.4 | vvr-11.5-jdk11-flink-1.20 |\n")
    L.append("| 数据规模 | **100,000,000**（PK 主表） | 100,000,000 | 100,000,000 |\n")
    L.append("| 计算资源 | Linux 8 核（与 VVR 同型号云服务器） | 3×ecs.g6a.xlarge = 12 vCPU / 48GiB | 8 CU ≈ 8 vCPU / 32 GiB（托管分布式集群，总资源=8C/32G，与 wfusion 同型号云服务器） |\n")
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
             "PK 边缘项 **q14**（vs VVR 最弱，Calculation 类、字符串处理重）与 **q18**（stats last，RSS 内存问题）为相对 VVR 的提升上限；均非 `match` 序列检测路径——q14 为 on-each 投影、q18 为 `stats` 聚合。\n")
    L.append(f"6. **VVR 相对 OSS 的平均倍数**：在 {c['n_base']} 个有基线查询上，VVR RPS ÷ OSS RPS 的"
             f"**几何平均 {c['vvr_over_oss_geo']:.2f}×、算术平均 {c['vvr_over_oss_avg']:.2f}×**，"
             "与白皮书公布的整体 **3.24×** 同量级（即 VVR 自身体现对开源 Flink 约 3~4× 的企业级优化）；"
             "wfusion 在此基础上进一步领先（见 §4）。\n")

    # 主表
    L.append("\n## 4. 性能 PK 主表（100M Linux · 2026-08-27 权威跑批 · v2.0.7）\n")
    L.append("> EPS/RPS 单位：条/秒。倍数 = wfusion EPS ÷ 对应基线 RPS。q13 白皮书未发布基线（重写后 O(1) snapshot join，已实测 8.17M），标 N/A。\n")
    L.append("| Query | 语义 | wfusion EPS | OSS RPS | VVR RPS | vs OSS | vs VVR | RSS(MB) |\n|---|---|---:|---:|---:|---:|---:|---:|\n")
    for q, sem, wf, oss, vvr, vo, vv, rss in BASE:
        L.append(f"| {q} | {sem} | {fmt_eps(wf)} | {fmt_eps(oss)} | {fmt_eps(vvr)} | {fxf(vo)} | {fxf(vv)} | {rss:,} |\n")
    L.append(f"\n**结论**：vs OSS **{c['oss_min']:.2f}×~{c['oss_max']:.2f}× 全面领先**（{c['n_base']}/{c['n_base']}）；"
             f"vs VVR **{c['vvr_min']:.2f}×~{c['vvr_max']:.2f}×，{c['n_base']}/{c['n_base']} 全部达 VVR**。\n")
    L.append(f"**平均倍数**：相对 OSS **几何 {c['wf_over_oss_geo']:.1f}× / 算术 {c['wf_over_oss_avg']:.1f}×**；"
             f"相对 VVR **几何 {c['wf_over_vvr_geo']:.1f}× / 算术 {c['wf_over_vvr_avg']:.1f}×**"
             f"（算术均值受 q20 等极端倍数抬升，几何均值更稳健）。\n")

    # §4.1 算术平均 vs 几何平均 解释
    L.append(f"\n### 4.1 关于平均倍数：算术平均 vs 几何平均\n\n")
    L.append(f"报告对每个「相对倍数」同时给出**算术平均**与**几何平均**两个数（如相对 VVR：算术 {c['wf_over_vvr_avg']:.1f}× / 几何 {c['wf_over_vvr_geo']:.1f}×）。"
             f"两者**来源完全相同**——都是 {c['n_base']} 个有基线查询的逐查询倍数——只是求平均的方式不同：\n\n")
    L.append(f"- **算术平均**（{c['wf_over_vvr_avg']:.1f}×）= 把 {c['n_base']} 个倍数直接相加 ÷ {c['n_base']}，等权、「加法式」平均。\n")
    L.append(f"- **几何平均**（{c['wf_over_vvr_geo']:.1f}×）= {c['n_base']} 个倍数相乘再开 {c['n_base']} 次方，等价于「先取对数求算术平均、再取指数」，「乘法式」平均。\n\n")
    L.append(f"对任意正数都有 **算术平均 ≥ 几何平均**（AM–GM 不等式），故 {c['wf_over_vvr_avg']:.1f} > {c['wf_over_vvr_geo']:.1f} 是数学必然，关键在差多少。\n\n")
    L.append(f"**为何差约 {c['wf_over_vvr_avg']/c['wf_over_vvr_geo']:.1f}×**：倍数分布**严重右偏**——约一半查询只有 1.76×~7.15× 的典型倍数（落在几何均值 {c['wf_over_vvr_geo']:.1f}× 下方），"
             f"但 5 个「明星查询」倍数极高：**q7 31×、q9 21.2×、q11 15.8×、q16 20×、q20 40×**，形成长尾。算术平均给每个查询等权重，这几个离群值把均值从典型区硬拽到 {c['wf_over_vvr_avg']:.1f}×；"
             f"几何平均对极端值天然压缩，只反映**典型倍数**，落在 {c['wf_over_vvr_geo']:.1f}×。\n\n")
    L.append(f"**为何报告以几何均值为头条**：对「提速倍数」这种**比率量**，几何均值更诚实——① 乘性对称（wfusion/VVR 几何 {c['wf_over_vvr_geo']:.1f}× ⇄ VVR/wfusion 几何 1/{c['wf_over_vvr_geo']:.1f}×≈{1/c['wf_over_vvr_geo']:.3f}×，自洽；算术做不到）；"
             f"② 倍数本就是乘法（「平均快 N 倍」意味着连乘而非连加）；③ 抗离群，不被少数明星查询绑架。\n\n")
    L.append(f"对照另两组也能印证这一规律：\n")
    L.append(f"- **VVR/OSS**：几何 {c['vvr_over_oss_geo']:.2f}× / 算术 {c['vvr_over_oss_avg']:.2f}×——差距很小，说明分布对称、无极端离群（与白皮书 3.24× 同量级）；\n")
    L.append(f"- **wfusion/OSS**：几何 {c['wf_over_oss_geo']:.1f}× / 算术 {c['wf_over_oss_avg']:.1f}×——差距巨大（{c['wf_over_oss_avg']/c['wf_over_oss_geo']:.1f}×），说明极度右偏（q20 达 231×），几何均值才不会被离群值带飞。\n\n")
    L.append(f"**怎么读这两个数**：几何均值 = 「随便挑一个查询，wfusion 大概比对手快多少倍」，**作结论头条**；"
             f"算术均值 = 「把 {c['n_base']} 个原始倍数无脑平摊」的结果，仅作**上限语境**（说明长尾明星查询把均值抬高了约 {c['wf_over_vvr_avg']/c['wf_over_vvr_geo']:.1f}×）。\n")
    L.append(f"边缘项：**{c['vvr_weak_q']} vs VVR {c['vvr_min']:.2f}×**（Calculation 类、字符串处理重，无状态投影，RSS 仅 3.9GB，为 vs VVR 最弱项）；"
             "q17 2.26×、q22 3.23×。**q13 白皮书无基线**。⚠ **q18 RSS 27.9GB** 为已知内存问题（100M 状态窗线性增长），建议跟进。\n")

    # 规模缩放
    L.append("\n## 5. 规模缩放观测（同机 30M vs 100M）\n")
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
             "基本不随规模退化（部分 100M 略快属机器相位）；**状态型退化收敛为 q5 0.58×~0.67×、q18 0.34×**（q17 复跑 100M 反而更快，1.33×）"
             "——均由窗口状态随数据量增长驱动（RSS 翻倍）。q18 0.32× 伴随 30GB RSS，为已知内存问题。\n")

    # 分析
    L.append("\n## 6. 结果分析\n")
    L.append("### 6.1 按查询形态分类\n")
    L.append("- **无状态投影/过滤（q1/q2/q3/q8/q10/q14/q21/q22）**：EPS 5~30M，受单连接读链限制，吞吐最高。\n")
    L.append("- **窗口/聚合/join（q4/q5/q7/q9/q11/q12/q15–q18/q19/q20）**：中高 4~15M，受窗口状态与 join 维护成本主导。\n")
    L.append("- **状态重查询（q13/q18）**：q13 重写后 8.17M（snapshot join O(1)，取消 2d 窗口全保留）；q18 仅 3.09M 且 RSS 27.9GB（已知内存问题）。\n")
    L.append("### 6.2 边缘项：vs VVR 最弱是 q14（1.76×），非 q17\n")
    L.append("新版 100M 数据（晚间复跑、负载干净）下，**相对 VVR 最弱项是 q14（1.76×）**——Calculation 类（`0.908×price` 过滤 + `HOUR` 分型 + `count_char` UDF，on-each 无状态投影），字符串处理重、RSS 仅 3.9GB。"
             "q17（stats 1d 桶）实测 **2.26×**，非最弱项；"
             "每命中事件需构建 detail 字符串，属 Flink 语义固有成本；diag 墙表显示主墙 = **输出链**（+54.6ns）与规则段（+42.1ns），"
             "差距本质来自**引擎行式 cell 求值**，非查询写法问题；改进需进 wp-reactor 做列式字符串/过滤求值（通用能力，建议独立立项）。\n")
    L.append("### 6.3 已知问题与口径说明\n")
    L.append("- **q18 内存（27.9GB）🔴**：100M 状态窗线性增长，监视器曾测 ~60GB；建议内存归因跟进。\n")
    L.append("- **q5/q18 规模退化（0.58×~0.67×/0.34×）**：窗口状态随数据量增长，RSS 翻倍，属预期内状态型退化，非 bug。\n")
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
    L.append(f"- **RSS 峰值（100M）**：无状态查询 4.0~5.6GB；状态重查询 **q18 {fmt_gb(28614)}**、q17 17.6GB、q4 18.0GB、q9 7.0GB。"
             "q18 27.9GB 为已知内存问题（状态窗随数据量线性增长）。其余随消费速度饱和、30M 后基本有界。\n")
    L.append("- **CPU（活跃窗核占）**：无状态查询受单连接读链限制 ~100% avg（满核但单连接供给瓶颈）；"
             "重查询 avg 400%+（多核充分占用）。CPU 0% 旧口径假象已修复（亚秒突发在采样器首个差分前烧完），新口径下 0% 才可信。\n")

    # 结论
    L.append("\n## 9. 结论\n")
    L.append(f"warp-fusion 在 NEXMark 全查询上相对 OSS Flink **量级领先 {c['oss_min']:.2f}×–{c['oss_max']:.2f}×**，"
             f"相对 VVR（实时计算 Flink）**全面达到且多数显著超出（{c['vvr_min']:.2f}×–{c['vvr_max']:.2f}×）**，"
             "正确性已达生产可用基线，21/21 套件查询端到端可运行（100M clean）。"
             "短板集中在：① q14/q17 类 Calculation / stats 的行式 cell 求值（通用引擎级优化）；"
             "② q18 状态窗内存（100M 27.9GB，已知问题）；③ Q12 处理时间窗（事件时间引擎固有，replay 下等价）。\n")

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
  <div class="note">⚠ <b>口径声明</b>：wfusion 与 VVR 使用<b>相同型号云服务器</b>，残余不对等仅剩计算资源计量口径（8 核 vs 8CU，OSS 3×12vCPU）；数据规模已对齐（100M × 100M），结论作<b>量级参照</b>，非逐位比较。详见 §3。</div>
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
  <h2>2. Query Case 覆盖场景与应用范围</h2>
  <p class="sub">NEXMark 的 <b>Q1–Q22</b> 是流处理社区定义的 22 条标准查询，覆盖从最简单无状态投影到复杂状态聚合、多流 join、会话窗口、Top-N 的完整原语谱系。wfusion 以 WFL 的 <b><code>match</code>（序列/状态机检测）</b> 与 <b><code>stats</code>（列式统计聚合）</b> 两类一等原语表达全部查询。下面按「场景类别」归类，并映射到真实 <b>实时数仓 / ETL、SIEM 安全检测、IoT 监控、风控</b> 四类应用范围。</p>
  <h3>2.1 场景类别与覆盖 Query</h3>
  <div class="tbl-scroll"><table>
    <tr><th>场景类别</th><th>覆盖 Query</th><th>流处理形态</th><th>典型应用范围</th></tr>
    <tr><td>无状态投影 / 标量计算</td><td>q1, q2, q3, q14, q22</td><td>字段变换、货币换算、条件选择、URL 解析</td><td>事件字段归一化与富化、黑白名单命中、日志标准化（ECS/CEF）</td></tr>
    <tr><td>全量落盘 / 热冷分流</td><td>q10, q21</td><td>每条事件一行输出、热通道映射 + 冷 URL</td><td>原始事件归档、取证留痕、热冷数据分层</td></tr>
    <tr><td>时间窗口统计（stats）</td><td>q4, q15, q16, q17, q18, q19</td><td>累积/日历天/会话窗口 + count/min/max/avg/sum/top-N/last</td><td>指标基线、行为统计、Top-N 异常排行、按天聚合看板</td></tr>
    <tr><td>滑窗 / 跳窗 / 会话窗</td><td>q5, q11</td><td>HOP 跳窗、session 会话窗</td><td>滑动时间窗热门/突增检测、用户会话行为分析</td></tr>
    <tr><td>序列检测 / 状态机（match）</td><td>q7</td><td>match&lt;key:dur&gt; + top_ties</td><td>攻击链序列匹配（暴力破解→提权→外联）、多步异常</td></tr>
    <tr><td>多流 Join</td><td>q8, q9, q13, q20</td><td>TUMBLE/asof/snapshot deferred join、展开关联</td><td>上下文富化（IP→资产、用户→部门）、关联告警归并、维表/快照关联</td></tr>
    <tr><td>处理时间窗</td><td>q12</td><td>每 bidder × 10s 处理时间窗计数</td><td>基于处理时间的限流/频控/去重</td></tr>
  </table></div>
  <h3>2.2 应用范围映射</h3>
  <ul>
    <li><b>实时数仓 / ETL</b>：q1/q2/q3/q10/q14/q21/q22 的投影、过滤、富化、落盘即典型流 ETL。</li>
    <li><b>SIEM / 安全检测</b>：q7（序列匹配）、q4/q15–q19（行为统计与 Top-N）、q8/q9/q13/q20（上下文关联）对应检测规则、告警归并与实体行为分析。</li>
    <li><b>IoT / 监控</b>：q5/q11（滑窗/会话突增）、q12（处理时间频控）对应指标异常与限流。</li>
    <li><b>风控</b>：q18（每实体最后状态）、q19（Top-N）对应实时名单与排行风控。</li>
  </ul>
  <div class="note">各 Query 的具体流处理形态见 §4 性能 PK 主表「语义」列；q6 因架构性慢移出 <code>all</code> 套件（单跑保留），不计入主表。</div>
</section>

<section>
  <h2>3. 三方配置与度量口径（公平性核心）</h2>
  <div class="tbl-scroll"><table>
    <tr><th>维度</th><th>warp-fusion</th><th>OSS Flink</th><th>VVR（实时计算 Flink）</th></tr>
    <tr><td>引擎版本</td><td>warp-fusion（wp-reactor）v2.0.7</td><td>1.20.4</td><td>vvr-11.5-jdk11-flink-1.20</td></tr>
    <tr><td>数据规模</td><td>100,000,000（PK 主表）</td><td>100,000,000</td><td>100,000,000</td></tr>
    <tr><td>计算资源</td><td>Linux 8 核（与 VVR 同型号云服务器）</td><td>3×ecs.g6a.xlarge = 12 vCPU/48GiB</td><td>8 CU ≈ 8 vCPU / 32 GiB（托管分布式集群，总资源=8C/32G，与 wfusion 同型号云服务器）</td></tr>
    <tr><td>sink</td><td>本地文件（blackhole 等价）</td><td>Blackhole</td><td>Blackhole</td></tr>
    <tr><td>指标</td><td>EPS = Σn/(max emit−min start)（哨兵）</td><td>RPS = 输入量/用时</td><td>RPS = 输入量/用时</td></tr>
    <tr><td>来源</td><td>本仓库 <code>bench.sh</code></td><td>阿里白皮书</td><td>阿里白皮书</td></tr>
  </table></div>
  <div class="note ok">本版 PK 主表与白皮书<b>同规模（100M × 100M）</b>；wfusion 与 VVR 使用<b>相同型号云服务器</b>，残余不对等仅剩<b>计算资源计量口径</b>（8 核 vs 8CU，OSS 为 3×12vCPU），结论作量级参照。wfusion 哨兵 EPS 与白皮书 RPS 思路同源（消化记录数 ÷ 耗时）。</div>
  <div class="note">VVR 相对 OSS 的平均倍数：在 {c['n_base']} 个有基线查询上，VVR RPS ÷ OSS RPS 的<b>几何平均 {c['vvr_over_oss_geo']:.2f}×、算术平均 {c['vvr_over_oss_avg']:.2f}×</b>，与白皮书公布的整体 <b>3.24×</b> 同量级（VVR 自身体现对开源 Flink 约 3~4× 的企业级优化）；wfusion 在此基础上进一步领先（见 §4）。</div>
</section>

<section>
  <h2>4. 性能 PK 主表（100M Linux · 2026-08-27 权威跑批 · v2.0.7）</h2>
  <p class="sub">EPS/RPS 单位：条/秒。倍数 = wfusion EPS ÷ 对应基线 RPS。q13 白皮书未发布基线（重写后 O(1) snapshot join，已实测 8.17M），标 N/A。</p>
  <div class="tbl-scroll"><table>
    <tr><th>Query</th><th>语义</th><th>wfusion EPS</th><th>OSS RPS</th><th>VVR RPS</th><th>vs OSS</th><th>vs VVR</th><th>RSS(MB)</th></tr>
    {main_rows}
  </table></div>
  <div class="note">结论：vs OSS <b>{c['oss_min']:.2f}×~{c['oss_max']:.2f}× 全面领先</b>（{c['n_base']}/{c['n_base']}）；vs VVR <b>{c['vvr_min']:.2f}×~{c['vvr_max']:.2f}×，{c['n_base']}/{c['n_base']} 全部达 VVR</b>。
  边缘项：<b>{c['vvr_weak_q']} vs VVR {c['vvr_min']:.2f}×</b>（Calculation 类、字符串处理重、RSS 仅 3.9GB，为 vs VVR 最弱项）；q17 2.26×、q22 3.23×。q13 白皮书无基线。⚠ <b>q18 RSS 27.9GB</b> 为已知内存问题。
  <br><b>平均倍数</b>：相对 OSS <b>几何 {c['wf_over_oss_geo']:.1f}× / 算术 {c['wf_over_oss_avg']:.1f}×</b>；相对 VVR <b>几何 {c['wf_over_vvr_geo']:.1f}× / 算术 {c['wf_over_vvr_avg']:.1f}×</b>（算术均值受 q20 等极端倍数抬升，几何均值更稳健）。</div>
  <div class="note"><b>关于平均倍数：算术平均 vs 几何平均</b><br>
  报告对每个相对倍数同时给出算术平均与几何平均（如相对 VVR：算术 {c['wf_over_vvr_avg']:.1f}× / 几何 {c['wf_over_vvr_geo']:.1f}×），来源完全相同（{c['n_base']} 个有基线查询的逐查询倍数），只求平均方式不同：
  <ul style="margin:6px 0 2px 18px;line-height:1.5">
    <li><b>算术平均</b>（{c['wf_over_vvr_avg']:.1f}×）：{c['n_base']} 个倍数直接相加 ÷ {c['n_base']}，等权加法式。</li>
    <li><b>几何平均</b>（{c['wf_over_vvr_geo']:.1f}×）：相乘再开 {c['n_base']} 次方，等价于先取对数求平均再取指数，乘法式。</li>
  </ul>
  对任意正数算术 ≥ 几何（AM–GM）。两者差约 {c['wf_over_vvr_avg']/c['wf_over_vvr_geo']:.1f}×，因倍数<b>严重右偏</b>：约一半查询仅 1.76×~7.15×（几何均值下方），但 <b>q7 31×、q9 21.2×、q11 15.8×、q16 20×、q20 40×</b> 五个明星查询形成长尾，把算术均值拽高。<b>报告以几何均值为头条</b>：比率量本就乘性、几何具乘性对称且抗离群。对照：VVR/OSS 几何 {c['vvr_over_oss_geo']:.2f}×/算术 {c['vvr_over_oss_avg']:.2f}×（差距小、分布对称）；wfusion/OSS 几何 {c['wf_over_oss_geo']:.1f}×/算术 {c['wf_over_oss_avg']:.1f}×（差距 {c['wf_over_oss_avg']/c['wf_over_oss_geo']:.1f}×，极度右偏）。<b>几何均值 = 典型领先幅度（头条）；算术均值 = 上限语境。</b></div>
  <div class="note">WFL 原语分工：wfusion 规则以 <code>match</code>（序列/状态机检测）与 <code>stats</code>（列式统计聚合）两类一等原语表达；PK 边缘项 <b>q14 / q17（vs VVR 最弱）与 q18（RSS 内存问题）均落在 <code>stats</code> 路径</b>，非 <code>match</code>。</div>
</section>

<section>
  <h2>5. 可视化：领先倍数</h2>
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
  <h2>6. 规模缩放观测（同机 30M vs 100M）</h2>
  <p class="sub">同一台 Linux 8 核机、同一 v2.0.7，30M 与 100M 背靠背跑批。比值 = 100M EPS ÷ 30M EPS；琥珀底行标记规模退化（&lt;0.8）。</p>
  <div class="tbl-scroll"><table>
    <tr><th>Query</th><th>30M EPS</th><th>100M EPS</th><th>100M/30M</th><th>类型</th></tr>
    {scale_rows}
  </table></div>
  <div class="note">无状态/轻量查询规模因子 0.94~1.24×，基本不随规模退化；<b>状态型退化收敛为 q5 0.58×~0.67×、q18 0.34×</b>（q17 复跑 100M 反而更快，1.33×）。q18 0.34× 伴随 27.9GB RSS，为已知内存问题。</div>
</section>

<section>
  <h2>7. 结果分析</h2>
  <h3>7.1 按查询形态分类</h3>
  <ul>
    <li><b>无状态投影/过滤</b>（q1/q2/q3/q8/q10/q14/q21/q22）：EPS 5~30M，受单连接读链限制，吞吐最高。</li>
    <li><b>窗口/聚合/join</b>（q4/q5/q7/q9/q11/q12/q15–q18/q19/q20）：中高 4~15M，受窗口状态与 join 维护成本主导。</li>
    <li><b>状态重查询</b>（q13/q18）：q13 重写后 8.17M（snapshot join O(1)）；q18 仅 3.09M 且 RSS 27.9GB（已知内存问题）。</li>
  </ul>
  <h3>7.2 边缘项：vs VVR 最弱是 q14（1.76×），非 q17</h3>
  <p>新版 100M 数据（晚间复跑、负载干净）下，<b>相对 VVR 最弱项是 q14（1.76×）</b>——Calculation 类（<code>0.908×price</code> 过滤 + <code>HOUR</code> 分型 + <code>count_char</code> UDF，on-each 无状态投影），字符串处理重、RSS 仅 3.9GB。q17（stats 1d 桶）实测 <b>2.26×</b>，非最弱项。每命中事件需构建 detail 字符串，属 Flink 语义固有成本。diag 墙表显示主墙 = <b>输出链</b>（+54.6ns）与规则段（+42.1ns），差距本质来自<b>引擎行式 cell 求值</b>，非查询写法问题；改进需进 wp-reactor 做列式字符串/过滤求值（通用能力，建议独立立项）。</p>
  <h3>7.3 已知问题与口径说明</h3>
  <ul>
    <li><b>q18 内存（27.9GB）🔴</b>：100M 状态窗线性增长，监视器曾测 ~60GB；建议内存归因跟进。</li>
    <li><b>q5/q18 规模退化（0.58×~0.67×/0.34×）</b>：窗口状态随数据量增长，RSS 翻倍，属预期内状态型退化，非 bug。</li>
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
    <li><b>RSS 峰值（100M）</b>：无状态查询 4.0~5.6GB；状态重查询 <b>q18 {fmt_gb(28614)}</b>、q17 17.6GB、q4 18.0GB、q9 7.0GB。q18 27.9GB 为已知内存问题（状态窗随数据量线性增长）。其余随消费速度饱和、30M 后基本有界。</li>
    <li><b>CPU（活跃窗核占）</b>：无状态查询受单连接读链限制 ~100% avg（满核但供给瓶颈）；重查询 avg 400%+。CPU 0% 旧口径假象已修复，新口径下 0% 才可信。</li>
  </ul>
</section>

<section>
  <h2>10. 结论</h2>
  <p>warp-fusion 在 NEXMark 全查询上相对 OSS Flink <b>量级领先 {c['oss_min']:.2f}×–{c['oss_max']:.2f}×</b>，相对 VVR <b>全面达到且多数显著超出（{c['vvr_min']:.2f}×–{c['vvr_max']:.2f}×）</b>，正确性已达生产可用基线，21/21 套件查询端到端可运行（100M clean）。短板集中在：① q14/q17 类 Calculation / stats 的行式 cell 求值（通用引擎级优化）；② q18 状态窗内存（100M 27.9GB，已知问题）；③ Q12 处理时间窗（事件时间引擎固有，replay 下等价）。</p>
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
