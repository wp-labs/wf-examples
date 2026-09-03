#!/usr/bin/env python3
"""nexmark_pk / qradar_pk diag.sh 的性能墙分析器。

输入全部走环境变量（参数多，位置参数易错）；输出打印到 stdout（墙表/墙判定/健康
校验），退出码 0=健康 / 1=存在硬失败（append 未追平、dropped_late 非零等）。

环境变量:
  QUERY        查询名（报告标题用）
  RULES_COUNT  规则数（报告标题用）
  N            本档数据量（整数）
  CTX          口径上下文行（并行度/限速/负载/时间戳）
  STAGE_NAMES  CSV：档名列表（含可选的 warmup 前缀档）
  APPEND_CUT_STAGES CSV：cut_append/cut_recv 档名（decode/recv 前序档——普通流
                 不 append, appended 期望 = (档数 − 前序档数) × N）
  CORES        机器核数（墙判定 CPU 占核比用）
  SENT_PATH    data/perf_sentinel.ndjson（EPS 单一事实源）
  SAMPLES_PATH 采样文件 "epoch_ns rss_mb cpu_pct"
  METRICS_PATH data/metrics.ndjson（健康计数器）
  LOG_PATH     daemon 日志（schema mismatch 计数用）
  STREAMS      CSV：输入流名（append_total 求和口径）
  FAM_COUNTS   "fam:规则数 ..."（家族档模式用，空=叠加式墙梯）
"""
import json
import os
import re
import sys

QUERY = os.environ.get("QUERY", "")
RULES_COUNT = os.environ.get("RULES_COUNT", "")
N_REQ = int(os.environ["N"])
CTX = os.environ.get("CTX", "")
STAGE_NAMES = [s for s in re.split(r"[,\s]+", os.environ.get("STAGE_NAMES", "")) if s]
APPEND_CUT_STAGES = [
    s for s in re.split(r"[,\s]+", os.environ.get("APPEND_CUT_STAGES", "")) if s
]
# COLD=1：每档独立 daemon（冷状态）。叠加式增量对它有状态规则不成立——
# 同数据重发时状态机功随档递减（热 emit 390k vs 冷 emit 124k, 2026-09-01 实验），
# 冷模式增量 = 阶段间成本差，负增量是正常现象而非测量偏差。
COLD_MODE = os.environ.get("COLD_MODE", "0") == "1"
CORES = int(os.environ.get("CORES", 0) or 0)
SENT_PATH = os.environ.get("SENT_PATH", "data/perf_sentinel.ndjson")
SAMPLES_PATH = os.environ.get("SAMPLES_PATH", "")
METRICS_PATH = os.environ.get("METRICS_PATH", "data/metrics.ndjson")
LOG_PATH = os.environ.get("LOG_PATH", "")
INPUTS = set(s for s in re.split(r"[,\s]+", os.environ.get("STREAMS", "")) if s)
FAM_RULES = dict(
    x.split(":") for x in os.environ.get("FAM_COUNTS", "").split() if ":" in x
)

FAIL = 0


def num(v):
    if isinstance(v, (int, float)):
        return int(v)
    try:
        return int(str(v))
    except Exception:
        return None


# ---- 哨兵记录：round → 档（EPS 单一事实源）----
sent = {}
try:
    for line in open(SENT_PATH):
        try:
            o = json.loads(line)
        except Exception:
            continue
        if o.get("record_type") != "sentinel":
            continue
        r, n, s, e = num(o.get("round")), num(o.get("n")), num(o.get("start_ns")), num(o.get("emit_ns"))
        if None in (r, n, s, e) or e <= s:
            continue
        sent.setdefault(r, (n, s, e))
except FileNotFoundError:
    pass

# ---- 采样：epoch_ns rss_mb cpu_pct ----
samples = []
try:
    for line in open(SAMPLES_PATH):
        p = line.split()
        if len(p) == 3:
            try:
                samples.append((int(p[0]), int(p[1]), float(p[2])))
            except ValueError:
                pass
except FileNotFoundError:
    pass

# ---- dirty 采样：epoch_ns dirty_mb（footprint，perf-diag-wall.toml mem_sample）----
# dirty = 物理真持有（RSS 含页表保留波动大）；非 macOS/footprint 缺失 → 空（列 n/a）。
DIRTY_SAMPLES_PATH = os.environ.get("DIRTY_SAMPLES_PATH", "")
dirty_samples = []
if DIRTY_SAMPLES_PATH:
    try:
        for line in open(DIRTY_SAMPLES_PATH):
            p = line.split()
            if len(p) == 2:
                try:
                    dirty_samples.append((int(p[0]), int(p[1])))
                except ValueError:
                    pass
    except FileNotFoundError:
        pass


def dirty_peak(s, e):
    return max((x[1] for x in dirty_samples if s <= x[0] <= e), default=None)


def window_stats(s, e):
    win = [x for x in samples if s <= x[0] <= e]
    if not win:
        return None
    return (max(x[1] for x in win),
            sum(x[2] for x in win) / len(win),
            max(x[2] for x in win), len(win))


def iter_metrics():
    try:
        for line in open(METRICS_PATH, errors="replace"):
            try:
                yield json.loads(line)
            except Exception:
                continue
    except FileNotFoundError:
        return


# ---------------------------------------------------------------------------
# 墙表
# ---------------------------------------------------------------------------
family_mode = any(s.startswith("fam_") for s in STAGE_NAMES)
who = "qradar_pk" if (QUERY or "").startswith("qradar") else "nexmark_pk"
head = ["档", "EPS", "耗时", "ns/事件", "增量ns", "占全链", "CPU%avg/max", "CPU核·s", "RSS_peak", "DIRTY_peak", "样本"]
fmt = "%-9s %13s %9s %11s %10s %8s %14s %8s %11s %11s %6s"
print("")
title = "== %s 性能墙定位 · %s" % (who, QUERY or "?")
title += " · N=%s" % format(N_REQ, ",")
if RULES_COUNT:
    title += " · 规则 %s" % RULES_COUNT
title += " =="
print(title)
if family_mode:
    print("（规则家族档：各档增量一律相对 floor 档计算）")
else:
    # 档名 → 语义，帮助读表（名字本身不传达切了什么/测什么）
    sem = {"recv": "注入+TCP接收(不解码)", "decode": "+解码(不append)",
           "floor": "净管道(注入+解码+窗口)", "rules": "+规则求值",
           "emit": "+输出构建(close列式+builder+通道投递)", "full": "+序列化+sink写"}
    desc = ["%s=%s" % (s, sem[s]) for s in STAGE_NAMES if s in sem]
    if COLD_MODE:
        print("（独立冷档：每档独立 daemon 冷状态；增量 = 阶段间成本差，非叠加式差分）")
    elif desc:
        print("（叠加式墙梯：%s；每档增量 = 相对上一档的成本）" % " → ".join(desc))
    else:
        print("（叠加式墙梯：每档增量 = 相对上一档的成本；占全链 = 该增量 / 末档总成本）")
print((fmt + " %s") % tuple(head + [""]))

rows, prev_ns, floor_ns = [], None, None
for k, name in enumerate(STAGE_NAMES):
    if k not in sent:
        print("%-9s %13s  (无哨兵记录：该档未完成——见 wfgen 输出/daemon 日志)" % (name, "n/a"))
        continue
    n, s, e = sent[k]
    dt = e - s
    eps, ns_per_ev = n * 1e9 / dt, dt / n
    st = window_stats(s, e)
    cpu_s = "%d/%d" % (st[1], st[2]) if st else "n/a"
    # CPU核·s = avg% × 时长（核·秒）——EPS 被帧页缓存/外部负载污染时的负载稳健口径
    # （同查询前后对照看 CPU 工作量而非 EPS；q5 H1 验证：CPU 工作量 -7~9% 而 EPS 在噪声内）。
    work_s = "%.1f" % (st[1] / 100.0 * dt / 1e9) if st else "n/a"
    rss_s = "%s MB" % format(st[0], ",") if st else "n/a"
    dirty = dirty_peak(s, e)
    dirty_s = "%s MB" % format(dirty, ",") if dirty else "n/a"
    cnt_s = str(st[3]) if st else "0"
    if name == "warmup":
        # 预热档：只负责把窗口/规则状态/输出链跑热，跑在「窗口全空 + 冷启动」的特殊状态，
        # 数字与 floor 起不可比（q1 无状态查询它反而慢、q9 join 查询它虚高），一律不显示。
        print("%-9s %13s  (预热档，数字不进结论)" % (name, "—"))
        continue
    if name == "floor":
        floor_ns = ns_per_ev
    # 家族档非叠加：增量一律相对 floor；叠加式墙梯：增量相对上一档
    base = floor_ns if (family_mode and name.startswith("fam_")) else prev_ns
    delta = None if base is None else ns_per_ev - base
    print("%-9s %13s %8.3fs %11.1f %10s %8s %14s %8s %11s %11s %6s" % (
        name, format(int(eps), ","), dt / 1e9, ns_per_ev,
        "—" if delta is None else "+%.1f" % delta if delta >= 0 else "%.1f" % delta,
        "—" if delta is None or ns_per_ev <= 0 else "%.1f%%" % (delta / ns_per_ev * 100),
        cpu_s, work_s, rss_s, dirty_s, cnt_s))
    rows.append({"name": name, "eps": eps, "dt": dt / 1e9, "ns": ns_per_ev,
                 "delta": delta, "cpu": st[1] if st else None,
                 "cpu_work": st[1] / 100.0 * dt / 1e9 if st else None,
                 "rss_peak": st[0] if st else None, "samples": st[3] if st else 0})
    prev_ns = ns_per_ev

# ---------------------------------------------------------------------------
# 墙判定
# ---------------------------------------------------------------------------
if len(rows) < 2:
    print("\n⚠ 有效档不足 2，无法判墙（哨兵记录缺失）")
    FAIL = 1
else:
    total_ns = rows[-1]["ns"] if not family_mode else max(r["ns"] for r in rows)
    print("\n-- 墙判定 --")
    walls = sorted([r for r in rows if r["delta"] is not None], key=lambda r: -r["delta"])
    top = walls[0]
    # 基线占比：读者需要知道「主墙增量」是加在多大的基线上（q9 例：+449ns 加在 1008ns 上）
    base_ns = floor_ns if family_mode else rows[rows.index(top) - 1]["ns"]
    print("主墙 = %s：相对基线（%s %.1f ns）增量 %+.1f ns/事件" % (
        top["name"], ("floor" if family_mode else rows[rows.index(top) - 1]["name"]), base_ns, top["delta"]))
    print("   └ 占全链 %.1f%%（全链 = 末档 %.1f ns/事件）——墙前基线本身占 %.1f%%" % (
        top["delta"] / total_ns * 100, total_ns, base_ns / total_ns * 100))
    # CPU 归因 + 深化建议
    if top["cpu"] is not None and CORES:
        ratio = top["cpu"] / (CORES * 100.0)
        if ratio > 0.5:
            kind = "忙墙：该段吃满多核（计算密集）"
            next_step = "→ 下一步：对 daemon 做 CPU 采样，定位段内热点函数"
        elif ratio < 0.15:
            kind = "等/供给墙：CPU 几乎空闲，墙不在计算而在等待"
            next_step = "→ 下一步：查等待什么——见下方「等墙细分」"
        else:
            kind = "混合墙：部分并行未打满（CPU %.0f%%）" % (ratio * 100)
            next_step = "→ 下一步：确认并行度作用域（source 实例/规则 worker），再做 CPU 采样"
        print("   └ CPU %d%% 平均（%d 核 = %.0f%% 占用）→ %s；%s" % (top["cpu"], CORES, ratio * 100, kind, next_step))
        # CPU 工作量（核·s）= 负载稳健口径：EPS 受帧页缓存/外部负载/网络污染，前后对照
        # 看工作量而非 EPS（q5 H1 验证：工作量 -7~9% 而 EPS 在单次噪声内）。
        if top.get("cpu_work") is not None:
            per_ev = top["cpu_work"] / (N_REQ or 1)
            print("   └ 主墙 CPU 工作量 %.1f 核·s（每事件 %.2f µs·核）——同查询前后对照以本列为准（负载稳健）" % (
                top["cpu_work"], per_ev * 1e6))
    else:
        print("   └ CPU 归属 n/a（该档过短或采样不足）→ 增大 N 或调小 SAMPLE_MS")
    # 等墙细分：低 CPU 时区分「数据在窗口堆积（窗口/join 容量）」vs「供给侧不足」
    if top["cpu"] is not None and CORES and top["cpu"] / (CORES * 100.0) < 0.15:
        rss_first = rows[0].get("rss_peak")
        rss_last = rows[-1].get("rss_peak")
        rss_growth = (rss_last / rss_first) if (rss_first and rss_last and rss_first > 0) else 1.0
        if rss_first and rss_last and rss_growth >= 1.5:
            print("   └ 等墙细分：RSS 从 %.0fMB 涨到 %.0fMB（%.1f×，显著上涌）→ 等的是「数据在窗口堆积」，"
                  "墙在窗口容量或 join 目标维护" % (rss_first, rss_last, rss_growth))
            print("     → 下一步：查窗口 max_window_bytes 与 join 目标窗口（右窗）是否持全量；"
                  "多流 join 查询常见于窗口 append/join 维护的串行路径，可用 RULE_PARALLELISM 交叉验证")
        elif rss_first and rss_last and rss_growth >= 1.2:
            print("   └ 等墙细分：RSS %.0fMB → %.0fMB（%.2f×，温和增长）= 数据逐档累积中，"
                  "但未到显著堆积" % (rss_first, rss_last, rss_growth))
            print("     → 下一步：优先查规则段内是「等待窗口/join」还是「同步串行」（CPU 采样看栈）；"
                  "供给侧（注入/预算）只在 RSS 平稳时才是首选嫌疑")
        else:
            print("   └ 等墙细分：RSS 平稳（%.0fMB → %.0fMB）→ 等的是供给侧（注入/TCP/解码）"
                  % (rss_first or 0, rss_last or 0))
            print("     → 下一步：查连接数、读盘速度；profiler 采到的是等待栈不是热点")
    # 各段贡献排行（按增量绝对值，噪声段折叠）
    print("各段贡献（增量绝对值排序）：")
    for r in walls:
        pct = r["delta"] / total_ns * 100 if total_ns else 0
        if abs(pct) < 5:
            print("   %-8s %+.1f ns/事件（%.1f%%）—— 噪声内/近无成本" % (r["name"], r["delta"], pct))
        else:
            print("   %-8s %+.1f ns/事件（%.1f%%）" % (r["name"], r["delta"], pct))
    if family_mode:
        print("\n家族每规则成本（增量 ÷ 该族规则数，125 条与 3 条不能直接比总量）：")
        for r in walls:
            fam = r["name"][4:]
            cnt = int(FAM_RULES.get(fam, 0))
            per = "%.2f ns/规则" % (r["delta"] / cnt) if cnt else "n/a"
            print("   %-8s %6s 条  %+9.1f ns/事件 · %s" % (r["name"], cnt or "?", r["delta"], per))
    print("注: RSS 为档内峰值、随墙梯累积（同一份数据被发 %d 次），不是该段内存成本；RSS 逐档上涌 = 窗口堆积证据" % len(sent))
    neg = [r for r in rows if r["delta"] is not None and total_ns and r["delta"] / total_ns < -0.05]
    if neg:
        if COLD_MODE:
            print("ℹ 负增量档: %s —— 冷档为独立跑批，阶段间成本差非叠加式，属正常现象（不判不可信）" % ", ".join(r["name"] for r in neg))
        else:
            print("⚠ 负增量档: %s —— 墙梯系统偏差大于该段真实成本，本次定位不可结论。" % ", ".join(r["name"] for r in neg))
            if "warmup" in STAGE_NAMES:
                print("  处理: 预热档已开依旧出现 → 增大 N；并同时段交错重跑取相位配对（后台负载波动）。")
            else:
                print("  处理: 开预热档（去掉 WARMUP=0，默认即开）+ 增大 N。")
    for r in rows:
        if r["samples"] < 3:
            print("⚠ %s 档仅 %.3fs / %d 个样本：CPU/RSS 归属不可信，固定开销占比高（增大 N）" % (
                r["name"], r["dt"], r["samples"]))

# ---------------------------------------------------------------------------
# 健康
# ---------------------------------------------------------------------------
emitted, appended, bad = 0, 0, {}
detail = {}
for o in iter_metrics():
    name, label = o.get("name"), o.get("label", "")
    try:
        v = int(float(o.get("value", 0) or 0))
    except (TypeError, ValueError):
        continue
    if name == "emitted_total":
        emitted += v
    elif name == "emitted_detail" and v:
        detail[label] = detail.get(label, 0) + v
    elif name == "append_total" and label in INPUTS:
        appended += v
    elif name in ("append_failed_total", "dropped_late_total", "memory_evicted_total") and v:
        bad[name] = bad.get(name, 0) + v
    elif name == "cursor_gap_total" and v:
        bad["cursor_gap[%s]" % label] = bad.get("cursor_gap[%s]" % label, 0) + v

print("\n-- 健康 --")
# recv/decode 档（cut_recv/cut_append）普通流不 append——期望 = (档数 − 前序
# 档数) × N。哨兵流豁免仍 append, 但普通流是行数主体, 前序档 append 近似为 0。
expect_app = N_REQ * (len(sent) - len(APPEND_CUT_STAGES))
ratio = appended / expect_app if expect_app else 0
if APPEND_CUT_STAGES:
    print("appended: %s / %s（%d 档 − %d 前序档）× N = %.1f%%" % (
        format(appended, ","), format(expect_app, ","), len(sent),
        len(APPEND_CUT_STAGES), ratio * 100))
else:
    print("appended: %s / %s（%d 档 × N）= %.1f%%" % (
        format(appended, ","), format(expect_app, ","), len(sent), ratio * 100))
if expect_app and ratio < 0.9:
    print("✗ append 未追平 → 数据未真正进系统，EPS 不可信。常见根因：")
    mism = 0
    try:
        for line in open(LOG_PATH):
            if "schema mismatch" in line or "append failed" in line:
                mism += 1
    except FileNotFoundError:
        pass
    if mism:
        print("  • 帧文件 schema 与当前 schemas 不匹配（日志 %d 条 schema mismatch）" % mism)
        print("    → 用当前版本重新生成帧：GEN_FRAMES=1 或 ./bench.sh q1 replay <total>")
    print("  • 迟到丢弃（见下方 dropped_late）/ 帧文件行数不足 / 源未路由 / 入流限速未解除")
    FAIL = 1
print("致命计数器: %s" % (" ".join("%s=%d" % kv for kv in sorted(bad.items())) or "clean"))
print("emitted_total: %s" % format(emitted, ","))
if FAM_RULES:
    print("emitted_detail 采样到的规则数: %d（detail 是抽样指标）" % len(detail))
    print("家族档 reload 软校验（emitted_detail 抽样，0 不必然是失败）：")
    for fam, cnt in sorted(FAM_RULES.items()):
        hit = sum(1 for k in detail if k.split("_")[0] == fam)
        flag = "" if hit else "  ⚠ 未采到该族发射"
        print("  家族 %-6s 规则 %3s 条 · 采到发射规则 %3d 条%s" % (fam, cnt, hit, flag))
elif detail:
    print("emitted_detail 采样到的规则数: %d（detail 是抽样指标，不等于触发规则数）" % len(detail))
if "dropped_late_total" in bad:
    print("✗ dropped_late 非零 = 墙梯口径污染：重发的数据被判迟到丢弃 → 后续档实际无数据。")
    print("  处理: 减小 total（跨度 ≤ allowed_lateness），或保持 LATENESS_FIX=1 让脚本放宽迟到窗口。")
    FAIL = 1
if "memory_evicted_total" in bad:
    print("⚠ memory_evicted 非零：诊断模式默认把全局窗口内存 cap 放到物理内存 60%")
    print("  （引擎侧，WF_DIAG_MAX_TOTAL_BYTES 可调，0=沿用配置）仍驱逐 = 单窗 max_window_bytes")
    print("  或数据量超过放量后的 cap。各档同策略、口径一致，增量墙归属仍成立；")
    print("  要消除则调大 WF_DIAG_MAX_TOTAL_BYTES / 减小 N / 调大 windows.toml 窗口预算。")
print("口径: %s" % CTX)
sys.exit(FAIL)
