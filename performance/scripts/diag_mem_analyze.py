#!/usr/bin/env python3
"""nexmark_pk / qradar_pk diag.sh 的内存墙分析器（MEMORY=1 模式）。

与 diag_analyze.py 共用数据源（哨兵 EPS、CPU/RSS 采样、metrics.ndjson），
但报告重心从「吞吐墙」切到「内存墙」：

  1. 每档 RSS 峰值增量 → 内存增长发生在哪一段。叠加式墙梯档间窗口/规则状态
     不清零，每档内存 = 前档持有 + 本档新增路径的持有；ΔRSS 即该段新增。
  2. 成分分账（metrics 末拍稳态 = full 档）：窗口 Σ / 每窗明细 / alloc
     commit / parse 在途 / fanout 排队——解释 RSS 由什么构成、有没有未归因。
  3. 健康校验（append 追平 + 致命计数器）与性能版一致。

为什么内存模式默认不预热（MEMORY=1 → WARMUP=0）：预热档会把窗口装满再测
增量，抬高每档基线、掩盖「从空窗口开始」的干净增量；EPS 冷启动偏差对内存
分析无意义。

环境变量（与 diag_analyze.py 相同）:
  QUERY        查询名（报告标题用）
  RULES_COUNT  规则数
  N            本档数据量（整数）
  CTX          口径上下文行
  STAGE_NAMES  CSV：档名列表（含可选的 warmup 前缀档）
  APPEND_CUT_STAGES CSV：cut_append/cut_recv 档名（appended 期望扣减）
  CORES        机器核数
  SENT_PATH    data/perf_sentinel.ndjson（档区间事实源）
  SAMPLES_PATH 采样文件 "epoch_ns rss_mb cpu_pct"
  METRICS_PATH data/metrics.ndjson（成分/健康）
  LOG_PATH     daemon 日志（schema mismatch 计数用）
  STREAMS      CSV：输入流名（append_total 求和口径）
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
CORES = int(os.environ.get("CORES", 0) or 0)
SENT_PATH = os.environ.get("SENT_PATH", "data/perf_sentinel.ndjson")
SAMPLES_PATH = os.environ.get("SAMPLES_PATH", "")
METRICS_PATH = os.environ.get("METRICS_PATH", "data/metrics.ndjson")
LOG_PATH = os.environ.get("LOG_PATH", "")
INPUTS = set(s for s in re.split(r"[,\s]+", os.environ.get("STREAMS", "")) if s)

FAIL = 0


def num(v):
    if isinstance(v, (int, float)):
        return int(v)
    try:
        return int(str(v))
    except Exception:
        return None


def gb(v):
    """字节 → GB（metrics 字节口径用 1 位小数）。"""
    return v / (1024 ** 3) if v is not None else None


def gb_mb(v):
    """MB（采样 RSS）→ GB。采样器输出 rss_kb//1024 = MB，勿与字节口径混用。"""
    return v / 1024 if v is not None else None


def fmt_gb(v):
    return "n/a" if v is None else "%.1fG" % v


# ---- 哨兵记录：round → 档（档区间事实源）----
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


def window_stats(s, e):
    win = [x for x in samples if s <= x[0] <= e]
    if not win:
        return None
    return (max(x[1] for x in win),
            sum(x[2] for x in win) / len(win),
            max(x[2] for x in win), len(win))


# ---- metrics.ndjson：成分（每 name+label 取最后出现值 = 末拍稳态）+ 健康 ----
# MetricsRecord 无时间戳；每 report_interval 全量快照导出一次，同一 (name,label)
# 每拍出现一次 → 最后出现的组合即 daemon 退出前的末拍（diag.sh 档末 sleep 2 保证
# 末拍落在 full 档稳态内）。全程峰值用于捕捉档内瞬时（如 fanout 排队高峰）。
# ⚠ name 是裸名（不带 stage 前缀，如 "peak_rss_bytes" / "memory_bytes"）；
#   stage 只是分类域。window 类指标用 label 区分窗口。
last = {}          # (name, label) → value（末拍）
peak = {}          # (name, label) → 全程峰值
bad = {}
appended_total, emitted_total = 0, 0
try:
    for line in open(METRICS_PATH, errors="replace"):
        try:
            o = json.loads(line)
        except Exception:
            continue
        name, label = o.get("name"), o.get("label", "")
        try:
            v = int(float(o.get("value", 0) or 0))
        except (TypeError, ValueError):
            continue
        key = (name, label)
        last[key] = v
        if v > peak.get(key, 0):
            peak[key] = v
        if name == "append_total" and label in INPUTS:
            appended_total += v
        elif name == "emitted_total":
            emitted_total += v
        elif name in ("serialize_failed_total", "dropped_late_total", "memory_evicted_total") and v:
            bad[name] = bad.get(name, 0) + v
        elif name == "cursor_gap_total" and v:
            bad["cursor_gap[%s]" % label] = bad.get("cursor_gap[%s]" % label, 0) + v
except FileNotFoundError:
    pass


def L(name, label=""):
    """末拍值（缺失 → None）。"""
    return last.get((name, label))


def P(name, label=""):
    """全程峰值（缺失 → None）。"""
    return peak.get((name, label))


# ---------------------------------------------------------------------------
# 墙表（内存视角：RSS 峰值增量）
# ---------------------------------------------------------------------------
who = "qradar_pk" if (QUERY or "").startswith("qradar") else "nexmark_pk"
print("")
title = "== %s 内存墙定位 · %s" % (who, QUERY or "?")
title += " · N=%s" % format(N_REQ, ",")
if RULES_COUNT:
    title += " · 规则 %s" % RULES_COUNT
title += " =="
print(title)
print("（叠加式墙梯：档间窗口/状态不清零，每档内存 = 前档持有 + 本档新增路径；")
print("  ΔRSS = 相对上一档的峰值增量，定位内存增长段；RSS 为进程采样峰值）")
print("  「末拍账」= metrics 最后快照（full 档稳态）；成分均为字节口径 → 与 RSS 差 = 分配器页/碎片/未归因）")
head = ["档", "EPS", "耗时", "RSS_peak", "ΔRSS", "CPU%avg/max", "样本"]
fmt = "%-9s %13s %9s %10s %8s %14s %6s"
print((fmt + " %s") % tuple(head + [""]))

rows, prev_rss = [], None
for k, name in enumerate(STAGE_NAMES):
    if k not in sent:
        print("%-9s %13s  (无哨兵记录：该档未完成——见 wfgen 输出/daemon 日志)" % (name, "n/a"))
        continue
    n, s, e = sent[k]
    dt = e - s
    eps = n * 1e9 / dt
    st = window_stats(s, e)
    rss = st[0] if st else None
    cpu_s = "%d/%d" % (st[1], st[2]) if st else "n/a"
    cnt_s = str(st[3]) if st else "0"
    if name == "warmup":
        print("%-9s %13s  (预热档，数字不进结论)" % (name, "—"))
        continue
    delta = None if prev_rss is None else (rss - prev_rss)
    print("%-9s %13s %8.3fs %10s %8s %14s %6s" % (
        name, format(int(eps), ","), dt / 1e9, fmt_gb(gb_mb(rss)),
        "—" if delta is None else "+%.1fG" % (delta / 1024),
        cpu_s, cnt_s))
    rows.append({"name": name, "eps": eps, "dt": dt / 1e9,
                 "rss": rss, "delta": delta, "cpu": st[1] if st else None,
                 "samples": st[3] if st else 0})
    prev_rss = rss

# ---------------------------------------------------------------------------
# 内存墙判定
# ---------------------------------------------------------------------------
if len(rows) < 2:
    print("\n⚠ 有效档不足 2，无法判墙（哨兵记录缺失）")
    FAIL = 1
else:
    last_rss = rows[-1]["rss"] or 0
    print("\n-- 内存墙判定 --")
    walls = sorted([r for r in rows if r["delta"] is not None], key=lambda r: -r["delta"])
    if not walls:
        print("⚠ 无增量数据（各档 RSS 均未采样到）")
        FAIL = 1
    else:
        top = walls[0]
        base = rows[rows.index(top) - 1]["rss"] or 0
        pct = top["delta"] / last_rss * 100 if last_rss else 0
        print("主内存墙 = %s：相对上一档（%s %.1fGB）增量 %+.1fGB（占末档 RSS 峰值 %.1fGB 的 %.1f%%）" % (
            top["name"], rows[rows.index(top) - 1]["name"], base / 1024,
            top["delta"] / 1024, last_rss / 1024, pct))
        if top["cpu"] is not None and CORES:
            ratio = top["cpu"] / (CORES * 100.0)
            if ratio > 0.5:
                kind = "忙墙：计算密集段的持有（实例/批在工作态，未及时释放）"
            elif ratio < 0.15:
                kind = "等/供给墙：CPU 几乎空闲，内存堆在等待路径（通道排队/窗口堆积）"
            else:
                kind = "混合墙：部分并行未打满（CPU %.0f%%）" % (ratio * 100)
            print("   └ CPU %d%% 平均（%d 核 = %.0f%% 占用）→ %s" % (top["cpu"], CORES, ratio * 100, kind))
        print("各段贡献（ΔRSS 绝对值排序）：")
        for r in walls:
            pct_i = r["delta"] / last_rss * 100 if last_rss else 0
            flag = "—— 噪声内/近无增长" if abs(pct_i) < 5 else ""
            print("   %-8s %+.1fGB（%.1f%%）%s" % (r["name"], r["delta"] / 1024, pct_i, flag))
    if any(r["samples"] < 3 for r in rows):
        print("⚠ 存在 <3 个样本的档：RSS 峰值归属不可信（增大 N 或调小 SAMPLE_MS）")

# ---------------------------------------------------------------------------
# 成分分账（末拍稳态 + 全程峰值）
# ---------------------------------------------------------------------------
print("\n-- 成分分账（末拍 = full 档稳态；peak = 全程峰值）--")
_cc, _pc = L("current_commit_bytes"), L("peak_commit_bytes")
# macOS mimalloc 的 mi_process_info commit 字段不可靠（实测恒定 ~4MB，不随
# 负载变化），与 RSS 对比会误导——低于 64MB 视为未报告（真实 commit 在
# q13 量级下是 GB 级）。
_commit_s = "n/a（macOS mimalloc 不报告 commit）" if ((_cc or 0) < (64 << 20) and (_pc or 0) < (64 << 20)) else \
    "peak_commit=%s · current_commit=%s" % (fmt_gb(gb(_pc)), fmt_gb(gb(_cc)))
print("   alloc: peak_rss=%s · current_rss=%s · %s" % (
    fmt_gb(gb(L("peak_rss_bytes"))), fmt_gb(gb(L("current_rss_bytes"))), _commit_s))
win_names = sorted({label for (name, label) in last if name == "memory_bytes"})
sum_mem_final = sum(L("memory_bytes", w) or 0 for w in win_names)
sum_alloc_final = sum(L("allocated_bytes", w) or 0 for w in win_names)
sum_mem_peak = sum(P("memory_bytes", w) or 0 for w in win_names)
print("   窗口合计: memory=%.1fG（末拍）· allocated=%.1fG（末拍）· memory_peak=%.1fG" % (
    gb(sum_mem_final), gb(sum_alloc_final), gb(sum_mem_peak)))
pq = sum(L("fanout_queued_batches", w) or 0 for w in win_names)
pc = sum(L("fanout_capacity_batches", w) or 0 for w in win_names)
print("   parse 在途: %s / 预算 %s · fanout 排队: %s / %s 批" % (
    fmt_gb(gb(L("inflight_bytes"))), fmt_gb(gb(L("budget_bytes"))),
    pq, pc))
if win_names:
    print("\n-- 每窗明细（末拍值）--")
    print("%-20s %10s %10s %10s %8s %8s %8s" % (
        "窗口", "memory", "allocated", "rows", "batches", "ack_lag", "fan_q"))
    for w in sorted(win_names, key=lambda x: -(L("memory_bytes", x) or 0)):
        print("%-20s %10s %10s %10s %8s %8s %8s" % (
            w, fmt_gb(gb(L("memory_bytes", w))), fmt_gb(gb(L("allocated_bytes", w))),
            L("rows", w) or 0, L("batches", w) or 0, L("acked_lag", w) or 0,
            L("fanout_queued_batches", w) or 0))

# ---------------------------------------------------------------------------
# 健康
# ---------------------------------------------------------------------------
print("\n-- 健康 --")
expect_app = N_REQ * (len(sent) - len(APPEND_CUT_STAGES))
ratio = appended_total / expect_app if expect_app else 0
if APPEND_CUT_STAGES:
    print("appended: %s / %s（%d 档 − %d 前序档）× N = %.1f%%" % (
        format(appended_total, ","), format(expect_app, ","), len(sent),
        len(APPEND_CUT_STAGES), ratio * 100))
else:
    print("appended: %s / %s（%d 档 × N）= %.1f%%" % (
        format(appended_total, ","), format(expect_app, ","), len(sent), ratio * 100))
if expect_app and ratio < 0.9:
    print("✗ append 未追平 → 数据未真正进系统，内存账无意义。常见根因：")
    mism = 0
    try:
        for line in open(LOG_PATH):
            if "schema mismatch" in line or "append failed" in line:
                mism += 1
    except FileNotFoundError:
        pass
    if mism:
        print("  • 帧文件 schema 与当前 schemas 不匹配（日志 %d 条 schema mismatch）" % mism)
    print("  • 迟到丢弃（见 dropped_late）/ 帧文件行数不足 / 源未路由 / 入流限速未解除")
    FAIL = 1
print("致命计数器: %s" % (" ".join("%s=%d" % kv for kv in sorted(bad.items())) or "clean"))
print("emitted_total: %s" % format(emitted_total, ","))
if "dropped_late_total" in bad:
    print("✗ dropped_late 非零 = 墙梯口径污染：重发数据被判迟到丢弃 → 后续档无数据。")
    print("  处理: 减小 total，或保持 LATENESS_FIX=1 放宽迟到窗口。")
    FAIL = 1
if "memory_evicted_total" in bad:
    print("⚠ memory_evicted 非零：墙梯把同一份数据发了 %d 次，窗口 max_window_bytes 被打满。" % len(sent))
    print("  各档同策略、口径一致；要消除则减小 N 或调大 windows.toml 的窗口预算。")
print("口径: %s" % CTX)
sys.exit(FAIL)
