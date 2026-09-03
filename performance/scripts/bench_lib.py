#!/usr/bin/env python3
"""nexmark_pk / rule_scale_test 基准与诊断脚本共享的度量工具库。

两个 shell 脚本（bench.sh / diag.sh）曾把这些逻辑用 heredoc 内嵌在 bash 里——
无法单独测试、改一行要重跑整个基准。现抽成独立文件，shell 只负责流程编排。
每个逻辑以子命令分派（argv[1]），便于 `python3 bench_lib.py <cmd> ...` 单独验证。

本文件纯标准库，无第三方依赖。不 print 任何副作用提示（调用方 shell 负责文案）。
"""
import json
import re
import subprocess
import sys
import time

# ---------------------------------------------------------------------------
# 通用工具
# ---------------------------------------------------------------------------

def comma(value):
    """千分位格式化（macOS bash 3.2 无 printf %'d）。非数字原样返回。"""
    try:
        return f"{int(float(value)):,}"
    except (TypeError, ValueError):
        return str(value)


def now_nanos():
    """SystemTime epoch 纳秒（i64 精度，与哨兵 start_ns/emit_ns 同域）。"""
    return int(time.time_ns())


def md5_short(text, n=8):
    import hashlib
    return hashlib.md5(text.encode()).hexdigest()[:n]


def parse_n(spec):
    """解析 "100k"/"1m"/"3g" 后缀 → 整数。裸数字原样。"""
    s = str(spec).strip().lower()
    mult = 1
    if s and s[-1] in "kmg":
        mult = {"k": 1000, "m": 1_000_000, "g": 1_000_000_000}[s[-1]]
        s = s[:-1]
    return int(float(s) * mult)


def eps(n, start_ns, end_ns):
    """EPS = n / (end_ns − start_ns)。入参为 epoch 纳秒（与哨兵同域），
    差值非正返回 0（保护除零）。"""
    try:
        dt = int(end_ns) - int(start_ns)
    except (TypeError, ValueError):
        return 0
    return int(int(n) * 1e9 / dt) if dt > 0 else 0


def mb(bytes_val):
    """字节 → MB（1 位小数）。"""
    try:
        return round(int(bytes_val) / 1048576, 1)
    except (TypeError, ValueError):
        return "?"


def diff_ns(end_ns, start_ns):
    """两纳秒读数之差 → 秒（1 位小数）。"""
    try:
        return "%.1f" % ((int(end_ns) - int(start_ns)) / 1e9)
    except (TypeError, ValueError):
        return "0"


# ---------------------------------------------------------------------------
# 引擎度量读取（metrics.ndjson / perf_sentinel.ndjson）
# ---------------------------------------------------------------------------

def _iter_metrics(path):
    with open(path, errors="replace") as f:
        for line in f:
            try:
                yield json.loads(line)
            except Exception:
                continue


def engine_appended(metrics_path, streams_csv):
    """三/六输入流 append_total 全文件求和（counter=区间差值，跨区间求和=累计）。"""
    streams = set(s for s in re.split(r"[,\s]+", streams_csv) if s)
    total = 0
    try:
        for o in _iter_metrics(metrics_path):
            if o.get("name") == "append_total" and o.get("label") in streams:
                total += int(float(o.get("value", 0) or 0))
    except FileNotFoundError:
        pass
    print(total)


def engine_acked_lag(metrics_path, streams_csv):
    """每窗口未完全消费的批数（0 = 所有规则已消费到最新）。

    口径 = `WindowProgress::completion_gap`（分组完成判定，2026-08-25 review）：
    key/行号分片（match/stats）窗口用 min（最慢分片追平才为 0）；round-robin
    （whole-batch）窗口用 max（每批归属唯一 shard，min 恒停在最慢 shard）。
    名单为空 = **所有被消费窗口**——中间管道窗口（bid_mod / auction_finals 等）
    的消费滞后也必须计入：q13（2026-08-23）只查三输入流会在中间管道下游
    消费滞后时提前 SIGTERM。nexmark_alerts 等无消费者窗口 gap 恒 0，不受影响。"""
    streams = set(s for s in re.split(r"[,\s]+", streams_csv) if s)
    lag = {}
    try:
        for o in _iter_metrics(metrics_path):
            if o.get("name") == "acked_lag" and (not streams or o.get("label") in streams):
                lag[o["label"]] = int(float(o.get("value", 0) or 0))
    except FileNotFoundError:
        pass
    print(sum(lag.values()))


def sentinel_tuple(sent_path):
    """汇总全部 sentinel 记录 → "total_n min_start max_emit count"。

    多连接分连接哨兵（round=连接号）聚合为批级完成信号；单连接 1 条。
    EPS = Σn / (max_emit − min_start)。空 = 尚未出现（打印空串）。
    """
    rows = []
    try:
        for line in open(sent_path):
            try:
                o = json.loads(line)
            except Exception:
                continue
            if o.get("record_type") == "sentinel":
                rows.append(o)
    except FileNotFoundError:
        pass
    if not rows:
        print()
        return
    total_n = sum(int(r["n"]) for r in rows)
    min_start = min(int(r["start_ns"]) for r in rows)
    max_emit = max(int(r["emit_ns"]) for r in rows)
    print(total_n, min_start, max_emit, len(rows))


def received(metrics_path, label="ingress"):
    """送达计数：rows_total 每区间 delta 累加（rule_scale_test 口径，source 侧接收行数）。"""
    total = 0
    try:
        for o in _iter_metrics(metrics_path):
            if o.get("name") == "rows_total" and o.get("label") == label:
                total += int(float(o.get("value", 0) or 0))
    except FileNotFoundError:
        pass
    print(total)


def alert_summary(metrics_path):
    """#18 门禁摘要：emitted_total 总量 + conn_rules（非 auth/dns/pr/fw/fl 前缀的
    emitted_detail）+ rules_seen（emitted_detail 采到的规则数）。
    输出 "emitted=N conn_rules=N rules_seen=N"，调用方 grep 解析。"""
    from collections import Counter
    tot = 0
    c = Counter()
    try:
        for o in _iter_metrics(metrics_path):
            if o.get("name") == "emitted_total":
                tot += int(float(o.get("value", 0) or 0))
            elif o.get("name") == "emitted_detail":
                c[o.get("label", "?")] += int(float(o.get("value", 0) or 0))
    except FileNotFoundError:
        pass
    conn = sum(v for k, v in c.items()
               if not k.startswith(("auth_", "dns_", "pr_", "fw_", "fl_")))
    print("emitted=%d conn_rules=%d rules_seen=%d" % (tot, conn, len(c)))


def correctness_summary(metrics_path):
    """正确性摘要：SUMMARY 行（致命计数器）+ 各规则 EMIT 行。

    致命计数器（append_failed/dropped_late/cursor_gap）非零即跑批作废。
    `memory_evicted_total` **不再致命**（2026-08-25）：内存驱逐在 min_acked /
    retention-pin 保护下只回收**已读/已广播**批次（消费者未读批次驱逐会触发
    `cursor_gap`——那才是真丢数据信号）。背压/字节 cap 下驱逐是常态，非零
    不表示正确性受损（q13 100M 背压下 2000+ 次驱逐全为已读回收，EMIT 完整）。
    输出格式与旧 heredoc 完全一致，调用方用 grep 解析。
    """
    from collections import defaultdict
    emitted = defaultdict(int)
    bad = defaultdict(int)
    try:
        for o in _iter_metrics(metrics_path):
            name, label = o.get("name"), o.get("label", "")
            try:
                v = int(float(o.get("value", 0) or 0))
            except (TypeError, ValueError):
                continue
            if name == "emitted_total":
                emitted[label] += v
            elif name in ("append_failed_total", "dropped_late_total") and v:
                bad[name] += v
            elif name == "cursor_gap_total" and v:
                bad["cursor_gap[%s]" % label] += v
    except FileNotFoundError:
        pass
    bad_str = " ".join("%s=%d" % kv for kv in sorted(bad.items())) or "clean"
    print("SUMMARY %s" % bad_str)
    for k in sorted(emitted):
        print("EMIT %s %d" % (k, emitted[k]))


# ---------------------------------------------------------------------------
# CPU%/RSS 采样（后台常驻，1s 周期）
# ---------------------------------------------------------------------------

def rss_sampler(pid, out_path, interval_s=1.0):
    """采样进程 RSS + 瞬时 CPU%，输出 "epoch_ns RSS_MB CPU_PCT" 每行。

    - ps %cpu 是生命周期平均，无意义；这里取 cputime 差分/墙钟差分 = 瞬时核占。
    - 首样本只初始化差分基线（无输出行），第 2 样本起每行都有 CPU 值。
    - epoch_ns 与哨兵 start_ns/emit_ns 同域（wall-clock）——调用方按引擎活跃窗
      过滤样本，否则粗采样 + 空闲期稀释会把亚秒级突发报成 0%（实测假象）。
    - ps 被权限拒绝时回退 macOS `footprint`（输出 "Footprint: 912 KB"）。
    - 静默跳过失败样本（不打印 traceback，避免污染输出文件）。
    """
    pid = int(pid)
    interval = max(float(interval_s), 0.1)
    units = {"": 1, "K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}

    def secs(s):
        v = 0.0
        for x in s.split(":"):
            v = v * 60 + float(x)
        return v

    def footprint_rss_kb():
        try:
            r = subprocess.run(["footprint", str(pid)], capture_output=True, text=True)
            m = __import__("re").search(r"Footprint:\s*([\d.]+)\s*([KMGT]?)B", r.stdout)
            if not m:
                return None
            return int(float(m.group(1)) * units[m.group(2)])
        except Exception:
            return None

    prev_ct = None
    prev_t = None
    with open(out_path, "w") as out:
        while True:
            try:
                r = subprocess.run(["ps", "-o", "rss=,cputime=", "-p", str(pid)],
                                   capture_output=True, text=True)
                parts = r.stdout.split()
                if len(parts) == 2:
                    rss_kb = int(parts[0])
                    ct = secs(parts[1])
                    now = time.time()
                    if prev_ct is not None and now > prev_t:
                        cpu = (ct - prev_ct) / (now - prev_t) * 100.0
                        print(f"{int(now * 1e9)} {rss_kb // 1024} {cpu:.1f}",
                              file=out, flush=True)
                    prev_ct, prev_t = ct, now
                else:
                    rss = footprint_rss_kb()
                    if rss is not None:
                        print(f"{int(time.time() * 1e9)} {rss // 1024} n/a",
                              file=out, flush=True)
                    prev_ct, prev_t = None, time.time()
            except Exception:
                prev_ct, prev_t = None, time.time()
            time.sleep(interval)


# ---------------------------------------------------------------------------
# 高精度采样（带 epoch 纳秒，诊断按档切分用）
# ---------------------------------------------------------------------------

def diag_sampler(pid, out_path, interval_ms=100):
    """输出 "epoch_ns rss_mb cpu_pct"。epoch_ns 与哨兵 start_ns/emit_ns 同域，
    分析脚本按档的 [start_ns, emit_ns] 区间切分归属 CPU%/RSS。"""
    pid = int(pid)
    interval = max(float(interval_ms), 20.0) / 1000.0

    def secs(s):
        v = 0.0
        for x in s.split(":"):
            v = v * 60 + float(x)
        return v

    prev_ct, prev_t = None, None
    with open(out_path, "w") as out:
        while True:
            try:
                r = subprocess.run(["ps", "-o", "rss=,cputime=", "-p", str(pid)],
                                   capture_output=True, text=True)
                parts = r.stdout.split()
                if len(parts) == 2:
                    rss_kb = int(parts[0])
                    ct = secs(parts[1])
                    now = time.time()
                    if prev_ct is not None and now > prev_t:
                        cpu = (ct - prev_ct) / (now - prev_t) * 100.0
                        print(f"{int(now * 1e9)} {rss_kb // 1024} {cpu:.1f}",
                              file=out, flush=True)
                    prev_ct, prev_t = ct, now
                else:
                    prev_ct, prev_t = None, None
            except Exception:
                prev_ct, prev_t = None, None
            time.sleep(interval)


def footprint_sampler(pid, out_path, interval_s=1.0):
    """采样进程 dirty 物理内存（macOS `footprint`），输出 "epoch_ns dirty_mb"。

    - RSS 含分配器页表保留（释放未归还），波动大（q13 实测 5-14G）；dirty 是
      物理真持有（跑批中 7-10G、稳态 5.6G 稳定）——内存判据用 dirty。
    - epoch_ns 与哨兵 start_ns/emit_ns 同域，分析脚本按档区间切分。
    - ⚠ `footprint` 每次 spawn 0.3-0.5s（1s 周期 ≈ 50% 核）——只在内存验证时
      开（perf-diag-wall.toml `mem_sample = true`），勿常驻污染 bench 结果。
    - 非 macOS / footprint 缺失 → 静默无输出（分析器缺列 → n/a，graceful）。
    """
    pid = int(pid)
    interval = max(float(interval_s), 0.5)
    units = {"": 1, "K": 1024, "M": 1024 ** 2, "G": 1024 ** 3, "T": 1024 ** 4}

    def footprint_mb():
        try:
            r = subprocess.run(["footprint", str(pid)], capture_output=True, text=True)
            m = re.search(r"Footprint:\s*([\d.]+)\s*([KMGT]?)B", r.stdout)
            if not m:
                return None
            return int(float(m.group(1)) * units[m.group(2)]) // (1024 ** 2)
        except Exception:
            return None

    with open(out_path, "w") as out:
        while True:
            d = footprint_mb()
            if d is not None:
                print(f"{int(time.time() * 1e9)} {d}", file=out, flush=True)
            time.sleep(interval)


# ---------------------------------------------------------------------------
# CLI 分派
# ---------------------------------------------------------------------------

def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    a = sys.argv[2:]
    if cmd == "comma" and a:
        print(comma(a[0]))
    elif cmd == "now-nanos" or cmd == "now":
        # now = now-nanos 的别名（历史脚本迁移用）；统一 epoch 纳秒口径
        print(now_nanos())
    elif cmd == "md5" and a:
        print(md5_short(a[0], int(a[1]) if len(a) > 1 else 8))
    elif cmd == "parse-n" and a:
        print(parse_n(a[0]))
    elif cmd == "eps" and len(a) >= 3:
        print(eps(a[0], a[1], a[2]))
    elif cmd == "mb" and a:
        print(mb(a[0]))
    elif cmd == "diff-ns" and len(a) >= 2:
        print(diff_ns(a[0], a[1]))
    elif cmd == "appended" and len(a) >= 2:
        engine_appended(a[0], a[1])
    elif cmd == "acked-lag" and len(a) >= 2:
        engine_acked_lag(a[0], a[1])
    elif cmd == "sentinel-tuple" and a:
        sentinel_tuple(a[0])
    elif cmd == "received" and a:
        received(a[0], a[1] if len(a) > 1 else "ingress")
    elif cmd == "alert-summary" and a:
        alert_summary(a[0])
    elif cmd == "correctness" and a:
        correctness_summary(a[0])
    elif cmd == "rss-sampler" and len(a) >= 2:
        rss_sampler(a[0], a[1], float(a[2]) if len(a) > 2 else 1.0)
    elif cmd == "diag-sampler" and len(a) >= 2:
        diag_sampler(a[0], a[1], float(a[2]) if len(a) > 2 else 100.0)
    elif cmd == "footprint-sampler" and len(a) >= 2:
        footprint_sampler(a[0], a[1], float(a[2]) if len(a) > 2 else 1.0)
    else:
        print(__doc__)
        sys.exit(2)


if __name__ == "__main__":
    main()
