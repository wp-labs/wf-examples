#!/usr/bin/env python3
"""解析 daemon 日志里的 per-rule 相位 profile 行（`phase="profile"`），
输出逐规则 CPU 归因表。

引擎侧：每个 RuleTask 每 1s 节流 dump 一次累计计数
（scan_nanos/advance_nanos/exec_nanos/close_exec_nanos/append_nanos/
fanout_nanos/emit_nanos，见 wp-reactor rule_task.rs dump_profiling）。
本脚本取**相邻两行的增量**并求和（消除规则启动时间差异），输出：

  规则           相位总量(ns)  占总   advance%  scan%  emit%   exec%  其它%
  c_sip_3           123.4ms   12.3%   45%     30%    15%     0%    10%

用 `scan_nanos+advance_nanos+...` 作为该规则消耗的 CPU 工作量代理
（规则 worker 独占相位计时，不含解析/窗口/输出链）。跨规则可比（同窗口内）。

用法: python3 rule_phase_profile.py <wfusion.log> [--top N] [--phase p]
"""
import re
import sys
from collections import defaultdict

PHASES = [
    "scan_nanos",
    "advance_nanos",
    "exec_nanos",
    "close_exec_nanos",
    "append_nanos",
    "fanout_nanos",
    "emit_nanos",
]
SHORT = {
    "scan_nanos": "scan",
    "advance_nanos": "advance",
    "exec_nanos": "exec",
    "close_exec_nanos": "close",
    "append_nanos": "append",
    "fanout_nanos": "fanout",
    "emit_nanos": "emit",
}

LINE_RE = re.compile(
    r"rule profiling rule=(?P<rule>\S+) phase=\"profile\""
    r"(?P<vals>(?:\s+\w+=\d+)+)"
)

# 日志带 ANSI 色码（`\x1b[3mrule=...`）——解析前剥离（diag_analyze 同款）。
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def parse(path):
    """每规则: 相位增量求和（相邻行差） + 总行数/首末行。"""
    last = {}
    acc = defaultdict(lambda: defaultdict(int))  # rule -> phase -> ns
    for line in open(path, errors="replace"):
        line = ANSI_RE.sub("", line)
        m = LINE_RE.search(line)
        if not m:
            continue
        vals = dict(
            (k, int(v))
            for k, v in re.findall(r"(\w+)=(\d+)", m.group("vals"))
        )
        rule = m.group("rule")
        prev = last.get(rule)
        last[rule] = vals
        if prev:
            for p in PHASES:
                acc[rule][p] += max(vals.get(p, 0) - prev.get(p, 0), 0)
    return acc, last


def main():
    path = sys.argv[1]
    top = 20
    phase_only = None
    if "--top" in sys.argv:
        top = int(sys.argv[sys.argv.index("--top") + 1])
    if "--phase" in sys.argv:
        phase_only = sys.argv[sys.argv.index("--phase") + 1]
    acc, last = parse(path)
    if not acc:
        print(f"无 phase=\"profile\" 行（{path}）——需 WF_RULE_PROFILING=1（默认开）+ 有实际处理时长")
        sys.exit(1)

    rows = []
    for rule, ps in acc.items():
        total = sum(ps.values())
        if total <= 0:
            continue
        rows.append((rule, total, ps))
    rows.sort(key=lambda r: -r[1])
    grand = sum(r[1] for r in rows) or 1

    print(f"{'规则':<22}{'相位总量':>12}{'占总':>8}   " + "  ".join(f"{SHORT[p]:>6}%" for p in PHASES))
    print("-" * 22 + "-" * 20 + "   " + "  ".join(["------"] * len(PHASES)))
    for rule, total, ps in rows[:top]:
        if phase_only and phase_only not in SHORT.values():
            continue
        share = total / grand * 100
        if phase_only:
            key = [k for k, v in SHORT.items() if v == phase_only][0]
            print(f"{rule:<22}{ps[key]/1e9:>10.3f}s{share:>8.1f}%")
        else:
            pct = "  ".join(f"{ps[p]/total*100:>6.0f}" for p in PHASES)
            print(f"{rule:<22}{total/1e9:>10.3f}s{share:>7.1f}%   {pct}")
    print(f"\n共 {len(rows)} 规则（前 {min(top, len(rows))} 条，占总 {sum(r[1] for r in rows[:top])/grand*100:.0f}%）")


if __name__ == "__main__":
    main()
