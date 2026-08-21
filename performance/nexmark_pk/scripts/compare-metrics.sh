#!/bin/bash
# 指标偏离检测——当前 bench 结果 vs 预期基线（VVR/OSS）
#
# 读 data/bench_*_replay.txt 的 EPS/RSS/CPU，与 OSS_VVR_BASELINE.md 的
# VVR/OSS RPS 对比，输出偏离表：< 0.5×VVR 标 ⚠（性能需优化）。
#
# 用法：scripts/compare-metrics.sh [data目录]
# 注意：插桩（PROFILE=1）跑批的 EPS 含 2-5× 插桩开销，对比前须用正常二进制
#       重跑；本脚本默认对比正常跑批结果（bench_q*_replay.txt）。

set -e
cd "$(dirname "$0")/.."
DATA_DIR="${1:-data}"
BASE="OSS_VVR_BASELINE.md"

python3 - "$DATA_DIR" "$BASE" <<'PYEOF'
import os, re, sys

data_dir, base = sys.argv[1:3]

def num(s):
    return int(s.replace(",", ""))

# 预期基线：OSS_VVR_BASELINE.md 表格（| qN | OSS ms | OSS RPS | VVR ms | VVR RPS | × |）
expect = {}
with open(base) as f:
    for line in f:
        m = re.match(r"\|\s*(q\d+)\s*\|\s*([\d,]+)\s*\|\s*([\d,]+)\s*\|\s*([\d,]+)\s*\|\s*([\d,]+)\s*\|", line)
        if m:
            q, oss_ms, oss_rps, vvr_ms, vvr_rps = m.groups()
            expect[q] = {"oss": num(oss_rps), "vvr": num(vvr_rps)}

# 当前结果：bench_q*_replay.txt 的 EPS/RSS/CPU
results = {}
for fn in sorted(os.listdir(data_dir)):
    m = re.match(r"bench_(q\d+)_replay\.txt", fn)
    if not m:
        continue
    q = m.group(1)
    text = open(os.path.join(data_dir, fn)).read()
    rm = re.search(r"EPS=([\d,]+) · RSS_peak=([\d,]+)MB · CPU ([\d.]+)%avg", text)
    if rm:
        eps, rss, cpu = num(rm.group(1)), num(rm.group(2)), float(rm.group(3))
        results[q] = {"eps": eps, "rss": rss, "cpu": cpu}

lines = []
lines.append("=" * 100)
lines.append("指标偏离检测：当前 bench vs 预期基线（OSS_VVR_BASELINE.md）")
lines.append("=" * 100)
lines.append(f"  {'Q':<5} {'当前EPS':>12} {'VVR':>12} {'OSS':>12} {'vsVVR':>7} {'vsOSS':>7} {'RSS':>8} {'CPU%':>6} 状态")
lines.append("-" * 100)
for q in sorted(results):
    r = results[q]
    e = expect.get(q)
    if not e:
        lines.append(f"  {q:<5} {r['eps']:>12,} {'—':>12} {'—':>12} {'—':>7} {'—':>7} {r['rss']:>8,} {r['cpu']:>6.0f}  无基线")
        continue
    vs_vvr = r["eps"] / e["vvr"]
    vs_oss = r["eps"] / e["oss"]
    flag = ""
    if vs_vvr < 0.5:
        flag = "⚠ 低于 VVR 50%（优化候选）"
    elif vs_vvr < 1.0:
        flag = "△ 低于 VVR"
    elif vs_vvr >= 1.0:
        flag = "✓ 达 VVR"
    lines.append(
        f"  {q:<5} {r['eps']:>12,} {e['vvr']:>12,} {e['oss']:>12,} "
        f"{vs_vvr:>6.2f}× {vs_oss:>6.2f}× {r['rss']:>8,} {r['cpu']:>6.0f}  {flag}"
    )

report = "\n".join(lines)
with open(f"{data_dir}/metrics-vs-baseline.txt", "w") as f:
    f.write(report + "\n")
print(report)
PYEOF
