#!/usr/bin/env python3
"""S2-M3a：把 producer 基线产物（baseline.ndjson）聚合导出为 provider CSV。

契约（§11.3）：输出保留可加三元组 n/sum/sum_sq，另附消费列 mu/sigma
（mean = sum/n，σ = √(max(0, sum_sq/n − mean²))）——join 判定直接用，
三元组保证可加性与方法可重推导。当前整段退化：不区分相位桶。
"""
import csv
import json
import math
import os
import sys

SRC = "data/baseline/baseline.ndjson"
DST = "data/detect/baseline_ref.csv"
METRIC = "qps"


def main() -> int:
    if not os.path.exists(SRC):
        print(f"ERROR: 缺少 producer 产物 {SRC}（先跑 m3a producer）")
        return 1

    agg = {}  # entity -> [n, sum, sum_sq]
    with open(SRC, encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            r = json.loads(line)
            if r.get("metric") != METRIC:
                continue
            e = r["entity"]
            g = agg.setdefault(e, [0.0, 0.0, 0.0])
            g[0] += float(r["n"])
            g[1] += float(r["sum"])
            g[2] += float(r["sum_sq"])

    os.makedirs(os.path.dirname(DST), exist_ok=True)
    with open(DST, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["entity", "n", "sum", "sum_sq", "mu", "sigma"])
        for e in sorted(agg):
            n, s, ss = agg[e]
            if n <= 0:
                continue
            mu = s / n
            var = ss / n - mu * mu
            sigma = math.sqrt(max(var, 0.0))
            w.writerow([e, f"{n:.0f}", repr(s), repr(ss), repr(mu), repr(sigma)])
            print(f"  {e}: n={n:.0f} mu={mu:.3f} sigma={sigma:.3f}")

    print(f"导出 {len(agg)} 行 -> {DST}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
