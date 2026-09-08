#!/usr/bin/env python3
"""把 producer 基线产物（baseline.ndjson）按 (entity, phase_bucket) 聚合导出为
provider 供给 CSV（2026-09-08 相位化：不再整段退化）。

契约（§11.3）：输出保留可加三元组 n/sum/sum_sq，另附消费列 mu/sigma
（mean = sum/n，σ = √(max(0, sum_sq/n − mean²))）——join 判定直接用，
三元组保证可加性与方法可重推导。相位折叠口径与事件打标一致（phase_cfg.py），
供 baseline_detect 按 (entity, phase_bucket) join 同相位供给行。
"""
import csv
import json
import math
import os
import sys
from datetime import datetime, timezone

import phase_cfg  # 相位折叠共享口径（与 gen_* 事件打标 / PG 供给 SQL 一致）

SRC = "data/baseline/baseline.ndjson"
DST = "data/detect/baseline_ref.csv"
METRIC = "flow"


def win_start_epoch_s(text: str) -> int:
    """引擎落盘的时间文本（'YYYY-MM-DD HH:MM:SS'，UTC）→ epoch 秒。"""
    s = text.strip().replace("T", " ").replace("Z", "")
    if "." in s:
        s = s.split(".")[0]
    dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    return int(dt.replace(tzinfo=timezone.utc).timestamp())


def main() -> int:
    if not os.path.exists(SRC):
        print(f"ERROR: 缺少 producer 产物 {SRC}（先跑 m3a producer）")
        return 1

    agg = {}  # (entity, phase_bucket) -> [n, sum, sum_sq]
    with open(SRC, encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            r = json.loads(line)
            if r.get("metric") != METRIC:
                continue
            e = r["entity"]
            p = phase_cfg.label(phase_cfg.bucket_of_epoch_sec(win_start_epoch_s(r["win_start"])))
            g = agg.setdefault((e, p), [0.0, 0.0, 0.0])
            g[0] += float(r["n"])
            g[1] += float(r["sum"])
            g[2] += float(r["sum_sq"])

    os.makedirs(os.path.dirname(DST), exist_ok=True)
    # 原子替换：先写临时文件再 rename，避免运行中的 loader refresh 读到半截 CSV。
    tmp = DST + ".tmp"
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["entity", "phase_bucket", "n", "sum", "sum_sq", "mu", "sigma"])
        for (e, p) in sorted(agg):
            n, s, ss = agg[(e, p)]
            if n <= 0:
                continue
            mu = s / n
            var = ss / n - mu * mu
            sigma = math.sqrt(max(var, 0.0))
            w.writerow([e, p, f"{n:.0f}", repr(s), repr(ss), repr(mu), repr(sigma)])
            print(f"  {e} 桶{p}: n={n:.0f} mu={mu:.3f} sigma={sigma:.3f}")
    os.replace(tmp, DST)
    print(f"导出 {len(agg)} 行 -> {DST}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
