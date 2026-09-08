#!/usr/bin/env python3
"""把 producer 收盘基线（baseline.ndjson）聚合导出为 **PG 供给表** SQL。

与 export_baseline_ref.py（CSV）同构：按 entity 聚合 n/sum/sum_sq，推导
mu = sum/n、σ = √(max(0, sum_sq/n − μ²))。输出整表 TRUNCATE + INSERT 的 SQL
（5 行，幂等），供 run.sh --pg 每轮经 `docker exec psql` 落库——引擎的
NamedSql(engine_pg) 1s 周期刷新随即把新值搬入 join 缓存。

用法: export_baseline_ref_pg.py < data/baseline/baseline.ndjson | psql …
      （run.sh --pg 内部：python 脚本 | docker exec -i <cid> psql …）
"""
import json
import math
import sys

SRC = "data/baseline/baseline.ndjson"
METRIC = "flow"
TABLE = "baseline_ref"


def main() -> int:
    agg = {}  # entity -> [n, sum, sum_sq]
    try:
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
    except FileNotFoundError:
        return 1  # 尚无收盘产物：本轮跳过（保持旧供给）

    if not agg:
        return 0

    out = sys.stdout
    out.write(f"TRUNCATE {TABLE};\n")
    for e in sorted(agg):
        n, s, ss = agg[e]
        if n <= 0:
            continue
        mu = s / n
        var = ss / n - mu * mu
        sigma = math.sqrt(max(var, 0.0))
        out.write(
            f"INSERT INTO {TABLE} (entity, n, sum, sum_sq, mu, sigma) VALUES ("
            f"'{e}', {n:.0f}, {s!r}, {ss!r}, {mu!r}, {sigma!r});\n"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
