#!/usr/bin/env python3
"""S2-M3a 判定对拍验证：
1) provider CSV（数据契约）内部自洽：mu ≈ sum/n；
2) baseline_alerts 恰好 1 条 = 5号线（9000/1000 = 8x > 5x），1~4号线 不告警。
"""
import csv
import json
import os
import sys

CSV = "data/detect/baseline_ref.csv"
ALERTS = "data/detect/alerts.ndjson"
EXPECTED_ANOMALY = "5号线"


def main() -> int:
    if not os.path.exists(CSV):
        print(f"ERROR: 缺 {CSV}")
        return 1

    ref = {}
    with open(CSV, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            ref[r["entity"]] = {
                "n": float(r["n"]),
                "sum": float(r["sum"]),
                "sum_sq": float(r["sum_sq"]),
                "mu": float(r["mu"]),
                "sigma": float(r["sigma"]),
            }
    if not ref:
        print("ERROR: provider CSV 为空")
        return 1

    # 契约自洽：三元组 → mu 复核（相对容差）
    for e, r in ref.items():
        if r["n"] <= 0:
            print(f"ERROR: {e} n<=0")
            return 1
        mu_re = r["sum"] / r["n"]
        if not (abs(mu_re - r["mu"]) <= 1e-9 * max(abs(mu_re), abs(r["mu"])) + 1e-6):
            print(f"ERROR: {e} mu 与 sum/n 不一致: csv={r['mu']} recompute={mu_re}")
            return 1

    alerts = []
    if os.path.exists(ALERTS):
        with open(ALERTS, encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    alerts.append(json.loads(line))

    by_entity = {}
    for a in alerts:
        by_entity.setdefault(a.get("entity"), []).append(a)

    bad = []
    if set(by_entity) != {EXPECTED_ANOMALY}:
        bad.append(f"告警实体集合不符: {sorted(by_entity)} 期望只含 {EXPECTED_ANOMALY}")
    for e, rows in by_entity.items():
        for a in rows:
            dev = float(a.get("deviation", 0.0))
            if e == EXPECTED_ANOMALY and not (7.5 <= dev <= 8.5):
                bad.append(f"{e} deviation 期望≈8.0, 实际 {dev}")
            if a.get("alert_type") != "flow_deviation":
                bad.append(f"{e} alert_type 异常: {a.get('alert_type')}")

    print(f"provider 实体数: {len(ref)}  告警数: {len(alerts)}")
    for e in sorted(ref):
        mark = "⚠ 异常命中" if e in by_entity else "正常（未告警）"
        print(f"  {e}: mu={ref[e]['mu']:.3f} sigma={ref[e]['sigma']:.3f}  {mark}")

    if bad:
        print("FAIL:")
        for b in bad:
            print("  -", b)
        return 1

    print("PASS：5号线 8x 偏离唯一告警，1~4号线 正常未告警，CSV 契约自洽")
    return 0


if __name__ == "__main__":
    sys.exit(main())
