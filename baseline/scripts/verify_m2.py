#!/usr/bin/env python3
"""S2-M2 judge 对拍验证（实时滚动基线 z-score）：
1) warm 数据自洽：judge 用全部历史窗（受控 5 条线路×4 窗×60）——producer 输出
   经 export_baseline_history.py 导出 CSV；
2) judge 输出恰 1 条 = 5号线（9000 vs μ≈1000 σ≈15 → z≈500+），1~4号线 |z|<1 不告警。
"""
import json
import os
import sys

ALERTS = "data/detect/judge.ndjson"
EXPECTED = "5号线"


def main() -> int:
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
    if set(by_entity) != {EXPECTED}:
        bad.append(f"告警实体集合不符: {sorted(by_entity)} 期望只含 {EXPECTED}")
    for e, rows in by_entity.items():
        for a in rows:
            z = float(a.get("z", 0.0))
            if e == EXPECTED and not (z > 100.0):
                bad.append(f"{e} z 期望 >>3（≈500+），实际 {z}")
            if a.get("alert_type") != "flow_z_outlier":
                bad.append(f"{e} alert_type 异常: {a.get('alert_type')}")

    print(f"judge 告警数: {len(alerts)}")
    for a in alerts:
        print(f"  {a.get('entity')}: value={a.get('value')} z={a.get('z')}")
    if bad:
        print("FAIL:")
        for b in bad:
            print("  -", b)
        return 1
    print("PASS：5号线 z>>3 唯一告警（实时滚动基线 judge），1~4号线 正常未告警")
    return 0


if __name__ == "__main__":
    sys.exit(main())
