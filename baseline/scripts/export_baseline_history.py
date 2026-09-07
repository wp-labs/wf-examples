#!/usr/bin/env python3
"""S2-M2：把 producer 基线产物（baseline.ndjson）导出为逐窗历史 CSV
（供 judge 启动 warm 共享 BaselineStore）。

CSV 头固定：entity,metric,win_start,win_end,n,sum,sum_sq
  win_start/win_end：epoch 纳秒整数（输入为 "YYYY-MM-DD HH:MM:SS" UTC 朴素串）
保留全部窗口记录（store 按 K 裁剪取最近；这里全量导出）。
"""
import datetime
import json
import os
import sys

SRC = "data/baseline/baseline.ndjson"
DST = "data/detect/baseline_history.csv"


def to_ns(text: str) -> int:
    dt = datetime.datetime.strptime(text.strip(), "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=datetime.timezone.utc
    )
    return int(dt.timestamp() * 1e9)


def main() -> int:
    if not os.path.exists(SRC):
        print(f"ERROR: 缺 {SRC}（先跑 m3a producer batch）")
        return 1
    rows = []
    with open(SRC, encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            r = json.loads(line)
            rows.append(
                (
                    r["entity"],
                    r["metric"],
                    to_ns(r["win_start"]),
                    to_ns(r["win_end"]),
                    float(r["n"]),
                    float(r["sum"]),
                    float(r["sum_sq"]),
                )
            )
    if not rows:
        print(f"ERROR: {SRC} 无记录")
        return 1
    os.makedirs(os.path.dirname(DST), exist_ok=True)
    with open(DST, "w", encoding="utf-8") as f:
        f.write("entity,metric,win_start,win_end,n,sum,sum_sq\n")
        for e, m, ws, we, n, s, ss in rows:
            f.write(f"{e},{m},{ws},{we},{n},{s},{ss}\n")
    print(f"导出 {len(rows)} 条逐窗记录 -> {DST}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
