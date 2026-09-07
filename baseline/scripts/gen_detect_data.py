#!/usr/bin/env python3
"""S2-M3a 受控数据生成：baseline 历史事件（5 实体 × 4 分钟 × 每秒 1 事件）
+ live 判定事件（每实体 1 条，svc_e 故意 9000 = 9×）。

事件行形状对齐 wfgen gen 输出（_stream/_timestamp/_window 元字段 + event_time
epoch 纳秒）。确定性（seed=42），供 producer → 导出 CSV → detect 判定对拍。
"""
import datetime
import json
import os
import random

NS0 = 1767225600000000000  # 2026-01-01T00:00:00Z (epoch ns)
OUT_DIR = "data/detect"
SVCS = ["svc_a", "svc_b", "svc_c", "svc_d", "svc_e"]


def iso_ns(ns: int) -> str:
    dt = datetime.datetime.fromtimestamp(ns / 1e9, tz=datetime.timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ns % 1_000_000_000:09d}"[:-3] + "Z"


def row(entity: str, value: float, t_ns: int) -> dict:
    return {
        "_stream": "metrics_stream",
        "_timestamp": iso_ns(t_ns),
        "_window": "metrics_stream",
        "entity": entity,
        "event_time": t_ns,
        "metric": "qps",
        "value": float(value),
    }


def write(path: str, rows) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    print(f"  {len(rows)} 行 -> {path}")


def main() -> None:
    rng = random.Random(42)
    t0 = NS0 + 10 * 60 * 1_000_000_000  # 00:10:00Z

    baseline = []
    for svc in SVCS:
        for i in range(240):  # 4 分钟 × 60s，落入 producer 的 4 个 1m 窗
            v = 1000 + rng.randint(-25, 25)
            baseline.append(row(svc, v, t0 + i * 1_000_000_000))

    live_t = t0 + 6 * 60 * 1_000_000_000  # 00:16:00Z（基线之后）
    live = []
    for i, svc in enumerate(SVCS):
        v = 9000 if svc == "svc_e" else 1000 + rng.randint(-10, 10)
        live.append(row(svc, v, live_t + i * 1_000_000_000))

    write(os.path.join(OUT_DIR, "baseline_events.jsonl"), baseline)
    write(os.path.join(OUT_DIR, "live_events.jsonl"), live)
    print("  预期：svc_e 偏离 (9000-1000)/1000 = 8.0x > 5x → 应告警；svc_a..d ≈0x → 不告警")


if __name__ == "__main__":
    main()
