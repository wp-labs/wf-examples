#!/usr/bin/env python3
"""相位同窗 e2e 事件生成（jsonl，供 wfgen send）。

用法:
  gen_metrics_phase.py <count> <span_s> <offset_s> [spike_idx]   生成 jsonl
  gen_metrics_phase.py --constants    打印相位常量 "period_s bucket_s"（run_phase 交叉校验用）
  gen_metrics_phase.py --selfcheck    自检（不加 daemon）：确定性断言生成器属性

生成语义（与 conf/loop.phase.wfusion.toml 相位参数强耦合——二者周期常量必须一致，
run_phase.sh 启动时会用 --constants 交叉校验）：
  周期 240s（16 个 15s 相位格）：忙时格 8..15 → 水平 3000，闲时格 0..7 → 水平
  1000，各加 ±25 均匀噪声（噪声有界 → 正常事件相对同档基线的 z 恒 ≤1.7）。
  每轮事件时间 +120s = 半个周期 → 奇轮全落忙半周期（slots 8..13）、偶轮全落闲半
  周期（slots 0..5）；同相位位置每 2 轮严格复现（该格历史窗隔轮产生）。
  spike_idx 同 gen_metrics_live：该序号的 5号线 事件压成 9000（确定性越界）。

实测语义（2026-09-08，批量注入 + 判定滞后 → 越界自窗先入其相位桶）：
  - judge 每轮**恰 1 条**（越界自窗使轮 1 即有告警、z≈10 被摊薄仍 ≫3）；
  - 忙时正常事件（3000±25）与上轮同相位窗（同为 3000±25）比 → 永不误报；
  - 相位未生效（滚动）时闲轮 1000 会撞忙轮基线 → 全量误报（与"轮 1/2 静默"的
    早期预期不同——精确隔离语义以 wf-cep baseline 单测为准，见 §11.7）。
"""
import json
import random
import sys

import phase_cfg  # 相位折叠/周期共享口径（detect 供给与事件打标一致）

BASE_NS = 1767225600000000000  # 2026-01-01T00:00:00Z（15s/240s 网格对齐）
PERIOD_S = phase_cfg.PERIOD_S
BUCKET_S = phase_cfg.BUCKET_S
BUSY_SLOTS = range(phase_cfg.BUCKETS // 2, phase_cfg.BUCKETS)  # 忙时相位格（后半周期）
BUSY_LEVEL = 3000.0
IDLE_LEVEL = 1000.0
LINES = ["1号线", "2号线", "3号线", "4号线", "5号线"]


def slot_of(t_ns):
    """事件时刻 → 忙/闲档判定用的相位槽（epoch 秒折叠，与 phase_cfg 同口径）。"""
    return phase_cfg.bucket_of_ns(t_ns)


def level_of(t_ns):
    return BUSY_LEVEL if slot_of(t_ns) in BUSY_SLOTS else IDLE_LEVEL


def build_rows(count, span_s, offset_s, spike_idx):
    """生成一轮事件（确定性：rng seed = 42 + offset_s）。"""
    rng = random.Random(42 + offset_s)
    rows = []
    for i in range(count):
        t = BASE_NS + offset_s * 1_000_000_000 + int(span_s * 1e9 * i / count)
        level = level_of(t)
        svc = LINES[i % len(LINES)]
        value = level + rng.randint(-25, 25)
        if i == spike_idx:
            assert svc == "5号线", f"spike_idx {spike_idx} 不是 5号线 事件"
            value = 9000
        rows.append(
            {
                "_stream": "metrics_stream",
                "_timestamp": "2026-01-01T00:00:00.000Z",
                "_window": "metrics_stream",
                "entity": svc,
                "event_time": t,
                "metric": "flow",
                "value": float(value),
                "phase_bucket": phase_cfg.label(slot_of(t)),  # detect 相位供给 join 键
            }
        )
    return rows


def emit(rows):
    for r in rows:
        print(json.dumps(r))


def selfcheck():
    """确定性断言（与 run_phase.sh 默认参数一致）：不依赖引擎，纯生成器属性。"""
    count, span_s, spike_idx = 3000, 90.0, 1504
    bad = []
    rounds = [(r * 120) for r in range(1, 9)]
    for offset_s in rounds:
        rows = build_rows(count, span_s, offset_s, spike_idx)
        label = f"offset={offset_s:>4}s"
        if len(rows) != count:
            bad.append(f"{label}: 行数 {len(rows)} != {count}")
        # 1) 事件时间严格单调递增且落在 [offset, offset+span)
        ts = [r["event_time"] for r in rows]
        if any(b <= a for a, b in zip(ts, ts[1:])):
            bad.append(f"{label}: event_time 非严格单调")
        lo = BASE_NS + offset_s * 1_000_000_000
        hi = lo + int(span_s * 1e9)
        if not (ts[0] == lo and ts[-1] < hi):
            bad.append(f"{label}: 时间范围越界 [{lo},{hi})")
        # 2) 5 条线路事件数均衡
        counts = {e: 0 for e in LINES}
        for r in rows:
            counts[r["entity"]] += 1
        if any(v != count // len(LINES) for v in counts.values()):
            bad.append(f"{label}: 线路不均衡 {counts}")
        # 3) 忙/闲轮纯度：非 spike 事件的档位必须与该轮半周期一致
        busy_half = offset_s % PERIOD_S == PERIOD_S // 2  # 120s → 忙
        for r in rows:
            if r["value"] == 9000.0:
                continue
            if (slot_of(r["event_time"]) in BUSY_SLOTS) != busy_half:
                bad.append(f"{label}: 档位与轮不符 t={r['event_time']}")
                break
            level = level_of(r["event_time"])
            if abs(r["value"] - level) > 25:
                bad.append(f"{label}: 噪声越界 value={r['value']} level={level}")
                break
        # 4) spike：恰 1 个、5号线、9000、档位与其轮一致
        spikes = [r for r in rows if r["value"] == 9000.0]
        if len(spikes) != 1:
            bad.append(f"{label}: spike 数 {len(spikes)} != 1")
        else:
            s = spikes[0]
            if s["entity"] != "5号线":
                bad.append(f"{label}: spike 实体 {s['entity']}")
            if (slot_of(s["event_time"]) in BUSY_SLOTS) != busy_half:
                bad.append(f"{label}: spike 档位与轮不符 slot={slot_of(s['event_time'])}")
        print(f"  selfcheck ok: {label}")
    # 5) 跨周期复现：同相位位置（offset 差整周期）的 (相位槽, 档位) 序列一致
    for offset_s in (120, 240):
        a = build_rows(count, span_s, offset_s, spike_idx)
        b = build_rows(count, span_s, offset_s + PERIOD_S, spike_idx)
        key = lambda r: (
            ((r["event_time"] - BASE_NS) // 1_000_000_000 // BUCKET_S) % (PERIOD_S // BUCKET_S),
            level_of(r["event_time"]),
        )
        if [key(r) for r in a] != [key(r) for r in b]:
            bad.append(f"offset={offset_s}: 跨周期同相位序列不一致")
    if bad:
        print("FAIL: gen_metrics_phase.py 自检未通过")
        for b in bad[:20]:
            print("  -", b)
        sys.exit(1)
    print("PASS: gen_metrics_phase.py 自检通过（8 轮：单调/均衡/忙闲纯度/spike/周期复现）")


def main():
    if len(sys.argv) >= 2 and sys.argv[1] == "--constants":
        print(f"{PERIOD_S} {BUCKET_S}")
        return
    if len(sys.argv) >= 2 and sys.argv[1] == "--selfcheck":
        selfcheck()
        return
    count = int(sys.argv[1])
    span_s = float(sys.argv[2])
    offset_s = int(sys.argv[3]) if len(sys.argv) > 3 else 0
    spike_idx = int(sys.argv[4]) if len(sys.argv) > 4 else -1
    emit(build_rows(count, span_s, offset_s, spike_idx))


if __name__ == "__main__":
    main()
