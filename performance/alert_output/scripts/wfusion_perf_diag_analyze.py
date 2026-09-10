#!/usr/bin/env python3
"""Analyze one wfusion_new perf-diag wall run.

The sentinel records are the timing source.  Metrics are interval deltas, so
counter values are summed across the run.  A large negative stage delta is
reported as a measurement anomaly instead of being promoted to a fake wall.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any


def number(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def positive_counter(value: Any) -> int:
    try:
        return max(0, int(float(value)))
    except (TypeError, ValueError):
        return 0


def read_jsonl(path: Path):
    try:
        with path.open(encoding="utf-8", errors="replace") as stream:
            for line in stream:
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue
    except FileNotFoundError:
        return


def read_sentinels(path: Path) -> tuple[dict[int, dict[str, int]], list[dict[str, int]]]:
    stages: dict[int, dict[str, int]] = {}
    measurements: list[dict[str, int]] = []
    for item in read_jsonl(path):
        record_type = item.get("record_type")
        if record_type == "stage":
            current = number(item.get("current"))
            if current is not None:
                stages.setdefault(current, {"current": current})
        elif record_type == "sentinel":
            values = {
                key: number(item.get(key))
                for key in ("round", "n", "start_ns", "emit_ns")
            }
            if all(value is not None for value in values.values()) and values["emit_ns"] > values["start_ns"]:
                measurements.append(values)  # type: ignore[arg-type]
    return stages, measurements


def read_samples(path: Path) -> list[tuple[int, float, float]]:
    samples: list[tuple[int, float, float]] = []
    try:
        with path.open(encoding="utf-8", errors="replace") as stream:
            for line in stream:
                fields = line.split()
                if len(fields) != 3:
                    continue
                try:
                    samples.append((int(fields[0]), float(fields[1]), float(fields[2])))
                except ValueError:
                    continue
    except FileNotFoundError:
        pass
    return samples


def metric_sum(path: Path, stage: str | None = None, name: str | None = None, label: str | None = None) -> int:
    total = 0
    for item in read_jsonl(path):
        if stage is not None and item.get("stage") != stage:
            continue
        if name is not None and item.get("name") != name:
            continue
        if label is not None and item.get("label", "") != label:
            continue
        total += positive_counter(item.get("value"))
    return total


def metric_sum_labels(path: Path, stage: str, name: str, prefix: str) -> int:
    total = 0
    for item in read_jsonl(path):
        if item.get("stage") != stage or item.get("name") != name:
            continue
        if str(item.get("label", "")).startswith(prefix):
            total += positive_counter(item.get("value"))
    return total


def peak_sample(samples: list[tuple[int, float, float]], start: int, end: int) -> tuple[float, float, int] | None:
    window = [item for item in samples if start <= item[0] <= end]
    if not window:
        return None
    return (
        max(item[1] for item in window),
        sum(item[2] for item in window) / len(window),
        len(window),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sentinels", type=Path, required=True)
    parser.add_argument("--samples", type=Path, required=True)
    parser.add_argument("--metrics", type=Path, required=True)
    parser.add_argument("--log", type=Path, required=True)
    parser.add_argument("--events", type=int, required=True)
    parser.add_argument("--stages", required=True, help="comma-separated stage names")
    parser.add_argument("--append-cut-stages", default="recv,decode")
    parser.add_argument("--cores", type=int, default=0)
    parser.add_argument("--sink", choices=("blackhole", "file"), required=True)
    parser.add_argument("--context", default="")
    return parser.parse_args()


def fmt_int(value: float | int) -> str:
    return f"{value:,.0f}"


def main() -> int:
    args = parse_args()
    if args.events <= 0:
        print("错误：事件数必须大于 0", file=sys.stderr)
        return 2

    stage_names = [item.strip() for item in re.split(r"[,\s]+", args.stages) if item.strip()]
    append_cut = {item.strip() for item in re.split(r"[,\s]+", args.append_cut_stages) if item.strip()}
    if len(stage_names) < 2:
        print("错误：至少需要两个诊断档", file=sys.stderr)
        return 2

    _stage_records, measurements = read_sentinels(args.sentinels)
    by_round: dict[int, dict[str, int]] = {}
    for item in measurements:
        by_round.setdefault(item["round"], item)
    missing_indices = [index for index in range(len(stage_names)) if index not in by_round]
    samples = read_samples(args.samples)

    print("")
    print(
        "== wfusion_new 性能墙定位 · N=%s · sink=%s =="
        % (fmt_int(args.events), "BlackHole" if args.sink == "blackhole" else "File")
    )
    print(
        "（叠加式墙梯：后一档保留前一档并增加一段；EPS 来自哨兵等待窗口排空后的时间）"
    )
    print(
        "%-10s %13s %9s %12s %12s %10s %13s %12s %8s"
        % ("档", "EPS", "耗时", "ns/事件", "增量ns", "占末档", "CPU avg/max", "RSS_peak MiB", "样本")
    )

    rows: list[dict[str, Any]] = []
    previous_ns: float | None = None
    for index, name in enumerate(stage_names):
        item = by_round.get(index)
        if item is None:
            continue
        n = item["n"]
        elapsed = (item["emit_ns"] - item["start_ns"]) / 1e9
        ns_per_event = (item["emit_ns"] - item["start_ns"]) / max(n, 1)
        eps = n / elapsed if elapsed > 0 else 0.0
        resource = peak_sample(samples, item["start_ns"], item["emit_ns"])
        if resource is None:
            rss_peak = cpu_avg = cpu_peak = None
            sample_count = 0
        else:
            rss_peak, cpu_avg, sample_count = resource
            cpu_values = [x[2] for x in samples if item["start_ns"] <= x[0] <= item["emit_ns"]]
            cpu_peak = max(cpu_values) if cpu_values else cpu_avg

        if name == "warmup":
            delta = None
        else:
            delta = None if previous_ns is None else ns_per_event - previous_ns
        if name != "warmup":
            previous_ns = ns_per_event

        rows.append(
            {
                "index": index,
                "name": name,
                "n": n,
                "elapsed": elapsed,
                "eps": eps,
                "ns": ns_per_event,
                "delta": delta,
                "rss": rss_peak,
                "cpu": cpu_avg,
                "cpu_peak": cpu_peak,
                "samples": sample_count,
            }
        )

    measured = [row for row in rows if row["name"] != "warmup"]
    total_ns = measured[-1]["ns"] if measured else 0.0
    rows_by_index = {row["index"]: row for row in rows}
    for index, name in enumerate(stage_names):
        row = rows_by_index.get(index)
        if row is None:
            print("%-10s %13s  （缺少哨兵完成记录）" % (name, "n/a"))
            continue
        if name == "warmup":
            print("%-10s %13s  （预热档，数字不进结论）" % (name, "—"))
            continue
        delta = row["delta"]
        share = "—" if delta is None or total_ns <= 0 else f"{delta / total_ns * 100:+.1f}%"
        print(
            "%-10s %13s %8.3fs %12.1f %12s %9s %13s %12s %8d"
            % (
                name,
                fmt_int(row["eps"]),
                row["elapsed"],
                row["ns"],
                "—" if delta is None else f"{delta:+.1f}",
                share,
                "n/a"
                if row["cpu"] is None
                else f"{row['cpu']:.0f}/{row['cpu_peak']:.0f}",
                "n/a" if row["rss"] is None else f"{row['rss']:.1f}",
                row["samples"],
            )
        )
    # Fill the full-chain percentage in a second pass so the denominator is
    # consistent for every row (unlike the historical diag table).
    if measured and total_ns > 0:
        print("\n-- 各段增量（统一除以 full/末档总成本）--")
        for row in measured:
            if row["delta"] is None:
                continue
            print(
                "  %-10s %+10.1f ns/事件  %+.1f%%"
                % (row["name"], row["delta"], row["delta"] / total_ns * 100)
            )

    print("\n-- 墙判定 --")
    negative = [
        row
        for row in measured
        if row["delta"] is not None and total_ns and row["delta"] / total_ns < -0.05
    ]
    candidates = [row for row in measured if row["delta"] is not None]
    if len(measured) < 2:
        print("⚠ 有效档不足 2 个，无法定位")
        wall_valid = False
    elif negative:
        print(
            "⚠ 检测到大幅负增量：%s；墙梯不满足单调性，本次不输出主墙。"
            % ", ".join(row["name"] for row in negative)
        )
        print("  这表示缓存、调度、背压或运行状态噪声已大于段成本；请增大 N，并重复交错运行。")
        wall_valid = False
    elif not candidates or max(row["delta"] for row in candidates) <= 0:
        print("⚠ 没有稳定的正增量，当前数据不足以定位主墙。")
        wall_valid = False
    else:
        top = max(candidates, key=lambda row: row["delta"])
        print(
            "主墙 = %s：相对上一档增量 %+0.1f ns/事件，占末档总成本 %.1f%%"
            % (top["name"], top["delta"], top["delta"] / total_ns * 100)
        )
        if top["cpu"] is None or not args.cores:
            print("  CPU 归属 n/a：采样不足或未提供核数。")
        else:
            ratio = top["cpu"] / (args.cores * 100)
            if ratio >= 0.5:
                kind = "忙墙（计算侧占用较高）"
            elif ratio <= 0.15:
                kind = "等/供给墙（CPU 较低，优先查 TCP、窗口背压或 sink 等待）"
            else:
                kind = "混合墙（并行度未完全打满）"
            print(
                "  CPU %.0f%% avg / %.0f%% max（%d 核总容量约 %.0f%%）→ %s"
                % (top["cpu"], top["cpu_peak"], args.cores, ratio * 100, kind)
            )
        wall_valid = True

    # Health counters are interval deltas; sum only the application window,
    # excluding the one-row __wf_sentinel window in every stage.
    appended = metric_sum(args.metrics, "window", "append_total", "sdm_event")
    expected_append = args.events * sum(name not in append_cut for name in stage_names)
    received = metric_sum_labels(args.metrics, "receiver", "rows_total", "auth_tcp")
    processed = metric_sum(args.metrics, "rule", "events_total", "sdm_alert")
    emitted = metric_sum(args.metrics, "alert", "emitted_total", "sdm_alert")
    dispatched = metric_sum(args.metrics, "alert", "dispatch_total", None)

    # These counters indicate data loss, a failed decode/route, or an
    # incomplete sink path.  Any non-zero value makes EPS directional only.
    bad_names = (
        ("append_failed_total", "alert"),
        ("dropped_late_total", "router"),
        ("cursor_gap_total", "rule"),
        ("decode_errors_total", "receiver"),
        ("read_errors_total", "receiver"),
        ("window_miss_total", "receiver"),
        ("route_errors_total", "router"),
        ("late_total", "window"),
        ("stats_over_limit_total", "rule"),
        ("sink_dispatch_failed_total", "alert"),
        ("channel_send_failed_total", "alert"),
        ("drain_dropped_records_total", "alert"),
        ("no_sink_records_total", "alert"),
        ("escalate_failed_total", "alert"),
    )
    bad: dict[str, int] = {}
    for name, stage in bad_names:
        value = metric_sum(args.metrics, stage, name)
        if value:
            bad[f"{stage}.{name}"] = value

    # These are useful context for a wall diagnosis but are not, by
    # themselves, proof of lost records: memory eviction is a normal bounded
    # window action, channel_full means the producer waited for the consumer,
    # and time eviction is the configured retention policy.
    soft_names = (
        ("memory_evicted_total", "evictor"),
        ("time_evicted_total", "evictor"),
        ("channel_full_total", "alert"),
    )
    soft: dict[str, int] = {}
    for name, stage in soft_names:
        value = metric_sum(args.metrics, stage, name)
        if value:
            soft[f"{stage}.{name}"] = value

    print("\n-- 健康 --")
    if missing_indices:
        print("✗ sentinel：%d / %d 档有有效完成记录" % (len(stage_names) - len(missing_indices), len(stage_names)))
    else:
        print("sentinel：%d / %d 档四元组完整" % (len(stage_names), len(stage_names)))
    print(
        "appended：%s / %s（recv/decode 档不 append）= %.1f%%"
        % (
            fmt_int(appended),
            fmt_int(expected_append),
            appended / expected_append * 100 if expected_append else 0,
        )
    )
    print("receiver rows：%s；rule events：%s；emitted：%s；dispatch：%s" % (fmt_int(received), fmt_int(processed), fmt_int(emitted), fmt_int(dispatched)))
    print("致命计数器：%s" % (" ".join(f"{key}={value}" for key, value in sorted(bad.items())) or "clean"))
    if soft:
        print("非致命运行计数器：%s" % " ".join(f"{key}={value}" for key, value in sorted(soft.items())))
    if expected_append and appended < expected_append * 0.9:
        print("✗ append 未追平：EPS 不能作为有效吞吐结论，请检查窗口容量、迟到丢弃和 schema。")
    for row in rows:
        if row["name"] != "warmup" and row["samples"] < 3:
            print("⚠ %s 仅有 %d 个资源样本；CPU/RSS 归属不稳定。" % (row["name"], row["samples"]))
    if bad:
        print("✗ 存在非零错误/丢弃计数，上述墙定位只能作为方向。")
    print("口径：%s" % args.context)

    health_failed = (bool(missing_indices) or (expected_append and appended < expected_append * 0.9) or bool(bad))
    # A successful run with a non-monotonic wall is intentionally exit-0: the
    # report is useful, but clearly says that no bottleneck conclusion exists.
    return 1 if health_failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
