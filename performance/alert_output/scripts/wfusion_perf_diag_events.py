#!/usr/bin/env python3
"""Generate deterministic NGSOC-style sdm_event JSONL for wfusion_new."""

from __future__ import annotations

import argparse
from functools import lru_cache
import json
import sys
from pathlib import Path


SAMPLE_PATH = Path(__file__).resolve().parents[1] / "models" / "samples" / "ngsoc_alert_23.json"


@lru_cache(maxsize=1)
def load_sample() -> dict[str, object]:
    """Load and validate the canonical 23-field NGSOC sample."""
    try:
        sample = json.loads(SAMPLE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"无法读取 NGSOC 样本 {SAMPLE_PATH}: {exc}") from exc
    if not isinstance(sample, dict):
        raise RuntimeError(f"NGSOC 样本必须是 JSON 对象：{SAMPLE_PATH}")
    payload_fields = [key for key in sample if not key.startswith("_")]
    if len(payload_fields) != 23:
        raise RuntimeError(
            f"NGSOC 样本业务字段数为 {len(payload_fields)}，期望 23：{SAMPLE_PATH}"
        )
    for key in ("event_id", "source_original_event_id"):
        if key not in sample:
            raise RuntimeError(f"NGSOC 样本缺少 {key}：{SAMPLE_PATH}")
    return sample


def build_event(event_id: str) -> dict[str, object]:
    """Return one NGSOC alert with 23 payload fields.

    ``_stream``, ``_window`` and ``_timestamp`` are frame routing metadata and
    are deliberately kept outside the payload count.  The payload is the
    normalized NGSOC shape used by the earlier alert benchmark, while the
    stream tag remains ``sdm_event`` so the current wfusion_new rule can be
    measured without changing the pipeline under test.
    """
    # The nested objects are immutable for this generator, so a shallow copy
    # is enough while avoiding a deep-copy cost for every event in large runs.
    event = dict(load_sample())
    event["event_id"] = event_id
    event["source_original_event_id"] = event_id
    return event


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, required=True, help="number of events")
    parser.add_argument("--prefix", default="wfdiag", help="event_id prefix")
    parser.add_argument("--output", type=Path, required=True, help="JSONL output")
    args = parser.parse_args()
    if args.count <= 0:
        parser.error("--count must be greater than zero")
    if not args.prefix or any(ch.isspace() for ch in args.prefix):
        parser.error("--prefix must be non-empty and contain no whitespace")
    return args


def main() -> int:
    args = parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    try:
        with args.output.open("w", encoding="utf-8", newline="\n") as stream:
            for index in range(1, args.count + 1):
                event = build_event(f"{args.prefix}_{index:012d}")
                stream.write(json.dumps(event, ensure_ascii=False, separators=(",", ":")))
                stream.write("\n")
    except RuntimeError as exc:
        print(f"错误：{exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
