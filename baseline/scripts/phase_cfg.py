#!/usr/bin/env python3
"""detect 全局周期基线供给的相位折叠口径（单一事实源，2026-09-08）。

所有参与"事件打标 / 供给聚合（export CSV、PG SQL）/ 判定 join"的代码必须用
同一 period/bucket 折叠，否则事件相位与供给行相位错位 → join miss。
   phase(ts) = ((ts_sec) % PERIOD_S) // BUCKET_S   （秒粒度，epoch 对齐）

- 事件打标：gen_metrics_live/phase/gen_detect_data 按事件 event_time 折桶；
- 供给聚合：export_baseline_ref.py 按记录 win_start（epoch 纳秒）折桶；
  PG 供给 SQL 按 win_start::timestamp 的 epoch 秒折桶（knowdb.pg.toml）；
- 桶编码为**十进制字符串**（chars）：引擎 join 键不接受 float（checker），
  故 provider 窗/事件字段都用 chars，等值 join。

改 period/bucket 必须同步：本文件 + knowdb.pg.toml query 字面量（+ run_phase
的 conf/生成器已是同一 240/15 演示档）。
"""
PERIOD_S = 240
BUCKET_S = 15
BUCKETS = PERIOD_S // BUCKET_S  # 16

NS = 1_000_000_000


def bucket_of_ns(t_ns: int) -> int:
    """事件/窗起点（epoch 纳秒）→ 相位桶 0..BUCKETS-1（秒粒度折叠）。"""
    return int(((t_ns // NS) % PERIOD_S) // BUCKET_S)


def bucket_of_epoch_sec(epoch_s: int) -> int:
    """供给 SQL 同口径（epoch 秒直接折叠）。"""
    return int((epoch_s % PERIOD_S) // BUCKET_S)


def label(bucket: int) -> str:
    """桶的 chars 编码：'p0'..'p15'（带字母 → loader 按 TEXT 建列；join 键接受
    chars 不接受 float，且纯数字字符串会被 loader 推断为 REAL 导致装载失败）。"""
    return f"p{bucket}"
