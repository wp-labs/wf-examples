#!/usr/bin/env python3
"""屏蔽 close_exec.rs 主循环的分段。用法：python3 shield_close.py <mode>
mode = all | entity | yield | commit
"""
import sys
from pathlib import Path

path = Path("/Users/zuowenjian/devspace/wp-labs/wfusion/wp-reactor/crates/wf-engine/src/match_engine/executor/close_exec.rs")
src = path.read_text()
mode = sys.argv[1] if len(sys.argv) > 1 else "all"

# 先确保是干净优化版（无残留屏蔽标记）
src = src.replace("            // [SHIELD", "            // [SHIELD")

if mode == "all":
    # 屏蔽整个主循环体：在 is_qualified 后插入早退
    old = """            if !is_qualified(close) {
                stats.rejected += 1;
                continue;
            }
            let origin = AlertOrigin::Close {"""
    new = """            if !is_qualified(close) {
                stats.rejected += 1;
                continue;
            }
            // [SHIELD-all] 屏蔽主循环全部逐行计算
            stats.appended += 1;
            continue;
            let origin = AlertOrigin::Close {"""
    assert old in src, "all: anchor not found"
    src = src.replace(old, new)

elif mode == "entity":
    # 屏蔽 entity + wfx_id + summary（保留 yield 循环）
    old_start = """            // entity（连续缓存：q19 同桶 top-10 条共享 scope_key，复用字符串"""
    old_end = """            let summary = build_summary_split(
                &self.plan.name,
                keys,
                &close.scope_key,
                &close.event_step_data,
                &close.close_step_data,
                &origin,
            );
"""
    i = src.find(old_start)
    j = src.find(old_end)
    assert i >= 0 and j >= 0 and j > i, "entity: anchors not found"
    j += len(old_end)
    stub = """            // [SHIELD-entity] 屏蔽 entity + wfx_id + summary
            let entity_id = String::new();
            let wfx_id = String::new();
            let summary = String::new();
            let fired_at = String::new();
            let origin_str = String::new();
"""
    src = src[:i] + stub + src[j:]

elif mode == "yield":
    # 屏蔽 yield 字段循环（保留 entity/wfx_id/summary 和落列）
    old_start = """            // Field yields: resolve each from keys / field_values / bind."""
    old_end = """            staged_rows.push(builder.take_staged());
"""
    i = src.find(old_start)
    j = src.find(old_end)
    assert i >= 0 and j >= 0 and j > i, "yield: anchors not found"
    j += len(old_end)
    # 落列段需要 wfx_id 等；staged_rows 需要是空 Vec
    stub = """            // [SHIELD-yield] 屏蔽 yield 字段循环
            let staged: Vec<(
                usize,
                wp_model_core::model::DataType,
                wp_model_core::model::Value,
            )> = Vec::new();
            staged_rows.push(staged);
"""
    src = src[:i] + stub + src[j:]

elif mode == "commit":
    # 屏蔽 commit_close_rows_batch（保留主循环）
    old = """        if !wfx_ids.is_empty() {
            builder.commit_close_rows_batch("""
    new = """        // [SHIELD-commit] 屏蔽 commit_close_rows_batch
        if false {
            builder.commit_close_rows_batch("""
    assert old in src, "commit: anchor not found"
    src = src.replace(old, new)

path.write_text(src)
print(f"shield mode={mode} applied")
