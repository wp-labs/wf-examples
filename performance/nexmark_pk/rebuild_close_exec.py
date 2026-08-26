#!/usr/bin/env python3
"""基于 HEAD 版 close_exec.rs 应用 4 处优化，生成优化版。"""
import re
from pathlib import Path

path = Path("/Users/zuowenjian/devspace/wp-labs/wfusion/wp-reactor/crates/wf-engine/src/match_engine/executor/close_exec.rs")
src = path.read_text()

# 1. import 行：加 CVec/cscalar_to_value + split 函数
src = src.replace(
    "use crate::alert::{AlertColumnBuilder, AlertOrigin, OutputRecord};\nuse crate::error::CoreResult;\nuse crate::match_engine::match_engine::{\n    CloseOutput, Event, StepData, Value, WindowLookup, eval_field_value, field_ref_name,\n    value_to_string,\n};\n\nuse super::EachDirectBatchStats;\nuse super::RuleExecutor;\nuse super::YieldKind;\nuse super::alert::{build_summary, build_wfx_id, format_nanos_utc, now_nanos};",
    "use crate::alert::{AlertColumnBuilder, AlertOrigin, OutputRecord};\nuse crate::error::CoreResult;\nuse crate::match_engine::columnar::{CVec, cscalar_to_value};\nuse crate::match_engine::match_engine::{\n    CloseOutput, Event, StepData, Value, WindowLookup, eval_field_value, field_ref_name,\n    value_to_string,\n};\n\nuse super::EachDirectBatchStats;\nuse super::RuleExecutor;\nuse super::YieldKind;\nuse super::alert::{\n    build_summary, build_summary_split, build_wfx_id, build_wfx_id_split, format_nanos_utc, now_nanos,\n};",
)

# 2. Lit yield 注册段：加 const_yields
src = src.replace(
    "        // Batch-constant literal yields: coerced + exported once here and\n        // registered as constant columns (per-row staging skipped, gap-filled\n        // by the commit). Field yields register as ordinary columns.\n        for (field, (name, field_type)) in",
    "        // Batch-constant literal yields: coerced + exported once here and\n        // registered as constant columns (per-row staging skipped, gap-filled\n        // by the commit). Field yields register as ordinary columns.\n        // 记录已注册 const 的字段（层 2 Part B：主循环跳过这些字段的逐行\n        // stage——commit gap-fill 常量，字节一致）。\n        let mut const_yields: std::collections::HashSet<&str> = std::collections::HashSet::new();\n        for (field, (name, field_type)) in",
)
src = src.replace(
    "                    if let Err(e) = builder.register_yield_column(name, Some((meta, model_value))) {\n                        log::warn!(\"alert export error: {e}\");\n                        stats.failed = closes.len();\n                        return stats;\n                    }\n                }",
    "                    if let Err(e) = builder.register_yield_column(name, Some((meta, model_value))) {\n                        log::warn!(\"alert export error: {e}\");\n                        stats.failed = closes.len();\n                        return stats;\n                    }\n                    const_yields.insert(field.name.as_str());\n                }",
)

# 3. fired_at_cache 后加 entity 缓存 + prepared
src = src.replace(
    "        let mut fired_at_cache: Option<(i64, String)> = None;\n\n        'close: for close in closes {",
    "        let mut fired_at_cache: Option<(i64, String)> = None;\n        // entity 连续缓存: 同 scope_key 相邻 close 复用 entity_id（q19 同桶\n        // top-10 条共享 auction, 免每 close 一次 resolve + value_to_string）。\n        let mut last_entity_key: Option<&[Value]> = None;\n        let mut last_entity_id: String = String::new();\n\n        // 层 1（2026-08-25）：列式批级 General yield cell（fmt/strftime/\n        // count_char——close_batch_prepare 物化引用字段为 Arrow 列并编译求值，\n        // 与 each 列式路径同一编译入口）。槽位 None（无 General / 编译失败 /\n        // 类型不一致）→ 循环内逐行解释回退。\n        let prepared = self.close_batch_prepare(closes);\n\n        'close: for (row_idx, close) in closes.iter().enumerate() {",
)

# 4. entity 段：缓存版
src = src.replace(
    "            // entity\n            let entity_id: String = if let Some(s) = entity_const {\n                s.to_string()\n            } else {\n                // eval_entity_id → eval_yield_expr falls back to an empty\n                // string when the field is absent (never errors) — mirror that\n                // instead of failing the close.\n                resolve_close_field(close, keys, entity_field_name.unwrap_or(\"\"))\n                    .map(|v| value_to_string(&v))\n                    .unwrap_or_default()\n            };",
    "            // entity（连续缓存：q19 同桶 top-10 条共享 scope_key，复用字符串\n            // 免每 close 一次 resolve + value_to_string）\n            let entity_id: String = if let Some(s) = entity_const {\n                s.to_string()\n            } else {\n                let key = close.scope_key.as_slice();\n                if last_entity_key == Some(key) {\n                    last_entity_id.clone()\n                } else {\n                    // eval_entity_id → eval_yield_expr falls back to an empty\n                    // string when the field is absent (never errors) — mirror that\n                    // instead of failing the close.\n                    let s = resolve_close_field(close, keys, entity_field_name.unwrap_or(\"\"))\n                        .map(|v| value_to_string(&v))\n                        .unwrap_or_default();\n                    last_entity_key = Some(key);\n                    last_entity_id = s.clone();\n                    s\n                }\n            };",
)

# 5. wfx_id/summary：split 版本
src = src.replace(
    "            // wfx_id / summary need the combined step data (same byte stream\n            // as build_wfx_id/build_summary on the per-record path).\n            let all_step_data = combine_step_data(close);\n            let wfx_id = build_wfx_id(\n                &self.plan.name,\n                &close.scope_key,\n                &fired_at,\n                &all_step_data,\n                &origin,\n            );\n            let summary = build_summary(\n                &self.plan.name,\n                keys,\n                &close.scope_key,\n                &all_step_data,\n                &origin,\n            );",
    "            // wfx_id / summary：split 版本直接引用 event/close 两段 step_data，\n            // 免 `combine_step_data` 深克隆（StepData 含 field_values HashMap，\n            // q19 top-10 每桶 10 条 × 每 close 全量深拷是纯浪费——两函数只用\n            // label + measure_value）。字节流 = per-record 路径，测试锁定。\n            let wfx_id = build_wfx_id_split(\n                &self.plan.name,\n                &close.scope_key,\n                &fired_at,\n                &close.event_step_data,\n                &close.close_step_data,\n                &origin,\n            );\n            let summary = build_summary_split(\n                &self.plan.name,\n                keys,\n                &close.scope_key,\n                &close.event_step_data,\n                &close.close_step_data,\n                &origin,\n            );",
)

# 6. yield 循环：enumerate + const_yields 跳过 + prepared cell + ctx lazy
src = src.replace(
    "            let mut ctx: Option<Event> = None;\n            for (field, (name, field_type)) in\n                self.plan.yield_plan.fields.iter().zip(yield_specs.iter())\n            {\n                let value = match &field.value {\n                    Expr::Field(_) => resolve_close_field(close, keys, field_ref_name_of(&field.value))\n                        .unwrap_or_else(|| Value::Str(String::new().into())),\n                    general => {\n                        let ctx = ctx.get_or_insert_with(|| {\n                            build_eval_context(\n                                keys,\n                                &close.scope_key,\n                                &all_step_data,\n                                &close.bind_data,\n                                &[],\n                                None,\n                                &self.close_ctx_fields,\n                            )\n                        });",
    "            let mut ctx: Option<Event> = None;\n            for (field_idx, (field, (name, field_type))) in\n                self.plan.yield_plan.fields.iter().zip(yield_specs.iter()).enumerate()\n            {\n                let value = match &field.value {\n                    // const 列（Lit yield）已在 execute 顶部注册 + 校验——跳过\n                    // 逐行 stage（commit 对缺 staged cell 的行 gap-fill 常量，\n                    // 字节一致；2026-08-25 层 2 Part B：省 Lit 字段的\n                    // coerce/export/staged push，q12/q15-q19 通用）。\n                    // 防御：非 const 注册的 Lit（理论不可达）仍走取值 + stage。\n                    Expr::Number(_) | Expr::StringLit(_) | Expr::Bool(_)\n                        if const_yields.contains(field.name.as_str()) =>\n                    {\n                        continue;\n                    }\n                    Expr::Number(n) => Value::Number(*n),\n                    Expr::StringLit(s) => Value::Str(s.clone().into()),\n                    Expr::Bool(b) => Value::Bool(*b),\n                    Expr::Field(_) => resolve_close_field(close, keys, field_ref_name_of(&field.value))\n                        .unwrap_or_else(|| Value::Str(String::new().into())),\n                    general => {\n                        // 列式批级 cell：命中直接取（null 行 → 空串，同解释路径\n                        // None→\"\"）；槽位 None → 逐行回退（轻量 ctx 求值）。\n                        match prepared\n                            .general_cvecs\n                            .get(field_idx)\n                            .and_then(|c| c.as_ref())\n                        {\n                            Some(cvec) => match cvec.scalar_at(row_idx) {\n                                Some(s) => cscalar_to_value(&s),\n                                None => Value::Str(SmolStr::default()),\n                            },\n                            None => {\n                                let ctx = ctx.get_or_insert_with(|| {\n                                    let all_step_data = combine_step_data(close);\n                                    build_eval_context(\n                                        keys,\n                                        &close.scope_key,\n                                        &all_step_data,\n                                        &close.bind_data,\n                                        &[],\n                                        None,\n                                        &self.close_ctx_fields,\n                                    )\n                                });",
)

# 7. yield 循环尾部闭合（原 None 分支的 }) 需要多一层 for prepared match）
src = src.replace(
    "                        with_yield_eval_scope(|| {\n                            eval_yield_expr_with_meta(general, ctx, yield_meta)\n                        })\n                        .expect(\"eval_yield_expr_with_meta never returns None\")\n                    }\n                };",
    "                                with_yield_eval_scope(|| {\n                                    eval_yield_expr_with_meta(general, ctx, yield_meta)\n                                })\n                                .expect(\"eval_yield_expr_with_meta never returns None\")\n                            }\n                        }\n                    }\n                };",
)

# 8. 末尾追加 CloseBatchVecs + close_batch_prepare（放在 combine_step_data 后）
#    幂等：先移除已存在的块再追加。
import re as _re
block_start = src.find("/// 列式 close 的 General yield 批级求值状态（层 1，2026-08-25）：")
if block_start >= 0:
    # 找到块结束：下一个顶层 fn/impl 前（combine_step_plans）
    next_anchor = src.find("fn combine_step_plans<'a>(", block_start)
    if next_anchor >= 0:
        src = src[:block_start] + src[next_anchor:]

close_batch = '''
/// 列式 close 的 General yield 批级求值状态（层 1，2026-08-25）：
/// [`RuleExecutor::close_batch_prepare`] 把一批 `CloseOutput` 引用字段物化为
/// Arrow 列 → `ColumnarBatch` 视图 → 编译 General yield（fmt/strftime/
/// count_char 等）→ `eval_vec` 批量 cell。槽位按 **yield 字段位置** 索引
/// （与 `yield_plan.fields` 对齐；Lit/Field 为 `None`）；`None` = 无 General /
/// 编译失败 / 字段类型不一致 → 逐行解释回退（与 each 路径同款契约）。
#[derive(Default)]
pub(crate) struct CloseBatchVecs {
    pub(crate) general_cvecs: Vec<Option<CVec>>,
}

impl RuleExecutor {
    /// Compile + batch-evaluate the columnar close General-yield state for one
    /// `closes` batch（窗口 close 一次调用，语义 = 解释路径的
    /// `build_eval_context`（Named 窄化/All）+ `eval_yield_expr_with_meta`）。
    ///
    /// 只物化 General 表达式实际引用的普通字段 + 键名（ctx 恒注入键）；缺失
    /// 字段 → 不建列 → `ColumnarBatch` 解析为 Null ColKind → null cell →
    /// 空串，与解释路径 None→"" 一致。Number→Float64 / Str→Utf8 / Bool→
    /// Boolean 列（`cscalar_to_value` 还原为原 `Value`，渲染字节一致）。
    pub(crate) fn close_batch_prepare(&self, closes: &[CloseOutput]) -> CloseBatchVecs {
        let n = closes.len();
        let slots = self.plan.yield_plan.fields.len();
        if n == 0 {
            return CloseBatchVecs {
                general_cvecs: (0..slots).map(|_| None).collect(),
            };
        }
        // 1. 引用字段集 = 键名（ctx 无条件注入）∪ General yield 引用的普通字段
        //    （close 编译不内联 let → 非内联收集，保持一致）
        let ref_fields = self.yield_ref_fields(false);
        if ref_fields.is_empty() {
            return CloseBatchVecs {
                general_cvecs: (0..slots).map(|_| None).collect(),
            };
        }
        // 2. 统一物化器 + 槽位编译（层 2 收口，`RuleExecutor::compile_general_slots`）；
        //    物化失败（类型不一致/结构化值）→ 整批回退逐行（保守）。close 传空
        //    lets：解释 close 路径（build_eval_context）无 let 视图，内联会分叉。
        let keys: &[FieldRef] = &self.plan.match_plan.keys;
        CloseBatchVecs {
            general_cvecs: self.compile_general_slots(
                &ref_fields,
                n,
                |row, name| resolve_close_field(&closes[row], keys, name),
                &[],
            ),
        }
    }
}
'''
# 插入 close_batch 块到 combine_step_plans 前（幂等：删除旧块后位置重新查找）
insert_at = src.find("fn combine_step_plans<'a>(")
assert insert_at >= 0, "combine_step_plans anchor missing"
src = src[:insert_at] + close_batch + "\n" + src[insert_at:]

path.write_text(src)
print("close_exec.rs 优化版重建完成")
