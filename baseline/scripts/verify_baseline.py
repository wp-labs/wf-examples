#!/usr/bin/env python3
"""baseline 生产对拍校验（设计方案 §4.1-§4.2）.

对拍口径：把输入 metrics 事件逐 (entity, metric) 聚合出总量 (n, sum, sum_sq)，
再与 baseline_out 输出的全部基线记录同键总量对比——两者必须一致。

引擎数值口径对齐：
- value 为 float 列，引擎累加时按 `float → i128 截断`（向零取整, D8），
  sumsq 为截断值的平方（饱和累加不会在小数值触发）。
- 输出 n/sum/sum_sq 为数值（float 编码的整数总量）。

用法: verify_baseline.py <events.jsonl> <baseline.ndjson>
"""
import json
import sys


def load_jsonl(path):
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def trunc_i128(v):
    """镜像引擎 Float64 → i128 的截断（向零取整）。"""
    f = float(v)
    return int(f)  # Python int(float) 同样向零截断


def main() -> int:
    ev_path, out_path = sys.argv[1], sys.argv[2]
    events = load_jsonl(ev_path)
    outputs = load_jsonl(out_path)

    if not outputs:
        print("ERROR: baseline_out 为空——stats 规则未产出任何基线记录")
        return 1

    # 事件侧逐 (entity, metric) 总量
    expected = {}
    for ev in events:
        if ev.get("value") is None:
            continue
        key = (ev.get("entity"), ev.get("metric"))
        if key[0] is None or key[1] is None:
            continue
        v = trunc_i128(ev["value"])
        e = expected.setdefault(key, [0, 0, 0])
        e[0] += 1
        e[1] += v
        e[2] += v * v

    # 输出侧逐键汇总全部窗口
    got = {}
    bad_rows = 0
    for r in outputs:
        key = (r.get("entity"), r.get("metric"))
        if key[0] is None or key[1] is None:
            continue
        n, s, ss = (float(r.get("n", 0)), float(r.get("sum", 0)), float(r.get("sum_sq", 0)))
        if n < 1 or ss < 0:
            bad_rows += 1
        g = got.setdefault(key, [0.0, 0.0, 0.0])
        g[0] += n
        g[1] += s
        g[2] += ss

    mismatches = []
    for key in set(expected) | set(got):
        e = expected.get(key)
        g = got.get(key)
        if e is None or g is None:
            mismatches.append((key, e, g, "键集合不一致"))
            continue
        # 各维: 相对 1e-9 / 绝对 1e-3 容差
        for name, ev, gv in (("n", e[0], g[0]), ("sum", e[1], g[1]), ("sum_sq", e[2], g[2])):
            if not (abs(gv - ev) <= 1e-3 + 1e-9 * max(abs(gv), abs(ev))):
                mismatches.append((key, e, g, f"{name} 不一致: event={ev} out={gv}"))

    print(f"事件行数: {len(events)}  基线记录数: {len(outputs)}  键数: {len(got)}")
    if bad_rows:
        print(f"WARN: {bad_rows} 行基线记录 n<1 或 sum_sq<0（预期无）")
    if mismatches:
        print(f"ERROR: {len(mismatches)} 处不一致")
        for m in mismatches[:10]:
            print("  ", m)
        return 1

    sample_keys = list(got)[:3]
    print("对拍通过：n/sum/sum_sq 总量逐键一致")
    for k in sample_keys:
        print(f"  样例键 {k}: n={got[k][0]:.0f} sum={got[k][1]:.0f} sum_sq={got[k][2]:.0f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
