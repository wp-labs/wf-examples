#!/usr/bin/env python3
"""分析 macOS `sample` 输出（旧版调用图格式）：
- 聚合每个符号的 inclusive 样本数（树中每帧计数 = 该帧在栈上出现的次数）
- 用 v0 mangled 名里的十进制长度前缀段做轻量 demangle（`10wf_runtime` → wf_runtime）
- 输出 top N 热点 + 线程/等待开销占比
- 额外输出「Sort by top of stack」段的**自耗时** top N（栈顶 = 该函数自身
  执行，不含子调用——定位段内热点函数的直接证据）

用法: analyze_sample.py <sample.txt> [top_n]
"""
import re
import sys
from collections import defaultdict

path = sys.argv[1]
top_n = int(sys.argv[2]) if len(sys.argv) > 2 else 40
text = open(path, errors="replace").read()

# ---- 1. 提取 Call graph 段（到 Binary Images 为止）----
start = text.find("Call graph:")
end = text.find("Binary Images")
if start < 0:
    print("未找到 Call graph 段")
    sys.exit(1)
cg = text[start:end] if end > start else text[start:]

# ---- 2. 逐帧解析: 缩进标记 + 计数 + 符号 ----
# 帧行形如:  `    +                       ! : 604 _RNC... (in wfusion) + 123 [0x...]`
# 计数可能以「空格缩进 + 数字」出现在符号名之前。
frame_re = re.compile(
    r"^\s*(?:[+!:|]\s*)*(\d+)\s+(_[A-Za-z0-9_]+|[\w_.:]+)\s+\(in\s+([^)]+)\)"
)
incl = defaultdict(int)          # 符号(inclusive) -> 样本数
dylib_tot = defaultdict(int)     # 所在镜像 -> 样本数（wfusion vs 系统库）
thread_tot = 0
for line in cg.splitlines():
    m = re.match(r"^\s*(\d+)\s+Thread_\S+", line)
    if m:
        thread_tot += int(m.group(1))
        continue
    m = frame_re.match(line)
    if m:
        n, sym, image = int(m.group(1)), m.group(2), m.group(3)
        incl[sym] += n
        dylib_tot[image] += n

# ---- 3. 轻量 demangle（v0：长度前缀段顺序解析；`10wf_runtime` → wf_runtime）----
# v0 段之间无分隔符（长度前缀定界），不能正则 findall——贪婪匹配会把 `_`
# 分隔符/后续段吞进前一段。且 crate 消歧符 `Cs<base62>_`、回退标记 `B<n>_`、
# 模块标记 `Ms<n>_` 里的数字会被误读为长度前缀（如 `6BMhHDU` 假段）。
# 做法：先剥掉这三类 token，再顺序扫描：读数字 → 截取声明长度字符 → 校验。
_CLEAN = [
    (re.compile(r"Cs[0-9a-zA-Z]+_"), ""),   # crate 消歧符 + 分隔符
    (re.compile(r"[A-Za-z]\d+_"), ""),     # 回退/模块标记（B7_/Ms0_ 等）
]


def demangle(sym: str) -> str:
    s = sym
    for rx, rep in _CLEAN:
        s = rx.sub(rep, s)
    segs = []
    i = 0
    n = len(s)
    while i < n:
        if s[i].isdigit():
            j = i
            while j < n and s[j].isdigit():
                j += 1
            declared = int(s[i:j])
            if 0 < declared <= n - j and len(s[j : j + declared]) == declared:
                segs.append(s[j : j + declared])
                i = j + declared
            else:
                i += 1  # hash/计数碎片，跳过一位继续
        else:
            i += 1  # 标记字符（Nv/Nt/C/Ms/B/E/s 等）
    if not segs:
        return sym
    return "::".join(segs)


readable = defaultdict(int)
raw_to_read = {}
for sym, n in incl.items():
    r = demangle(sym)
    readable[r] += n
    raw_to_read[sym] = r

# ---- 4. 输出 inclusive ----
print(f"线程根样本合计（含空闲等待）: {thread_tot}")
print(f"镜像分布: " + ", ".join(f"{k}={v} ({100.0*v/thread_tot:.0f}%)" if thread_tot else f"{k}={v}"
      for k, v in sorted(dylib_tot.items(), key=lambda kv: -kv[1])[:5]))

print(f"\n── 热点 top {top_n}（inclusive，可读名，含系统/等待）──")
for name, n in sorted(readable.items(), key=lambda kv: -kv[1])[:top_n]:
    pct = 100.0 * n / thread_tot if thread_tot else 0
    print(f"  {n:>6} ({pct:5.1f}%)  {name}")

# 只看 wf_engine / wf_runtime 的（引擎真实热点；名字里可能带 crate hash 噪音，
# 子串匹配并从 crate 名处截断显示）
print(f"\n── 引擎侧（wf_engine/wf_runtime）top 25 ──")
eng = []
for name, n in readable.items():
    for crate in ("wf_engine", "wf_runtime"):
        idx = name.find(crate)
        if idx >= 0:
            eng.append((name[idx:], n))
            break
agg = defaultdict(int)
for name, n in eng:
    agg[name] += n
for name, n in sorted(agg.items(), key=lambda kv: -kv[1])[:25]:
    pct = 100.0 * n / thread_tot if thread_tot else 0
    print(f"  {n:>6} ({pct:5.1f}%)  {name}")

# ---- 5. 自耗时：Sort by top of stack 段（栈顶 = 该函数自身执行）----
stos_start = text.find("Sort by top of stack")
stos_end = text.find("Binary Images", stos_start) if stos_start >= 0 else -1
if stos_start < 0 or stos_end < 0:
    print("\n── 自耗时: 未找到 Sort by top of stack 段 ──")
else:
    self_raw = defaultdict(int)
    for line in text[stos_start:stos_end].splitlines():
        # 形如: `        _RINv... (in wfusion)        291`
        m = re.match(r"^\s*(_\S+)\s+\(in\s+([^)]+)\)\s+(\d+)\s*$", line)
        if not m:
            continue
        sym, image, n = m.group(1), m.group(2), int(m.group(3))
        # 系统空闲/等待帧（kevent/cvwait/信号量/时钟）不算引擎自耗时
        if image.startswith("libsystem") or image.startswith("libdispatch"):
            continue
        self_raw[demangle(sym)] += n

    # 自耗时 top 30（仅可执行代码；排除已知空闲系统调用）
    print("\n── 自耗时 top 30（栈顶，仅非系统空闲帧）──")
    for name, n in sorted(self_raw.items(), key=lambda kv: -kv[1])[:30]:
        pct = 100.0 * n / thread_tot if thread_tot else 0
        print(f"  {n:>6} ({pct:5.1f}%)  {name}")

    # 引擎侧自耗时（wf_engine/wf_runtime）
    print("\n── 引擎侧自耗时 top 25（段内热点函数）──")
    eng_self = defaultdict(int)
    for name, n in self_raw.items():
        for crate in ("wf_engine", "wf_runtime"):
            idx = name.find(crate)
            if idx >= 0:
                eng_self[name[idx:]] += n
                break
    for name, n in sorted(eng_self.items(), key=lambda kv: -kv[1])[:25]:
        pct = 100.0 * n / thread_tot if thread_tot else 0
        print(f"  {n:>6} ({pct:5.1f}%)  {name}")
