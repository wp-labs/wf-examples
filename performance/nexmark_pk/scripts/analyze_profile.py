import re
import sys
from collections import defaultdict

path = sys.argv[1] if len(sys.argv) > 1 else '/tmp/wf_profile.txt'
lines = open(path).read().splitlines()
start = 23
end = next(i for i, l in enumerate(lines) if 'Sort by top of stack' in l)

# 每个线程段: 以 "Thread_..." 开头(格式 "  \d+ Thread_xxx: name")
threads = []  # (name, [(sym, cnt, depth)])
cur = None
for l in lines[start:end]:
    m = re.match(r'^\s+(\d+) Thread_\d+[;:\s]+(.*)$', l)
    if m:
        cur = {'name': m.group(2), 'total': int(m.group(1)), 'frames': []}
        threads.append(cur)
        continue
    if cur is None:
        continue
    m2 = re.match(r'^\s+[+|!: ]*(\d+)\s+(\S+)\s+\(in\s+([^)]+)\)', l)
    if not m2:
        continue
    indent = len(m2.group(0)) - len(m2.group(0).lstrip())
    sym, lib, cnt = m2.group(2), m2.group(3), int(m2.group(1))
    cur['frames'].append((indent, sym, lib, cnt))

print(f"线程段数: {len(threads)}")
# 每个线程:总样本 = 段头样本;忙 = 非 __psynch/semaphore/kevent 的帧样本总和(粗略:取每帧 self 样本难,用根样本×忙占比近似)
for t in threads:
    total = t['total']
    # 找该线程最深的非等待调用(最后一个非等待帧)
    busy_frames = [f for f in t['frames'] if f[2] not in ('libsystem_kernel.dylib', 'libsystem_pthread.dylib')]
    top_busy = busy_frames[-1] if busy_frames else None
    wait = t['frames'][-1] if t['frames'] else None
    wait_sym = wait[1] if wait else '?'
    if wait_sym in ('__psynch_cvwait', 'semaphore_wait_trap', 'kevent', '_pthread_cond_wait'):
        state = 'WAIT'
    else:
        state = 'BUSY'
    print(f"  {total:6d} {state:4s} thread={t['name'][:46]:46s} top_busy={top_busy[1][:58] if top_busy else '-'}")

# 按顶层业务函数(rule task / parse worker / window actor / sink / source)聚合忙样本
print("\n--- 顶层业务任务忙样本(深度<=6 的 wf 符号,按帧计数) ---")
agg = defaultdict(int)
for t in threads:
    for d, sym, lib, cnt in t['frames']:
        if lib != 'wfusion' or d > 6:
            continue
        short = sym
        for key in ['run_rule_task', 'run_parse_worker', 'run_window_actor', 'run_sink_consumer',
                    'run_metrics_task', 'run_evictor', 'replay_arrow_framed_file', 'run_conv_stage_task',
                    'spawn_rule_tasks', 'process_push', 'process_batch', 'execute_each_direct_batch',
                    'run_metrics', 'route_parse', 'dispatch_parsed']:
            if key in sym:
                short = key
                break
        agg[(d, short)] += cnt
for (d, s), c in sorted(agg.items(), key=lambda x: -x[1])[:20]:
    print(f"  d{d} {c:6d} {s}")
