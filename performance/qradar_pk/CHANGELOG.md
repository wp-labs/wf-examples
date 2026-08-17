# qradar_pk — Case Changelog

本文件记录 **qradar_pk**（450 条有状态规则高压吞吐 + QRadar EP 对标）这个 case 的
变更与调查记录。基准数据见 `README.md`、`PK_REPORT_MAC.md`（M3 Max）、
`PK_REPORT_LINUX.md`（Linux 8 核对等口径）。

---

## [2026-08-18] 大 N 冻结调查（10M 卡死 → 窗口反压）

> 完整根因分析与设计方案：wp-reactor `docs/issues/window-overload-drop-vs-backpressure.md`

### 现象

- `./run.sh 10000000`（1000 万事件）在 Linux 8 核云主机（AMD EPYC，30 GiB）**卡死**：
  `wfgen send` 永久阻塞、脚本停在 step 3，daemon 陷入内存驱逐风暴
- 本地 3M 复现：**72 条驱逐、RSS 12.3GB、#18 门禁 FAIL**（1M 基线：驱逐 0 / 6.7GB）
- conn_events 窗口 `memory_bytes` 顶到 4GB cap（`max_window_bytes`），每批新事件被弹掉

### 根因（两段式）

1. **窗口满时丢弃，不对源反压**——`buffer/mod.rs` append 超 `max_window_bytes`
   弹最旧批次（有损），规则（瓶颈 ~150k）永远追不上 → 积压涨到 cap → 丢数据
2. **时间老化被"所有规则已消费"卡住**——`evict_expired` 要求 `expired && consumed`
   （`acked_floor = min_acked`），450 规则 ack 滞后 → 老化无法释放
3. **事件时间压缩放大**——gen_events 1µs/事件把 N 个事件压进 N µs（10M 仅 10s），
   远小于窗口 `over=2m` → 一个桶装下全部 N → 内容 ∝ N

### 缓解（已应用，部分有效非根治）

| 改动 | 效果 |
|---|---|
| `scripts/gen_events.py` event_time 步长 1µs → **300µs** | RSS 12.3→6.9GB；窗口稳态内容 ~1GB（2m × 3333 事件/s）；驱逐仍 65 |
| `conf/wfusion.toml` + `max_ingest_rate=150k` | 驱逐 62；限速须 ≤ 规则实际吞吐（~118k）才有效，但这样测的"最大"是假的且脆弱 |

### 文档

- **wp-reactor** `docs/issues/window-overload-drop-vs-backpressure.md`：根因 + 完整
  设计方案（高/低水位滞回门控 + 背压触发 sweep + Notify，复用 mailbox permits 背压链）

### 待办（治本）

- **wp-reactor 窗口反压修复**：window actor 满时停消费源（mailbox 填满 → 源节流到
  规则速率），无损、自调节、任意 N 内存有界、1M 基线不变
- 反压落地后：**回退 `max_ingest_rate`**，**保留 gen_events 300µs**（合理的事件时间节奏）

### 本轮相关提交

- **wp-reactor** `fd61097` Release 1.0.2（preread 预算 content 记账，P0-②）
- **wp-reactor** `057f691` + `4c405eb`（issue + 设计，合并为单文档）
- **warp-fusion** `e7378fe` Release 0.3.1（对齐 wp-reactor 1.0.2 + shard-frames 分片文件）
