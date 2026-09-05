# nginx_log_stats — Nginx access 日志统计分析示例

wfusion 的**最小业务示例**：对 Nginx access 日志做**持续**流式统计 + 5xx 突发检测，
页面实时展示。适合作为 `getting_started` 之后了解 **stats 窗口规则**、**match 告警规则**
与 daemon 持续运行形态的起点。

## 快速开始（两种模式）

```bash
cd nginx_log_stats
./run.sh            # ① 持续运行：wfusion daemon + wfgen stream，Ctrl-C 停止
./view.sh           #   另开终端：实时看板（每 3s 自动刷新）→ http://localhost:8123/view/

./smoke.sh          # ② 一次性确定性验证（lint → 生成 → batch 回放 → 摘要）
```

- **持续模式**（演示/联调主路径）：`run.sh` 启动 daemon（TCP :9800）+ wfgen stream
  按**完整场景周期**持续注入（INTERVAL=60 完整跑场景、RATE=0 用场景 gen 100/s，避免
  场景截断循环导致事件时间回绕），引擎输出**实时追加**到 `data/alerts/nginx.ndjson`；
  统计固定桶约每 3~5s 关闭 → 累计请求/分布/时间线持续推进，`view` 每 3s 重读展示；
  可 `./run.sh 30s` 限时自停。
- **冒烟模式**：`smoke.sh` 确定性跑通一遍（生成 2m 样本 → batch 回放 → 断言非空）。

产物（均在 `data/`，已被 `.gitignore` 忽略）：

- `data/alerts/nginx.ndjson` — 统计行 + 告警行（`alert_type` 区分），持续追加
- `data/generated/nginx_access_quick.jsonl` — 冒烟模式生成的样本事件（静态）
- `data/wfusion.log` / `data/logs/*.log` — 引擎 / 注入日志

冒烟实测输出：**345 行**（45 条分桶统计行 + 300 条 5xx 突发告警）。
持续模式（daemon）下统计行每 5s 追加、告警持续增长。

## 查看页面（统计 & 检测看板）

```bash
./view.sh            # 启动本地静态服务并打开 http://localhost:8123/view/
```

页面**直接从文件读取引擎输出**并**每 3s 自动刷新**（持续模式下跟随 `nginx.ndjson` 增长）：

| 区块 | 数据源（直读文件） | 展示 |
|------|--------------------|------|
| 实时访问统计 | `data/alerts/nginx.ndjson` 的 `nginx_status_stats` + `nginx_ip_stats` 行（每 5s 固定桶追加） | 累计请求/独立 IP/累计 5xx/5xx 占比/统计桶数卡片；状态码分布条；请求时间线（每桶）；**独立 IP 列表（Top 10 + 总数）**；分桶 × 状态码表 |
| 5xx 突发检测（实时） | 同上文件 `http_5xx_surge` 行 | 告警数/涉及 IP/首末时间卡片 + 突发明细表（时间/IP/URI） |

也可用「选择文件」在本地直接打开（`file://` 可用）。

## 目录结构

```
nginx_log_stats/
├── run.sh                        # 持续运行：daemon + stream（Ctrl-C 停止 / 传时长限时）
├── smoke.sh                      # batch 一次性确定性验证（lint → 生成 → 回放）
├── view.sh                       # 启动看板本地服务
├── conf/wfusion.toml             # daemon 配置
├── view/index.html               # 实时统计 & 检测看板（直读引擎输出 ndjson，每 3s 自动刷新）
├── connectors/                   # 连接器定义（file 源 / tcp 源 / file json sink）
├── models/
│   ├── schemas/
│   │   ├── nginx.wfs             # nginx_access 事件窗 + nginx_alerts 输出窗
│   │   └── windows.toml          # 窗口物理参数（over_cap / 内存预算）
│   ├── rules/
│   │   ├── 01-stats/nginx_status_stats.wfl   # 状态码统计（stats 窗口，5s 固定桶）
│   │   ├── 01-stats/nginx_ip_stats.wfl       # 按来源 IP 统计（每桶每 IP 一行，供独立 IP 列表）
│   │   └── 02-alert/nginx_5xx_surge.wfl      # 5xx 突发告警（match 规则）
│   ├── scenarios/nginx_access_quick.wfg      # 冒烟演示场景（2m，batch 用）
│   └── scenarios/live/nginx_access_live.wfg   # daemon 持续注入场景（虚拟 2h，防循环回绕冻结）
├── test/wfusion.batch.toml       # batch 回放配置
├── topology/sources/ingress.toml # TCP :9800 输入（daemon 用）
└── topology/sinks/               # 输出路由（nginx_alerts → nginx.ndjson）
```

## 事件模型（`models/schemas/nginx.wfs`）

一条 Nginx access log 抽象为 `nginx_access` 窗口事件：

| 字段 | 类型 | 对应 access log |
|------|------|-----------------|
| `client_ip` | `ip` | `$remote_addr` |
| `method` | `chars` | `$request_method` |
| `uri` | `chars` | `$request_uri` |
| `http_status` | `chars` | `$status` |
| `bytes_sent` | `digit` | `$body_bytes_sent` |
| `user_agent` | `chars` | `$http_user_agent` |
| `event_time` | `time` | `$time_iso8601` |

> 字段命名/取型备注（实现实录）：
> - 避免用 `status` 作字段名（事件过滤中与引擎保留语义冲突，比较恒不命中）；
> - `http_status` 用 `chars` 存码位（"200"/"500"）：场景注入的 digit 值以浮点
>   落列，events 子句数值比较与 stats 分组在该形态下有引擎差异；字符码位的
>   等值过滤与分组是已验证路径。5xx 语义在示例里用精确码位 `== "500"` 表达。

## 规则解读

### 1. 状态码统计 — `nginx_status_stats`（stats 窗口）

```wfl
rule nginx_status_stats {
    events { c : nginx_access }
    stats<5s:fixed> group by (c.http_status) {
        c | count as total;
        c | count as total_5xx where c.http_status == "500";
        c | distinct_count(c.client_ip) as clients;
    }
    entity(digit, 1)
    yield nginx_alerts (
        alert_type = "nginx_status_stats",
        detail = fmt("status={} total={} clients={} 5xx={}",
            c.http_status, stat.value(final(total)),
            stat.value(final(clients)), stat.value(final(total_5xx)))
    )
    ...
}
```

- `stats<5s:fixed>`：5 秒**固定桶**，桶关闭（watermark 越过桶末）时每 (桶 × 状态码) 输出一行；
  演示取 5s 是为了实时看板高频可见新统计（改大即更长时间粒度）；
- 行内容：`status / total（请求量）/ clients（独立客户端数）/ 5xx（其中 5xx 数）`；
- 这就是“流式报表”：改 `group by (c.uri)` 即得 URI 维度流量分布，改 `stats<1d:fixed>`
  即得按天汇总。

### 2. 5xx 突发告警 — `nginx_5xx_surge`（match 规则）

```wfl
rule nginx_5xx_surge {
    events {
        c : nginx_access
            && c.http_status == "500"
    }
    match<client_ip:1m> {
        on event { c | count >= 3; }
    } -> score(70.0)
    entity(ip, c.client_ip)
    ...
}
```

- 只接收 5xx 事件，按 `client_ip` 分窗（1 分钟）；
- 同一 IP 在窗口内第 3 次 5xx 时立即产出 `http_5xx_surge` 告警（`on event` 触发，`origin=event`）。

## 场景注入说明

`models/scenarios/nginx_access_quick.wfg` 模拟 2 分钟流量（100 事件/s），两类注入：

| 类 | 占比 | 形态 | 预期 |
|----|------|------|------|
| `miss` | 90% | 单条 `http_status="200"` | 正常流量（统计入 200 组） |
| `hit` | 10% | 同一 `client_ip` 连续 4 条 `http_status="500"` | 触发 `http_5xx_surge`（每突发 1 条） |

`expect` 断言 hit 类对 `nginx_5xx_surge` 的命中率 ≥ 75%，miss 误报 ≤ 1%。

> 增加 301/404 等状态码、或真实 Nginx 日志导入（把 access log 转成 ndjson 后
> 指向 batch source），只需改场景或 source，规则无需变化。

## 扩展阅读

- 更完整的检测规则集（17 条）与 TCP daemon 联调：`../getting_started/`
- stats/join 高压场景：`../performance/nexmark_pk/`（q16 为本文 stats 形态出处）
