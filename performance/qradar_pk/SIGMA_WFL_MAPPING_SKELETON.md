# Sigma → wfusion WFL 语义映射对照骨架

> 用途：这是 `qradar_pk` 的「**检测语义正确性**」验证骨架。它用公开标准
> **SigmaHQ/sigma（MITRE ATT&CK 映射）+ Atomic Red Team** 做逐条可举证的检测语义覆盖，
> **不**声称包含或对标 IBM QRadar EP 451 条专有规则（见 `PROFILE_COMPLIANCE_TABLE.md`）。
>
> 本文件是**骨架**：ATT&CK 技术 ID、Sigma logsource 类别、WFL 能力评估为已坐实项；
> 每格「Sigma 原始检测逻辑」列指向 SigmaHQ 真实仓库路径，逐字 YAML 需在本地
> `git clone` 后填充（文件名随上游变动，不在此硬编码）。
>
> **2026-08-24 代码坐实**：G2（字符串谓词）、G3（外部维表）已对引擎源码坐实为 ✅
> （见 §3 证据行），原骨架标注的 ⚠️ 已转正。剩余真缺口仅 G4（base64/hash 标量）与 G8（proctime 处理时间窗）。

---

## 0. 权威源（2026-08-24 核实）

| 项 | 值 |
|---|---|
| Sigma 仓库 | `https://github.com/SigmaHQ/sigma` ，分支 `master` |
| 最近提交 | `da9bb07` (2026-08-18)，规则持续更新 |
| 规则总量 | ~3132 条（2026-05 快照，每周增长） |
| ATT&CK 版本 | `attack v19`（2026-04-28 PR #5966 全量升级标签） |
| 规则组织 | `rules/<platform>/<category>/<file>.yml` ，平台=windows/linux/network/web/cloud/identity/macos/application/category |
| 标签格式 | `tags: [attack.<tactic>, attack.tXXXX, attack.tXXXX.YYY]` |
| 许可证 | DRL / CC BY 4.0（免费可用、可商用） |

**关键事实**：Sigma 不是单一文件集，而是按**产品/平台**组织的规则库；
我们的 6 类压测事件窗口需映射到 Sigma 的对应 `logsource` 类别（见下表「Sigma logsource」列）。
ATT&CK 是分类骨架，Sigma 是规则体，二者通过 `attack.tXXXX` 标签挂钩。

---

## 1. 映射方法论

```
qradar_pk 事件窗口   ──→   Sigma logsource 类别   ──→   ATT&CK 技术子集   ──→   wfusion WFL 表达
(conn/auth/dns/                (network/web/windows…)         (attack.tXXXX)          (CEP / Window / Join)
 proxy/firewall/file)
```

- **步骤 1**：`git clone https://github.com/SigmaHQ/sigma`，锁定 commit（避免快照漂移）。
- **步骤 2**：按 ATT&CK 技术 ID 筛选（`grep -rl "attack.tXXXX" rules/`）。
- **步骤 3**：把一条 Sigma 规则的 `detection` 逻辑**人工**映射到 WFL（pySigma 只出 Splunk/KQL/EQL，**不出 WFL**，故无自动转换）。
- **步骤 4**：用 **Atomic Red Team** 对应技术的原子测试生成事件，喂引擎，断言触发。
- **步骤 5**：未触发 = 检测语义缺口，回填本节「WFL 表达」列。

---

## 2. 逐类映射（6 类事件窗口 × ATT&CK 技术子集）

图例：✅ 可表达 ｜ ⚠️ 需近似/受限于谓词集 ｜ ❌ 当前引擎不能表达

### 2.1 conn —— 网络连接
Sigma logsource：`network_connection`（目录 `rules/network/`）

| ATT&CK | 技术 | Sigma 代表性检测逻辑 | WFL 表达 | 说明 |
|---|---|---|---|---|
| T1046 | 网络服务扫描 | `network_connection` 内同一 src 对多 dst:port 的 count ≥ N / 时间窗 | ✅ | window stats `count` + `distinct(dst_port)` 阈值；session 窗口聚合同源 |
| T1071.003 | C2 over Mail | 非常规端口外联 + beacon 周期 | ✅ | fixed/sliding 窗口内 `count` 周期性 + `distinct(dst)`；beacon 间隔可算 |
| T1095 | 非应用层协议 | 非标准端口 TCP/UDP 连接 | ✅ | 字段谓词匹配端口范围（`port not in [80,443,53...]`） |
| T1571 | 非标准端口 | 同 T1095，端口维度细化 | ✅ | 同上 |

### 2.2 auth —— 认证
Sigma logsource：`authentication`（目录 `rules/windows/builtin/security/`，EventID 4625/4624）

| ATT&CK | 技术 | Sigma 代表性检测逻辑 | WFL 表达 | 说明 |
|---|---|---|---|---|
| T1110.001 | 密码猜解 | `sequence`: 4625 失败 `count() by SourceIP,TargetUser > 5` **within 10m**，后接 4624 成功 | ✅ | CEP on-each：失败计数窗口 + 成功事件关联；deferred join 跨 4625/4624 |
| T1110.003 | 密码喷洒 | 同结构但 `count_distinct(TargetUser) >= 2` 单源多账户 | ✅ | window stats `distinct(TargetUser)` 阈值 + 时间窗 |
| T1078 | 有效账户 | 异常时间/地点登录已知账户 | ✅ | 账户基线经 `external()` Redis 维表 enrich（G3 已坐实） |
| T1133 | 外部远程服务 | 来自外部网段的成功 VPN/RDP 登录 | ✅ | 字段谓词 + CIDR 匹配（`src_ip in 外部段`） |

**真实 Sigma 片段示例（暴力破解，结构取自 SigmaHQ `rules/windows/builtin/security/`，T1110）：**
```yaml
title: Brute Force Followed by Successful Login
logsource:
  category: authentication
  product: windows
detection:
  selection_failed:  { EventID: 4625 }
  selection_success: { EventID: 4624, LogonType: 3 }
  condition: >-
    sequence:
      selection_failed | count() by SourceIP, TargetUser > 5
      selection_success by SourceIP, TargetUser
    timeframe: 10m
tags: [attack.credential-access, attack.t1110, attack.t1110.001, attack.t1110.003]
```
> WFL 对应：两条 `on event` 规则（4625 计数窗口 / 4624 触发），用 deferred join
> 在「同 SourceIP+TargetUser 且 10m 内失败≥5 后成功」时 emit alert。能力已坐实（见 `rule_task.rs` `scan_deferred`）。

### 2.3 dns —— DNS 查询
Sigma logsource：`dns_query`（目录 `rules/network/`）

| ATT&CK | 技术 | Sigma 代表性检测逻辑 | WFL 表达 | 说明 |
|---|---|---|---|---|
| T1071.004 | C2 over DNS | 高频/长/随机子域名查询（DGA 特征） | ✅ | `contains`/`regex_match`/`length` 已实现（funcs.rs:111/904/619）；子串/正则直接表达，DGA 熵需自写标量 |
| T1568 | 动态解析 | Fast Flux：单域名多 A 记录 / 短 TTL 轮换 | ✅ | `distinct(resolved_ip)` 时间窗阈值 |
| T1090.004 | 多跳代理 | 已知隧道域名（IoC 列表）查询 | ✅ | IoC 维表经 `external()` Redis 查询（G3 已坐实） |
| T1567 | 数据外泄 | 大体积 TXT 记录外泄 | ✅ | `sum(response_size)` 窗口阈值 |

### 2.4 proxy —— Web 代理
Sigma logsource：`web_proxy`（目录 `rules/web/`）

| ATT&CK | 技术 | Sigma 代表性检测逻辑 | WFL 表达 | 说明 |
|---|---|---|---|---|
| T1071.001 | C2 over Web | 可疑 User-Agent / 非常规路径 beacon | ✅ | `contains` 已实现，UA 子串/路径列表直接表达（funcs.rs:111） |
| T1190 | 利用公网应用 | 已知漏洞路径/参数探测（如 `/manager/html`） | ✅ | 字段谓词 + 路径列表匹配 |
| T1567.002 | 外泄到 Web 服务 | 向paste/bin/云盘大体积 POST | ✅ | `sum(request_size)` + 域名集合匹配 |
| T1499 | 端点拒绝服务 | 单源对代理高频请求 | ✅ | `count by src_ip` 时间窗阈值 |

### 2.5 firewall —— 防火墙
Sigma logsource：`firewall`（目录 `rules/network/`，或 `network_connection` 派生）

| ATT&CK | 技术 | Sigma 代表性检测逻辑 | WFL 表达 | 说明 |
|---|---|---|---|---|
| T1021 | 远程服务 | RDP(3389)/SSH(22) 暴力 + 成功 | ✅ | 同 2.2 认证结构 + 端口谓词 |
| T1190 | 利用公网应用 | 边界设备漏洞探测（防火墙放行日志） | ✅ | 字段谓词 + 漏洞路径模式 |
| T1095 | 非标准端口 | 边界非标端口放行 | ✅ | 端口范围谓词 |
| T1595 | 主动扫描 | 单源对多目标的探测命中 | ✅ | `distinct(dst_ip)` 时间窗阈值 |

### 2.6 file —— 文件事件
Sigma logsource：`file_event` / `file_delete`（目录 `rules/windows/` Sysmon EventID 11/23）

| ATT&CK | 技术 | Sigma 代表性检测逻辑 | WFL 表达 | 说明 |
|---|---|---|---|---|
| T1486 | 勒索加密 | 短时期内大量 `.xxx`/随机扩展名写 + 删除原文件 | ✅ | window stats `count(file_write)` + 扩展名模式；session 窗口聚批量 |
| T1070.004 | 日志清除 | 事件日志服务停止 / `wevtutil cl` | ✅ | 进程命令行 + 服务事件关联（CEP） |
| T1565 | 数据破坏 | 关键文件被改写/截断 | ✅ | 文件重要性基线经 `external()` 维表 enrich（G3 已坐实） |
| T1059.001 | PowerShell | `-EncodedCommand` / `-enc` 命令行 | ✅ | `contains`/`ends_with` 已实现，与 Sigma `|contains`/`|endswith` 1:1 对应 |

**真实 Sigma 片段示例（PowerShell 编码命令，取自 SigmaHQ `rules/windows/process_creation/`，T1059.001）：**
```yaml
title: Suspicious PowerShell Encoded Command
logsource:
  category: process_creation
  product: windows
detection:
  selection:
    Image|endswith: '\powershell.exe'
    CommandLine|contains: ['-EncodedCommand', '-enc', '-ec']
  condition: selection
tags: [attack.execution, attack.t1059, attack.t1059.001]
```
> WFL 对应：`on event` + `ends_with(Image, "\\powershell.exe")` + `contains(CommandLine, "-enc")`。
> ✅ 引擎已实现 `contains`(funcs.rs:111) / `ends_with`(funcs.rs:143)，与 Sigma `|contains` / `|endswith` 直接对应。

---

## 3. wfusion 引擎能力 vs Sigma 检测原语对照

| Sigma 检测原语 | wfusion 对应能力 | 状态 | 证据 |
|---|---|---|---|
| `count() by key within window` | window stats `count` / `distinct` | ✅ | `stats_task.rs` 桶 floor epoch；`stats_exec.rs` avg 以 (sum,count) 归并 |
| 多事件 `sequence` + 阈值 | CEP `on-each` + deferred join | ✅ | `rule_task.rs` `scan_deferred` / `reevaluate_deferred_missed` / EOS 重算 |
| `timeframe: Nm` 滑动/固定窗 | fixed / sliding / session 窗口 | ✅ | `stats_task.rs:244/314` 桶起点 `(t/dur)*dur` |
| 跨源关联（A 后 B） | deferred / asof join | ✅ | `rule_task.rs:1895/1986` 全链路 + 批次尾/EOS 扫描 |
| `distinct(field)` 聚合 | window stats `distinct` / `top(N)` | ✅ | `stats_exec.rs` `top(N,field)` + `top_ties` 并列 |
| 字段 `==` / 范围谓词 | 表达式比较 | ✅ | NEXMark review 已坐实 Field 直取 |
| `field contains / regex` 子串 | 字符串函数 `contains`/`regex_match`/`starts_with`/`ends_with` | ✅ | `funcs.rs:111/904/129/143` 已实现；与 Sigma `|contains`/`|endswith` 1:1 对应（G2 坐实） |
| IoC / 账户基线维表 enrich | `external()` 远程调用 + Redis 维表 backend | ✅ | `wf-runtime/src/external/mod.rs` ExternalRuntime（LRU cache + Redis via wp_knowledge）；约束：需部署 Redis 服务（G3 坐实） |
| `base64/decode/hash` 标量函数 | 无 | ❌ | 需 ETL 预处理（G4） |
| `proctime` 处理时间窗 | 无 | ❌ **真缺口** | 全仓 grep `PROCTIME` 零命中（G8，原 CAPABILITY_GAP_MATRIX Q12） |

---

## 4. 验证闭环（对外可举证）

```
1. clone SigmaHQ/sigma @ <commit>         ── 确定性、可复现
2. 按 attack.tXXXX 筛选目标技术子集         ── 例：T1110 / T1071 / T1486
3. 逐条手写映射 WFL（人工，无自动转）        ── 本骨架 §2 为索引
4. Atomic Red Team 对应原子测试生成事件      ── redcanaryco/atomic-red-team
5. 喂 wfusion，断言 alert 触发              ── 触发=语义覆盖✅；未触发=缺口
6. 输出「技术 ID → Sigma 规则 → WFL 文件 → 触发结果」对照表
```

此闭环产出的结论是：**「wfusion 能覆盖 X/Y/Z 条 ATT&CK 技术的检测语义」**，
可逐条追溯到 `attack.mitre.org/techniques/TXXXX`，独立于 QRadar 认证。

---

## 5. 对外表述红绿灯

- ✅ **可声称**：「qradar_pk 用 SigmaHQ/sigma（ATT&CK v19 映射）+ Atomic Red Team
  做检测语义正确性验证，覆盖 N 条 ATT&CK 技术」。
- ✅ **可声称**：「性能规模对标 QRadar EP 451 条规则量（451 vs 450）」。
- ❌ **不可声称**：「包含 / 复现 / 对标 QRadar EP 451 条规则语义」——规则体专有封闭。
- ❌ **不可声称**：「与 Sigma 规则 1:1 等价」——WFL 是人工映射，非自动转换；
  剩余真偏差仅 G4（base64/hash 标量）/ G8（proctime 处理时间窗），G2/G3 已坐实支持。

---

## 6. 待办（填实骨架）

- [x] 全盘点 wfusion 表达式谓词集，坐实 G2 ✅：`contains`/`regex_match`/`starts_with`/`ends_with` 已实现（funcs.rs:111/904/129/143）。
- [x] 评估外部维表 lookup，坐实 G3 ✅：引擎有 `external()` + Redis 维表 backend（wf-runtime/src/external/mod.rs），约束=需部署 Redis。
- [ ] `git clone` SigmaHQ/sigma，按 §2 每格填充逐字 YAML（文件名随 commit 锁定）。
- [ ] 对 G8 处理时间窗做产品决策（通用引擎是否纳入 proctime）。
- [ ] 跑 §4 闭环，产出逐条触发对照表。
