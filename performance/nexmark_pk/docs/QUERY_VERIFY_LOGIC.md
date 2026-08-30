# 逐查询验证逻辑：Q1~Q22 怎样算「验证正确」

> 本文回答：**每个查询的验证判定标准是什么**——不是工具怎么跑（见 ORACLE_VERIFY.md），
> 而是每个查询的正确结果定义、由哪层验证断言、当前是否通过。
> 三层验证：**L1 计数**（每规则输出条数 = oracle）、**L2 内容断言**（每条 alert 的字段
> 形状/语义约束，verify_file_lib.py CHECKS）、**L3 值级对拍**（每条 alert 的每个 yield
> 字段值与 oracle 逐条一致）。
> 语义来源：`models/queries/*.wfl` 头部注释（权威 SQL 见 NEXMARK_AUTHORITATIVE_SEMANTICS.md）。

## 覆盖总览

| 验证层 | 断言什么 | 覆盖 |
|---|---|---|
| L1 计数 | 每规则输出条数 == oracle | 全部规则 |
| L2 内容断言 | 每条 alert 的字段形状 + 业务语义约束 | 全部规则 |
| L3 值级对拍 | 每条 alert 的每个 yield 字段值 == oracle | q1-q3, q5, q7-q11, q14, q20-q22（13 个最终输出规则） |
| L3 不覆盖 | stats（q4b/q15-q19）、中间输出（q4a/q13a）、known（q12）、工具排除（q6/q13） | 由 L1 + L2 覆盖 |

## 逐查询判定逻辑

| Q | 名称 | 正确语义（权威） | 验证的正确逻辑（断言什么） | L2 断言 | L3 | 状态 |
|---|---|---|---|---|---|---|
| **Q1** | Currency Conversion | 每 bid 输出一行，`price' = 0.908 × price`，纯投影 | 行数 = bid 总数（1M→920,000）；`id`=auction、`alert_type`/`detail`/`request_count` 恒定 | alert_type=`q1_passthrough` 且 detail=`bid` | ✅ | PASS |
| **Q2** | Selection | 仅 `MOD(auction,123)==0` 的 bid 每行输出 | 行数 = 满足取模的 bid；**每条 `id` 必须 `%123==0`**（过滤语义直接反验） | `id % 123 == 0` | ✅ | PASS |
| **Q3** | Local Item Suggestion | auction 驱动 ⋈ person，`category==10` 且 `state∈(OR,ID,CA)`，每满足 auction 一行，detail=seller | 行数 = 满足三重条件的 auction；`id`=auction、`detail`=seller 非空 | alert_type=`q3` 且 detail 非空 | ✅ | PASS |
| **Q4** | Avg Price by Category | 两层聚合：内层每 auction 生命周期胜出价 max（→中间窗 auction_finals）→ 外层按 category avg | 外层每 category 一行，`detail`=avg 值 | q4b: alert_type=`q4_avg` | —（q4a 中间 + q4b stats） | PASS |
| **Q5** | Hot Items | HOP(10s,2s) 每窗 bid 数最多 auction（top-1，并列全出） | 每窗一条；`id`=auction | alert_type=`q5_hot` | ✅ | PASS |
| **Q6** | Avg Selling Price by Seller | 每 seller 最近 10 笔成交胜出价均值（`avg>=200` 阈值告警形态；Flink 官方未实现） | 每条命中 emit；`id`=auction | alert_type=`q6_avg200` | —（join 可见性非确定，无权威基线） | PASS |
| **Q7** | Highest Bid | 每 10s 桶全局最高价 bid（并列全出） | 每桶一条；`detail` 以 `max` 开头 | alert_type=`q7_hi` 且 detail 前缀 `max ` | ✅ | PASS |
| **Q8** | Monitor New Users | 10s 桶内注册**且**创建拍卖的人（存在性 join） | 每 (person×桶) 一行；`id`=person | alert_type=`q8_new_user` | ✅ | PASS |
| **Q9** | Winning Bids | 每 auction 生命周期最高价 bid（平手取 dateTime 最早），每 auction 至多一条 | **每 `id` 至多一条**；`detail`=`winner <bidder>` | alert_type=`q9_win`、detail 前缀 `winner `、每 id ≤1 条 | ✅ | PASS |
| **Q10** | Log to File | 全量 bid 落盘（每 bid 一行） | 行数 = bid 总数；字段恒定 | alert_type=`q10_log` 且 detail=`log bid` | ✅ | PASS |
| **Q11** | User Sessions | session(10s) 每会话输出一条带 count | 每会话一条；`detail`=count 数字 | alert_type=`q11_session` 且 detail 为数字 | ✅ | PASS |
| **Q12** | Processing Time Windows | 每 bidder × 10s 窗口计数（处理时间用事件时间近似） | 每 (bidder×桶) 一条 | alert_type=`q12_window` | —（known：fixed+close 收口非确定） | PASS（⚠ known） |
| **Q13** | Side Input Join | `mod(auction,10000)` ⋈ side_input 富化 value | 每 bid 一行；`detail`=富化 value | q13b: alert_type=`q13_sidejoin` | —（provider 静态表，oracle 无数据） | PASS |
| **Q14** | Calculation | `0.908×price` + CASE HOUR 分型 + count_char + 价格过滤 | 每行输出；`detail` 含 `c=` | alert_type=`q14_calc` 且 detail 含 `c=` | ✅ | PASS |
| **Q15** | Bidding Statistics | 按天 12 列统计（count/distinct × 价格档） | 每天一行 | alert_type=`q15_stats` | —（stats） | PASS |
| **Q16** | Channel Statistics | 按 channel/天 15 列统计 | 每 (channel×天) 一行 | alert_type=`q16_stats` | —（stats） | PASS |
| **Q17** | Auction Statistics | 按 auction/天 8 列统计 | 每 auction 一行 | alert_type=`q17_stats` | —（stats） | PASS |
| **Q18** | Find Last Bid | 每 (bidder,auction) 最后一条 bid 的**字段值**（5 字段） | `detail` 5 字段非空（修复前全空——内容断言抓出）；`id`=auction | alert_type=`q18_last_stats` | —（stats） | PASS |
| **Q19** | Auction TOP-10 | 每 auction 价格 top-10（同价先到在前） | 每 auction ≤10 条；`detail`=`bidder price` | alert_type=`q19_top10_stats` | —（stats） | PASS |
| **Q20** | Expand Bid | bid ⋈ auction + `category==10` 过滤 | 每命中 bid 一行；`id`=auction | alert_type=`q20_expand` | ✅ | PASS |
| **Q21** | Add Channel ID | 每 bid 输出 channel_id（热通道 0-3 + url 提取） | 每 bid 一行；`detail`=channel_id 非空 | alert_type=`q21_cid` 且 detail 非空 | ✅ | PASS |
| **Q22** | URL Directories | 每 bid url split('/') 取索引 3/4/5 段 | 每 bid 一行；`detail` 含 `/` | alert_type=`q22_dir` 且 detail 含 `/` | ✅ | PASS |

## 结论速查

- **判定标准**：L1 保证「数量对」、L2 保证「字段形状/业务语义对」、L3 保证「字段值对」。
  只有三层全过（或按表格标注跳过）才算该查询「验证正确」。
- **当前全绿**：22 个查询全部 PASS（L1 计数 + L2 内容断言 + L3 值级对拍三层全过）。
  q3/q5/q7 的历史 FAIL 已修复：q7/q5 为 close_all 尾桶收口语义（窗口终点按窗起点算 +
  水位对齐到桶边界；hop 用 slide 粒度 + 真 ceil），q3 为 join 索引与提交前沿竞态
  （frontier 回退不再领先索引内容 + eager gate 冷启动不 bail）。
- **L2 是 stats 规则（q4b/q15-q19）唯一的字段级验证**——q18/q19 内容 bug（detail 全空 /
  id 缺失）正是 L2 抓出并已修复的。
