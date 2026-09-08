-- PG 事实库表：producer 收盘基线记录（PG sink 后端，--pg 模式）。
-- 列名对齐 baseline_out 产出字段（win_* 存 text：引擎时间字段落库字符串）。
-- 幂等：IF NOT EXISTS + TRUNCATE（每次 run.sh --pg 从头计数）。
CREATE TABLE IF NOT EXISTS baseline_records (
  entity    text,
  metric    text,
  win_start text,
  win_end   text,
  n         double precision,
  sum       double precision,
  sum_sq    double precision
);

TRUNCATE baseline_records;
