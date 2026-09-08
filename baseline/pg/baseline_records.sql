-- PG 模式唯一事实源表：producer 收盘基线记录（引擎 PG sink 逐窗追加）。
-- 列名对齐 baseline_out 产出字段（win_* 存 text：引擎时间字段落库字符串）。
-- 幂等：建表 + 清空（每次 run.sh --pg 从头计数）。
-- 供给不再落外部中转表：引擎每次装载/刷新直接对本表聚合（见 knowdb.pg.toml
-- 的 query），此处顺带清理旧版遗留的 baseline_ref 表。

DROP TABLE IF EXISTS baseline_ref;

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
