-- 全局周期基线供给表（PG）：地铁客流 μ/σ 基线，schema 对齐
-- models/schemas/baseline_ref.wfs（entity chars → text；数值列 double precision）。
-- 种子与 CSV 变体同构：5 条线路、μ≈1000、σ=20（n/sum/sum_sq 三元组自洽：
-- sum_sq = n·(μ²+σ²) = 240·1_000_400 = 240_096_000）。
-- 用法: docker compose exec -T postgres psql -U postgres -d postgres -f /tmp/x.sql
--   （run_pg_refresh.sh 内部经 docker exec 执行）

CREATE TABLE IF NOT EXISTS baseline_ref (
  entity   text PRIMARY KEY,
  n        double precision NOT NULL,
  sum      double precision NOT NULL,
  sum_sq   double precision NOT NULL,
  mu       double precision NOT NULL,
  sigma    double precision NOT NULL
);

TRUNCATE baseline_ref;

INSERT INTO baseline_ref (entity, n, sum, sum_sq, mu, sigma) VALUES
  ('1号线', 240, 240000.0, 240096000.0, 1000.0, 20.0),
  ('2号线', 240, 240000.0, 240096000.0, 1000.0, 20.0),
  ('3号线', 240, 240000.0, 240096000.0, 1000.0, 20.0),
  ('4号线', 240, 240000.0, 240096000.0, 1000.0, 20.0),
  ('5号线', 240, 240000.0, 240096000.0, 1000.0, 20.0);
