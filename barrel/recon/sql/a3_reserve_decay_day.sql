-- A3 calibration distributions, dry-run on one day's P graduates (2026-09-01):
--  (1) reserve decay: pool quote reserves at +1h, +24h, +7d relative to the first post-graduation
--      state (events carry PRE-swap reserves; the last event before each horizon gives the state)
--  (2) depth-floor ratio for a $20 ticket at day 7: exit-adjusted / spot = B / (B + b20), where b20
--      is the base a $20 buy would return at entry (G=240). SOL/USD fixed at 200 for the dry run
--      (labelled; the real run uses the block-time series).
WITH g AS (SELECT mint, pool, evt_block_time AS g_time FROM pumpdotfun_solana.pump_evt_completepumpammmigrationevent
           WHERE evt_block_date = DATE '2026-09-01' AND quote_mint = '11111111111111111111111111111111'),
ev AS (
  SELECT pool, evt_block_time AS t, CAST(pool_quote_token_reserves AS double) AS q, CAST(pool_base_token_reserves AS double) AS b FROM pumpdotfun_solana.pump_amm_evt_buyevent  WHERE evt_block_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-09'
  UNION ALL
  SELECT pool, evt_block_time, CAST(pool_quote_token_reserves AS double), CAST(pool_base_token_reserves AS double) FROM pumpdotfun_solana.pump_amm_evt_sellevent WHERE evt_block_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-09'),
st AS (SELECT g.mint, e.t, e.q, e.b, g.g_time FROM ev e JOIN g ON g.pool = e.pool WHERE e.t >= g.g_time AND e.t < g.g_time + INTERVAL '7' DAY),
snap AS (
  SELECT mint,
         arbitrary(q) FILTER (WHERE rn0 = 1) AS q0,
         max_by(q, t) FILTER (WHERE t < g_time + INTERVAL '1' HOUR)  AS q1h,
         max_by(q, t) FILTER (WHERE t < g_time + INTERVAL '4' HOUR)  AS q4h,
         max_by(b, t) FILTER (WHERE t < g_time + INTERVAL '4' HOUR)  AS b4h,
         max_by(q, t) FILTER (WHERE t < g_time + INTERVAL '24' HOUR) AS q24h,
         max_by(q, t) AS q7d, max_by(b, t) AS b7d
  FROM (SELECT *, row_number() OVER (PARTITION BY mint ORDER BY t) AS rn0 FROM st) GROUP BY 1),
calc AS (
  SELECT mint, q1h/q0 AS r1h, q4h/q0 AS r4h, q24h/q0 AS r24h, q7d/q0 AS r7d,
         -- $20 at 200 USD/SOL = 0.1 SOL = 1e8 lamports; base returned at G=240 state
         b4h * 1e8 / (q4h + 1e8) AS b20,
         b7d / (b7d + b4h * 1e8 / (q4h + 1e8)) AS depth_ratio_7d
  FROM snap WHERE q0 > 0 AND q4h IS NOT NULL AND q7d IS NOT NULL)
SELECT COUNT(*) AS tokens,
       approx_percentile(r1h, 0.1) AS r1h_p10, approx_percentile(r1h, 0.5) AS r1h_p50, approx_percentile(r1h, 0.9) AS r1h_p90,
       approx_percentile(r24h, 0.1) AS r24h_p10, approx_percentile(r24h, 0.5) AS r24h_p50, approx_percentile(r24h, 0.9) AS r24h_p90,
       approx_percentile(r7d, 0.1) AS r7d_p10, approx_percentile(r7d, 0.5) AS r7d_p50, approx_percentile(r7d, 0.9) AS r7d_p90,
       COUNT_IF(r7d < 0.2) AS reserves_down_80pct_7d,
       approx_percentile(depth_ratio_7d, 0.01) AS depth_p01, approx_percentile(depth_ratio_7d, 0.1) AS depth_p10, approx_percentile(depth_ratio_7d, 0.5) AS depth_p50,
       COUNT_IF(depth_ratio_7d < 0.2) AS below_provisional_F20
FROM calc
