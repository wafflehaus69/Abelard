-- 1.4 census, ONE WEEK sample (2026-06-01 .. 2026-06-07), stratum P: launches, graduations,
-- share of graduates with >= 1 swap in the 7 days after graduation. Validates shape and cost
-- before any window-wide run.
WITH launches AS (
  SELECT date_trunc('week', evt_block_time) AS wk, COUNT(*) AS launches
  FROM pumpdotfun_solana.pump_evt_createevent
  WHERE evt_block_date BETWEEN DATE '2026-06-01' AND DATE '2026-06-07' GROUP BY 1),
grads AS (
  SELECT mint, pool, evt_block_time AS g_time, date_trunc('week', evt_block_time) AS wk
  FROM pumpdotfun_solana.pump_evt_completepumpammmigrationevent
  WHERE evt_block_date BETWEEN DATE '2026-06-01' AND DATE '2026-06-07' AND quote_mint = '11111111111111111111111111111111'),
swaps AS (
  SELECT pool, MIN(evt_block_time) AS first_swap FROM (
    SELECT pool, evt_block_time FROM pumpdotfun_solana.pump_amm_evt_buyevent  WHERE evt_block_date BETWEEN DATE '2026-06-01' AND DATE '2026-06-14'
    UNION ALL
    SELECT pool, evt_block_time FROM pumpdotfun_solana.pump_amm_evt_sellevent WHERE evt_block_date BETWEEN DATE '2026-06-01' AND DATE '2026-06-14')
  GROUP BY 1)
SELECT l.wk, l.launches, COUNT(g.mint) AS graduations,
       ROUND(100.0 * COUNT(g.mint) / l.launches, 3) AS grad_rate_pct,
       ROUND(100.0 * COUNT_IF(s.first_swap IS NOT NULL AND s.first_swap <= g.g_time + INTERVAL '7' DAY) / NULLIF(COUNT(g.mint), 0), 1) AS pct_swapped_7d
FROM launches l LEFT JOIN grads g ON g.wk = l.wk LEFT JOIN swaps s ON s.pool = g.pool
GROUP BY 1, 2
