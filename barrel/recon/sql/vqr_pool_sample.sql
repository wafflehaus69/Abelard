-- MR-14 order 2: a deterministic sample of stratum-P pools for the virtual-reserve check over RPC.
-- One pool per sampled graduation day: every second day of the post-BOOST era, plus four pre-BOOST
-- control days. The pool taken is the smallest address of the day, which is arbitrary and repeatable.
WITH comp AS (
  SELECT mint, evt_block_date AS d FROM pumpdotfun_solana.pump_evt_completeevent
  WHERE evt_block_date BETWEEN DATE '2026-07-06' AND DATE '2026-09-20'),
pc AS (
  SELECT base_mint AS mint, pool, evt_block_date AS d FROM pumpdotfun_solana.pump_amm_evt_createpoolevent
  WHERE evt_block_date BETWEEN DATE '2026-07-06' AND DATE '2026-09-21'
    AND quote_mint = 'So11111111111111111111111111111111111111112')
SELECT CAST(pc.d AS varchar) AS grad_day, min(pc.pool) AS pool,
       CASE WHEN pc.d >= DATE '2026-07-21' THEN 'post_boost' ELSE 'pre_boost' END AS era, count(*) AS pools_that_day
FROM pc JOIN comp ON comp.mint = pc.mint AND comp.d = pc.d
WHERE pc.d IN (DATE '2026-07-06', DATE '2026-07-13', DATE '2026-07-19', DATE '2026-07-20', DATE '2026-07-21', DATE '2026-07-23', DATE '2026-07-25', DATE '2026-07-27', DATE '2026-07-29', DATE '2026-07-31', DATE '2026-08-02', DATE '2026-08-04', DATE '2026-08-06', DATE '2026-08-08', DATE '2026-08-10', DATE '2026-08-12', DATE '2026-08-14', DATE '2026-08-16', DATE '2026-08-18', DATE '2026-08-20', DATE '2026-08-22', DATE '2026-08-24', DATE '2026-08-26', DATE '2026-08-28', DATE '2026-08-30', DATE '2026-09-01', DATE '2026-09-03', DATE '2026-09-05', DATE '2026-09-07', DATE '2026-09-09', DATE '2026-09-11', DATE '2026-09-13', DATE '2026-09-15', DATE '2026-09-17', DATE '2026-09-19')
GROUP BY 1, 3
