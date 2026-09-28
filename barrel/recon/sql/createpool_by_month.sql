-- Coverage check: PumpSwap pool-creation events by month over the window, with the share
-- created by the single most frequent creator (the migration path should dominate).
WITH c AS (
  SELECT date_trunc('month', evt_block_time) AS month, creator
  FROM pumpdotfun_solana.pump_amm_evt_createpoolevent
  WHERE evt_block_date BETWEEN DATE '2025-03-20' AND DATE '2026-09-27'),
top AS (SELECT month, creator, COUNT(*) AS n, row_number() OVER (PARTITION BY month ORDER BY COUNT(*) DESC) AS rn FROM c GROUP BY 1, 2)
SELECT c.month, COUNT(*) AS pools_created, MAX(CASE WHEN t.rn = 1 THEN t.n END) AS top_creator_pools, MAX(CASE WHEN t.rn = 1 THEN substr(t.creator, 1, 10) END) AS top_creator
FROM c LEFT JOIN top t ON t.month = c.month AND t.rn = 1 GROUP BY 1 ORDER BY 1
