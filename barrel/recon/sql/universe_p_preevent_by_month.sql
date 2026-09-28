-- P universe by month via the validated pre-event rule (CompleteEvent -> PumpSwap pool on the
-- mint, WSOL, within 1 day), 2025-03-20 .. 2026-06-30. Overlaps the event table for May-June
-- as a cross-check. Both source tables are small.
WITH c AS (
  SELECT mint, evt_block_time AS ct FROM pumpdotfun_solana.pump_evt_completeevent
  WHERE evt_block_date BETWEEN DATE '2025-03-20' AND DATE '2026-06-30'),
p AS (
  SELECT base_mint AS mint, MIN(evt_block_time) AS pt, arbitrary(quote_mint) AS quote_mint
  FROM pumpdotfun_solana.pump_amm_evt_createpoolevent
  WHERE evt_block_date BETWEEN DATE '2025-03-20' AND DATE '2026-07-01' GROUP BY 1),
j AS (
  SELECT c.mint, c.ct, p.pt, p.quote_mint FROM c JOIN p ON p.mint = c.mint AND p.pt >= c.ct AND p.pt < c.ct + INTERVAL '1' DAY),
jm AS (
  SELECT date_trunc('month', ct) AS month, COUNT(*) AS completes_with_pool,
         COUNT_IF(quote_mint = 'So11111111111111111111111111111111111111112') AS p_wsol
  FROM j GROUP BY 1),
cm AS (SELECT date_trunc('month', ct) AS month, COUNT(*) AS completes_total FROM c GROUP BY 1)
SELECT cm.month, cm.completes_total, COALESCE(jm.completes_with_pool, 0) AS completes_with_pool, COALESCE(jm.p_wsol, 0) AS p_wsol
FROM cm LEFT JOIN jm ON jm.month = cm.month ORDER BY 1
