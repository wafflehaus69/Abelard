-- Storage sizing for the derived trade table (MR-4.5): PumpSwap buy+sell event rows per month
-- over the P window. COUNT only, partition column only, literal bounds.
SELECT m, SUM(n) AS rows FROM (
  SELECT date_trunc('month', evt_block_date) AS m, COUNT(*) AS n FROM pumpdotfun_solana.pump_amm_evt_buyevent
  WHERE evt_block_date BETWEEN DATE '2025-03-20' AND DATE '2026-09-27' GROUP BY 1
  UNION ALL
  SELECT date_trunc('month', evt_block_date), COUNT(*) FROM pumpdotfun_solana.pump_amm_evt_sellevent
  WHERE evt_block_date BETWEEN DATE '2025-03-20' AND DATE '2026-09-27' GROUP BY 1)
GROUP BY 1 ORDER BY 1
