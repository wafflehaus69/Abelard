-- Stratum P admission for the pre-event era (2025-03-20 .. 2026-04-30), sample day 2025-09-15:
-- bonding-curve CompleteEvent joined to a PumpSwap createpoolevent on the same mint, WSOL quote,
-- pool created at or after completion within 1 day. Reports match rate and lag.
WITH c AS (
  SELECT mint, evt_block_time AS complete_time FROM pumpdotfun_solana.pump_evt_completeevent WHERE evt_block_date = DATE '2025-09-15'),
p AS (
  SELECT base_mint AS mint, pool, creator, evt_block_time AS pool_time, quote_mint
  FROM pumpdotfun_solana.pump_amm_evt_createpoolevent WHERE evt_block_date BETWEEN DATE '2025-09-15' AND DATE '2025-09-16')
SELECT COUNT(DISTINCT c.mint) AS completes,
       COUNT(DISTINCT CASE WHEN p.pool IS NOT NULL THEN c.mint END) AS with_pumpswap_pool,
       COUNT(DISTINCT CASE WHEN p.quote_mint = 'So11111111111111111111111111111111111111112' THEN c.mint END) AS wsol_quoted,
       approx_percentile(date_diff('second', c.complete_time, p.pool_time), 0.5) AS lag_s_p50,
       approx_percentile(date_diff('second', c.complete_time, p.pool_time), 0.9) AS lag_s_p90,
       COUNT(DISTINCT p.creator) AS distinct_pool_creators
FROM c LEFT JOIN p ON p.mint = c.mint AND p.pool_time >= c.complete_time
