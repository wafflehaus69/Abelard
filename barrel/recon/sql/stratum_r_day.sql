-- Stratum R admission (pre-2025-03-20): pump.fun bonding curve completed, then a Raydium v4
-- pool initialised on that mint with WSOL as the pc (quote) side. Sample day 2025-02-15.
-- Reports the complete->pool lag distribution and the initialising signer, which should be
-- one address (pump.fun's migration authority) if the admission rule is right.
WITH c AS (
  SELECT mint, evt_block_time AS complete_time
  FROM pumpdotfun_solana.pump_evt_completeevent
  WHERE evt_block_date = DATE '2025-02-15'),
r AS (
  SELECT account_coinMint AS mint, account_pcMint AS pc_mint, account_amm AS pool,
         call_block_time AS pool_time, call_tx_signer AS signer
  FROM raydium_amm_solana.raydium_amm_call_initialize2
  WHERE call_block_date BETWEEN DATE '2025-02-15' AND DATE '2025-02-16')
SELECT COUNT(DISTINCT c.mint)                                             AS completes,
       COUNT(DISTINCT r.mint)                                             AS completes_with_v4_pool,
       COUNT_IF(r.pc_mint = 'So11111111111111111111111111111111111111112') AS pools_wsol_pc,
       approx_percentile(date_diff('second', c.complete_time, r.pool_time), 0.5) AS lag_s_p50,
       approx_percentile(date_diff('second', c.complete_time, r.pool_time), 0.9) AS lag_s_p90,
       COUNT(DISTINCT r.signer)                                            AS distinct_signers,
       arbitrary(r.signer)                                                 AS a_signer
FROM c LEFT JOIN r ON r.mint = c.mint AND r.pool_time >= c.complete_time
