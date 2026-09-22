-- Stratum R diagnostic. (1) For pump mints completing on 2025-02-15, every Raydium v4 init
-- on EITHER side within +/-3 days, with signer and lag. (2) Pump-mint inits that day by
-- signer: the migration authority should dominate. Either-side join, no time bound in (2).
WITH c AS (
  SELECT mint, evt_block_time AS complete_time
  FROM pumpdotfun_solana.pump_evt_completeevent WHERE evt_block_date = DATE '2025-02-15'),
r AS (
  SELECT account_coinMint AS coin, account_pcMint AS pc, account_amm AS pool,
         call_block_time AS pool_time, call_tx_signer AS signer
  FROM raydium_amm_solana.raydium_amm_call_initialize2
  WHERE call_block_date BETWEEN DATE '2025-02-12' AND DATE '2025-02-18'),
j AS (
  SELECT c.mint, c.complete_time, r.pool_time, r.signer,
         CASE WHEN r.coin = c.mint THEN 'coin' ELSE 'pc' END AS mint_side,
         CASE WHEN r.coin = c.mint THEN r.pc ELSE r.coin END AS other_side,
         date_diff('second', c.complete_time, r.pool_time) AS lag_s
  FROM c JOIN r ON r.coin = c.mint OR r.pc = c.mint)
SELECT 'matched_completes' AS k, CAST(COUNT(DISTINCT mint) AS varchar) AS v, CAST(COUNT(*) AS varchar) AS n FROM j
UNION ALL
SELECT 'by_signer:' || signer, CAST(COUNT(DISTINCT mint) AS varchar), CAST(approx_percentile(lag_s, 0.5) AS varchar)
FROM j GROUP BY signer ORDER BY 3 DESC LIMIT 6
