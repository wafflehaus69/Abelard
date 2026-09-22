-- Labelled universe for one day (parameter: the two DATE literals). Strata P / P_alt / R per
-- the validated rules; era per MR-4; DEGRADED per MR-5.6. N is added once its rule validates.
-- Owner-wallet exclusion is applied downstream at the trade layer (wallets in barrel/private/).
WITH p AS (
  SELECT mint, pool, evt_block_time AS grad_time, evt_block_slot AS grad_slot,
         CASE WHEN quote_mint = '11111111111111111111111111111111' THEN 'P' ELSE 'P_alt' END AS stratum,
         quote_mint AS evt_quote
  FROM pumpdotfun_solana.pump_evt_completepumpammmigrationevent
  WHERE evt_block_date = DATE '2026-09-01'),
r AS (
  SELECT c.mint, i.account_amm AS pool, i.call_block_time AS grad_time, i.call_block_slot AS grad_slot,
         'R' AS stratum, i.account_coinMint AS evt_quote
  FROM pumpdotfun_solana.pump_evt_completeevent c
  JOIN raydium_amm_solana.raydium_amm_call_initialize2 i
    ON i.account_pcMint = c.mint AND i.account_coinMint = 'So11111111111111111111111111111111111111112'
   AND i.call_tx_signer = '39azUYFWPz3VHgKCf3VChUwbpURdCHRxjWVowf5jUJjg' AND i.call_block_time >= c.evt_block_time
  WHERE c.evt_block_date = DATE '2026-09-01' AND i.call_block_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-02'),
u AS (SELECT * FROM p UNION ALL SELECT * FROM r)
SELECT stratum,
       CASE WHEN grad_time < TIMESTAMP '2026-07-21 00:00:00' THEN 'pre_boost' ELSE 'post_boost' END AS era,
       CAST(grad_time AS date) BETWEEN DATE '2025-08-05' AND DATE '2025-08-11' AS degraded,
       COUNT(*) AS tokens, COUNT(DISTINCT mint) AS distinct_mints
FROM u GROUP BY 1, 2, 3 ORDER BY 1, 2
