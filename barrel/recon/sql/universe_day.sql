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
n AS (  -- stratum N per MR-6: CP-swap init (v4 negligible; CLMM excluded pending re-check; Meteora leg via first trade, birth_is_proxy)
  SELECT base_mint AS mint, pool, call_block_time AS grad_time, call_block_slot AS grad_slot, 'N' AS stratum, 'So11111111111111111111111111111111111111112' AS evt_quote
  FROM (SELECT CASE WHEN account_token0Mint = 'So11111111111111111111111111111111111111112' THEN account_token1Mint ELSE account_token0Mint END AS base_mint,
               account_poolState AS pool, call_block_time, call_block_slot, call_tx_signer,
               (account_token0Mint = 'So11111111111111111111111111111111111111112' OR account_token1Mint = 'So11111111111111111111111111111111111111112') AS sol_pair
        FROM raydium_cp_solana.raydium_cp_swap_call_initialize WHERE call_block_date = DATE '2026-09-01')
  WHERE sol_pair AND call_tx_signer <> '39azUYFWPz3VHgKCf3VChUwbpURdCHRxjWVowf5jUJjg'
    AND base_mint NOT IN (SELECT mint FROM pumpdotfun_solana.pump_evt_createevent WHERE evt_block_date BETWEEN DATE '2025-01-01' AND DATE '2026-09-01')
    AND base_mint NOT IN (SELECT base_mint_param FROM raydium_solana.raydium_launchpad_evt_poolcreateevent WHERE evt_block_date BETWEEN DATE '2025-04-01' AND DATE '2026-09-01')),
u AS (SELECT * FROM p UNION ALL SELECT * FROM r UNION ALL SELECT * FROM n)
SELECT stratum,
       CASE WHEN grad_time < TIMESTAMP '2026-07-21 00:00:00' THEN 'pre_boost' ELSE 'post_boost' END AS era,
       CAST(grad_time AS date) BETWEEN DATE '2025-08-05' AND DATE '2025-08-11' AS degraded,
       COUNT(*) AS tokens, COUNT(DISTINCT mint) AS distinct_mints
FROM u GROUP BY 1, 2, 3 ORDER BY 1, 2
