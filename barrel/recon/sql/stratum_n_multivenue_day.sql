-- Stratum N, multi-venue, sample day 2026-09-01. A pool is N if its non-WSOL mint has no
-- launchpad origin (no pump CreateEvent, no LaunchLab pool-create) and, for Raydium, the
-- init was not signed by the pump migration authority. Venue legs:
--   v4  : raydium_amm_call_initialize2 (creation event)
--   cp  : raydium_cp_swap_call_initialize (creation event)
--   met : meteora_*.trades — NO creation table on Dune; pool birth = first trade (proxy, labelled)
WITH wsol AS (SELECT 'So11111111111111111111111111111111111111112' AS m),
pump_created AS (SELECT DISTINCT mint FROM pumpdotfun_solana.pump_evt_createevent WHERE evt_block_date BETWEEN DATE '2025-01-01' AND DATE '2026-09-01'),
launchlab AS (SELECT DISTINCT base_mint_param AS mint FROM raydium_solana.raydium_launchpad_evt_poolcreateevent WHERE evt_block_date BETWEEN DATE '2025-04-01' AND DATE '2026-09-01'),
v4 AS (
  SELECT 'raydium_v4' AS venue, account_amm AS pool,
         CASE WHEN account_coinMint = (SELECT m FROM wsol) THEN account_pcMint ELSE account_coinMint END AS base_mint,
         (account_coinMint = (SELECT m FROM wsol) OR account_pcMint = (SELECT m FROM wsol)) AS sol_pair,
         call_tx_signer = '39azUYFWPz3VHgKCf3VChUwbpURdCHRxjWVowf5jUJjg' AS by_migrator
  FROM raydium_amm_solana.raydium_amm_call_initialize2 WHERE call_block_date = DATE '2026-09-01'),
cp AS (
  SELECT 'raydium_cp' AS venue, account_poolState AS pool,
         CASE WHEN account_token0Mint = (SELECT m FROM wsol) THEN account_token1Mint ELSE account_token0Mint END AS base_mint,
         (account_token0Mint = (SELECT m FROM wsol) OR account_token1Mint = (SELECT m FROM wsol)) AS sol_pair,
         call_tx_signer = '39azUYFWPz3VHgKCf3VChUwbpURdCHRxjWVowf5jUJjg' AS by_migrator
  FROM raydium_cp_solana.raydium_cp_swap_call_initialize WHERE call_block_date = DATE '2026-09-01'),
met_first AS (
  SELECT project_program_id AS pool_prog, token_bought_vault AS vault_key, MIN(block_time) AS first_t,
         arbitrary(CASE WHEN token_bought_mint_address = (SELECT m FROM wsol) THEN token_sold_mint_address ELSE token_bought_mint_address END) AS base_mint,
         bool_or(token_bought_mint_address = (SELECT m FROM wsol) OR token_sold_mint_address = (SELECT m FROM wsol)) AS sol_pair
  FROM meteora_version_dlmm.base_trades
  WHERE block_time >= TIMESTAMP '2026-06-01 00:00:00' AND block_time < TIMESTAMP '2026-09-02 00:00:00'
  GROUP BY 1, 2),
met AS (
  SELECT 'meteora_dlmm_firsttrade' AS venue, vault_key AS pool, base_mint, sol_pair, false AS by_migrator
  FROM met_first WHERE CAST(first_t AS date) = DATE '2026-09-01'),
u AS (SELECT * FROM v4 UNION ALL SELECT * FROM cp UNION ALL SELECT * FROM met)
SELECT venue, COUNT(*) AS pools,
       COUNT_IF(NOT sol_pair) AS not_sol_pair, COUNT_IF(by_migrator) AS by_migrator,
       COUNT_IF(base_mint IN (SELECT mint FROM pump_created)) AS pump_origin,
       COUNT_IF(base_mint IN (SELECT mint FROM launchlab)) AS launchlab_origin,
       COUNT_IF(sol_pair AND NOT by_migrator AND base_mint NOT IN (SELECT mint FROM pump_created) AND base_mint NOT IN (SELECT mint FROM launchlab)) AS n_admitted
FROM u GROUP BY 1 ORDER BY 1
