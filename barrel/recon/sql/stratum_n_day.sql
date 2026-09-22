-- Stratum N candidate rule, sample day 2026-09-01: Raydium v4 pools initialised whose pump-side
-- mint (either side, non-WSOL) has NO launchpad origin: no pump CreateEvent ever, no LaunchLab
-- pool-create, not signed by the pump migration authority. Reports how many v4 inits survive
-- the anti-join, and how many were removed by each reason, for the day.
WITH i AS (
  SELECT account_amm AS pool, call_tx_signer AS signer, call_block_time AS t,
         CASE WHEN account_coinMint = 'So11111111111111111111111111111111111111112' THEN account_pcMint ELSE account_coinMint END AS base_mint,
         CASE WHEN account_coinMint = 'So11111111111111111111111111111111111111112' OR account_pcMint = 'So11111111111111111111111111111111111111112' THEN 1 ELSE 0 END AS sol_pair
  FROM raydium_amm_solana.raydium_amm_call_initialize2 WHERE call_block_date = DATE '2026-09-01'),
pump_created AS (SELECT DISTINCT mint FROM pumpdotfun_solana.pump_evt_createevent WHERE evt_block_date BETWEEN DATE '2025-01-01' AND DATE '2026-09-01'),
launchlab AS (SELECT DISTINCT base_mint_param AS mint FROM raydium_solana.raydium_launchpad_evt_poolcreateevent WHERE evt_block_date BETWEEN DATE '2025-04-01' AND DATE '2026-09-01')
SELECT COUNT(*) AS v4_inits,
       COUNT_IF(sol_pair = 0) AS not_sol_pair,
       COUNT_IF(signer = '39azUYFWPz3VHgKCf3VChUwbpURdCHRxjWVowf5jUJjg') AS by_pump_migrator,
       COUNT_IF(base_mint IN (SELECT mint FROM pump_created)) AS pump_origin,
       COUNT_IF(base_mint IN (SELECT mint FROM launchlab)) AS launchlab_origin,
       COUNT_IF(sol_pair = 1 AND signer <> '39azUYFWPz3VHgKCf3VChUwbpURdCHRxjWVowf5jUJjg'
                AND base_mint NOT IN (SELECT mint FROM pump_created)
                AND base_mint NOT IN (SELECT mint FROM launchlab)) AS n_candidates
FROM i
